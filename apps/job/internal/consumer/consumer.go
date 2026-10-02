// Package consumer Kafka 消费侧入站续链。
//
// 这是全链路 trace 在 **异步边界** 上的关键一跳：srv-order 的 outbox 把
// traceparent 写进消息头（messaging.outbox.send span），job 消费时必须把它
// Extract 回来，receive span 才会挂在同一条 trace 上。少了这一步，send 与
// receive 在 Jaeger 上是两条互不相识的 trace，「这条消息是谁发的」永远查不到。
//
// 开关语义同生产侧（apps/srv-order/internal/service/outbox.go）：
//   - Trace.On("mq") 开 → Extract + 打 receive span
//   - 关 → 既不 Extract 也不打 span（关了就别埋，零开销）
//
// 派发失败的消息投递到 <topic>.dlq（保留 traceparent，供 job retry 续链重放）。
package consumer

import (
	"context"
	"fmt"
	"strings"

	"github.com/gospacex/logx"
	"github.com/gospacex/mqx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"hubx/micro_apps/apps/job/internal/handler"
	"hubx/micro_apps/infra/telemetry"
	"hubx/micro_apps/pkg/events"
)

// topic → span entity 的裁剪规则：
// order.completed.v1 → order.completed；DLQ 重放沿用同一 entity。
//
// traceHeaderKey 与生产侧（outbox/srv-coupon）写入的键逐字同源，改一处即可。
const (
	versionSuffix  = ".v1"
	dlqSuffix      = handler.DLQSuffix
	traceHeaderKey = events.TraceHeaderKey
)

// Dispatcher 业务派发（handler.Dispatcher 实现）
type Dispatcher interface {
	Dispatch(ctx context.Context, msg *mqx.Message) error
}

// Consumer 消费侧宿主：一条消息 = 一个 receive span + 一次业务派发
type Consumer struct {
	cons    mqx.Consumer
	disp    Dispatcher
	dlq     mqx.Producer
	logger  logx.Logger
	traceOn bool
}

// New 构造消费者。
//
// traceOn = pkg/config 的 Trace.On("mq")；dlq 可为 nil（nil 时派发失败直接冒泡，
// 由 mqx 自身的重投策略处理，不写死信 topic）。
func New(cons mqx.Consumer, disp Dispatcher, logger logx.Logger, traceOn bool, dlq mqx.Producer) *Consumer {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Consumer{cons: cons, disp: disp, dlq: dlq, logger: logger, traceOn: traceOn}
}

// Run 阻塞消费（mqx 驱动负责分区分配与提交）
func (c *Consumer) Run(ctx context.Context) error {
	if c.cons == nil {
		return fmt.Errorf("consumer: nil mqx consumer")
	}
	return c.cons.Consume(ctx, c.OnMessage)
}

// OnMessage 单条消息处理：续链 → 打 receive span → 派发 → 失败进 DLQ。
//
// 返回 nil 表示这条消息可以 ack；返回错误交给 mqx 的重投策略。
func (c *Consumer) OnMessage(ctx context.Context, msg *mqx.Message) error {
	if msg == nil {
		return fmt.Errorf("consumer: nil message")
	}

	if c.traceOn {
		// 【续链】先 Extract 再 Start：Extract 出的 span context 成为 receive
		// span 的父，send/receive 因此落在同一条 trace 上。顺序反了（先 Start
		// 再 Extract）receive span 就是新根，等价没做。
		ctx = ExtractContext(ctx, msg.Headers)
		var span trace.Span
		ctx, span = telemetry.Start(ctx, "messaging", EntityOf(msg.Topic), "receive")
		defer span.End()
		span.SetAttributes(
			attribute.String("messaging.topic", msg.Topic),
			attribute.String("messaging.key", msg.Key),
		)
	}

	err := c.disp.Dispatch(ctx, msg)
	if err == nil {
		return nil
	}
	c.logger.ErrorContext(ctx, "job: dispatch failed", logx.Err(err), logx.String("topic", msg.Topic))

	if c.dlq == nil {
		return err
	}
	if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
		// DLQ 也写不进去：必须把错误抛出去让消息不被 ack，
		// 否则失败的消息凭空消失，连重放的机会都没有。
		return fmt.Errorf("consumer: dispatch failed (%v) and dlq send failed: %w", err, dlqErr)
	}
	c.logger.WarnContext(ctx, "job: message routed to dlq", logx.String("topic", msg.Topic+dlqSuffix))
	return nil
}

// sendToDLQ 把失败消息投递到 <topic>.dlq，并把当前 span 的 traceparent
// 注入消息头——重放时才能续回原链路。
func (c *Consumer) sendToDLQ(ctx context.Context, msg *mqx.Message) error {
	headers := make(map[string]string, len(msg.Headers)+1)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	return c.dlq.Send(ctx, &mqx.Message{
		Topic:   msg.Topic + dlqSuffix,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// ExtractContext 从消息头重建调用链上下文。
//
// 只认 traceparent/tracestate/baggage 三个 W3C 头（carrier 直接用消息头
// map，mqx 的 header 就是 map[string]string）；没有 traceparent 时返回原
// ctx，后续 Start 自然开新根——这正是「无 traceparent 头」矩阵组合的预期行为。
func ExtractContext(ctx context.Context, headers map[string]string) context.Context {
	// 没有 traceparent 就没有可续的父：直接返回（后续 Start 开新根）。
	// 这也是矩阵组合「Kafka 消息无 traceparent 头」的预期行为——job 侧是
	// 自建根 trace，而不是挂在一个不存在的父下。
	if _, ok := headers[traceHeaderKey]; !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

// EntityOf topic → span entity（去掉 .dlq 与 .v1 后缀）
func EntityOf(topic string) string {
	e := strings.TrimSuffix(topic, dlqSuffix)
	e = strings.TrimSuffix(e, versionSuffix)
	return e
}
