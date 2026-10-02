// Package retry DLQ 重放：把进死信的消息按原 topic 重新派发。
//
// 两条硬约束：
//  1. **必须收敛**：失败→回写 DLQ→再消费→再失败 是最典型的无限循环。计数头
//     job-replay-count 递增，达到 MaxReplay 即放弃并告警（放弃也必须留痕，
//     不能静默丢）。
//  2. **按原 topic 派发**：DLQ topic 是 <原 topic>.dlq，带着 .dlq 直接派发
//     会被 Dispatcher 判为未知 topic，重放就永远失败。
//
// 重放同样入链（messaging.<entity>.replay），且续接消息头里的 traceparent
// ——消费者拿到的就是当初失败那条消息，重放必须挂在原 trace 上才查得出因果。
package retry

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gospacex/logx"
	"github.com/gospacex/mqx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"hubx/micro_apps/apps/job/internal/consumer"
	"hubx/micro_apps/infra/telemetry"
)

// MaxReplay 最大重放次数：达到即放弃（避免失败消息在 DLQ 里无限打转）
const MaxReplay = 3

// HeaderReplayCount 重放次数消息头
const HeaderReplayCount = "job-replay-count"

// Dispatcher 同消费侧：按 topic 派发业务动作
type Dispatcher interface {
	Dispatch(ctx context.Context, msg *mqx.Message) error
}

// Replayer DLQ 重放宿主
type Replayer struct {
	cons    mqx.Consumer
	prod    mqx.Producer
	disp    Dispatcher
	logger  logx.Logger
	traceOn bool
}

// New 构造重放器（traceOn = Trace.On("mq")）
func New(cons mqx.Consumer, prod mqx.Producer, disp Dispatcher, logger logx.Logger, traceOn bool) *Replayer {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Replayer{cons: cons, prod: prod, disp: disp, logger: logger, traceOn: traceOn}
}

// Run 阻塞消费 DLQ topic
func (r *Replayer) Run(ctx context.Context) error {
	if r.cons == nil {
		return fmt.Errorf("retry: nil mqx consumer")
	}
	return r.cons.Consume(ctx, r.OnMessage)
}

// OnMessage 单条 DLQ 消息：续链 → 按原 topic 派发 → 失败回写/放弃。
//
// 返回 nil 表示可 ack：重放成功、或已决定放弃（放弃前必有 ERROR 日志）。
func (r *Replayer) OnMessage(ctx context.Context, msg *mqx.Message) error {
	if msg == nil {
		return fmt.Errorf("retry: nil message")
	}
	orig := strings.TrimSuffix(msg.Topic, ".dlq")
	if orig == msg.Topic {
		r.logger.ErrorContext(ctx, "job: replay got non-dlq topic（订阅配置错了）", logx.String("topic", msg.Topic))
		return nil
	}

	if r.traceOn {
		ctx = consumer.ExtractContext(ctx, msg.Headers)
		var span trace.Span
		ctx, span = telemetry.Start(ctx, "messaging", consumer.EntityOf(msg.Topic), "replay")
		defer span.End()
		span.SetAttributes(
			attribute.String("messaging.topic", msg.Topic),
			attribute.String("messaging.original_topic", orig),
		)
	}

	// 按原 topic 派发：DLQ 后缀只是投递位置，不是业务语义
	err := r.disp.Dispatch(ctx, &mqx.Message{
		Topic:   orig,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: msg.Headers,
	})
	if err == nil {
		r.logger.InfoContext(ctx, "job: dlq replay succeeded", logx.String("topic", orig))
		return nil
	}

	count := replayCount(msg.Headers) + 1
	if count > MaxReplay {
		// 放弃：必须 ERROR 留痕（这是唯一能发现「业务侧有消息永远处理不了」的地方）
		r.logger.ErrorContext(ctx, "job: dlq replay gave up", logx.Err(err),
			logx.String("topic", orig), logx.Int("attempts", count))
		return nil
	}
	r.logger.WarnContext(ctx, "job: dlq replay failed, requeue", logx.Err(err),
		logx.String("topic", orig), logx.Int("attempt", count))
	return r.requeue(ctx, msg, count)
}

// requeue 回写 DLQ（带上递增后的次数）。写不进去就把错误抛出去，让消息不被 ack。
func (r *Replayer) requeue(ctx context.Context, msg *mqx.Message, count int) error {
	if r.prod == nil {
		return fmt.Errorf("retry: nil producer（无法回写 DLQ）")
	}
	headers := make(map[string]string, len(msg.Headers)+1)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	headers[HeaderReplayCount] = strconv.Itoa(count)
	return r.prod.Send(ctx, &mqx.Message{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// replayCount 读重放次数（缺省 0；坏值按 0 处理）
func replayCount(headers map[string]string) int {
	raw, ok := headers[HeaderReplayCount]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
