// Package handler job 消费侧业务派发：topic → 业务动作。
//
// job 是这套案例里唯一「无端口」的服务：没有 HTTP/RPC 入站，只有 Kafka
// 消费与定时任务两条入口。因此消费侧派发必须自己做**边界判定**：
//   - 未知 topic：显式报错，绝不静默 ack（消息被确认却什么都没做，是最难查的丢数据）
//   - 坏载荷：同上，报错交给消费侧的 DLQ 分支
//   - 金额不足 1 元：不发放 0 积分（避免给下游刷无意义调用）
//
// 【红线】job 不直连任何业务库（对账/发积分一律走 srv 的 gRPC 只读或写接口，
// openspec 红线「库内 JOIN，跨库走 RPC」在 job 侧的落点）。
package handler

import (
	"context"
	"fmt"

	"github.com/gospacex/logx"
	"github.com/gospacex/mqx"

	"hubx/micro_apps/pkg/events"
	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
)

// 订阅 topic（与 configs/services/job.yaml 的 consumer.topics 逐字一致）
const (
	// TopicOrderCompleted srv-order 下单完成事件（outbox 补发投递）
	TopicOrderCompleted = "order.completed.v1"
	// TopicCouponExchanged srv-coupon 用券核销事件
	TopicCouponExchanged = "coupon.exchanged.v1"
	// DLQSuffix 死信 topic 后缀（派发失败的消息投递到 <topic>.dlq，由 job retry 重放）
	DLQSuffix = ".dlq"
)

// pointsPerYuan 每消费 1 元积 1 分（演示用简化规则，真实系统通常走积分规则表）
const pointsPerYuan = 100 // 分/元

// CouponClient 券/积分域窄接口（便于单测注入 fake，也便于后续替换实现）
type CouponClient interface {
	GrantPoints(ctx context.Context, req *couponpb.GrantPointsReq) (*couponpb.GrantPointsResp, error)
}

// Dispatcher 按 topic 派发业务动作
type Dispatcher struct {
	coupons CouponClient
	logger  logx.Logger
}

// New 构造派发器（logger 为 nil 时取 logx 全局 logger）
func New(coupons CouponClient, logger logx.Logger) *Dispatcher {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Dispatcher{coupons: coupons, logger: logger}
}

// Dispatch 按 topic 派发。返回错误即代表这条消息需要进 DLQ。
func (d *Dispatcher) Dispatch(ctx context.Context, msg *mqx.Message) error {
	if msg == nil {
		return fmt.Errorf("handler: nil message")
	}
	switch msg.Topic {
	case TopicOrderCompleted:
		return d.onOrderCompleted(ctx, msg)
	case TopicCouponExchanged:
		return d.onCouponExchanged(ctx, msg)
	default:
		return fmt.Errorf("handler: unknown topic %q", msg.Topic)
	}
}

// onOrderCompleted 订单完成 → 按金额发放积分（走 srv-coupon gRPC）
func (d *Dispatcher) onOrderCompleted(ctx context.Context, msg *mqx.Message) error {
	ev, err := events.UnmarshalOrderCompleted(msg.Value)
	if err != nil {
		return fmt.Errorf("handler: unmarshal order.completed: %w", err)
	}
	points := PointsOfAmount(ev.Amount)
	if points <= 0 {
		// 不到 1 元的订单不发积分：这是业务规则，不是错误（不进 DLQ）
		d.logger.WarnContext(ctx, "job: order amount below 1 yuan, skip granting points",
			logx.Any("order_id", ev.OrderID), logx.Any("amount", ev.Amount))
		return nil
	}
	return d.grant(ctx, &couponpb.GrantPointsReq{
		UserId:  ev.UserID,
		Points:  points,
		Source:  "order.completed",
		OrderId: ev.OrderID,
	})
}

// onCouponExchanged 券核销 → 按抵扣金额发放积分
func (d *Dispatcher) onCouponExchanged(ctx context.Context, msg *mqx.Message) error {
	ev, err := events.UnmarshalCouponExchanged(msg.Value)
	if err != nil {
		return fmt.Errorf("handler: unmarshal coupon.exchanged: %w", err)
	}
	points := PointsOfAmount(ev.Discount)
	if points <= 0 {
		d.logger.WarnContext(ctx, "job: coupon discount below 1 yuan, skip granting points",
			logx.Any("coupon_id", ev.CouponID))
		return nil
	}
	return d.grant(ctx, &couponpb.GrantPointsReq{
		UserId:  ev.UserID,
		Points:  points,
		Source:  "coupon.exchanged",
		OrderId: ev.OrderID,
	})
}

// grant 调 srv-coupon 发放积分。
//
// 这里刻意不吞错误：发放失败必须冒泡到消费侧，由它决定进 DLQ 还是重放，
// 否则一次下游抖动就永久丢一笔积分。
func (d *Dispatcher) grant(ctx context.Context, req *couponpb.GrantPointsReq) error {
	if d.coupons == nil {
		return fmt.Errorf("handler: coupon client not wired（未注册 srv-coupon）")
	}
	resp, err := d.coupons.GrantPoints(ctx, req)
	if err != nil {
		return fmt.Errorf("handler: grant points: %w", err)
	}
	if resp.GetCode() != 0 {
		return fmt.Errorf("handler: grant points rejected: code=%d msg=%s", resp.GetCode(), resp.GetMessage())
	}
	d.logger.InfoContext(ctx, "job: points granted",
		logx.Any("user_id", req.GetUserId()),
		logx.Int("points", int(req.GetPoints())),
		logx.String("source", req.GetSource()))
	return nil
}

// PointsOfAmount 金额（分）→ 积分：每满 1 元积 1 分，不足 1 元为 0。
func PointsOfAmount(amountCents int64) int32 {
	if amountCents < pointsPerYuan {
		return 0
	}
	return int32(amountCents / pointsPerYuan)
}
