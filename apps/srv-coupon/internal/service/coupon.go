// Package service srv-coupon 业务编排层。
//
// 语义（brief §srv-coupon）：
//   - Consume：user_coupons held → used 事务 + used_order_id 回填
//     （唯一约束 uk_user_coupon 承担幂等；used 重放 → 6001；过期/停用 → 6002）；
//     核销成功同事务写 outbox CouponExchanged 事件（3s 轮询补发）
//   - GrantPoints：points_log 插入 + outbox PointsGranted（事务发件箱，
//     模式同 srv-order；outbox 表在 coupon_db，DDL 已同步两处）
//   - ListCoupons：user_coupons ⋈ coupons 2 表 JOIN Find，includeUsed 过滤
//
// 业务错误一律经 Response.Code/Message 返回（pkg/codes），网络/内部错误
// 走 Go error（rpcx 错误体），两套不混用。
package service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/gospacex/logx"
	"github.com/gospacex/mqx"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"

	"hubx/micro_apps/apps/srv-coupon/internal/repository"
	"hubx/micro_apps/pkg/codes"
	"hubx/micro_apps/pkg/events"
)

// ConsumeResp Discount 分：amount 型券 = 面额；percent 型 = 折扣百分比
// （coupon_db.coupons.discount 语义随 type 变化，brief 原样透传）

// CouponService 券业务编排
type CouponService struct {
	repo    *repository.CouponRepository
	outbox  *OutboxRunner
	traceOn bool
	logger  logx.Logger
}

// NewCouponService 构造（outbox 允许 nil：MQ 门控关闭形态，事件持续累积）
func NewCouponService(
	repo *repository.CouponRepository,
	outbox *OutboxRunner,
	traceOn bool,
	logger logx.Logger,
) *CouponService {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &CouponService{repo: repo, outbox: outbox, traceOn: traceOn, logger: logger}
}

// Consume 用券核销（幂等：used 重放 → 6001）
func (s *CouponService) Consume(ctx context.Context, req *couponpb.ConsumeReq) (*couponpb.ConsumeResp, error) {
	resp := &couponpb.ConsumeResp{UserId: req.GetUserId(), CouponId: req.GetCouponId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if req.GetCouponId() <= 0 || req.GetOrderId() <= 0 {
		c := codes.Fail(codes.CodeInvalidParam, "coupon and order required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}

	// 券模板面额（事件体 discount 字段；查失败不阻断核销——面额降级 0）
	var discount int64
	if c, cerr := s.repo.FindCoupon(ctx, uint64(req.GetCouponId())); cerr == nil {
		discount = c.Discount
	} else if !errors.Is(cerr, repository.ErrNotFound) {
		s.logger.WarnContext(ctx, "coupon template lookup failed (discount=0)", logx.Err(cerr))
	}

	// 核销事务：user_coupons held→used + outbox CouponExchanged（同事务）
	evTraceparent := s.traceparentOf(ctx)
	evTraceID := evTraceID(ctx)
	in := repository.ConsumeTxInput{
		UserID:   uint64(req.GetUserId()),
		CouponID: uint64(req.GetCouponId()),
		OrderID:  uint64(req.GetOrderId()),
		OutboxFill: func() (payload, traceparent string) {
			ev := events.CouponExchanged{
				UserID:     req.GetUserId(),
				CouponID:   req.GetCouponId(),
				OrderID:    req.GetOrderId(),
				Discount:   discount,
				SchemaVer:  events.SchemaVer1,
				TraceID:    evTraceID,
				OccurredAt: time.Now().UnixMilli(),
			}
			b, merr := events.Marshal(ev)
			if merr != nil {
				// 序列化失败不阻断核销主流程：空 payload 时 Runner 跳过该事件
				s.logger.WarnContext(ctx, "coupon event marshal failed", logx.Err(merr))
				return "", evTraceparent
			}
			return string(b), evTraceparent
		},
	}
	wasUsed, err := s.repo.ConsumeTx(ctx, in)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrCouponUsed):
			// 幂等重放（券已核销）：返回 6001 + 既有面额
			resp.Discount = discount
			c := codes.Fail(codes.CodeCouponUsed, "coupon already used")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		case errors.Is(err, repository.ErrCouponExpired):
			c := codes.Fail(codes.CodeCouponExpired, "coupon expired")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		case errors.Is(err, repository.ErrCouponHeld):
			c := codes.Fail(codes.CodeCouponNotMatch, "coupon not held")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		default:
			s.logger.WarnContext(ctx, "consume tx failed", logx.Err(err))
			c := codes.Fail(codes.CodeInternal, err.Error())
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
	}
	if wasUsed {
		resp.Discount = discount
		c := codes.Fail(codes.CodeCouponUsed, "coupon already used")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	resp.Discount = discount
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// traceparentOf 取当前链路的 W3C traceparent（随 outbox header 透传；
// 调用链关闭或无活动 span 时为空串）
func (s *CouponService) traceparentOf(ctx context.Context) string {
	if !s.traceOn {
		return ""
	}
	carrier := propagation.MapCarrier(map[string]string{})
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get(events.TraceHeaderKey)
}

// evTraceID 事件体内嵌 trace_id（span 的 TraceID；无活动 span 时为空）
func evTraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

// GrantPoints 积分发放（points_log + outbox PointsGranted 同事务；
// 后台 Runner 3s 补发 points.granted.v1）
func (s *CouponService) GrantPoints(ctx context.Context, req *couponpb.GrantPointsReq) (*couponpb.GrantPointsResp, error) {
	resp := &couponpb.GrantPointsResp{UserId: req.GetUserId(), Points: req.GetPoints()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if req.GetPoints() <= 0 {
		c := codes.Fail(codes.CodeInvalidParam, "points must be positive")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	source := req.GetSource()
	if source == "" {
		source = "order.completed"
	}

	evTraceparent := s.traceparentOf(ctx)
	evTraceID := evTraceID(ctx)
	_, err := s.repo.GrantPointsTx(ctx, uint64(req.GetUserId()), int(req.GetPoints()), source, uint64(req.GetOrderId()),
		repository.TopicPointsGranted, repository.OutboxEventTypePointsGranted,
		func(uint64) (payload, traceparent string) {
			ev := events.PointsGranted{
				UserID:     req.GetUserId(),
				Points:     int(req.GetPoints()),
				Source:     source,
				OrderID:    req.GetOrderId(),
				SchemaVer:  events.SchemaVer1,
				TraceID:    evTraceID,
				OccurredAt: time.Now().UnixMilli(),
			}
			b, merr := events.Marshal(ev)
			if merr != nil {
				// 序列化失败不阻断发放主流程：空 payload 时 Runner 跳过该事件
				s.logger.WarnContext(ctx, "points event marshal failed", logx.Err(merr))
				return "", evTraceparent
			}
			return string(b), evTraceparent
		})
	if err != nil {
		s.logger.WarnContext(ctx, "grant points tx failed", logx.Err(err))
		c := codes.Fail(codes.CodePointsFailed, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// ListCoupons 用户持有的券
func (s *CouponService) ListCoupons(ctx context.Context, req *couponpb.ListCouponsReq) (*couponpb.ListCouponsResp, error) {
	resp := &couponpb.ListCouponsResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	rows, err := s.repo.ListCoupons(ctx, uint64(req.GetUserId()), req.GetIncludeUsed())
	if err != nil {
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	out := make([]*couponpb.CouponInfo, 0, len(rows))
	for i := range rows {
		// used 态券回填 used_order_id（事件体对齐）
		out = append(out, &couponpb.CouponInfo{
			CouponId:    int64(rows[i].UserCoupon.CouponID),
			Name:        rows[i].Coupon.Name,
			Discount:    rows[i].Coupon.Discount,
			Type:        rows[i].Coupon.Type,
			Status:      rows[i].UserCoupon.Status,
			UsedOrderId: int64(rows[i].UserCoupon.UsedOrderID),
		})
	}
	resp.Coupons = out
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// ---------- OutboxRunner（模式同 srv-order，topic 为 coupon_db 两类事件） ----------

// 轮询参数（与 srv-order 一致：3s 间隔 / 批 50 / 重试上限 10）
const (
	outboxPollInterval = 3 * time.Second
	outboxBatchLimit   = 50
	outboxRetryCap     = 10
)

// OutboxRunner 后台补发器：3s ticker 拉取 coupon_db.outbox 待发事件，
// 经 mqx.Producer 发送；发送成功 → status=1；失败 retry_count+1，
// 超 outboxRetryCap 放弃（status=2，人工对账兜底）。
// 发送 key 取事件体内嵌 CouponID/OrderID（Unmarshal 失败回退 outbox.id）。
type OutboxRunner struct {
	repo     *repository.CouponRepository
	producer mqx.Producer
	logger   logx.Logger

	stop chan struct{}
	done chan struct{}
	once chan struct{}
}

// NewOutboxRunner 构造（producer 允许 nil：Start 不启动，仅累积待发事件）
func NewOutboxRunner(repo *repository.CouponRepository, producer mqx.Producer, logger logx.Logger) *OutboxRunner {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &OutboxRunner{repo: repo, producer: producer, logger: logger, stop: make(chan struct{}), done: make(chan struct{}), once: make(chan struct{})}
}

// Start 启动 ticker（幂等；producer 为 nil 时 no-op）
func (r *OutboxRunner) Start() {
	if r.producer == nil {
		return
	}
	select {
	case <-r.once:
		return
	default:
	}
	close(r.once)
	go r.loop()
}

// loop ticker 主循环（flushCtx 随 stop 收敛）
func (r *OutboxRunner) loop() {
	defer close(r.done)
	t := time.NewTicker(outboxPollInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.flushOnce()
		}
	}
}

// Stop 停止（幂等；阻塞至 loop 收敛）
func (r *OutboxRunner) Stop() {
	if r.producer == nil {
		return
	}
	select {
	case <-r.stop:
		<-r.done
		return
	default:
	}
	close(r.stop)
	<-r.done
}

// flushOnce 单轮补发（导出便于单测免等 ticker）：
// 拉待发批次 → 逐条 Send（key 取事件内嵌 ID，traceparent 随 Headers）
// → 成功 MarkSent / 失败 MarkRetry（超限 Abandon）。
func (r *OutboxRunner) flushOnce() {
	if r == nil || r.producer == nil {
		return
	}
	pending, err := r.repo.PendingOutbox(context.Background(), outboxBatchLimit)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("outbox poll failed", logx.Err(err))
		}
		return
	}
	if len(pending) == 0 {
		return
	}
	var sent []uint64
	var failed []uint64
	var abandon []uint64
	for i := range pending {
		ev := pending[i]
		if ev.Payload == "" {
			// 事件体写失败形态（payload 空）：直接放弃，不无限重试
			abandon = append(abandon, ev.ID)
			continue
		}
		msg, ok := r.buildMessage(ev)
		if !ok {
			if r.logger != nil {
				r.logger.WarnContext(context.Background(), "outbox event malformed (abandon)",
					logx.Int("outbox.id", int(ev.ID)))
			}
			abandon = append(abandon, ev.ID)
			continue
		}
		if serr := r.producer.Send(context.Background(), msg); serr != nil {
			if r.logger != nil {
				r.logger.WarnContext(context.Background(), "outbox send failed",
					logx.Int("outbox.id", int(ev.ID)), logx.Err(serr))
			}
			failed = append(failed, ev.ID)
			continue
		}
		sent = append(sent, ev.ID)
	}
	ctx := context.Background()
	if len(sent) > 0 {
		if err := r.repo.MarkOutboxSent(ctx, sent); err != nil {
			r.warn(ctx, "mark outbox sent failed", err)
		}
	}
	if len(failed) > 0 {
		if err := r.repo.MarkOutboxRetry(ctx, failed); err != nil {
			r.warn(ctx, "mark outbox retry failed", err)
		}
		// 超限判定：retry_count 达上限者转入放弃
		over := r.overCap(ctx, failed)
		if len(over) > 0 {
			rest := subtract(failed, over)
			abandon = append(abandon, over...)
			if len(rest) > 0 {
				// 未超限的保持待发（下轮补发）
			}
			failed = rest
		}
	}
	if len(abandon) > 0 {
		if err := r.repo.AbandonOutbox(ctx, abandon); err != nil {
			r.warn(ctx, "abandon outbox failed", err)
		}
	}
}

// buildMessage 事件 → mqx Message（key 取内嵌业务 ID，traceparent 非空才带 header）
func (r *OutboxRunner) buildMessage(ev repository.OutboxEvent) (*mqx.Message, bool) {
	switch ev.EventType {
	case repository.OutboxEventTypeCouponExchanged:
		parsed, err := events.UnmarshalCouponExchanged([]byte(ev.Payload))
		if err != nil || parsed == nil {
			return nil, false
		}
		key := strOf(int64(parsed.CouponID))
		if parsed.CouponID == 0 {
			key = strOf(int64(parsed.OrderID))
		}
		if key == "0" {
			key = strOf(int64(ev.ID))
		}
		return &mqx.Message{
			Topic:     ev.Topic,
			Key:       key,
			Value:     []byte(ev.Payload),
			Headers:   traceHeaders(ev.Traceparent),
			Timestamp: time.Now(),
		}, true
	case repository.OutboxEventTypePointsGranted:
		parsed, err := events.UnmarshalPointsGranted([]byte(ev.Payload))
		if err != nil || parsed == nil {
			return nil, false
		}
		key := strOf(int64(parsed.UserID))
		if parsed.UserID == 0 {
			key = strOf(int64(ev.ID))
		}
		return &mqx.Message{
			Topic:     ev.Topic,
			Key:       key,
			Value:     []byte(ev.Payload),
			Headers:   traceHeaders(ev.Traceparent),
			Timestamp: time.Now(),
		}, true
	default:
		// 未知事件类型（schema 演进）：按 outbox.id 兜底发送（事件体原样）
		return &mqx.Message{
			Topic:     ev.Topic,
			Key:       strOf(int64(ev.ID)),
			Value:     []byte(ev.Payload),
			Headers:   traceHeaders(ev.Traceparent),
			Timestamp: time.Now(),
		}, true
	}
}

// traceHeaders traceparent 非空才挂 header（调用链关闭时 mqx 收不到该 header，
// 与 srv-order 同语义）
func traceHeaders(tp string) map[string]string {
	if tp == "" {
		return nil
	}
	return map[string]string{events.TraceHeaderKey: tp}
}

// overCap 查 failed 中 retry_count 已达上限者（PendingOutbox 已不含这些行——
// MarkOutboxRetry 未改 status，仍待发；按 retry_count 过滤）
func (r *OutboxRunner) overCap(ctx context.Context, ids []uint64) []uint64 {
	rows, err := r.repo.PendingOutbox(ctx, outboxBatchLimit*2)
	if err != nil {
		return nil
	}
	set := make(map[uint64]struct{}, len(ids))
	for i := range ids {
		set[ids[i]] = struct{}{}
	}
	var out []uint64
	for i := range rows {
		if _, ok := set[rows[i].ID]; ok && rows[i].RetryCount >= outboxRetryCap {
			out = append(out, rows[i].ID)
		}
	}
	return out
}

// subtract 集合差（a 中非 b 成员）
func subtract(a, b []uint64) []uint64 {
	exclude := make(map[uint64]struct{}, len(b))
	for i := range b {
		exclude[b[i]] = struct{}{}
	}
	var out []uint64
	for i := range a {
		if _, ok := exclude[a[i]]; !ok {
			out = append(out, a[i])
		}
	}
	return out
}

// strOf int64 → key 字符串
func strOf(v int64) string {
	if v <= 0 {
		return "0"
	}
	return strconv.FormatInt(v, 10)
}

func (r *OutboxRunner) warn(ctx context.Context, msg string, err error) {
	if r.logger != nil {
		r.logger.WarnContext(ctx, msg, logx.Err(err))
	}
}
