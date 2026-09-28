// Package service srv-order 业务编排层（outbox 补发器）。
//
// OutboxRunner 事务发件箱补发：3s ticker 拉 status=0 的待发事件经
// mqx 生产者发出；发送成功标记 sent（0→1，WHERE status=0 守卫防重复
// 迁移），失败 retry_count+1，超 outboxRetryCap 转放弃（status=2，
// 人工对账兜底）。traceparent 随 mqx Message.Headers[events.TraceHeaderKey]
// 透传（批 2 已核实：mqx 无独立 Header 类型，header 走 Message.Headers map）。
//
// producer 为 nil（pkg/config Trace.On("mq") 关）时 Start 不启动，Stop 幂等。
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/logx"
	"github.com/gospacex/mqx"

	"hubx/micro_apps/apps/srv-order/internal/repository"
	"hubx/micro_apps/pkg/events"
)

// outboxPollInterval 3s 轮询补发间隔（brief §srv-order）
const outboxPollInterval = 3 * time.Second

// outboxBatchLimit 单轮补发上限（防积压时一次性打爆 broker）
const outboxBatchLimit = 50

// outboxRetryCap 补发重试上限：超过即放弃（status=2，人工对账兜底）
const outboxRetryCap = 10

// OutboxRunner 事务发件箱补发器
type OutboxRunner struct {
	repo     *repository.OrderRepository
	producer mqx.Producer
	logger   logx.Logger
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

// NewOutboxRunner 构造（producer 允许 nil：On("mq") 关时由调用方传 nil）
func NewOutboxRunner(repo *repository.OrderRepository, producer mqx.Producer, logger logx.Logger) *OutboxRunner {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &OutboxRunner{repo: repo, producer: producer, logger: logger}
}

// Start 启动 3s ticker 补发（幂等：重复 Start 忽略；producer 为 nil 不启动）
func (r *OutboxRunner) Start() {
	r.once.Do(func() {
		if r.producer == nil {
			return
		}
		r.stop = make(chan struct{})
		r.done = make(chan struct{})
		go r.loop()
	})
}

// Stop 停止补发（优雅退出调用；阻塞至本轮结束；未 Start 时 no-op）
func (r *OutboxRunner) Stop() {
	if r.stop == nil {
		return
	}
	select {
	case <-r.done:
		return
	default:
		close(r.stop)
		<-r.done
	}
}

func (r *OutboxRunner) loop() {
	ticker := time.NewTicker(outboxPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			close(r.done)
			return
		case <-ticker.C:
			r.flushOnce()
		}
	}
}

// flushOnce 单轮补发（导出供单测直接驱动，避免依赖 3s ticker 等待）
func (r *OutboxRunner) flushOnce() {
	ctx := context.Background()
	pending, err := r.repo.PendingOutbox(ctx, outboxBatchLimit)
	if err != nil {
		r.logger.ErrorContext(ctx, "outbox: list pending failed", logx.Err(err))
		return
	}
	if len(pending) == 0 {
		return
	}
	sent, retried := []uint64{}, []uint64{}
	for i := range pending {
		ev := &pending[i]
		if ev.Payload == "" {
			// 事件体为空（下单主流程 marshal 失败已记 warn）：直接放弃
			r.logger.WarnContext(ctx, "outbox: empty payload, abandoning", logx.Int("id", int(ev.ID)))
			if err := r.repo.AbandonOutbox(ctx, []uint64{ev.ID}); err != nil {
				r.logger.ErrorContext(ctx, "outbox: abandon empty-payload event failed", logx.Err(err))
			}
			continue
		}
		if err := r.sendOne(ctx, ev); err != nil {
			r.logger.WarnContext(ctx, "outbox: send failed, will retry",
				logx.Int("id", int(ev.ID)), logx.Err(err))
			retried = append(retried, ev.ID)
			continue
		}
		sent = append(sent, ev.ID)
	}
	if len(sent) > 0 {
		if err := r.repo.MarkOutboxSent(ctx, sent); err != nil {
			r.logger.ErrorContext(ctx, "outbox: mark sent failed", logx.Err(err))
		}
	}
	// 重试计数 +1；超限的转放弃（status=2）
	if len(retried) > 0 {
		if err := r.repo.MarkOutboxRetry(ctx, retried); err != nil {
			r.logger.ErrorContext(ctx, "outbox: mark retry failed", logx.Err(err))
			return
		}
		abandon := make([]uint64, 0, len(retried))
		for i := range pending {
			if pending[i].RetryCount+1 >= outboxRetryCap {
				abandon = append(abandon, pending[i].ID)
			}
		}
		if len(abandon) > 0 {
			if err := r.repo.AbandonOutbox(ctx, abandon); err != nil {
				r.logger.ErrorContext(ctx, "outbox: abandon failed", logx.Err(err))
				return
			}
			r.logger.WarnContext(ctx, "outbox: retry cap reached, abandoned for manual reconcile",
				logx.Int("count", len(abandon)))
		}
	}
}

// sendOne 单条事件发送（topic 来自 outbox 行；traceparent 走 header，
// 仅当写入时非空才携带——mq 关时为空串，与 trace 总闸语义一致）
func (r *OutboxRunner) sendOne(ctx context.Context, ev *repository.OutboxEvent) error {
	// key 取事件内嵌的订单 ID（消费端按订单分区去重）；
	// 解析失败回退 outbox.id（顺序语义不变）
	key := fmt.Sprintf("%d", ev.ID)
	if parsed, err := events.UnmarshalOrderCompleted([]byte(ev.Payload)); err == nil && parsed.OrderID > 0 {
		key = fmt.Sprintf("%d", parsed.OrderID)
	}
	msg := &mqx.Message{
		Topic:   ev.Topic,
		Key:     key,
		Value:   []byte(ev.Payload),
		Headers: map[string]string{},
	}
	if ev.Traceparent != "" {
		msg.Headers[events.TraceHeaderKey] = ev.Traceparent
	}
	return r.producer.Send(ctx, msg)
}
