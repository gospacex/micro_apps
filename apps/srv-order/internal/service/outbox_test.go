// service 层单测：OutboxRunner.flushOnce 补发语义
// （sqlite repository + fake mqx.Producer；避免 3s ticker 等待）。
package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gospacex/mqx"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"hubx/micro_apps/apps/srv-order/internal/repository"
	"hubx/micro_apps/pkg/events"
)

// newSQLiteRepo 建内存 sqlite（表结构与 repository 测试一致）
func newSQLiteRepo(t *testing.T) *repository.OrderRepository {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&repository.Order{}, &repository.OrderItem{}, &repository.OutboxEvent{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	return repository.NewOrderRepository(db)
}

// seedOrderWithEvent 造一条订单 + outbox 事件（payload 为 OrderCompleted JSON）
func seedOrderWithEvent(t *testing.T, repo *repository.OrderRepository, orderID int64, tp string) {
	t.Helper()
	ctx := context.Background()
	ev := events.OrderCompleted{
		OrderID:    orderID,
		UserID:     42,
		Amount:     100,
		SchemaVer:  events.SchemaVer1,
		OccurredAt: time.Now().UnixMilli(),
	}
	payload, err := events.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := repo.CreateOrderTx(ctx, repository.CreateOrderInput{
		UserID:      42,
		OrderNo:     "O-SVC-" + time.Now().String(),
		TotalAmount: 100,
		OutboxFill:  func(uint64) (string, string) { return string(payload), tp },
	}); err != nil {
		t.Fatalf("CreateOrderTx: %v", err)
	}
}

// fakeProducer 记录发送
type fakeProducer struct {
	mu     sync.Mutex
	sent   []*mqx.Message
	fails  int // 前 N 次 Send 返回错误（模拟补发失败）
	calls  int
	closed bool
}

func (f *fakeProducer) Send(_ context.Context, msg *mqx.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.fails {
		return errors.New("kafka: broker unavailable")
	}
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeProducer) SendBatch(_ context.Context, _ []*mqx.Message) error { return nil }
func (f *fakeProducer) Close() error                                        { f.closed = true; return nil }
func (f *fakeProducer) Name() string                                        { return "fake" }
func (f *fakeProducer) HealthCheck() bool                                   { return true }

func TestOutboxRunnerFlush(t *testing.T) {
	repo := newSQLiteRepo(t)
	prod := &fakeProducer{}
	runner := NewOutboxRunner(repo, prod, nil)

	// 两条待发（一条带 traceparent）
	seedOrderWithEvent(t, repo, 101, "00-aa-bb-00")
	seedOrderWithEvent(t, repo, 102, "")

	runner.flushOnce()

	prod.mu.Lock()
	sent := len(prod.sent)
	var withTP, withoutTP int
	var keys []string
	for _, m := range prod.sent {
		hdr := m.Headers[events.TraceHeaderKey]
		if hdr == "00-aa-bb-00" {
			withTP++
		} else if hdr == "" {
			withoutTP++
		}
		keys = append(keys, m.Key)
	}
	prod.mu.Unlock()

	if sent != 2 {
		t.Fatalf("sent = %d, want 2", sent)
	}
	// key 取事件内嵌 OrderID（"101"/"102"），而非 outbox.id
	if len(keys) != 2 {
		t.Fatalf("keys = %v", keys)
	}
	if withTP != 1 || withoutTP != 1 {
		t.Fatalf("traceparent headers: with=%d without=%d (want 1/1)", withTP, withoutTP)
	}
	// 全部转 sent
	pending, err := repo.PendingOutbox(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingOutbox: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending rest = %+v (want 0)", pending)
	}
}

func TestOutboxRunnerFailureRetryAndAbandon(t *testing.T) {
	repo := newSQLiteRepo(t)
	prod := &fakeProducer{fails: 1 << 30} // 永远失败
	runner := NewOutboxRunner(repo, prod, nil)

	seedOrderWithEvent(t, repo, 201, "")

	// 连续补发到重试上限 → 放弃（status=2）
	for i := 0; i < outboxRetryCap; i++ {
		runner.flushOnce()
	}
	pending, err := repo.PendingOutbox(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingOutbox: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("after %d failed retries, want all abandoned, got %+v", outboxRetryCap, pending)
	}
}

func TestOutboxRunnerStartStopIdempotent(t *testing.T) {
	repo := newSQLiteRepo(t)
	// producer 为 nil → Start 不启动，Stop 幂等 no-op
	runner := NewOutboxRunner(repo, nil, nil)
	runner.Start()
	runner.Start()
	runner.Stop()
	runner.Stop()

	// 正常 producer：Start 起 goroutine，Stop 收敛（done 关闭）
	prod := &fakeProducer{}
	runner2 := NewOutboxRunner(repo, prod, nil)
	seedOrderWithEvent(t, repo, 301, "")
	runner2.Start()
	runner2.Start() // 幂等
	runner2.Stop()
	runner2.Stop()
}
