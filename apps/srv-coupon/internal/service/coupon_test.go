// service 层单测：srv-coupon OutboxRunner.flushOnce 补发语义
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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/apps/srv-coupon/internal/repository"
	"hubx/micro_apps/pkg/events"
)

// newSQLiteRepo 建内存 sqlite（AutoMigrate 四个 Go 模型）
func newSQLiteRepo(t *testing.T) *repository.CouponRepository {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&repository.Coupon{}, &repository.UserCoupon{}, &repository.PointsLog{}, &repository.OutboxEvent{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	return repository.NewCouponRepository(db)
}

// seedPointsEvent 造一条积分发放 outbox 事件（payload 为 PointsGranted JSON）
func seedPointsEvent(t *testing.T, repo *repository.CouponRepository, userID int64, tp string) {
	t.Helper()
	ev := events.PointsGranted{
		UserID:     userID,
		Points:     10,
		Source:     "coupon.exchanged",
		SchemaVer:  events.SchemaVer1,
		OccurredAt: time.Now().UnixMilli(),
	}
	payload, err := events.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	_, err = repo.GrantPointsTx(context.Background(), 1, 10, "coupon.exchanged", 0,
		repository.TopicPointsGranted, repository.OutboxEventTypePointsGranted,
		func(uint64) (string, string) { return string(payload), tp })
	if err != nil {
		t.Fatalf("GrantPointsTx: %v", err)
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
	seedPointsEvent(t, repo, 201, "00-aa-bb-00")
	seedPointsEvent(t, repo, 202, "")

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
	// key 取事件内嵌 UserID（"201"/"202"），而非 outbox.id
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

// TestFlushOnceContinuesTrace：补发 loop 里的 send 必须挂回发放链路
// （回归 producer.Send(context.Background(), msg) 丢链）。
func TestFlushOnceContinuesTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	// Extract 需要真实传播器：OTel 默认是 noop，不装则 traceparent 读不回来
	otel.SetTextMapPropagator(propagation.TraceContext{})

	repo := newSQLiteRepo(t)
	prod := &fakeProducer{}
	runner := NewOutboxRunner(repo, prod, nil, true)

	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	seedPointsEvent(t, repo, 501, "00-"+tid+"-00f067aa0ba902b7-01")
	runner.flushOnce()

	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "messaging.outbox.send" {
		t.Fatalf("expected messaging.outbox.send span, got %+v", spans)
	}
	if got := spans[0].SpanContext.TraceID().String(); got != tid {
		t.Fatalf("send span not continued from outbox traceparent: %s != %s", got, tid)
	}
}

// TestFlushOnceNoSpanWhenTraceOff：mq 开关关闭时补发不落 messaging span。
func TestFlushOnceNoSpanWhenTraceOff(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	repo := newSQLiteRepo(t)
	runner := NewOutboxRunner(repo, &fakeProducer{}, nil, false)
	seedPointsEvent(t, repo, 502, "")
	runner.flushOnce()

	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("trace off: want no span, got %+v", spans)
	}
}

func TestOutboxRunnerFailureRetryAndAbandon(t *testing.T) {
	repo := newSQLiteRepo(t)
	prod := &fakeProducer{fails: 1 << 30} // 永远失败
	runner := NewOutboxRunner(repo, prod, nil)

	seedPointsEvent(t, repo, 301, "")

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
	seedPointsEvent(t, repo, 401, "")
	runner2.Start()
	runner2.Start() // 幂等
	runner2.Stop()
	runner2.Stop()
}
