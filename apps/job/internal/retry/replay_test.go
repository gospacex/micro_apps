// Package retry 测试：DLQ 重放的收敛性。
//
// 重放最怕两件事：无限循环（失败→回写 DLQ→再消费→再失败）和静默丢失
// （超限后既不告警也不留痕）。这里用计数头钉住「最多重放 N 次，超过即放弃」。
package retry

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/gospacex/mqx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type fakeDispatcher struct {
	topics []string
	err    error
}

func (f *fakeDispatcher) Dispatch(_ context.Context, msg *mqx.Message) error {
	f.topics = append(f.topics, msg.Topic)
	return f.err
}

type fakeProducer struct{ sent []*mqx.Message }

func (f *fakeProducer) Send(_ context.Context, msg *mqx.Message) error {
	f.sent = append(f.sent, msg)
	return nil
}
func (f *fakeProducer) SendBatch(_ context.Context, msgs []*mqx.Message) error {
	for _, m := range msgs {
		f.sent = append(f.sent, m)
	}
	return nil
}
func (f *fakeProducer) Close() error      { return nil }
func (f *fakeProducer) Name() string      { return "fake" }
func (f *fakeProducer) HealthCheck() bool { return true }

type fakeConsumer struct{}

func (fakeConsumer) Consume(context.Context, func(ctx context.Context, msg *mqx.Message) error) error {
	return nil
}
func (fakeConsumer) Start(context.Context, func(ctx context.Context, msg *mqx.Message) error) error {
	return nil
}
func (fakeConsumer) Close() error      { return nil }
func (fakeConsumer) Name() string      { return "fake" }
func (fakeConsumer) HealthCheck() bool { return true }

// TestReplayStripsDLQSuffix：重放必须按**原 topic** 派发（带 .dlq 会被 dispatcher 判为未知 topic）
func TestReplayStripsDLQSuffix(t *testing.T) {
	disp := &fakeDispatcher{}
	r := New(fakeConsumer{}, &fakeProducer{}, disp, nil, true)
	if err := r.OnMessage(context.Background(), &mqx.Message{
		Topic:   "order.completed.v1.dlq",
		Value:   []byte(`{}`),
		Headers: map[string]string{},
	}); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if len(disp.topics) != 1 || disp.topics[0] != "order.completed.v1" {
		t.Fatalf("dispatched topics = %v, want [order.completed.v1]", disp.topics)
	}
}

// TestReplaySuccessStops：重放成功就不再回写 DLQ
func TestReplaySuccessStops(t *testing.T) {
	disp := &fakeDispatcher{}
	prod := &fakeProducer{}
	r := New(fakeConsumer{}, prod, disp, nil, true)
	if err := r.OnMessage(context.Background(), &mqx.Message{Topic: "order.completed.v1.dlq"}); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if len(prod.sent) != 0 {
		t.Fatalf("success must not requeue, got %d", len(prod.sent))
	}
}

// TestReplayRequeueWithCount：失败回写 DLQ 且带上递增的重放次数
func TestReplayRequeueWithCount(t *testing.T) {
	disp := &fakeDispatcher{err: errors.New("still down")}
	prod := &fakeProducer{}
	r := New(fakeConsumer{}, prod, disp, nil, true)
	msg := &mqx.Message{Topic: "order.completed.v1.dlq", Headers: map[string]string{}}
	if err := r.OnMessage(context.Background(), msg); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if len(prod.sent) != 1 {
		t.Fatalf("want requeue, got %d", len(prod.sent))
	}
	if got := prod.sent[0].Headers[HeaderReplayCount]; got != "1" {
		t.Fatalf("replay count header = %q, want \"1\"", got)
	}
}

// TestReplayGivesUpAfterMax：超过上限即放弃（不再回写，避免无限循环）
func TestReplayGivesUpAfterMax(t *testing.T) {
	disp := &fakeDispatcher{err: errors.New("permanently down")}
	prod := &fakeProducer{}
	r := New(fakeConsumer{}, prod, disp, nil, true)
	msg := &mqx.Message{
		Topic:   "order.completed.v1.dlq",
		Headers: map[string]string{HeaderReplayCount: strconv.Itoa(MaxReplay)},
	}
	if err := r.OnMessage(context.Background(), msg); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if len(prod.sent) != 0 {
		t.Fatalf("over cap must give up (no requeue), got %d", len(prod.sent))
	}
}

// TestReplaySpanName：重放同样入链（trace 开时必须能数到 replay span）
func TestReplaySpanName(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	r := New(fakeConsumer{}, &fakeProducer{}, &fakeDispatcher{}, nil, true)
	if err := r.OnMessage(context.Background(), &mqx.Message{Topic: "order.completed.v1.dlq"}); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "messaging.order.completed.replay" {
		t.Fatalf("want messaging.order.completed.replay, got %+v", spans)
	}
}

// TestReplayNoSpanWhenTraceOff
func TestReplayNoSpanWhenTraceOff(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	r := New(fakeConsumer{}, &fakeProducer{}, &fakeDispatcher{}, nil, false)
	if err := r.OnMessage(context.Background(), &mqx.Message{Topic: "order.completed.v1.dlq"}); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("trace off: want no span, got %+v", spans)
	}
}
