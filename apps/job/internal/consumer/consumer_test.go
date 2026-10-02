// Package consumer 测试：入站 traceparent 续链（send/receive 同 trace 的锚点）。
//
// 守两件事：
//  1. 消息头带 traceparent 时，receive span 必须挂在**原 trace** 上（回归
//     context.Background() 丢链：那样 send 与 receive 会分属两条 trace，
//     Jaeger 上看不出「这条消息是这条下单链路产出的」）。
//  2. mq 开关关闭时既不 Extract 也不打 span（关了就别埋）。
package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/gospacex/mqx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"hubx/micro_apps/pkg/events"
)

// fakeDispatcher 记录收到的消息，可按用例返回错误
type fakeDispatcher struct {
	got  []*mqx.Message
	err  error
	seen context.Context
}

func (f *fakeDispatcher) Dispatch(ctx context.Context, msg *mqx.Message) error {
	f.got = append(f.got, msg)
	f.seen = ctx
	return f.err
}

// fakeConsumer 把 Consume 退化成直接调用一次 handler（不起 Kafka）
type fakeConsumer struct {
	handler func(ctx context.Context, msg *mqx.Message) error
}

func (f *fakeConsumer) Consume(ctx context.Context, h func(ctx context.Context, msg *mqx.Message) error) error {
	f.handler = h
	return nil
}
func (f *fakeConsumer) Start(ctx context.Context, h func(ctx context.Context, msg *mqx.Message) error) error {
	f.handler = h
	return nil
}
func (f *fakeConsumer) Close() error      { return nil }
func (f *fakeConsumer) Name() string      { return "fake" }
func (f *fakeConsumer) HealthCheck() bool { return true }

// fakeProducer 记录 DLQ 投递
type fakeProducer struct {
	sent []*mqx.Message
	err  error
}

func (f *fakeProducer) Send(_ context.Context, msg *mqx.Message) error {
	f.sent = append(f.sent, msg)
	return f.err
}
func (f *fakeProducer) SendBatch(_ context.Context, msgs []*mqx.Message) error {
	for _, m := range msgs {
		f.sent = append(f.sent, m)
	}
	return f.err
}
func (f *fakeProducer) Close() error      { return nil }
func (f *fakeProducer) Name() string      { return "fake" }
func (f *fakeProducer) HealthCheck() bool { return true }

// TestOnMessageContinuesTrace：receive span 必须续接消息头里的 traceparent
func TestOnMessageContinuesTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	disp := &fakeDispatcher{}
	c := New(&fakeConsumer{}, disp, nil, true, nil)

	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	msg := &mqx.Message{
		Topic:   "order.completed.v1",
		Key:     "1",
		Value:   []byte(`{"order_id":1}`),
		Headers: map[string]string{events.TraceHeaderKey: "00-" + tid + "-00f067aa0ba902b7-01"},
	}
	if err := c.OnMessage(context.Background(), msg); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("want 1 receive span, got %d: %+v", len(spans), spans)
	}
	if spans[0].Name != "messaging.order.completed.receive" {
		t.Fatalf("span name = %q, want messaging.order.completed.receive", spans[0].Name)
	}
	if got := spans[0].SpanContext.TraceID().String(); got != tid {
		t.Fatalf("receive span trace = %s, want %s（未续接 traceparent）", got, tid)
	}
	if len(disp.got) != 1 {
		t.Fatalf("dispatcher not called")
	}
}

// TestOnMessageNoSpanWhenTraceOff：mq 开关关 → 零 span
func TestOnMessageNoSpanWhenTraceOff(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	disp := &fakeDispatcher{}
	c := New(&fakeConsumer{}, disp, nil, false, nil)
	msg := &mqx.Message{
		Topic:   "order.completed.v1",
		Headers: map[string]string{events.TraceHeaderKey: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
	if err := c.OnMessage(context.Background(), msg); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("trace off: want no span, got %+v", spans)
	}
}

// TestOnMessageFailureGoesToDLQ：派发失败必须进 DLQ 且保留 traceparent
// （丢了头，重放时就续不回原链路）
func TestOnMessageFailureGoesToDLQ(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	disp := &fakeDispatcher{err: errors.New("coupon down")}
	prod := &fakeProducer{}
	c := New(&fakeConsumer{}, disp, nil, true, prod)

	msg := &mqx.Message{Topic: "order.completed.v1", Value: []byte(`{}`), Headers: map[string]string{}}
	if err := c.OnMessage(context.Background(), msg); err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if len(prod.sent) != 1 {
		t.Fatalf("want 1 DLQ message, got %d", len(prod.sent))
	}
	if prod.sent[0].Topic != "order.completed.v1.dlq" {
		t.Fatalf("dlq topic = %q", prod.sent[0].Topic)
	}
	if _, ok := prod.sent[0].Headers[events.TraceHeaderKey]; !ok {
		t.Fatalf("dlq message missing traceparent: %+v", prod.sent[0].Headers)
	}
}

// TestOnMessageDLQSendFailureSurfaces：DLQ 也写不进去时必须把错误抛出来
// （否则消息被 ack，失败的消息凭空消失）
func TestOnMessageDLQSendFailureSurfaces(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	disp := &fakeDispatcher{err: errors.New("coupon down")}
	prod := &fakeProducer{err: errors.New("broker down")}
	c := New(&fakeConsumer{}, disp, nil, true, prod)
	if err := c.OnMessage(context.Background(), &mqx.Message{Topic: "order.completed.v1"}); err == nil {
		t.Fatal("DLQ send failure must surface")
	}
}

func TestEntityOf(t *testing.T) {
	cases := map[string]string{
		"order.completed.v1":     "order.completed",
		"coupon.exchanged.v1":    "coupon.exchanged",
		"order.completed.v1.dlq": "order.completed", // DLQ 重放沿用同一 entity
		"nodefault":              "nodefault",
	}
	for in, want := range cases {
		if got := EntityOf(in); got != want {
			t.Errorf("EntityOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestExtractContextNoHeader(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx := ExtractContext(context.Background(), nil)
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("no traceparent header: extracted span context must be invalid（应开新根）")
	}
}
