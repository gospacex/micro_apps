package rpc

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestGuardSpanEnds：guard 产出的分层 span 必须真实导出（End 后可见），
// 回归「ctx, _ = telemetry.Start」span 泄漏缺陷。
func TestGuardSpanEnds(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	s := &Server{}
	ctx, span := s.guard(context.Background(), "Pay")
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 exported span, got %d", len(spans))
	}
	if name := spans[0].Name; name != "service.pay.Pay" {
		t.Fatalf("unexpected span name %q", name)
	}
	_ = ctx
}
