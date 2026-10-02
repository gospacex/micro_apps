package rpcx

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	userpb "hubx/micro_apps/pkg/kitex_gen/user"
	usercli "hubx/micro_apps/pkg/kitex_gen/user/user"
)

// TestClientSpanIsExported 钉住「出站 RPC 必须产出一个可上报的 client span」。
//
// 这是 2026-10-02 修掉的实测缺陷的回归闸门：此前 bff→srv 的 span 树在边界
// 断成两棵子树——下游能收到正确的 traceparent（同 trace_id），父 span id 却
// 指向一个从不存在的 span。只断言 option 数量挡不住这类回归，所以这里真的
// 走一次 kitex 调用链，看 span 有没有落到 exporter。
//
// 调用目标是必然连不上的地址：middleware 在 endpoint chain 外层，无论 RPC
// 成败都会执行，因此不需要真起服务端。
func TestClientSpanIsExported(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exp)))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	// 父 span：模拟调用方的业务 span，client span 必须挂在它下面。
	parentCtx, parent := tp.Tracer("test").Start(context.Background(), "service.caller.Do")

	cli, err := usercli.NewClient("User", BuildClientOptions(ClientOptions{
		ServiceName:  "caller",
		Target:       "127.0.0.1:1", // 必然连不上
		ConnType:     "short",
		TraceEnabled: true,
		RPCTimeout:   500 * time.Millisecond,
	})...)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, 2*time.Second)
	defer cancel()
	_, _ = cli.GetProfile(ctx, &userpb.GetProfileReq{UserId: 1})
	parent.End()

	spans := exp.GetSpans()
	var clientSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].SpanKind == oteltrace.SpanKindClient {
			clientSpan = &spans[i]
			break
		}
	}
	if clientSpan == nil {
		t.Fatalf("no client span exported; got %d spans: %v",
			len(spans), spanNames(spans))
	}
	if got := clientSpan.Name; got != "client.User.GetProfile" {
		t.Errorf("client span name = %q, want %q", got, "client.User.GetProfile")
	}
	if clientSpan.Parent.SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("client span parent = %s, want caller span %s",
			clientSpan.Parent.SpanID(), parent.SpanContext().SpanID())
	}
	if !clientSpan.EndTime.After(clientSpan.StartTime) {
		t.Errorf("client span not ended: start=%v end=%v", clientSpan.StartTime, clientSpan.EndTime)
	}
}

// TestClientSpanDisabled 开关关闭时不产出任何 span（零开销路径）。
func TestClientSpanDisabled(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exp)))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	cli, err := usercli.NewClient("User", buildNoTraceOptions()...)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = cli.GetProfile(ctx, &userpb.GetProfileReq{UserId: 1})

	if n := len(exp.GetSpans()); n != 0 {
		t.Errorf("TraceEnabled=false produced %d spans, want 0: %v", n, spanNames(exp.GetSpans()))
	}
}

// buildNoTraceOptions 关掉链路的客户端选项（短超时，避免测试卡住）。
func buildNoTraceOptions() []client.Option {
	return BuildClientOptions(ClientOptions{
		ServiceName: "caller",
		Target:      "127.0.0.1:1",
		ConnType:    "short",
		RPCTimeout:  300 * time.Millisecond,
	})
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}
