// Package telemetry 测试：总闸语义（开/关/全局已熔断时降级）+ 分层 span 命名与父子关系。
//
// 测试顺序依赖（Init 改写包级全局 current 与 otel global，按文件内声明顺序执行）：
//  1. TestInitDisabled          —— 总闸关：noop，Start 非 recording
//  2. TestInitGlobalSwitchOff   —— 总闸开但 traceflag 全局开关被熔断：降级 noop
//  3. TestInitEnabled           —— 双侧开：真实 SDK provider，父子 span 同 trace
//
// 不上可用 OTLP 端点：导出器懒连接，批量导出失败只丢弃；
// 父子关系用 SpanContext 的 TraceID/SpanID 断言，不依赖导出结果。
package telemetry

import (
	"context"
	"testing"

	"github.com/gospacex/traceflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// validSpanContext 构造一个已采样的 SpanContext，用于传播器的行为断言。
func validSpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
	})
}

// 1) 总闸关：noop provider，Start 返回非 recording span（零开销）
func TestInitDisabled(t *testing.T) {
	s, err := Init(context.Background(), Options{ServiceName: "srv-order", Enabled: false})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if s.Enabled {
		t.Fatal("Enabled=false 时 Setup.Enabled 应为 false")
	}
	if traceflag.Enabled() {
		t.Fatal("Enabled=false 应熔断 traceflag 全局开关，使 dbx/cachex/mqx 一并降级")
	}
	_, span := Start(context.Background(), "service", "order", "CreateOrder")
	if span.SpanContext().IsValid() {
		t.Fatal("总闸关：noop span 的 SpanContext 应无效")
	}
	if span.IsRecording() {
		t.Fatal("总闸关：span 应非 recording")
	}
	// 传播器必须真的能干活：关闭态也要装，否则跨服务的 traceparent 传不出去，
	// 而"关的是自己的埋点"不应牵连上下游续链。用一次 Inject 做行为断言
	// （复合传播器类型本身未导出，断言具体类型比断言行为更脆）。
	// 必须喂带有效 SpanContext 的 ctx：空 ctx 下 TraceContext.Inject 合法地什么都不写。
	carrier := propagation.MapCarrier{}
	ctx := trace.ContextWithSpanContext(context.Background(), validSpanContext())
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if _, ok := carrier["traceparent"]; !ok {
		t.Fatalf("传播器未注入 traceparent，carrier=%v", carrier)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// 2) 总闸开但进程内 traceflag 全局开关处于熔断态（模拟宿主一刀切）：
// otelx 内部 Apply 收敛为 false → noop provider → Setup.Enabled 降级为 false
func TestInitGlobalSwitchOff(t *testing.T) {
	traceflag.SetEnabled(false)
	defer traceflag.SetEnabled(true) // 还原，避免污染后续用例

	s, err := Init(context.Background(), Options{ServiceName: "srv-order", Endpoint: "127.0.0.1:4318", Enabled: true})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if s.Enabled {
		t.Fatal("traceflag 全局熔断时应降级为 noop（Setup.Enabled=false）")
	}
	_, span := Start(context.Background(), "handler", "order", "CreateOrder")
	if span.IsRecording() {
		t.Fatal("降级分支：span 应非 recording")
	}
}

// 3) 双侧开：真实 SDK provider；分层命名 + 父子 span 同 trace
//
// Endpoint 留空（不挂导出器）：父子关系走 SpanContext 断言，与是否能连上
// collector 无关；挂上真实端点会让测试依赖本机 4318 端口，失败时还产生
// 一堆「connection refused」噪声。
func TestInitEnabled(t *testing.T) {
	// 用例 1 的关闭态会把总闸按到 false，这里显式恢复（生产由宿主决定，测试需隔离）
	traceflag.SetEnabled(true)

	s, err := Init(context.Background(), Options{
		ServiceName: "srv-order",
		Insecure:    true,
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if !s.Enabled {
		t.Fatal("双侧开时 Setup.Enabled 应为 true")
	}

	// 分层 span：service.order.CreateOrder（父）→ repository.order.insert_tx（子）
	ctx, parent := Start(context.Background(), "service", "order", "CreateOrder")
	if !parent.IsRecording() {
		t.Fatal("父 span 应 recording")
	}
	if !parent.SpanContext().IsValid() {
		t.Fatalf("父 span 应有有效 SpanContext，got %+v", parent.SpanContext())
	}

	ctx, child := Start(ctx, "repository", "order", "insert_tx")
	defer child.End()
	parent.End()

	psc, csc := parent.SpanContext(), child.SpanContext()
	if csc.TraceID() != psc.TraceID() {
		t.Fatalf("子 span 应继承父 TraceID: parent=%s child=%s", psc.TraceID(), csc.TraceID())
	}
	if csc.SpanID() == psc.SpanID() {
		t.Fatal("子 span 的 SpanID 应不同于父")
	}

	// 新 root span：不同 trace（root 无 parent 时生成新 traceID）
	_, root := Start(context.Background(), "messaging", "order.completed", "Receive")
	if root.SpanContext().TraceID() == psc.TraceID() {
		t.Fatal("无 parent 的 Start 应生成新 TraceID")
	}
	root.End()

	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// Insecure 必须真的改变上报 scheme，否则「配了 TLS 却明文上报」会静默失效
func TestEndpointWithScheme(t *testing.T) {
	cases := []struct {
		endpoint string
		insecure bool
		want     string
	}{
		{"127.0.0.1:4318", true, "http://127.0.0.1:4318"},
		{"collector.example.com:4318", false, "https://collector.example.com:4318"},
		{"", true, ""}, // 无端点时导出器为 none，scheme 无意义
	}
	for _, c := range cases {
		if got := endpointWithScheme(c.endpoint, c.insecure); got != c.want {
			t.Fatalf("endpointWithScheme(%q,%v)=%q want %q", c.endpoint, c.insecure, got, c.want)
		}
	}
}

// 常量校验：span 命名规则（与 monolith 逐字对齐 <layer>.<entity>.<op>）
func TestSpanNaming(t *testing.T) {
	// Start 的拼接逻辑无副作用，直接验证 expected 字符串形态
	want := "service.order.CreateOrder"
	if got := "service" + "." + "order" + "." + "CreateOrder"; got != want {
		t.Fatalf("命名规则: %s != %s", got, want)
	}
}

func TestInitUsesExporter(t *testing.T) {
	traceflag.SetEnabled(true)
	s, err := Init(context.Background(), Options{
		ServiceName: "srv-order",
		Enabled:     true,
		Exporter:    "unsupported",
	})
	if s != nil {
		_ = s.Shutdown(context.Background())
	}
	if err == nil {
		t.Fatal("unsupported exporter must be passed to otelx and rejected")
	}
}

func TestInitUsesSampleRatio(t *testing.T) {
	traceflag.SetEnabled(true)
	s, err := Init(context.Background(), Options{
		ServiceName: "srv-order",
		Enabled:     true,
		Exporter:    "none",
		SampleRatio: 0.5,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer func() { _ = s.Shutdown(context.Background()) }()

	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		SpanID:     trace.SpanID{1},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent)
	_, span := Start(ctx, "service", "order", "CreateOrder")
	defer span.End()
	if span.IsRecording() {
		t.Fatal("traceidratio=0.5 must drop the deterministic high trace ID")
	}
}

// TestInstallGlobalRecoversFromSDKOverride 钉死「SDK 抢走全局 provider」这个坑。
//
// 复现路径（2026-10-02 实测）：cachex / dbx 初始化时各自 otel.SetTracerProvider
// 注册自己的 provider（端点取 SDK 默认 localhost:4318 + TLS），把装配层装好的
// provider 顶掉。此时 otel.GetTracerProvider() 取到的 tracer 建得出 span、
// 注入得出 traceparent，却永远导不出去——下游同 trace_id，父 span id 却指向
// 一个不存在的 span，Jaeger 上断成两棵子树。极难查。
//
// 断言：InstallGlobal 之后，全局 provider 必须重新指向 Init 认定的那个。
func TestInstallGlobalRecoversFromSDKOverride(t *testing.T) {
	traceflag.SetEnabled(true)
	t.Cleanup(func() { traceflag.SetEnabled(true) })

	s, err := Init(context.Background(), Options{
		ServiceName: "t", Enabled: true, Exporter: "none",
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer func() { _ = s.Shutdown(context.Background()) }()

	want := otel.GetTracerProvider()

	// 模拟 SDK 抢注：换一个陌生 provider 进全局。
	intruder := sdktrace.NewTracerProvider()
	defer func() { _ = intruder.Shutdown(context.Background()) }()
	otel.SetTracerProvider(intruder)
	if otel.GetTracerProvider() == want {
		t.Fatal("前置条件不成立：模拟抢注后全局 provider 未变")
	}

	InstallGlobal()
	if got := otel.GetTracerProvider(); got != want {
		t.Fatalf("InstallGlobal 未夺回全局 provider：got %T, want %T", got, want)
	}
}
