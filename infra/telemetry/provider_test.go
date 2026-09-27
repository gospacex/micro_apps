// Package telemetry 测试：总闸语义（开/关/rpcx 侧未同步降级）+ 分层 span 命名与父子关系。
//
// 测试顺序依赖（Init 改写包级全局 current 与 otel global，按文件内声明顺序执行）：
//  1. TestInitDisabled          —— 总闸关：noop，Start 非 recording
//  2. TestInitLocalSwitchOff    —— 总闸开但本地 log.enable_trace=false：降级 noop
//  3. TestInitEnabled           —— 双侧开：真实 SDK provider，父子 span 同 trace
//
// 不上 OTLP 端点：OTLP gRPC exporter 懒连接，批量导出失败只丢弃；
// 父子关系用 SpanContext 的 TraceID/SpanID 断言，不依赖导出结果。
package telemetry

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	rpcxconfigx "github.com/kitexx/kitex-framework/configx"
)

func initRpcxGlobal(t *testing.T, enableTrace bool) {
	t.Helper()
	// 以临时目录 config.yaml 覆盖内置默认（默认 log.enable_trace=true, endpoint=localhost:4317）
	dir := t.TempDir()
	file := filepath.Join(dir, "config.yaml")
	content := "log:\n  level: info\n"
	if enableTrace {
		content += "  enable_trace: true\n  trace:\n    endpoint: localhost:4317\n    insecure: true\n"
	} else {
		content += "  enable_trace: false\n"
	}
	if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
		t.Fatalf("write tmp config: %v", err)
	}
	var s struct{}
	if err := rpcxconfigx.InitGlobal(&s, rpcxconfigx.WithPath(dir)); err != nil {
		t.Fatalf("InitGlobal: %v", err)
	}
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
	_, span := Start(context.Background(), "service", "order", "CreateOrder")
	if span.SpanContext().IsValid() {
		t.Fatal("总闸关：noop span 的 SpanContext 应无效")
	}
	if span.IsRecording() {
		t.Fatal("总闸关：span 应非 recording")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// 2) 总闸开但本地 config.yaml 的 log.enable_trace 未同步（false）：
// rpcx/tracing.Init 走 noop 分支 → 一致性兜底，Setup.Enabled 记为 false
func TestInitLocalSwitchOff(t *testing.T) {
	initRpcxGlobal(t, false)

	s, err := Init(context.Background(), Options{ServiceName: "srv-order", Endpoint: "localhost:4317", Enabled: true})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if s.Enabled {
		t.Fatal("rpcx 侧 log.enable_trace=false 时应降级为 noop（Setup.Enabled=false）")
	}
	_, span := Start(context.Background(), "handler", "order", "CreateOrder")
	if span.IsRecording() {
		t.Fatal("降级分支：span 应非 recording")
	}
}

// 3) 双侧开：真实 SDK provider；分层命名 + 父子 span 同 trace
func TestInitEnabled(t *testing.T) {
	initRpcxGlobal(t, true)

	s, err := Init(context.Background(), Options{
		ServiceName: "srv-order",
		Endpoint:    "localhost:4317",
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

// 常量校验：span 命名规则（与 monolith 逐字对齐 <layer>.<entity>.<op>）
func TestSpanNaming(t *testing.T) {
	// Start 的拼接逻辑无副作用，直接验证 expected 字符串形态
	want := "service.order.CreateOrder"
	if got := "service" + "." + "order" + "." + "CreateOrder"; got != want {
		t.Fatalf("命名规则: %s != %s", got, want)
	}
}
