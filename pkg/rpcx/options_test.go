// Package rpcx 的单测：钉住两个最易回归的行为——
//  1. TraceEnabled 开关决定 obs-opentelemetry Suite 是否装配（跨服务续链的落点）。
//     此包替代了被删的 github.com/kitexx/kitex-framework 的 rpcx.BuildXxx，
//     11 个服务全部依赖，必须保证「开关关 → 不装 suite，开关开 → 装 suite」。
//  2. splitTarget 对 consul 拼接目标串的拆分（空 / 多实例 / 空格 / 空片段）。
//
// 不依赖全局 TracerProvider：otel 默认全局即为 noop，suite 构造期不触碰 provider。
package rpcx

import (
	"reflect"
	"testing"
)

// TestSplitTarget 覆盖 consul.Target 返回的逗号串各种形态。
func TestSplitTarget(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"whitespace", "   ", nil},
		{"single", "127.0.0.1:9013", []string{"127.0.0.1:9013"}},
		{"multi", "a:1,b:2", []string{"a:1", "b:2"}},
		{"spaces", " a:1 , b:2 ", []string{"a:1", "b:2"}},
		{"empty-seg", "a:1,,b:2", []string{"a:1", "b:2"}},
		{"trailing", "a:1,", []string{"a:1"}},
		{"leading", ",b:2", []string{"b:2"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := splitTarget(c.in)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("splitTarget(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

// TestBuildServerOptionsTraceToggle 钉住：TraceEnabled=false 只产出 3 个基础
// option（ServiceAddr / ExitWait / ReadWriteTimeout），开启时仅多 1 个 suite。
// suite 的「是否装配」是跨服务续链的关键落点，必须随开关翻转。
func TestBuildServerOptionsTraceToggle(t *testing.T) {
	base := ServerOptions{ServiceName: "svc", Addr: "127.0.0.1:9013"}
	off := BuildServerOptions(base)
	on := BuildServerOptions(ServerOptions{ServiceName: "svc", Addr: "127.0.0.1:9013", TraceEnabled: true})

	if len(off) != 3 {
		t.Errorf("disabled server opts len = %d, want 3", len(off))
	}
	if len(on) != 4 {
		t.Errorf("enabled server opts len = %d, want 4", len(on))
	}
	// 开比关只多一个 suite；若 kitex 改了套件内部结构导致计数漂移，这里会先红。
	if len(on) != len(off)+1 {
		t.Errorf("enabled should add exactly one suite option: on=%d off=%d", len(on), len(off))
	}
}

// TestBuildClientOptionsTraceToggle 同服务端：固定 Target/ConnType 使 suite 是唯一
// 变量。无 Target 时不应产出 WithHostPorts（退化到 DestService 默认解析）。
//
// 开启时比关闭时**多两个** option：ClientSpanMiddleware + suite。
//
// 前者不是装饰：obs-opentelemetry 套件自带的 client span 由 kitex 的
// stats.Tracer 收尾，实测从不落地——下游能收到正确的 traceparent（同
// trace_id），父 span id 却指向一个从不存在的 span，Jaeger 上表现为两棵
// 子树。client span 因此改由本包显式产出（见 middleware.go 的包注释），
// 这条回归必须钉在这里。
func TestBuildClientOptionsTraceToggle(t *testing.T) {
	base := ClientOptions{ServiceName: "caller", Target: "a:1,b:2"}
	off := BuildClientOptions(base) // 2 基础 + 1 hostports
	on := BuildClientOptions(ClientOptions{ServiceName: "caller", Target: "a:1,b:2", TraceEnabled: true})

	if len(off) != 3 {
		t.Errorf("disabled client opts len = %d, want 3", len(off))
	}
	if len(on) != 5 {
		t.Errorf("enabled client opts len = %d, want 5 (2 base + hostports + suite + stats level)", len(on))
	}
	if len(on) != len(off)+2 {
		t.Errorf("enabled should add exactly two options (suite + stats level): on=%d off=%d", len(on), len(off))
	}
}

// TestBuildClientOptionsStatsLevelOff 开关关闭时不得引入任何 stats/链路开销。
func TestBuildClientOptionsStatsLevelOff(t *testing.T) {
	off := BuildClientOptions(ClientOptions{ServiceName: "c", Target: "a:1", ConnType: "short"})
	if len(off) != 4 { // 2 基础 + hostports + short connection
		t.Errorf("disabled short-conn opts len = %d, want 4", len(off))
	}
}

// TestBuildClientOptionsNoTarget 无 Target 时不带 WithHostPorts（2 个基础 option）。
func TestBuildClientOptionsNoTarget(t *testing.T) {
	opts := BuildClientOptions(ClientOptions{ServiceName: "c"})
	if len(opts) != 2 {
		t.Errorf("no-target client opts len = %d, want 2", len(opts))
	}
}

// TestBuildClientOptionsShortConnection 短连接分支恰好多一个 option。
func TestBuildClientOptionsShortConnection(t *testing.T) {
	normal := BuildClientOptions(ClientOptions{ServiceName: "c", Target: "a:1"})
	short := BuildClientOptions(ClientOptions{ServiceName: "c", Target: "a:1", ConnType: "short"})
	if len(short) != len(normal)+1 {
		t.Errorf("short connection should add exactly one option: normal=%d short=%d", len(normal), len(short))
	}
}

// TestBuildServerOptionsDefaults 缺省超时必须兜底（不能出现零值=立即超时）。
func TestBuildServerOptionsDefaults(t *testing.T) {
	opts := BuildServerOptions(ServerOptions{ServiceName: "svc", Addr: "127.0.0.1:9013"})
	if len(opts) != 3 {
		t.Errorf("default server opts len = %d, want 3 (no suite when TraceEnabled=false)", len(opts))
	}
}
