// Package rpcx 收敛 kitex 服务端/客户端 Option 的装配。
//
// 背景：此前 11 个服务的 main.go 依赖外部模块 github.com/kitexx/kitex-framework
// 的 rpcx.BuildServerOptions / rpcx.BuildClientOptions。该模块位于 ../rpcx
// 本地工作树，已被工作区的「静默删除」现象清除且全仓无副本，导致整个
// micro_apps 模块无法编译。
//
// 本包把这两个构造器内联到仓内：依赖面只有 kitex 官方 API +
// kitex-contrib/obs-opentelemetry 的 tracing suite，两者都已在 go.mod 里
// 显式 require，不再有外部不可控的隐式依赖。
//
// 调用链：套件挂上 obs-opentelemetry 的 Server/Client Suite 后，kitex 的
// 入站/出站调用会经 propatagor 注入与提取 traceparent —— 这是跨服务续链的
// 落点。全局 TracerProvider 必须**先**由 infra/telemetry.Init 装配好，
// 本套件只是从 otel.GetTracerProvider() 取用（见 Options.TraceEnabled 注释）。
package rpcx

import (
	"strings"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/server"
	kitextracing "github.com/kitex-contrib/obs-opentelemetry/tracing"
)

// 缺省超时。不填 ClientOptions/ServerOptions 的时长字段时用这些值，
// 避免出现「零值 = 立即超时」或「零值 = 无限等待」两种都不期望的行为。
const (
	// defaultRPCTimeout 单 RPC 调用超时（含重试）
	defaultRPCTimeout = 3 * time.Second
	// defaultConnectTimeout 建连超时
	defaultConnectTimeout = 500 * time.Millisecond
	// defaultReadWriteTimeout 服务端读写超时
	defaultReadWriteTimeout = 30 * time.Second
	// defaultExitWait 优雅退出时等待 in-flight 请求排空的上限
	defaultExitWait = 5 * time.Second
)

// ServerOptions kitex 服务端装配参数
type ServerOptions struct {
	// ServiceName 服务名（与注册表 KV hubx/svc/<ServiceName> 对齐）
	ServiceName string
	// Addr 监听地址，形如 "127.0.0.1:9013"
	Addr string
	// TraceEnabled 是否挂 obs-opentelemetry 服务端套件（跨服务续链）。
	// 关闭时不装任何中间件与 meta handler：非 recording span 的开销比
	// 中间件本身还小，没必要为一个 noop 链路付出 fixed cost。
	TraceEnabled bool
	// ExitWait 优雅退出排空等待时长，0 用 defaultExitWait
	ExitWait time.Duration
	// ReadWriteTimeout 连接读写超时，0 用 defaultReadWriteTimeout
	ReadWriteTimeout time.Duration
}

// BuildServerOptions 生成 kitex server.NewServer 的选项列表。
//
// 注意顺序：service name 之后必须先带 ServiceAddr，再叠加 tracing suite ——
// suite 内部也注册 option，后注册的 WithTransportProtocol 等会覆盖同位配置，
// 把地址放在前面可以保证它不会被 suite 的传输设置冲掉。
func BuildServerOptions(o ServerOptions) []server.Option {
	exitWait := o.ExitWait
	if exitWait <= 0 {
		exitWait = defaultExitWait
	}
	rwTimeout := o.ReadWriteTimeout
	if rwTimeout <= 0 {
		rwTimeout = defaultReadWriteTimeout
	}

	opts := []server.Option{
		server.WithServiceAddr(utils.NewNetAddr("tcp", o.Addr)),
		server.WithExitWaitTime(exitWait),
		server.WithReadWriteTimeout(rwTimeout),
	}
	if o.TraceEnabled {
		opts = append(opts, server.WithSuite(kitextracing.NewServerSuite()))
	}
	return opts
}

// ClientOptions kitex 客户端装配参数
type ClientOptions struct {
	// ServiceName 调用方自身的服务名（用于 span 的 source peer 属性）
	ServiceName string
	// Target 目标地址，逗号分隔的多实例 "host:port,host:port"。
	// 由 infra/consul.Target 读出并拼接。
	Target string
	// ConnType 连接形态："short" 短连接 / "long" 长连接（默认长连接）。
	// 演示环境实例少且 Leader 会漂移，默认短连接更稳：连接断了下一轮新建。
	ConnType string
	// TraceEnabled 同 ServerOptions.TraceEnabled
	TraceEnabled bool
	// RPCTimeout 单调用超时，0 用 defaultRPCTimeout
	RPCTimeout time.Duration
	// ConnectTimeout 建连超时，0 用 defaultConnectTimeout
	ConnectTimeout time.Duration
}

// BuildClientOptions 生成 kitex client.NewClient 的选项列表。
//
// Target 为空时不产出 WithHostPorts：kitex 会回退到 DestService 的默认解析，
// 比给出一个空列表报错更易于排查「哪个环节没传目标」。
func BuildClientOptions(o ClientOptions) []client.Option {
	rpcTimeout := o.RPCTimeout
	if rpcTimeout <= 0 {
		rpcTimeout = defaultRPCTimeout
	}
	connTimeout := o.ConnectTimeout
	if connTimeout <= 0 {
		connTimeout = defaultConnectTimeout
	}

	opts := []client.Option{
		client.WithRPCTimeout(rpcTimeout),
		client.WithConnectTimeout(connTimeout),
	}
	if o.TraceEnabled {
		opts = append(opts, client.WithSuite(kitextracing.NewClientSuite()))
	}
	if addrs := splitTarget(o.Target); len(addrs) > 0 {
		opts = append(opts, client.WithHostPorts(addrs...))
	}
	// 短连接才需要显式叠加（kitex 默认即长连接 + 默认 IdleConfig），
	// 因此非 short 分支刻意不加任何选项，避免硬编码一份 IdleConfig 后
	// 与框架默认值漂移。
	if strings.EqualFold(strings.TrimSpace(o.ConnType), "short") {
		opts = append(opts, client.WithShortConnection())
	}
	return opts
}

// splitTarget 拆分逗号分隔的目标串，丢弃空片段。
//
// consul.Target 返回 joinComma(addrs)，ad-hoc 调用可能传入带空格的
// "a:1, b:2"，逐段 TrimSpace 比整体 strings.Split 更健壮。
func splitTarget(target string) []string {
	if strings.TrimSpace(target) == "" {
		return nil
	}
	parts := strings.Split(target, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
