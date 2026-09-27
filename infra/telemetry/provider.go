// Package telemetry 统一装配调用链，并提供「分层埋点」助手（移植自 monolith
// internal/telemetry，ServiceName 参数化，供 11 个微服务各自传入）。
//
// 装配总公式（plan §0.1 第 2 步）：
//  1. 调用方先 hubx/configx 加载本地 config.yaml（其 log.enable_trace = hubx trace 段总闸），
//     再 rpcx/configx.InitGlobal（注意：必须 InitGlobal 而非 Load/MustLoad——
//     rpcx/tracing.Init 从全局 viper 读 log.enable_trace，只有 InitGlobal 会存全局句柄）；
//  2. telemetry.Init 委托 rpcx/tracing.Init 安装全局 TracerProvider +
//     TraceContext/Baggage 复合传播器，并把 logx 日志的 trace 提取器对齐；
//  3. 总闸（Options.Enabled）关闭时：返回 noop provider，显式 logx.DisableTrace()，
//     DB/缓存/MQ 回调降级 no-op（"总闸比中间件宽"的落点）。
//
// Init 必须先于 dbx/cachex/mqx 装配：它们从 OTel 全局 TracerProvider 取 tracer，
// 先装配才能让后续组件的 span 落到同一条链上。
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	rpcxlogx "github.com/kitexx/kitex-framework/logx"
	rpcxtracing "github.com/kitexx/kitex-framework/tracing"
)

// tracerName 生成 hubx 服务的 tracer 名（前缀与 monolith 的 "hubx-monolith" 对齐）
func tracerName(service string) string {
	if service == "" {
		return "hubx.app"
	}
	return "hubx." + service
}

// current 全局 tracer 句柄。初值取全局 delegate（otel global 在 provider 注册前后均可用）；
// Init 后按开关指向真实 SDK provider 或显式 noop provider。
var current trace.Tracer = otel.GetTracerProvider().Tracer("hubx.app")

// Options 调用链初始化选项
type Options struct {
	// ServiceName 服务名（rpcx 服务端/客户端 ServiceName 保持一致，11 服务各自传入）
	ServiceName string
	// Endpoint OTLP gRPC 上报地址（jaeger:4317 / localhost:4317）；
	// 为空则不挂 exporter（span 仅本地记录，不导出）
	Endpoint string
	// Insecure 非加密 OTLP 连接
	Insecure bool
	// Enabled 是 hubx trace 段总闸（调用方传 pkg/config 的 Trace.Enabled 或本地
	// config.yaml 的 log.enable_trace）。关闭时全 noop + 关 logx trace。
	Enabled bool
}

// Setup 调用链装配结果
type Setup struct {
	// Enabled 记录实际生效状态（总闸开但 rpcx 侧 log.enable_trace 关 → 降级 noop，此时为 false）
	Enabled bool
	// Tracer 供需要手动建 span 的场景（一般直接用 Start）
	Tracer trace.Tracer
	// shutdown 进程退出前调用（OTel flush；BatchSpanProcessor 未 flush 会丢链路，
	// 优雅退出反序时给 ≥10s）
	shutdown func(context.Context) error
}

// Init 装配调用链。见包注释的装配总公式。
func Init(ctx context.Context, opts Options) (*Setup, error) {
	name := tracerName(opts.ServiceName)

	if !opts.Enabled {
		rpcxlogx.DisableTrace()
		// 显式 noop provider：不依赖 otel global（global 可能已被先前 enabled 的 Init 污染）
		tr := trace.NewNoopTracerProvider().Tracer(name)
		current = tr
		return &Setup{Enabled: false, Tracer: tr, shutdown: noopShutdown}, nil
	}

	shutdown, err := rpcxtracing.Init(rpcxtracing.Options{
		ServiceName: opts.ServiceName,
		Endpoint:    opts.Endpoint,
		Insecure:    opts.Insecure,
	})
	if err != nil {
		return nil, fmt.Errorf("telemetry: tracing init: %w", err)
	}

	// 一致性兜底：rpcx 侧读的是 log.enable_trace；若总闸开但本地 yaml 没同步
	// （log.enable_trace=false），tracing.Init 会走 noop 分支并 DisableTrace，
	// 此时按关闭处理，避免 Setup.Enabled 与实际行为不一致。
	if !rpcxlogx.TraceEnabled() {
		rpcxlogx.DisableTrace()
		tr := trace.NewNoopTracerProvider().Tracer(name)
		current = tr
		return &Setup{Enabled: false, Tracer: tr, shutdown: noopShutdown}, nil
	}

	tr := otel.GetTracerProvider().Tracer(name)
	current = tr
	return &Setup{Enabled: true, Tracer: tr, shutdown: shutdown}, nil
}

// Shutdown 刷出队列中的 span（OTLP BatchSpanProcessor 默认 5s 批量，
// 优雅退出反序调用方须给 ≥10s 超时）
func (s *Setup) Shutdown(ctx context.Context) error {
	if s == nil || s.shutdown == nil {
		return nil
	}
	return s.shutdown(ctx)
}

func noopShutdown(context.Context) error { return nil }
