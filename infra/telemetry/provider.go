// Package telemetry 统一装配调用链，并提供「分层埋点」助手（移植自 monolith
// internal/telemetry，ServiceName 参数化，供 11 个微服务各自传入）。
//
// 装配总公式（plan §0.1 第 2 步）：
//  1. 调用方先 hubx/configx 加载本地 config.yaml（其 trace.enabled = 本服务调用链总闸）；
//  2. telemetry.Init 经 hubx/otelx 装配全局 TracerProvider（采样器 + OTLP 导出器），
//     安装 Composite(TraceContext, Baggage) 传播器，并把开关接入 hubx/traceflag；
//  3. 总闸（Options.Enabled）关闭时：返回 noop provider，并 traceflag.SetEnabled(false)
//     熔断全局，DB/缓存/MQ 回调随之降级为 no-op（"总闸比中间件宽"的落点）。
//
// Init 必须先于 dbx/cachex/mqx 装配：它们从 OTel 全局 TracerProvider 取 tracer，
// 先装配才能让后续组件的 span 落到同一条链上。
//
// 依赖变更记录：本包此前依赖外部模块 github.com/kitexx/kitex-framework 的
// tracing / logx 两个子包（本地 ../rpcx 工作树，已丢失且全仓无副本）。现改由
// hubx 已有的 otelx（装配）+ traceflag（开关）承担，两者均在 go.mod 中显式
// replace 到 ../otelx 与 ../traceflag，行为一一对应：
//
//	rpcxtracing.Init(Options{ServiceName, Endpoint, Insecure})  →  otelx.New(*otelx.Config)
//	rpcxlogx.TraceEnabled()                                    →  traceflag.EnabledFor("otelx")
//	rpcxlogx.DisableTrace()                                    →  traceflag.SetEnabled(false)
package telemetry

import (
	"context"
	"fmt"
	"strings"

	"github.com/gospacex/otelx"
	"github.com/gospacex/traceflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
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
	// Endpoint OTLP 上报地址（jaeger / collector）。
	// 为空则不挂 exporter（span 仅本地记录，不导出）。
	//
	// 注意：hubx/otelx 的 OTLP 导出器走 HTTP（collector 的 4318 端口），
	// 与旧 rpcx/tracing 的 gRPC（4317）通道不同。Jaeger 自 1.35 起两个
	// 端口都原生接收 OTLP，切换不需要改 collector 配置。
	Endpoint string
	// Insecure 是否以明文上报，决定是否给 Endpoint 加 scheme：
	// true → http://，false → https://。缺省（零值 false）会走 TLS。
	Insecure bool
	// Enabled 是本服务调用链总闸（调用方传 pkg/config 的 Trace.Enabled）。
	// 关闭时全 noop + 熔断 traceflag 全局开关。
	Enabled bool
	// SampleRatio 采样率 0~1（0 = 不采样，1 = 全采；trace 段 sample_ratio 供给）。
	// 默认 0 视为 1（缺省全采，与既有 always_on 行为兼容）。
	SampleRatio float64
	// Exporter 导出类型：otlp / stdout / none（trace 段 exporter 供给；空则按
	// Endpoint 是否为空裁决 otlp/none，保持旧行为）。
	Exporter string
}

// Setup 调用链装配结果
type Setup struct {
	// Enabled 记录实际生效状态（总闸开但 traceflag 全局已熔断 → 降级 noop，此时为 false）
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

	// 传播器必须显式安装且早于任何 provider 装配：OTel 默认的传播器是 noop
	//（既不注入也不提取），不装则跨服务的 traceparent 传不出去，网关诞生的根
	// span 到下游就断链。放在两个分支之前，保证关闭态也不会漏。
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if !opts.Enabled {
		// 熔断 hubx 统一调用链总闸：dbx / cachex / mqx / webx 都已接入 traceflag，
		// 这里一刀切后它们的埋点全部降级 no-op（"总闸比中间件宽"的落点）。
		traceflag.SetEnabled(false)
		// 显式 noop provider：不依赖 otel global（global 可能已被先前 enabled 的 Init 污染）
		tr := noop.NewTracerProvider().Tracer(name)
		current = tr
		return &Setup{Enabled: false, Tracer: tr, shutdown: noopShutdown}, nil
	}

	// 注意：这里**不**做 traceflag.SetEnabled(true)。
	//
	// 总闸的语义是"宿主应急熔断"，本 Init 只被允许把它往下压，不能往上抬——
	// 否则宿主先 SetEnabled(false) 一刀切之后，任何一个服务调到 Init
	// 就会把熔断悄悄解开，配置项与最终行为再次错位。
	// 若总闸此刻确实处于熔断态，下面的 otelx.New 会经内部的
	// traceflag.Apply 收敛为 noop，由 EnabledFor 分支统一降级。

	exported := strings.ToLower(strings.TrimSpace(opts.Exporter))
	if exported == "" {
		exported = "none"
		if opts.Endpoint != "" {
			exported = "otlp"
		}
	}
	ratio := opts.SampleRatio
	if ratio <= 0 {
		ratio = 1
	}
	sampler := otelx.SamplerConfig{Type: "always_on"}
	if ratio < 1 {
		sampler = otelx.SamplerConfig{Type: "traceidratio", Ratio: ratio}
	}
	p, err := otelx.New(&otelx.Config{
		ServiceName: opts.ServiceName,
		Tracer: otelx.TracerConfig{
			EnableTrace: true,
			Exported:    exported,
			Endpoint:    endpointWithScheme(opts.Endpoint, opts.Insecure),
		},
		Sampler: sampler,
		// Meter 不启用：本套 demo 不暴露 Prometheus 指标，让其统一走 noop，
		// 避免出现「配了 MetricsEnabled 却拿不到 meter」的半成品状态。
		Meter: otelx.MeterConfig{Enabled: false},
	})
	if err != nil {
		return nil, fmt.Errorf("telemetry: tracing init: %w", err)
	}

	// 一致性兜底：otelx.New 内部会 traceflag.Apply("otelx", true) 收敛总闸，
	// 若进程内另有调用方在此期间作了全局熔断，Apply 会返回 false 并给出 noop
	// provider。此处对齐 Setup.Enabled 与实际行为，避免返回值撒谎。
	if !traceflag.EnabledFor("otelx") {
		traceflag.SetEnabled(false)
		_ = p.Close()
		tr := noop.NewTracerProvider().Tracer(name)
		current = tr
		return &Setup{Enabled: false, Tracer: tr, shutdown: noopShutdown}, nil
	}

	// provider.Close 签名是 func() error（otelx 内部固定 5s 超时），
	// 这里包一层转成 Setup.shutdown 的形态；入参 ctx 只用于提前判定已取消。
	tr := p.TracerProvider().Tracer(name)
	current = tr
	return &Setup{Enabled: true, Tracer: tr, shutdown: providerShutdown(p)}, nil
}

// endpointWithScheme 把 Insecure 映射成 OTLP endpoint 的 scheme。
//
// otelx 允许裸写 host:port（无 scheme 时按明文处理），这里显式带上是为了让
// 配置项与行为严格对应 —— 否则 "Insecure: false" 的调用也会走明文，属于静默失效。
// Endpoint 为空时返回空串（otelx 侧 exported=none，不读 endpoint）。
func endpointWithScheme(endpoint string, insecure bool) string {
	if endpoint == "" {
		return ""
	}
	if insecure {
		return "http://" + endpoint
	}
	return "https://" + endpoint
}

// Shutdown 刷出队列中的 span（OTLP BatchSpanProcessor 默认 5s 批量，
// 优雅退出反序调用方须给 ≥10s 超时）
//
// 已知限制：otelx.Provider.Close 内部固定 5s 超时且不读入参 ctx，
// 因此本方法的实际执行上限取「调用方 ctx 剩余时间」与「otelx 内部 5s」的较小者。
// 需要更长排空窗口时应调整 otelx 的 shutdownTimeout，而非延长这里的 ctx。
func (s *Setup) Shutdown(ctx context.Context) error {
	if s == nil || s.shutdown == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.shutdown(ctx)
}

func noopShutdown(context.Context) error { return nil }

// providerShutdown 适配 otelx.Provider.Close 到 Setup.shutdown 的签名。
//
// 外层已判定过 ctx.Err()，这里只负责转发，不额外追加 goroutine ——
// Close 自身幂等且带内部超时，包一层等待 select 反而会在超时后留下悬挂协程。
func providerShutdown(p otelx.Provider) func(context.Context) error {
	return func(context.Context) error { return p.Close() }
}
