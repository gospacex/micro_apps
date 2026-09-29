// gateway：HTTP 入口网关（webx 监听 :8080，配置驱动路由表 HTTP 转发至 BFF；
// 无 DB、无 MQ，只挂限流计数器缓存）。
//
// 装配总公式（6 步，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.Load(ctx, "config.yaml", "hubx.gateway", "HUBX")：
//     本地 config.yaml 兜底 + 远程 Nacos key 级裁决（dataID 空则纯本地；
//     Nacos 地址 env HUBX_NACOS_SERVER 覆盖，缺省 127.0.0.1:8848）。
//     读出 pkg/config 各段（trace 总闸 + 三段细分开关，经 Resolve 裁决官方段）
//     与 gateway 私有段（jwt / routes / rate_limit，routing.Load 统一读）。
//  2. rpcx/configx.InitGlobal（同模板——telemetry.Init 的 rpcx/tracing.Init
//     读全局 viper 的 log.enable_trace，只有 InitGlobal 存全局句柄）+
//     infra/telemetry.Init：全局 TracerProvider + Composite(TraceContext,
//     Baggage) 传播器就绪。【红线】必须先于第 3 步：cachex 的 OTel 回调
//     从全局 TracerProvider 取 tracer，晚装配会拿到 noop tracer。
//     网关是整条 trace 的根：proxy.Forward 里 telemetry.Start(ctx,
//     "handler", "gateway", "Proxy:"+prefix) 诞生根 span，traceparent 经
//     传播器注入出站 Header，下游 BFF 入站 Extract 续链。
//  3. infra/drivers：gateway 无 DB，infra 只装缓存——drivers.NewCache
//     （pkg/config Trace.On("cache") 门控，缓存挂 fail-open 降级：限流放行）。
//  4. infra/consul.Register("127.0.0.1:8500","gateway","127.0.0.1:8080")；
//     目标地址经 consul.Resolve(serviceID) 读 KV 转发（HTTP→BFF HTTP 端口）。
//  5. webx.New（/healthz 探针 + catch-all 转发 handler，proxy.Register）。
//  6. signal 优雅退出（反序）：app.Shutdown → cache close →
//     consul.Deregister → tracing Shutdown（OTLP BatchSpanProcessor 批量
//     flush 默认 5s，退出超时必须 ≥10s，已知坑：flush 未完成会丢 span）
//     → logger close。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 logx zap provider（必须空导入）
	"github.com/gospacex/webx"

	rpcxconfigx "github.com/kitexx/kitex-framework/configx"

	"hubx/micro_apps/apps/gateway/internal/auth"
	"hubx/micro_apps/apps/gateway/internal/proxy"
	"hubx/micro_apps/apps/gateway/internal/ratelimit"
	"hubx/micro_apps/apps/gateway/internal/routing"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"
)

// 服务常量（端口唯一事实源：docs/superpowers/plans/local-docker-env.md）
const (
	serviceName = "gateway"
	listenAddr  = "127.0.0.1:8080"
	consulAgent = "127.0.0.1:8500"
	configPath  = "config.yaml" // 与 configs/services/gateway.yaml 逐字同构（服务自包含）

	// jaegerEndpoint docker compose 的 jaeger（OTLP gRPC 4317），服务跑在宿主侧
	jaegerEndpoint = "127.0.0.1:4317"
)

func main() {
	// ---------- 1) 配置：本地 config.yaml 兜底 + Nacos（dataId hubx.gateway） ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := nacos.Load(ctx, configPath, "hubx.gateway", "HUBX")
	if err != nil {
		log.Fatalf("gateway: load config: %v", err)
	}
	defer func() { _ = h.Close() }()

	// pkg/config 段：先 Default() 打底（总闸/三段均开，exporter otlp），
	// 再 Unmarshal 本地显式 trace 段覆盖，最后 Resolve 裁决官方段
	// （xmiddleware.cachex.instances.gateway.enable_trace，仅当 trace 键未显式写出）
	pcfg := pbconfig.Default()
	if h.IsSet("trace") {
		if err := h.Unmarshal("trace", &pcfg.Trace); err != nil {
			log.Fatalf("gateway: unmarshal trace section: %v", err)
		}
	}
	pcfg.Trace.Resolve(h, serviceName)

	// gateway 私有段：路由表 + jwt + 全局限流参数（配置驱动，不硬编码）
	gw, table, err := routing.Load(h)
	if err != nil {
		log.Fatalf("gateway: load routes: %v", err)
	}

	// ---------- 2) rpcx 全局配置 + 调用链（必须先于 3） ----------
	// 同模板 InitGlobal：telemetry.Init 的 rpcx/tracing.Init 读全局 viper
	var logSection struct{}
	if err := rpcxconfigx.InitGlobal(&logSection, rpcxconfigx.WithPath(configPath)); err != nil {
		log.Printf("gateway: rpcx configx init fallback to builtin defaults: %v", err)
	}

	traceSetup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    jaegerEndpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
	})
	if err != nil {
		log.Fatalf("gateway: init telemetry: %v", err)
	}

	// 日志：logx 框架默认日志（OTel 打开时自动携带 trace_id/span_id）
	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		log.Fatalf("gateway: init logger: %v", err)
	}
	logx.SetGlobal(logger) // 未显式注入的组件（cachex 内部）统一用这个
	defer func() { _ = logger.Close() }()

	// ---------- 3) 中间件驱动：gateway 无 DB，只装缓存（On("cache") 门控） ----------
	var cache *drivers.Cache
	var limiter *ratelimit.Limiter
	if pcfg.Trace.On("cache") {
		if cache, err = drivers.NewCache(ctx, h, serviceName, "", true); err != nil {
			// 缓存不可用降级：限流器 nil（fail-open），网关继续服务
			logger.Warn("cache open failed (rate-limit disabled)", logx.Err(err))
			cache = nil
		}
	}
	if gw.RateLimit.Limit > 0 && cache != nil {
		limiter = ratelimit.NewLimiter(cache.Instance(serviceName), gw.RateLimit.Limit, gw.RateLimit.WindowDuration())
	}
	defer func() { _ = cache.Close() }() // nil 安全（Cache.Close 已判空）

	// 业务装配：验签器（密钥经 gateway.jwt 段，env 优先）+ 转发器
	// （traceOn 总闸关时不注入 traceparent，退化为裸转发）
	verifier := auth.NewVerifier(gw.JWT.ResolvedSecret(), gw.JWT.Issuer)
	forwarder := proxy.NewForwarder(consulAgent, pcfg.Trace.Enabled)

	// 就绪探针：缓存门控开且可用时 PING 缓存（monolith resources.Ping 同形态）；
	// 门控关闭 / 缓存挂 fail-open → 恒 ready（限流放行，缓存故障不摘流量）
	var readyCheck func(context.Context) error
	if cache != nil {
		inst := cache.Instance(serviceName)
		readyCheck = func(rctx context.Context) error { return inst.HealthCheck(rctx) }
	}

	// ---------- 4) 服务注册（KV hubx/svc/gateway） ----------
	registered := false
	if rerr := consul.Register(consulAgent, serviceName, listenAddr); rerr != nil {
		logger.Warn("consul register failed (service still running unregistered)", logx.Err(rerr))
	} else {
		registered = true
	}

	// ---------- 5) webx HTTP（/healthz 探针 + catch-all 转发） ----------
	deps := &proxy.Deps{
		Table:     table,
		Limiter:   limiter,
		Verifier:  verifier,
		Forwarder: forwarder,
		Log:       logger,
	}
	app, err := webx.New(
		webx.WithConfig(webx.Config{
			Addr:           listenAddr,
			Logger:         logger,
			Tracer:         traceSetup.Tracer,
			MetricsEnabled: true,
		}),
		webx.WithReadyCheck(readyCheck),
		webx.WithPreRun(func(a *webx.App) { proxy.Register(a.Engine, deps) }),
	)
	if err != nil {
		log.Fatalf("gateway: webx.New: %v", err)
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "gateway listening",
			logx.String("addr", listenAddr),
			logx.Any("trace", traceSetup.Enabled),
			logx.Any("routes", table.Len()),
			logx.Any("ratelimit", limiter != nil))
		srvErr <- app.Run()
	}()

	// ---------- 6) 信号优雅退出（反序） ----------
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-quit:
		logger.Info("signal received, shutting down", logx.String("signal", s.String()))
	case e := <-srvErr:
		if e != nil {
			logger.Error("server stopped with error", logx.Err(e))
		}
	}
	// 先摘流量：排空 in-flight 转发（超时 10s）
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if serr := app.Shutdown(shutCtx); serr != nil {
		logger.Error("webx shutdown failed", logx.Err(serr))
	}
	shutCancel()
	if registered {
		if derr := consul.Deregister(consulAgent, serviceName, listenAddr); derr != nil {
			logger.Warn("consul deregister failed", logx.Err(derr))
		}
	}
	// OTel flush 超时必须 ≥10s（BatchSpanProcessor 批量 5s，
	// 已知坑：flush 未完成即退出会丢 span）
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if serr := traceSetup.Shutdown(flushCtx); serr != nil {
		logger.Error("tracing shutdown failed", logx.Err(serr))
	}
	flushCancel()
}
