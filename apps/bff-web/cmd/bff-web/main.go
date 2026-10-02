// bff-web：PC Web 端 BFF（webx 监听 :8092，聚合 srv-user / srv-product /
// srv-order / srv-coupon；无 DB、无 MQ，只挂展示层缓存）。
//
// 装配总公式（6 步，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.LoadUnified(ctx, "config.yaml", "hubx.bff-web", "HUBX")：
//     私有 dataId + 统一 trace dataId（hubx.trace 单一事实源，缺省回退本地/默认）；
//     pkg/config Default() 打底后由 ApplyTrace 裁决统一 trace 段。
//  2. infra/telemetry.Init：全局 TracerProvider + Composite(TraceContext,
//     Baggage) 传播器就绪；随后初始化 logx。【红线】必须先于第 3 步：
//     cachex 的 OTel 回调从全局 TracerProvider 取 tracer。
//  3. infra/drivers.NewCache：Trace.On("cache") 只门控 OTel 回调，
//     不门控构造；缓存不可用降级为无缓存运行。
//  4. 业务装配：kitex client 桩 ×4（rpcx.BuildClientOptions，
//     TraceEnabled 透传）+ handler/service/cache 分层；随后注册 Consul。
//  5. webx.New：挂路由与就绪探针，监听 127.0.0.1:8092。
//  6. signal 优雅退出（反序）：web → cache → consul → OTel flush
//     （≥10s 窗口）→ logger close。
//
// 入站续链：webx Otel 中间件 Extract 入站 traceparent（gateway 注入），
// 本服务 handler/service/cache/rpc client 各层 span 全部挂同一条 trace。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 logx zap provider（必须空导入）
	"github.com/gospacex/webx"

	"hubx/micro_apps/apps/bff-web/internal/cache"
	"hubx/micro_apps/apps/bff-web/internal/client"
	"hubx/micro_apps/apps/bff-web/internal/handler"
	"hubx/micro_apps/apps/bff-web/internal/service"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"
)

// 服务常量（端口唯一事实源：docs/superpowers/plans/local-docker-env.md）
const (
	serviceName = "bff-web"
	listenAddr  = "127.0.0.1:8092"
	consulAgent = "127.0.0.1:8500"
	configPath  = "config.yaml" // 与 configs/services/bff-web.yaml 逐字同构（服务自包含）

	// displayTTL 展示层缓存 TTL（PC Web：60s，命中期间不打下游 srv）
	displayTTL = 60 * time.Second
)

// main 只负责把 run 的失败转成进程退出码。
//
// 刻意不让装配过程直接 log.Fatalf：os.Exit(1) 会跳过所有 defer，已经打开的
// Nacos 长轮询句柄与 logger 都不会被收尾。资源释放挂在 run 中，失败必须 return。
func main() {
	if err := run(); err != nil {
		log.Fatalf("bff-web: %v", err)
	}
}

func run() error {
	// ---------- 1) 配置：私有 dataId + 统一 trace dataId ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, ht, err := nacos.LoadUnified(ctx, configPath, "hubx."+serviceName, "HUBX")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	defer func() { _ = h.Close() }()
	if ht != nil {
		defer func() { _ = ht.Close() }()
	}

	pcfg := pbconfig.Default()
	pcfg.ApplyTrace(ht, h, serviceName)

	// ---------- 2) 调用链与日志（必须先于 3） ----------
	traceSetup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    pcfg.Trace.Endpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
		SampleRatio: pcfg.Trace.SampleRatio,
		Exporter:    pcfg.Trace.Exporter,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}

	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	logx.SetGlobal(logger)
	defer func() { _ = logger.Close() }()

	// ---------- 3) 展示层缓存（On("cache") 只门控 span，不门控构造） ----------
	cacheH, err := drivers.NewCache(ctx, h, serviceName, "", pcfg.Trace.On("cache"))
	if err != nil {
		logger.Warn("cache open failed (display cache disabled)", logx.Err(err))
		cacheH = nil
	}
	var displayCache *cache.DisplayCache
	if cacheH != nil {
		displayCache = cache.New(cacheH.Instance(serviceName), displayTTL)
	}

	// ---------- 4) 业务装配与服务注册 ----------
	clients := client.New(serviceName, consulAgent, pcfg.Trace.Enabled, logger)
	agg := service.NewAggregator(clients, displayCache, logger)
	api := handler.NewAPI(agg, logger)

	var readyCheck func(context.Context) error
	if cacheH != nil {
		inst := cacheH.Instance(serviceName)
		readyCheck = func(rctx context.Context) error { return inst.HealthCheck(rctx) }
	}

	registered := false
	if rerr := consul.Register(consulAgent, serviceName, listenAddr); rerr != nil {
		logger.Warn("consul register failed (service still running unregistered)", logx.Err(rerr))
	} else {
		registered = true
	}

	// ---------- 5) webx HTTP（路由 + 探针） ----------
	app, err := webx.New(
		webx.WithConfig(webx.Config{
			Addr:           listenAddr,
			Logger:         logger,
			Tracer:         traceSetup.Tracer,
			MetricsEnabled: true,
		}),
		webx.WithReadyCheck(readyCheck),
		webx.WithPreRun(func(a *webx.App) { handler.Register(a.Engine, api) }),
	)
	if err != nil {
		return fmt.Errorf("webx.New: %w", err)
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "bff-web listening",
			logx.String("addr", listenAddr),
			logx.Any("trace", traceSetup.Enabled))
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
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if serr := app.Shutdown(shutCtx); serr != nil {
		logger.Error("webx shutdown failed", logx.Err(serr))
	}
	shutCancel()
	if cacheH != nil {
		if cerr := cacheH.Close(); cerr != nil {
			logger.Error("cache close failed", logx.Err(cerr))
		}
	}
	if registered {
		if derr := consul.Deregister(consulAgent, serviceName, listenAddr); derr != nil {
			logger.Warn("consul deregister failed", logx.Err(derr))
		}
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if serr := traceSetup.Shutdown(flushCtx); serr != nil {
		logger.Error("tracing shutdown failed", logx.Err(serr))
	}
	flushCancel()
	return nil
}
