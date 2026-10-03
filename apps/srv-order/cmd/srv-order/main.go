// srv-order：订单域微服务（rpcx/kitex，监听 :9013，私有库 order_db / 账号 order_rw）。
//
// 装配总公式（6 步，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.Load(ctx, "config.yaml", "hubx.srv-order", "HUBX")：
//     本地 config.yaml 兜底 + 远程 Nacos key 级裁决（dataID 空则纯本地；
//     Nacos 地址 env HUBX_NACOS_SERVER 覆盖，缺省 127.0.0.1:8848）。
//     读出 pkg/config 各段（trace 总闸 + 三段细分开关，经 Resolve 裁决官方段）。
//  2. rpcx/configx.InitGlobal（⚠ InitGlobal 而非 Load/MustLoad——rpcx/tracing.Init
//     读全局 viper 的 log.enable_trace，只有 InitGlobal 存全局句柄）+
//     infra/telemetry.Init：全局 TracerProvider + Composite(TraceContext,Baggage)
//     传播器就绪。【红线】必须先于第 3 步：dbx / cachex / mqx 的 OTel 回调从
//     全局 TracerProvider 取 tracer，晚装配会拿到 noop tracer。
//  3. infra/drivers：dbx 经 dbxconfig.LoadDBConfigFromConfigx("mysql","srv-order")
//     打开 GORM（Trace.On("db") 门控 Open/OpenNoTrace）；drivers.NewCache
//     （On("cache") 门控）；drivers.NewProducer（On("mq") 门控）。
//  4. infra/consul.Register("127.0.0.1:8500","srv-order","127.0.0.1:9013")；
//     跨调 srv-product 经 consul.Target 解析地址 + rpcx.BuildClientOptions。
//  5. order.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{
//     ServiceName:"srv-order", Addr:":9013"})...)。
//  6. signal 优雅退出（反序）：svr.Stop → outbox.Stop → producer/cache/db
//     close → consul.Deregister → tracing Shutdown（OTLP BatchSpanProcessor
//     批量 flush 默认 5s，退出超时必须 ≥10s，已知坑：flush 未完成会丢 span）
//     → logger close。
//
// 【退出顺序红线】任何会标 span 的资源都必须在 tracing.Shutdown **之前**关闭：
// 否则它们关闭动作产生的那批 span 还躺在 BatchSpanProcessor 缓冲里，flush 一走
// 就永久丢失，链路末端永远缺一小段。因此这些资源刻意不用 defer —— defer 在
// 手动 flush 之后才执行。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/configx"
	dbxconfig "github.com/gospacex/dbx/config"
	dbxorm "github.com/gospacex/dbx/orm"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 logx zap provider（必须空导入）
	"gorm.io/gorm"

	"hubx/micro_apps/apps/srv-order/internal/middleware"
	"hubx/micro_apps/apps/srv-order/internal/repository"
	srco "hubx/micro_apps/apps/srv-order/internal/rpc"
	"hubx/micro_apps/apps/srv-order/internal/service"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"
	"hubx/micro_apps/pkg/rpcx"

	orderrpc "hubx/micro_apps/pkg/kitex_gen/order/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	productclient "hubx/micro_apps/pkg/kitex_gen/product/product"
)

// 服务常量（端口唯一事实源：docs/superpowers/plans/local-docker-env.md，批 2 修订）
const (
	serviceName = "srv-order"
	listenAddr  = "127.0.0.1:9013"
	consulAgent = "127.0.0.1:8500"
	configPath  = "config.yaml" // 与 configs/services/srv-order.yaml 逐字同构（服务自包含）

	// 自保护参数（brief §srv-order 限流）：固定窗口 limit/window
	rateLimitPerMinute = 100
	rateLimitWindow    = time.Minute
	rateLimitPrefix    = "rate:srv-order"
)

// expandDB 包装 dbxconfig.DBConfig：DSN 里的 ${HUBX_DB_PW} 占位经
// os.ExpandEnv 展开。gospx configx 不做 env 展开，DBConfig.DSN() 原样
// 返回含占位的 DSN（mysql.ParseDSN 已把整段占位当密码解析进字段，
// MySQLConfig.DSN() 再拼回），须在 Open 前展开成真实密码。
type expandDB struct {
	base dbxconfig.DBConfig
}

func (e expandDB) DriverName() string { return e.base.DriverName() }

func (e expandDB) DSN() string { return os.ExpandEnv(e.base.DSN()) }

// productAdapter 把 kitex 生成的 product.Client（带可变参数 callOptions）
// 适配成 service.ProductClient 窄接口（便于单测注入 fake）
type productAdapter struct {
	cli productclient.Client
}

func (a *productAdapter) ReserveStock(ctx context.Context, req *productpb.ReserveStockReq) (*productpb.ReserveStockResp, error) {
	return a.cli.ReserveStock(ctx, req)
}

// main 只负责把 run 的失败转成进程退出码。
//
// 刻意不让装配过程直接 log.Fatalf：os.Exit(1) 会**跳过所有 defer**，
// 已经打开的 configx 句柄（内含 Nacos 长轮询 goroutine）与 logger 都不会
// 被收尾。真正的资源释放都挂 defer，因此失败必须走 return 让它们执行。
func main() {
	if err := run(); err != nil {
		log.Fatalf("srv-order: %v", err)
	}
}

func run() error {
	// ---------- 1) 配置：本地 config.yaml 兜底 + Nacos（dataID hubx.srv-order） ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, ht, err := nacos.LoadUnified(ctx, configPath, "hubx.srv-order", "HUBX")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	defer func() { _ = h.Close() }()
	if ht != nil {
		defer func() { _ = ht.Close() }()
	}

	// 统一 trace 段：hubx.trace（单一事实源）优先，缺省回退本地/默认；
	// 细分开关三段正交由 ApplyTrace 内 Resolve 完成（唯一开关入口 Trace.On）。
	pcfg := pbconfig.Default()
	pcfg.ApplyTrace(ht, h, serviceName)

	// ---------- 2) 调用链（必须先于 3） ----------
	// infra/telemetry.Init 装配全局 TracerProvider + Composite(TraceContext,
	// Baggage) 传播器。【红线】必须先于第 3 步：dbx / cachex / mqx 从全局取 tracer。
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

	// 日志：logx 框架默认日志（OTel 打开时自动携带 trace_id/span_id）
	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	logx.SetGlobal(logger) // 未显式注入的组件（dbx/cachex/mqx 内部）统一用这个
	defer func() { _ = logger.Close() }()

	// ---------- 3) 中间件驱动：db / cache / mq ----------
	// db 不走 defer 关闭：见文件头「退出顺序红线」，连接关闭动作本身会标 span，
	// 而 defer 要等到手动 tracing flush 之后才执行，那时 flush 早已错峰过去。
	db, err := openDB(h, pcfg, logger)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	if sqlDB, derr := db.DB(); derr == nil {
		if perr := sqlDB.PingContext(ctx); perr != nil {
			// 数据库不可用不阻断启动（dev 演示形态：链路/限流仍可用，业务路径降级）
			logger.Warn("db ping failed (degraded mode)", logx.Err(perr))
		}
	}

	// 【红线】Trace.On(kind) 的语义是"这类操作是否入链"，**不是**"要不要构造组件"。
	// 此前两者被混为一谈，后果是致命且无声的：Trace.On("mq") 一关，producer
	// 就变成 nil，OutboxRunner.Start 直接 return —— 下单继续往 outbox 写
	// status=0 的行却再无投递者，直到表无限堆积也没人报警。
	// 正确做法：组件照常构造，开关只透传给 drivers 决定是否注册 OTel 回调。
	cache, err := drivers.NewCache(ctx, h, serviceName, "", pcfg.Trace.On("cache"))
	if err != nil {
		// 缓存不可用降级：限流器 nil（fail-open），主链路继续
		logger.Warn("cache open failed (rate-limit disabled)", logx.Err(err))
		cache = nil
	}

	producer, err := drivers.NewProducer(ctx, h, serviceName, pcfg.Trace.On("mq"))
	if err != nil {
		// MQ 不可用同样要显式告警：outbox 会持续累积且不会被补发
		logger.Warn("mq producer open failed (outbox events will accumulate unflushed)", logx.Err(err))
		producer = nil
	}

	// ---------- 业务装配：repository → outbox → service → rpc ----------
	repo := repository.NewOrderRepository(db)
	runner := service.NewOutboxRunner(repo, producer, logger)
	runner.Start()

	var productCli service.ProductClient
	if target, terr := consul.Target(consulAgent, "srv-product"); terr == nil {
		if cli, cerr := productclient.NewClient("Product", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName:  serviceName,
			Target:       target,
			ConnType:     "short",
			TraceEnabled: pcfg.Trace.Enabled,
		})...); cerr == nil {
			productCli = &productAdapter{cli: cli}
		} else {
			logger.Warn("product client init failed (create-order without reservation)", logx.Err(cerr))
		}
	} else {
		// 无服务发现（纯本地单库运行）：跳过预占，总额按明细计算（deviation 已记录）
		logger.Warn("srv-product not registered in consul (create-order without reservation)", logx.Err(terr))
	}

	svc := service.NewOrderService(repo, runner, productCli, pcfg.Trace.Enabled, logger)

	var limiter *middleware.RateLimiter
	if cache != nil {
		limiter = middleware.NewRateLimiter(cache.Instance(serviceName), rateLimitPrefix, rateLimitPerMinute, rateLimitWindow)
	}

	// ---------- 4) 服务注册（KV hubx/svc/srv-order） ----------
	registered := false
	if rerr := consul.Register(consulAgent, serviceName, listenAddr); rerr != nil {
		logger.Warn("consul register failed (service still running unregistered)", logx.Err(rerr))
	} else {
		registered = true
	}

	// ---------- 5) rpcx 服务端 ----------
	impl := &srco.Server{Svc: svc, Limiter: limiter}
	svr := orderrpc.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{
		ServiceName:  serviceName,
		Addr:         listenAddr,
		TraceEnabled: pcfg.Trace.Enabled,
	})...)

	srvErr := make(chan error, 1)
	go func() { srvErr <- svr.Run() }()
	logger.InfoContext(ctx, "srv-order listening",
		logx.String("addr", listenAddr),
		logx.Any("trace", traceSetup.Enabled),
		logx.Any("cache", cache != nil),
		logx.Any("mq", producer != nil))

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
	_ = svr.Stop()
	runner.Stop()
	if producer != nil {
		if cerr := producer.Close(); cerr != nil {
			logger.Error("producer close failed", logx.Err(cerr))
		}
	}
	if cache != nil {
		if cerr := cache.Close(); cerr != nil {
			logger.Error("cache close failed", logx.Err(cerr))
		}
	}
	if sqlDB, derr := db.DB(); derr == nil {
		if derr := sqlDB.Close(); derr != nil {
			logger.Error("db close failed", logx.Err(derr))
		}
	}
	if registered {
		if derr := consul.Deregister(consulAgent, serviceName, listenAddr); derr != nil {
			logger.Warn("consul deregister failed", logx.Err(derr))
		}
	}
	// OTel flush 超时必须 ≥10s（BatchSpanProcessor 批量 5s，
	// 已知坑：flush 未完成即退出会丢 span）；放在所有会标 span 的资源之后
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if serr := traceSetup.Shutdown(flushCtx); serr != nil {
		logger.Error("tracing shutdown failed", logx.Err(serr))
	}
	flushCancel()
	return nil
}

// openDB 经 dbx 官方 configx 入口打开 GORM（xmiddleware.sqlx.databases.srv-order）。
// DSN 中的 ${HUBX_DB_PW} 由 expandDB 经 os.ExpandEnv 展开（见类型注释）。
func openDB(h configx.Config, pcfg *pbconfig.Config, logger logx.Logger) (*gorm.DB, error) {
	dbc, _, err := dbxconfig.LoadDBConfigFromConfigx(h, "mysql", serviceName)
	if err != nil {
		return nil, fmt.Errorf("open db: load config: %w", err)
	}
	// 调用链开关统一走 pkg/config 的单一入口 Trace.On("db")：已折总闸
	// Enabled 与细分开关 DB（含官方段 xmiddleware.sqlx...enable_trace 裁决），
	// 与 cache/mq 的 On("cache")/On("mq") 保持一致，不回读 dbx 原生
	// tracing.enable_trace 双开关（统一层刻意覆盖之）。开则 Open（注册
	// OTel 回调），关则 OpenNoTrace（即便全局有 TracerProvider 也不产生 SQL span）。
	traceOn := pcfg.Trace.On("db")
	exp := expandDB{base: dbc}
	opts := []gorm.Option{
		dbxorm.WithLogxLogger(logger, dbxorm.LogxLoggerConfig{
			Level:                "warn",
			SlowThreshold:        200 * time.Millisecond,
			IgnoreRecordNotFound: true,
		}),
	}
	var db *gorm.DB
	if traceOn {
		db, err = dbxorm.Open(exp, opts...)
	} else {
		db, err = dbxorm.OpenNoTrace(exp, opts...)
	}
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	// 【必须】dbx 在 traceOn 分支会自建 TracerProvider 并注册到全局（端点取
	// SDK 默认 localhost:4318 +TLS），把装配层装好的 provider 顶掉，导致走
	// otel.GetTracerProvider() 的组件（RPC client span）只建不导。夺回。
	telemetry.InstallGlobal()
	// 连接池直接用官方 loader 解析出的 PoolConfig（已过 Validate，零值已补默认）
	if sqlDB, perr := db.DB(); perr == nil {
		if p := dbc.GetPool(); p != nil {
			if p.MaxOpenConns > 0 {
				sqlDB.SetMaxOpenConns(p.MaxOpenConns)
			}
			if p.MaxIdleConns > 0 {
				sqlDB.SetMaxIdleConns(p.MaxIdleConns)
			}
			if p.ConnMaxLifetime > 0 {
				sqlDB.SetConnMaxLifetime(time.Duration(p.ConnMaxLifetime) * time.Second)
			}
			if p.ConnMaxIdleTime > 0 {
				sqlDB.SetConnMaxIdleTime(time.Duration(p.ConnMaxIdleTime) * time.Second)
			}
		}
	}
	logger.Info("db ready", logx.String("db.driver", "mysql"), logx.Any("db.trace", traceOn))
	return db, nil
}
