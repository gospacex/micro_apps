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
//  6. signal 优雅退出（反序）：svr.Stop → outbox.Stop → producer/cache close →
//     consul.Deregister → tracing Shutdown（OTLP BatchSpanProcessor 批量 flush
//     默认 5s，退出超时必须 ≥10s，已知坑：flush 未完成会丢 span）→ db close。
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
	"github.com/gospacex/mqx"
	"gorm.io/gorm"

	rpcxconfigx "github.com/kitexx/kitex-framework/configx"
	"github.com/kitexx/kitex-framework/rpcx"

	"hubx/micro_apps/apps/srv-order/internal/middleware"
	"hubx/micro_apps/apps/srv-order/internal/repository"
	srco "hubx/micro_apps/apps/srv-order/internal/rpc"
	"hubx/micro_apps/apps/srv-order/internal/service"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"

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

	// jaegerEndpoint docker compose 的 jaeger（OTLP gRPC 4317），服务跑在宿主侧
	jaegerEndpoint = "127.0.0.1:4317"

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

func main() {
	// ---------- 1) 配置：本地 config.yaml 兜底 + Nacos（dataID hubx.srv-order） ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := nacos.Load(ctx, configPath, "hubx.srv-order", "HUBX")
	if err != nil {
		log.Fatalf("srv-order: load config: %v", err)
	}
	defer func() { _ = h.Close() }()

	// pkg/config 段：先 Default() 打底（总闸/三段均开，exporter otlp），
	// 再 Unmarshal 本地显式 trace 段覆盖，最后 Resolve 裁决官方段
	// （xmiddleware.*.enable_trace / call_chain_enabled，仅当 trace 键未显式写出）
	pcfg := pbconfig.Default()
	if h.IsSet("trace") {
		if err := h.Unmarshal("trace", &pcfg.Trace); err != nil {
			log.Fatalf("srv-order: unmarshal trace section: %v", err)
		}
	}
	pcfg.Trace.Resolve(h, serviceName)

	// ---------- 2) rpcx 全局配置 + 调用链（必须先于 3） ----------
	// InitGlobal 把 config.yaml（本服务无 log 段则回退内置默认）存入全局 viper，
	// rpcx/tracing.Init 经 log.enable_trace 裁决是否挂 obs-opentelemetry suite
	var logSection struct{}
	if err := rpcxconfigx.InitGlobal(&logSection, rpcxconfigx.WithPath(configPath)); err != nil {
		log.Printf("srv-order: rpcx configx init fallback to builtin defaults: %v", err)
	}

	traceSetup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    jaegerEndpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
	})
	if err != nil {
		log.Fatalf("srv-order: init telemetry: %v", err)
	}

	// 日志：logx 框架默认日志（OTel 打开时自动携带 trace_id/span_id）
	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		log.Fatalf("srv-order: init logger: %v", err)
	}
	logx.SetGlobal(logger) // 未显式注入的组件（dbx/cachex/mqx 内部）统一用这个
	defer func() { _ = logger.Close() }()

	// ---------- 3) 中间件驱动：db / cache / mq（On(kind) 门控） ----------
	db, err := openDB(h, pcfg, logger)
	if err != nil {
		log.Fatalf("srv-order: open db: %v", err)
	}
	defer func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	}()
	if sqlDB, derr := db.DB(); derr == nil {
		if perr := sqlDB.PingContext(ctx); perr != nil {
			// 数据库不可用不阻断启动（dev 演示形态：链路/限流仍可用，业务路径降级）
			logger.Warn("db ping failed (degraded mode)", logx.Err(perr))
		}
	}

	var cache *drivers.Cache
	if pcfg.Trace.On("cache") {
		if cache, err = drivers.NewCache(ctx, h, serviceName, "", true); err != nil {
			// 缓存不可用降级：限流器 nil（fail-open），主链路继续
			logger.Warn("cache open failed (rate-limit disabled)", logx.Err(err))
			cache = nil
		}
	}

	var producer mqx.Producer
	if pcfg.Trace.On("mq") {
		if producer, err = drivers.NewProducer(ctx, h, serviceName, true); err != nil {
			// MQ 不可用降级：outbox 事件持续累积，Runner 未挂 producer 时跳过补发
			logger.Warn("mq producer open failed (outbox not flushed)", logx.Err(err))
			producer = nil
		}
	}

	// ---------- 业务装配：repository → outbox → service → rpc ----------
	repo := repository.NewOrderRepository(db)
	runner := service.NewOutboxRunner(repo, producer, logger)
	runner.Start()

	var productCli service.ProductClient
	if target, terr := consul.Target(consulAgent, "srv-product"); terr == nil {
		if cli, cerr := productclient.NewClient("Product", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: serviceName,
			Target:      target,
			ConnType:    "short",
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
		ServiceName: serviceName,
		Addr:        listenAddr,
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
		_ = producer.Close()
	}
	if cache != nil {
		_ = cache.Close()
	}
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

// openDB 经 dbx 官方 configx 入口打开 GORM（xmiddleware.sqlx.databases.srv-order）。
// DSN 中的 ${HUBX_DB_PW} 由 expandDB 经 os.ExpandEnv 展开（见类型注释）。
func openDB(h configx.Config, pcfg *pbconfig.Config, logger logx.Logger) (*gorm.DB, error) {
	dbc, tracing, err := dbxconfig.LoadDBConfigFromConfigx(h, "mysql", serviceName)
	if err != nil {
		return nil, fmt.Errorf("open db: load config: %w", err)
	}
	// 调用链开关取「统一开关 trace.db」与「官方段 enable_trace」的逻辑与：
	// 两者都开才装 OTel 回调（Open），否则 OpenNoTrace（不注册回调，
	// 即便全局有 TracerProvider 也不会产生 SQL span）
	traceOn := pcfg.Trace.On("db") && (tracing == nil || tracing.Enabled)
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
