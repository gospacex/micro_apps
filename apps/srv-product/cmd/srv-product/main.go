// srv-product：商品域微服务（rpcx/kitex，监听 :9012，私有库 product_db / 账号 product_rw）。
//
// 装配总公式（6 步，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.Load(ctx, "config.yaml", "hubx.srv-product", "HUBX")：
//     本地 config.yaml 兜底 + 远程 Nacos key 级裁决（dataID 空则纯本地）。
//     读出 pkg/config 各段（trace 总闸 + 三段细分开关，经 Resolve 裁决官方段）。
//  2. rpcx/configx.InitGlobal（⚠ InitGlobal 而非 Load/MustLoad——rpcx/tracing.Init
//     读全局 viper 的 log.enable_trace，只有 InitGlobal 存全局句柄）+
//     infra/telemetry.Init：全局 TracerProvider + Composite(TraceContext,Baggage)
//     传播器就绪。【红线】必须先于第 3 步：dbx / cachex 的 OTel 回调从
//     全局 TracerProvider 取 tracer，晚装配会拿到 noop tracer。
//  3. infra/drivers：dbx 经 dbxconfig.LoadDBConfigFromConfigx("mysql","srv-product")
//     打开 GORM（Trace.On("db") 门控 Open/OpenNoTrace）；drivers.NewCache
//     （On("cache") 门控，GetProduct 热点缓存 product:hot:{id} TTL 30s）。
//  4. infra/consul.Register("127.0.0.1:8500","srv-product","127.0.0.1:9012")。
//  5. product.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{
//     ServiceName:"srv-product", Addr:":9012"})...)。
//  6. signal 优雅退出（反序）：svr.Stop → cache close → consul.Deregister →
//     tracing Shutdown（OTLP BatchSpanProcessor 批量 flush 默认 5s，退出
//     超时必须 ≥10s，已知坑：flush 未完成会丢 span）→ db close。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/configx"
	dbxconfig "github.com/gospacex/dbx/config"
	dbxorm "github.com/gospacex/dbx/orm"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 logx zap provider（必须空导入）
	"gorm.io/gorm"

	rpcxconfigx "github.com/kitexx/kitex-framework/configx"
	"github.com/kitexx/kitex-framework/rpcx"

	"hubx/micro_apps/apps/srv-product/internal/repository"
	srvpo "hubx/micro_apps/apps/srv-product/internal/rpc"
	"hubx/micro_apps/apps/srv-product/internal/service"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"

	productrpc "hubx/micro_apps/pkg/kitex_gen/product/product"
)

// 服务常量（端口唯一事实源：docs/superpowers/plans/local-docker-env.md，批 2 修订）
const (
	serviceName = "srv-product"
	listenAddr  = "127.0.0.1:9012"
	consulAgent = "127.0.0.1:8500"
	configPath  = "config.yaml" // 与 configs/services/srv-product.yaml 逐字同构（服务自包含）

	// jaegerEndpoint docker compose 的 jaeger（OTLP gRPC 4317），服务跑在宿主侧
	jaegerEndpoint = "127.0.0.1:4317"
)

// expandDB 包装 dbxconfig.DBConfig：DSN 里的 ${HUBX_DB_PW} 占位经
// os.ExpandEnv 展开（gospx configx 不做 env 展开，与 srv-order 同形态）。
type expandDB struct {
	base dbxconfig.DBConfig
}

func (e expandDB) DriverName() string { return e.base.DriverName() }

func (e expandDB) DSN() string { return os.ExpandEnv(e.base.DSN()) }

func main() {
	// ---------- 1) 配置：本地 config.yaml 兜底 + Nacos（dataID hubx.srv-product） ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := nacos.Load(ctx, configPath, "hubx.srv-product", "HUBX")
	if err != nil {
		log.Fatalf("srv-product: load config: %v", err)
	}
	defer func() { _ = h.Close() }()

	pcfg := pbconfig.Default()
	if h.IsSet("trace") {
		if err := h.Unmarshal("trace", &pcfg.Trace); err != nil {
			log.Fatalf("srv-product: unmarshal trace section: %v", err)
		}
	}
	pcfg.Trace.Resolve(h, serviceName)

	// ---------- 2) rpcx 全局配置 + 调用链（必须先于 3） ----------
	var logSection struct{}
	if err := rpcxconfigx.InitGlobal(&logSection, rpcxconfigx.WithPath(configPath)); err != nil {
		log.Printf("srv-product: rpcx configx init fallback to builtin defaults: %v", err)
	}

	traceSetup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    jaegerEndpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
	})
	if err != nil {
		log.Fatalf("srv-product: init telemetry: %v", err)
	}

	// 日志：logx 框架默认日志（OTel 打开时自动携带 trace_id/span_id）
	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		log.Fatalf("srv-product: init logger: %v", err)
	}
	logx.SetGlobal(logger)
	defer func() { _ = logger.Close() }()

	// ---------- 3) 中间件驱动：db / cache（On(kind) 门控；本服务无 mqx 段） ----------
	db, err := openDB(h, pcfg, logger)
	if err != nil {
		log.Fatalf("srv-product: open db: %v", err)
	}
	defer func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	}()
	if sqlDB, derr := db.DB(); derr == nil {
		if perr := sqlDB.PingContext(ctx); perr != nil {
			logger.Warn("db ping failed (degraded mode)", logx.Err(perr))
		}
	}

	var cache *drivers.Cache
	if pcfg.Trace.On("cache") {
		if cache, err = drivers.NewCache(ctx, h, serviceName, "", true); err != nil {
			// 缓存不可用降级：GetProduct 热点缓存失效（fail-open 直接查 DB）
			logger.Warn("cache open failed (product cache disabled)", logx.Err(err))
			cache = nil
		}
	}

	// ---------- 业务装配：repository → service → rpc ----------
	repo := repository.NewProductRepository(db)
	svc := service.NewProductService(
		repo,
		cacheInstance(cache),
		pcfg.Trace.Enabled,
		logger,
	)

	// ---------- 4) 服务注册（KV hubx/svc/srv-product） ----------
	registered := false
	if rerr := consul.Register(consulAgent, serviceName, listenAddr); rerr != nil {
		logger.Warn("consul register failed (service still running unregistered)", logx.Err(rerr))
	} else {
		registered = true
	}

	// ---------- 5) rpcx 服务端 ----------
	impl := &srvpo.Server{Svc: svc}
	svr := productrpc.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{
		ServiceName: serviceName,
		Addr:        listenAddr,
	})...)

	srvErr := make(chan error, 1)
	go func() { srvErr <- svr.Run() }()
	logger.InfoContext(ctx, "srv-product listening",
		logx.String("addr", listenAddr),
		logx.Any("trace", traceSetup.Enabled),
		logx.Any("cache", cache != nil))

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

// cacheInstance 取本服务私有缓存实例（drivers.Cache 为 nil 时返回 nil，
// 服务层 fail-open 直查 DB）
func cacheInstance(c *drivers.Cache) cachex.Cache {
	if c == nil {
		return nil
	}
	return c.Instance(serviceName)
}

// openDB 经 dbx 官方 configx 入口打开 GORM（xmiddleware.sqlx.databases.srv-product）
func openDB(h configx.Config, pcfg *pbconfig.Config, logger logx.Logger) (*gorm.DB, error) {
	dbc, tracing, err := dbxconfig.LoadDBConfigFromConfigx(h, "mysql", serviceName)
	if err != nil {
		return nil, fmt.Errorf("open db: load config: %w", err)
	}
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
