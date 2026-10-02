// srv-coupon：优惠券域微服务（rpcx/kitex，监听 :9015，私有库 coupon_db / 账号 coupon_rw）。
//
// 装配总公式（6 步，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.Load(ctx, "config.yaml", "hubx.srv-coupon", "HUBX")：
//     本地 config.yaml 兜底 + 远程 Nacos key 级裁决（dataID 空则纯本地）。
//     读出 pkg/config 各段（trace 总闸 + 三段细分开关，经 Resolve 裁决官方段）。
//  2. infra/telemetry.Init：经 hubx/otelx 装配全局 TracerProvider，并安装
//     Composite(TraceContext, Baggage) 传播器、把开关接入 hubx/traceflag。
//     【红线】必须先于第 3 步：dbx / cachex / mqx 的 OTel 回调从全局
//     TracerProvider 取 tracer，晚装配会拿到 noop tracer。
//  3. infra/drivers：dbx 经 dbxconfig.LoadDBConfigFromConfigx("mysql","srv-coupon")
//     打开 GORM（Trace.On("db") 门控 Open/OpenNoTrace）；drivers.NewProducer
//     （On("mq") 门控，Kafka coupon.exchanged.v1 / points.granted.v1 发布）。
//  4. infra/consul.Register("127.0.0.1:8500","srv-coupon","127.0.0.1:9015")。
//  5. coupon.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{
//     ServiceName:"srv-coupon", Addr:":9015"})...)。
//  6. signal 优雅退出（反序）：svr.Stop → outbox.Stop → producer.Close →
//     consul.Deregister → tracing Shutdown（OTLP BatchSpanProcessor 批量
//     flush 默认 5s，退出超时必须 ≥10s，已知坑：flush 未完成会丢 span）→ db close。
//
// 【退出顺序红线】任何会标 span 的资源（db / cache / producer）都必须在
// tracing.Shutdown **之前**关闭：否则它们关闭动作产生的那批 span 还躺在
// BatchSpanProcessor 的缓冲里就被 flush 丢弃，链路末端永远缺一小段。
// 因此这些资源刻意不用 defer（defer 等到手动 flush 之后才执行）。
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

	"hubx/micro_apps/apps/srv-coupon/internal/repository"
	srvco "hubx/micro_apps/apps/srv-coupon/internal/rpc"
	"hubx/micro_apps/apps/srv-coupon/internal/service"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"
	"hubx/micro_apps/pkg/rpcx"

	couponrpc "hubx/micro_apps/pkg/kitex_gen/coupon/coupon"
)

// 服务常量（端口唯一事实源：docs/superpowers/plans/local-docker-env.md，批 2 修订）
const (
	serviceName = "srv-coupon"
	listenAddr  = "127.0.0.1:9015"
	consulAgent = "127.0.0.1:8500"
	configPath  = "config.yaml" // 与 configs/services/srv-coupon.yaml 逐字同构（服务自包含）
)

// expandDB 包装 dbxconfig.DBConfig：DSN 里的 ${HUBX_DB_PW} 占位经
// os.ExpandEnv 展开（gospx configx 不做 env 展开，与 srv-order 同形态）。
type expandDB struct {
	base dbxconfig.DBConfig
}

func (e expandDB) DriverName() string { return e.base.DriverName() }

func (e expandDB) DSN() string { return os.ExpandEnv(e.base.DSN()) }

// main 只负责把 run 的失败转成进程退出码。
//
// 刻意不让装配过程直接 log.Fatalf：os.Exit(1) 会**跳过所有 defer**，
// 已经打开的 configx 句柄（内含 Nacos 长轮询 goroutine）与 logger 都不会
// 被收尾。真正的资源释放都挂 defer，因此失败必须走 return 让它们执行。
func main() {
	if err := run(); err != nil {
		log.Fatalf("srv-coupon: %v", err)
	}
}

func run() error {
	// ---------- 1) 配置：本地 config.yaml 兜底 + Nacos（dataID hubx.srv-coupon） ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, ht, err := nacos.LoadUnified(ctx, configPath, "hubx.srv-coupon", "HUBX")
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
	logx.SetGlobal(logger)
	defer func() { _ = logger.Close() }()

	// ---------- 3) 中间件驱动：db / mq（On(kind) 门控；本服务无 cachex 读路径） ----------
	db, err := openDB(h, pcfg, logger)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	if sqlDB, derr := db.DB(); derr == nil {
		if perr := sqlDB.PingContext(ctx); perr != nil {
			logger.Warn("db ping failed (degraded mode)", logx.Err(perr))
		}
	}

	// 【红线】Trace.On("mq") 的语义是"这类操作是否入链"，**不是**"要不要构造组件"：
	// 组件承担着业务职责（这里连带影响 coupon-grant events will accumulate unflushed），把它当构造开关会让
	// 观测开关反过来决定可用性。正确做法是组件照常构造，开关只透传给 drivers。
	producer, err := drivers.NewProducer(ctx, h, serviceName, pcfg.Trace.On("mq"))
	if err != nil {
		// MQ 不可用降级：outbox 事件持续累积，Runner 未挂 producer 时跳过补发
		logger.Warn("coupon-grant events will accumulate unflushed disabled", logx.Err(err))
		producer = nil
	}

	// ---------- 业务装配：repository → outbox → service → rpc ----------
	repo := repository.NewCouponRepository(db)
	runner := service.NewOutboxRunner(repo, producer, logger)
	runner.Start()
	svc := service.NewCouponService(repo, runner, pcfg.Trace.Enabled, logger)

	// ---------- 4) 服务注册（KV hubx/svc/srv-coupon） ----------
	registered := false
	if rerr := consul.Register(consulAgent, serviceName, listenAddr); rerr != nil {
		logger.Warn("consul register failed (service still running unregistered)", logx.Err(rerr))
	} else {
		registered = true
	}

	// ---------- 5) rpcx 服务端 ----------
	impl := &srvco.Server{Svc: svc}
	svr := couponrpc.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{
		ServiceName:  serviceName,
		Addr:         listenAddr,
		TraceEnabled: pcfg.Trace.Enabled,
	})...)

	srvErr := make(chan error, 1)
	go func() { srvErr <- svr.Run() }()
	logger.InfoContext(ctx, "srv-coupon listening",
		logx.String("addr", listenAddr),
		logx.Any("trace", traceSetup.Enabled),
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
	// 已知坑：flush 未完成即退出会丢 span）
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if serr := traceSetup.Shutdown(flushCtx); serr != nil {
		logger.Error("tracing shutdown failed", logx.Err(serr))
	}
	flushCancel()
	return nil
}

// openDB 经 dbx 官方 configx 入口打开 GORM（xmiddleware.sqlx.databases.srv-coupon）
func openDB(h configx.Config, pcfg *pbconfig.Config, logger logx.Logger) (*gorm.DB, error) {
	dbc, tracing, err := dbxconfig.LoadDBConfigFromConfigx(h, "mysql", serviceName)
	if err != nil {
		return nil, fmt.Errorf("open db: load config: %w", err)
	}
	// 调用链开关取「统一开关 trace.db」与「官方段 enable_trace」的逻辑与
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
