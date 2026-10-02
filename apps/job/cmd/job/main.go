// job：定时任务 + Kafka 消费者（本案例里唯一**无端口**的可部署单元）。
//
// 三个子命令：
//   - job consume：消费业务事件（order.completed.v1 / coupon.exchanged.v1），
//     续接消息头里的 traceparent，失败消息进 <topic>.dlq
//   - job retry：消费 *.dlq，按原 topic 重放，最多 MaxReplay 次后放弃
//   - job schedule：gocron 周期任务（对账 / 库存日清 / 报表），
//     `--once <task>` 单跑一次（验收与排障用）
//
// 装配总公式（6 步，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.LoadUnified(ctx, "config.yaml", "hubx.job", "HUBX")：
//     本地 config.yaml 兜底 + 远程 Nacos 双 dataId 裁决
//     （hubx.trace 单一事实源 + hubx.job 私有配置）。
//  2. infra/telemetry.Init：全局 TracerProvider + Composite(TraceContext,Baggage)
//     传播器就绪。【红线】必须先于第 3 步：mqx 的 OTel 回调从全局取 tracer，
//     晚装配会拿到 noop tracer，send/receive 全链路直接断在异步边界。
//  3. infra/drivers：producer（DLQ 投递）/ consumer（消费），trace 开关
//     一律经 pcfg.Trace.On("mq")；缓存（重放防重）经 On("cache")。
//  4. 业务装配：coupon 客户端（经 consul.Target 解析 srv-coupon）+
//     handler.Dispatcher + consumer.Consumer / retry.Replayer /
//     schedule.Scheduler。
//  5. 无端口服务不做服务注册：job 不被任何上游 RPC 调用，注册进 Consul
//     只会让别人以为可以调用它（它根本没有监听）。
//  6. signal 优雅退出（反序）：consumer/producer close → tracing.Shutdown
//     （OTLP BatchSpanProcessor 批量 flush 默认 5s，退出超时必须 ≥10s，
//     已知坑：flush 未完成会丢 span，异步链路末端正好是最容易丢的那段）。
//
// 【红线】job 不直连任何业务库：对账、库存、报表一律走各 srv 的 gRPC
// 只读接口（openspec「库内 JOIN，跨库走 RPC」在 job 侧的落点）。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gospacex/configx"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 logx zap provider（必须空导入）
	"github.com/spf13/cobra"

	"hubx/micro_apps/apps/job/internal/consumer"
	"hubx/micro_apps/apps/job/internal/handler"
	"hubx/micro_apps/apps/job/internal/retry"
	"hubx/micro_apps/apps/job/internal/schedule"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"
	"hubx/micro_apps/pkg/rpcx"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	couponclient "hubx/micro_apps/pkg/kitex_gen/coupon/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	orderclient "hubx/micro_apps/pkg/kitex_gen/order/order"
	paypb "hubx/micro_apps/pkg/kitex_gen/pay"
	payclient "hubx/micro_apps/pkg/kitex_gen/pay/pay"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	productclient "hubx/micro_apps/pkg/kitex_gen/product/product"
)

// 服务常量（job 无监听端口；端口事实源 docs/superpowers/plans/local-docker-env.md）
const (
	serviceName = "job"
	configPath  = "config.yaml" // 与 configs/services/job.yaml 逐字同构（服务自包含）
	consulAgent = "127.0.0.1:8500"

	// 消费组（consume 与 retry 必须不同组：否则两边抢同一分区，重放会被业务消费吃掉）
	groupConsume = "job-order-v1"
	groupRetry   = "job-retry-v1"

	// 默认订阅/重放 topic
	flushTimeout = 15 * time.Second
)

// businessTopics 业务事件 topic（consume 默认订阅）
var businessTopics = []string{handler.TopicOrderCompleted, handler.TopicCouponExchanged}

// dlqTopics 死信 topic（retry 订阅，由业务 topic + .dlq 推导）
var dlqTopics = []string{
	handler.TopicOrderCompleted + handler.DLQSuffix,
	handler.TopicCouponExchanged + handler.DLQSuffix,
}

// runtime 命令共享的装配产物
type runtime struct {
	handle configx.Config
	pcfg   *pbconfig.Config
	setup  *telemetry.Setup
	logger logx.Logger
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		// 刻意不用 log.Fatalf：os.Exit(1) 会跳过 defer，Nacos 长轮询
		// goroutine 与 logger 都收不了尾。错误由 cobra 打印，退出码交给 main。
		fmt.Fprintf(os.Stderr, "job: %v\n", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "job",
		Short: "hubx job：Kafka 消费 + DLQ 重放 + 定时任务（无端口服务）",
	}
	root.AddCommand(newConsumeCmd(), newRetryCmd(), newScheduleCmd())
	return root
}

// boot 装配总公式第 1~3 步（三个子命令共用）
func boot(ctx context.Context) (*runtime, error) {
	// ---------- 1) 配置：本地 config.yaml 兜底 + Nacos 双 dataId ----------
	h, ht, err := nacos.LoadUnified(ctx, configPath, "hubx."+serviceName, "HUBX")
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	pcfg := pbconfig.Default()
	pcfg.ApplyTrace(ht, h, serviceName)

	// ---------- 2) 调用链（必须先于 3） ----------
	setup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    pcfg.Trace.Endpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
		SampleRatio: pcfg.Trace.SampleRatio,
		Exporter:    pcfg.Trace.Exporter,
	})
	if err != nil {
		return nil, fmt.Errorf("init telemetry: %w", err)
	}

	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}
	logx.SetGlobal(logger)

	return &runtime{handle: h, pcfg: pcfg, setup: setup, logger: logger}, nil
}

// shutdown 退出反序：资源先关，最后 flush span
func (r *runtime) shutdown(closers ...func() error) {
	for _, c := range closers {
		if c == nil {
			continue
		}
		if err := c(); err != nil {
			r.logger.Error("job: close failed", logx.Err(err))
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	if err := r.setup.Shutdown(ctx); err != nil {
		r.logger.Error("tracing shutdown failed", logx.Err(err))
	}
	if err := r.logger.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "job: logger close: %v\n", err)
	}
}

// couponClient 经 Consul 解析 srv-coupon 并建 kitex 客户端；未注册返回 nil
// （nil 时 handler 派发会显式报错 → 消息进 DLQ，而不是假装成功）。
func (r *runtime) couponClient() (handler.CouponClient, error) {
	target, err := consul.Target(consulAgent, "srv-coupon")
	if err != nil {
		return nil, err
	}
	cli, err := couponclient.NewClient("Coupon", rpcx.BuildClientOptions(rpcx.ClientOptions{
		ServiceName:  serviceName,
		Target:       target,
		ConnType:     "short",
		TraceEnabled: r.pcfg.Trace.Enabled,
	})...)
	if err != nil {
		return nil, err
	}
	return &couponAdapter{cli: cli}, nil
}

// couponAdapter 把 kitex 生成的 coupon.Client 适配成 handler 窄接口
type couponAdapter struct{ cli couponclient.Client }

func (a *couponAdapter) GrantPoints(ctx context.Context, req *couponpb.GrantPointsReq) (*couponpb.GrantPointsResp, error) {
	return a.cli.GrantPoints(ctx, req)
}

// ---------- consume ----------

func newConsumeCmd() *cobra.Command {
	var topics []string
	cmd := &cobra.Command{
		Use:   "consume",
		Short: "消费业务事件（Kafka），失败消息进 <topic>.dlq",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(topics) == 0 {
				topics = businessTopics
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			rt, err := boot(ctx)
			if err != nil {
				return err
			}
			defer rt.shutdown()

			mqOn := rt.pcfg.Trace.On("mq")
			// DLQ 生产者：消费失败时投递死信（无它则失败消息只能靠 mqx 重投，
			// 重投次数用尽后消息永久消失，连重放的机会都没有）
			prod, perr := drivers.NewProducer(ctx, rt.handle, serviceName, mqOn)
			if perr != nil {
				rt.logger.Warn("mq producer open failed (failed messages cannot be routed to dlq)", logx.Err(perr))
			}

			coupons, cerr := rt.couponClient()
			if cerr != nil {
				rt.logger.Warn("srv-coupon not resolvable (dispatch will fail into dlq)", logx.Err(cerr))
			}
			disp := handler.New(coupons, rt.logger)

			// 一个 topic 一个 consumer（mqx 的 ConsumerConfig 只有单 Topic）
			var closers []func() error
			for _, topic := range topics {
				cons, err := drivers.NewConsumerWithTopic(ctx, rt.handle, serviceName, groupConsume, topic, mqOn)
				if err != nil {
					rt.shutdown(closers...)
					return fmt.Errorf("job consume: %w", err)
				}
				closers = append(closers, cons.Close)
				c := consumer.New(cons, disp, rt.logger, mqOn, prod)
				go func(c *consumer.Consumer, topic string) {
					rt.logger.Info("job: consuming", logx.String("topic", topic), logx.Any("trace", mqOn))
					if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
						rt.logger.Error("job: consume loop exited", logx.Err(err), logx.String("topic", topic))
					}
				}(c, topic)
			}
			if prod != nil {
				closers = append(closers, func() error { return prod.Close() })
			}
			defer rt.shutdown(closers...)

			<-ctx.Done()
			rt.logger.Info("job: consume stopped")
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&topics, "topic", nil,
		"订阅 topic（可重复或逗号分隔），默认 "+strings.Join(businessTopics, ","))
	return cmd
}

// ---------- retry ----------

func newRetryCmd() *cobra.Command {
	var topics []string
	cmd := &cobra.Command{
		Use:   "retry",
		Short: "消费 *.dlq 并按原 topic 重放（最多 " + fmt.Sprint(retry.MaxReplay) + " 次）",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(topics) == 0 {
				topics = dlqTopics
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			rt, err := boot(ctx)
			if err != nil {
				return err
			}
			defer rt.shutdown()

			mqOn := rt.pcfg.Trace.On("mq")
			prod, perr := drivers.NewProducer(ctx, rt.handle, serviceName, mqOn)
			if perr != nil {
				rt.logger.Warn("mq producer open failed (requeue disabled)", logx.Err(perr))
			}
			coupons, cerr := rt.couponClient()
			if cerr != nil {
				rt.logger.Warn("srv-coupon not resolvable (replay will fail)", logx.Err(cerr))
			}
			disp := handler.New(coupons, rt.logger)

			var closers []func() error
			for _, topic := range topics {
				cons, err := drivers.NewConsumerWithTopic(ctx, rt.handle, serviceName, groupRetry, topic, mqOn)
				if err != nil {
					rt.shutdown(closers...)
					return fmt.Errorf("job retry: %w", err)
				}
				closers = append(closers, cons.Close)
				r := retry.New(cons, prod, disp, rt.logger, mqOn)
				go func(r *retry.Replayer, topic string) {
					rt.logger.Info("job: replaying dlq", logx.String("topic", topic))
					if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
						rt.logger.Error("job: replay loop exited", logx.Err(err), logx.String("topic", topic))
					}
				}(r, topic)
			}
			if prod != nil {
				closers = append(closers, func() error { return prod.Close() })
			}
			defer rt.shutdown(closers...)

			<-ctx.Done()
			rt.logger.Info("job: retry stopped")
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&topics, "topic", nil,
		"重放 topic（默认 "+strings.Join(dlqTopics, ",")+"）")
	return cmd
}

// ---------- schedule ----------

func newScheduleCmd() *cobra.Command {
	var (
		once     string
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "gocron 周期任务：reconcile / stock-daily / report",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			rt, err := boot(ctx)
			if err != nil {
				return err
			}
			defer rt.shutdown()

			sch := schedule.New(rt.scheduleClients(), rt.logger, rt.pcfg.Trace.Enabled)
			if once != "" {
				// 单跑一次后退出：验收「task.* 根 span」与排障都靠它
				if err := sch.RunOnce(ctx, once); err != nil {
					return err
				}
				rt.logger.Info("job: task done", logx.String("task", once))
				return nil
			}
			sch.WithInterval(interval)
			rt.logger.Info("job: scheduler starting", logx.Any("tasks", sch.Names()))
			return sch.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&once, "once", "", "只跑一次指定任务后退出（"+strings.Join([]string{schedule.TaskReconcile, schedule.TaskStockDaily, schedule.TaskReport}, "/")+"）")
	cmd.Flags().DurationVar(&interval, "interval", time.Minute, "调度周期（演示用；真实环境应改 cron 表达式）")
	return cmd
}

// scheduleClients 解析三个只读 srv 客户端（任一未注册 → 对应任务降级不崩）
func (r *runtime) scheduleClients() schedule.Clients {
	var c schedule.Clients
	if cli, err := newOrderClient(r); err != nil {
		r.logger.Warn("srv-order not resolvable (reconcile/report degraded)", logx.Err(err))
	} else {
		c.Orders = cli
	}
	if cli, err := newPayClient(r); err != nil {
		r.logger.Warn("srv-pay not resolvable (reconcile degraded)", logx.Err(err))
	} else {
		c.Pays = cli
	}
	if cli, err := newProductClient(r); err != nil {
		r.logger.Warn("srv-product not resolvable (stock-daily degraded)", logx.Err(err))
	} else {
		c.Products = cli
	}
	return c
}

// ---- 客户端适配器：kitex 生成的 Client 带可变 callOptions，需适配成
// schedule 的窄接口（便于单测注入 fake，也避免业务层依赖生成代码）----

type orderAdapter struct{ cli orderclient.Client }

func (a *orderAdapter) ListOrders(ctx context.Context, req *orderpb.ListOrdersReq) (*orderpb.ListOrdersResp, error) {
	return a.cli.ListOrders(ctx, req)
}
func (a *orderAdapter) Report(ctx context.Context, req *orderpb.ReportReq) (*orderpb.ReportResp, error) {
	return a.cli.Report(ctx, req)
}

type payAdapter struct{ cli payclient.Client }

func (a *payAdapter) ListPayFlows(ctx context.Context, req *paypb.ListPayFlowsReq) (*paypb.ListPayFlowsResp, error) {
	return a.cli.ListPayFlows(ctx, req)
}

type productAdapter struct{ cli productclient.Client }

func (a *productAdapter) ListProducts(ctx context.Context, req *productpb.ListProductsReq) (*productpb.ListProductsResp, error) {
	return a.cli.ListProducts(ctx, req)
}

func newOrderClient(r *runtime) (schedule.OrderClient, error) {
	target, err := consul.Target(consulAgent, "srv-order")
	if err != nil {
		return nil, err
	}
	cli, err := orderclient.NewClient("Order", rpcx.BuildClientOptions(rpcx.ClientOptions{
		ServiceName: serviceName, Target: target, ConnType: "short", TraceEnabled: r.pcfg.Trace.Enabled,
	})...)
	if err != nil {
		return nil, err
	}
	return &orderAdapter{cli: cli}, nil
}

func newPayClient(r *runtime) (schedule.PayClient, error) {
	target, err := consul.Target(consulAgent, "srv-pay")
	if err != nil {
		return nil, err
	}
	cli, err := payclient.NewClient("Pay", rpcx.BuildClientOptions(rpcx.ClientOptions{
		ServiceName: serviceName, Target: target, ConnType: "short", TraceEnabled: r.pcfg.Trace.Enabled,
	})...)
	if err != nil {
		return nil, err
	}
	return &payAdapter{cli: cli}, nil
}

func newProductClient(r *runtime) (schedule.ProductClient, error) {
	target, err := consul.Target(consulAgent, "srv-product")
	if err != nil {
		return nil, err
	}
	cli, err := productclient.NewClient("Product", rpcx.BuildClientOptions(rpcx.ClientOptions{
		ServiceName: serviceName, Target: target, ConnType: "short", TraceEnabled: r.pcfg.Trace.Enabled,
	})...)
	if err != nil {
		return nil, err
	}
	return &productAdapter{cli: cli}, nil
}
