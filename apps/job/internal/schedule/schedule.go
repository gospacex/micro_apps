// Package schedule job 定时任务宿主（gocron）。
//
// 三个任务的共同约束：
//  1. **不直连业务库**：job 没有自己的库，也不许碰别人的库（openspec 红线）。
//     对账/库存日清/报表一律走各 srv 的 gRPC 只读接口——这正是「跨库 JOIN
//     必须走 RPC」这条红线在 job 侧的落点。
//  2. **自建根 span**：定时任务没有入站请求可续，若不自建 task.<name>.Run
//     根 span，Jaeger 上根本看不出任务跑没跑、跑了多久。
//  3. **降级不崩**：对账是旁路任务，某个 srv 没注册或超时，只告警不退出——
//     一个旁路任务把 job 打挂，等于把消费侧一起带走。
package schedule

import (
	"context"
	"fmt"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/trace"

	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	paypb "hubx/micro_apps/pkg/kitex_gen/pay"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"

	"hubx/micro_apps/infra/telemetry"
)

// 任务名（job schedule --once 的参数取值，也是 span entity）
const (
	// TaskReconcile 订单 vs 支付流水对账
	TaskReconcile = "reconcile"
	// TaskStockDaily 库存日清（低库存预警，走 srv-product 只读接口）
	TaskStockDaily = "stock-daily"
	// TaskReport 报表聚合（走 srv-order Report）
	TaskReport = "report"
)

// LowStockThreshold 低库存阈值（低于它进预警清单）
const LowStockThreshold = 10

// 对账/列表单次拉取上限（演示规模足够，避免一次拉爆下游）
const listLimit = 500

// OrderClient srv-order 只读窄接口
type OrderClient interface {
	ListOrders(ctx context.Context, req *orderpb.ListOrdersReq) (*orderpb.ListOrdersResp, error)
	Report(ctx context.Context, req *orderpb.ReportReq) (*orderpb.ReportResp, error)
}

// PayClient srv-pay 只读窄接口
type PayClient interface {
	ListPayFlows(ctx context.Context, req *paypb.ListPayFlowsReq) (*paypb.ListPayFlowsResp, error)
}

// ProductClient srv-product 只读窄接口
type ProductClient interface {
	ListProducts(ctx context.Context, req *productpb.ListProductsReq) (*productpb.ListProductsResp, error)
}

// Clients job 依赖的下游客户端集合（任一为 nil 表示未注册，对应任务降级）
type Clients struct {
	Orders   OrderClient
	Pays     PayClient
	Products ProductClient
}

// Result 对账结果
type Result struct {
	// Orders 拉到的订单流水条数
	Orders int
	// Pays 拉到的支付流水条数
	Pays int
	// Mismatch 判定不一致的订单数（已支付但缺支付流水，或金额对不上）
	Mismatch int
}

// Scheduler 定时任务宿主
type Scheduler struct {
	clients  Clients
	logger   logx.Logger
	traceOn  bool
	interval time.Duration
}

// New 构造调度器。interval 为演示用周期（真实环境走 cron 表达式）。
func New(clients Clients, logger logx.Logger, traceOn bool) *Scheduler {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Scheduler{clients: clients, logger: logger, traceOn: traceOn, interval: time.Minute}
}

// WithInterval 覆盖周期（测试与 `--interval` 参数用）
func (s *Scheduler) WithInterval(d time.Duration) *Scheduler {
	if d > 0 {
		s.interval = d
	}
	return s
}

// Names 注册的任务名清单
func (s *Scheduler) Names() []string {
	return []string{TaskReconcile, TaskStockDaily, TaskReport}
}

// RunOnce 单次执行指定任务（job schedule --once，也是单测入口）
func (s *Scheduler) RunOnce(ctx context.Context, name string) error {
	switch name {
	case TaskReconcile:
		return s.runReconcile(ctx)
	case TaskStockDaily:
		return s.runStockDaily(ctx)
	case TaskReport:
		return s.runReport(ctx)
	default:
		return fmt.Errorf("schedule: unknown task %q", name)
	}
}

// Run 启动 gocron 周期调度，阻塞至 ctx 取消
func (s *Scheduler) Run(ctx context.Context) error {
	sch, err := gocron.NewScheduler()
	if err != nil {
		return fmt.Errorf("schedule: new scheduler: %w", err)
	}
	for _, name := range s.Names() {
		task := name // 闭包捕获
		if _, err := sch.NewJob(
			gocron.DurationJob(s.interval),
			gocron.NewTask(func(ctx context.Context) {
				// 任务内部错误只告警：一个任务失败不能把整个调度器带崩
				if err := s.RunOnce(ctx, task); err != nil {
					s.logger.ErrorContext(ctx, "job: scheduled task failed", logx.Err(err), logx.String("task", task))
				}
			}),
			gocron.WithName(task),
		); err != nil {
			return fmt.Errorf("schedule: register task %q: %w", task, err)
		}
	}
	sch.Start()
	s.logger.Info("job: scheduler started", logx.Any("tasks", s.Names()), logx.Any("interval", s.interval.String()))
	<-ctx.Done()
	return sch.Shutdown()
}

// startSpan 任务根 span（trace 开时才有）
func (s *Scheduler) startSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	if !s.traceOn {
		return ctx, trace.SpanFromContext(ctx)
	}
	return telemetry.Start(ctx, "task", name, "Run")
}

func (s *Scheduler) runReconcile(ctx context.Context) error {
	ctx, span := s.startSpan(ctx, TaskReconcile)
	defer span.End()
	day := time.Now().Format("2006-01-02")
	res, err := s.Reconcile(ctx, day)
	if err != nil {
		return err
	}
	s.logger.InfoContext(ctx, "job: reconcile done",
		logx.String("date", day),
		logx.Int("orders", res.Orders),
		logx.Int("pays", res.Pays),
		logx.Int("mismatch", res.Mismatch))
	return nil
}

// Reconcile 订单 vs 支付流水对账（跨库 JOIN 走 RPC 的示范：两边各拉各的，
// 在内存里按 order_id 对齐）。
//
// 单侧失败不算失败（降级：另一侧的流水照常统计）；双侧都失败才返回错误。
func (s *Scheduler) Reconcile(ctx context.Context, day string) (*Result, error) {
	res := &Result{}
	failed := 0

	var orders []*orderpb.OrderFlow
	if s.clients.Orders == nil {
		s.logger.WarnContext(ctx, "job: srv-order not registered, reconcile degraded")
		failed++
	} else {
		resp, err := s.clients.Orders.ListOrders(ctx, &orderpb.ListOrdersReq{FromDate: day, ToDate: day, Limit: listLimit})
		if err != nil {
			s.logger.WarnContext(ctx, "job: list orders failed, reconcile degraded", logx.Err(err))
			failed++
		} else if resp.GetCode() != 0 {
			s.logger.WarnContext(ctx, "job: list orders rejected", logx.String("msg", resp.GetMessage()))
			failed++
		} else {
			orders = resp.GetFlows()
			res.Orders = len(orders)
		}
	}

	paid := make(map[int64]int64, len(orders))
	if s.clients.Pays == nil {
		s.logger.WarnContext(ctx, "job: srv-pay not registered, reconcile degraded")
		failed++
	} else {
		resp, err := s.clients.Pays.ListPayFlows(ctx, &paypb.ListPayFlowsReq{FromDate: day, ToDate: day, Limit: listLimit})
		if err != nil {
			s.logger.WarnContext(ctx, "job: list pay flows failed, reconcile degraded", logx.Err(err))
			failed++
		} else if resp.GetCode() != 0 {
			s.logger.WarnContext(ctx, "job: list pay flows rejected", logx.String("msg", resp.GetMessage()))
			failed++
		} else {
			flows := resp.GetFlows()
			res.Pays = len(flows)
			for _, f := range flows {
				if f.GetStatus() == "success" {
					paid[f.GetOrderId()] = f.GetAmount()
				}
			}
		}
	}

	if failed >= 2 {
		return res, fmt.Errorf("schedule: reconcile: both order and pay sides unavailable")
	}

	// 已支付订单必须有等额的成功支付流水，否则即不一致
	for _, o := range orders {
		if o.GetStatus() != "paid" {
			continue
		}
		amount, ok := paid[o.GetOrderId()]
		if !ok {
			res.Mismatch++
			s.logger.WarnContext(ctx, "job: paid order without pay flow",
				logx.Any("order_id", o.GetOrderId()))
			continue
		}
		if amount != o.GetTotalAmount() {
			res.Mismatch++
			s.logger.WarnContext(ctx, "job: pay amount mismatch",
				logx.Any("order_id", o.GetOrderId()),
				logx.Any("order_amount", o.GetTotalAmount()),
				logx.Any("pay_amount", amount))
		}
	}
	return res, nil
}

func (s *Scheduler) runStockDaily(ctx context.Context) error {
	ctx, span := s.startSpan(ctx, TaskStockDaily)
	defer span.End()
	if s.clients.Products == nil {
		s.logger.WarnContext(ctx, "job: srv-product not registered, stock-daily degraded")
		return nil
	}
	resp, err := s.clients.Products.ListProducts(ctx, &productpb.ListProductsReq{Limit: listLimit})
	if err != nil {
		s.logger.WarnContext(ctx, "job: list products failed, stock-daily degraded", logx.Err(err))
		return nil
	}
	low := 0
	for _, p := range resp.GetProducts() {
		if p.GetStock() < LowStockThreshold {
			low++
		}
	}
	s.logger.InfoContext(ctx, "job: stock daily done",
		logx.Int("products", len(resp.GetProducts())),
		logx.Int("low_stock", low))
	return nil
}

func (s *Scheduler) runReport(ctx context.Context) error {
	ctx, span := s.startSpan(ctx, TaskReport)
	defer span.End()
	if s.clients.Orders == nil {
		s.logger.WarnContext(ctx, "job: srv-order not registered, report degraded")
		return nil
	}
	day := time.Now().Format("2006-01-02")
	resp, err := s.clients.Orders.Report(ctx, &orderpb.ReportReq{FromDate: day, ToDate: day})
	if err != nil {
		s.logger.WarnContext(ctx, "job: report failed, degraded", logx.Err(err))
		return nil
	}
	s.logger.InfoContext(ctx, "job: report done",
		logx.String("date", day), logx.Int("rows", len(resp.GetRows())))
	return nil
}
