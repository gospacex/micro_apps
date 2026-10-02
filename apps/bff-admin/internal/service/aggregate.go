// Package service bff-admin 业务编排层：运营看板聚合（五个 srv 只读接口并发拉取）、
// 降级兜底与展示层缓存。
//
// 每个出口方法产生 service.bff-admin.<Op> span；Dashboard 的并发分支产生
// service.bff-admin.Dashboard.<branch> 子 span，降级时标记 degraded=true。
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"hubx/micro_apps/apps/bff-admin/internal/cache"
	"hubx/micro_apps/apps/bff-admin/internal/client"
	"hubx/micro_apps/infra/telemetry"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	paypb "hubx/micro_apps/pkg/kitex_gen/pay"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// 展示层缓存 key 形态：bff:admin:{resource}:{user_id}
const dashboardKeyFmt = "bff:admin:dashboard:%d"

// statusTexts 后台表格的状态枚举文案表（srv 返回枚举 → 中文展示文案）。
var statusTexts = map[string]string{
	"created":  "待支付",
	"paying":   "支付中",
	"paid":     "已支付",
	"shipped":  "已发货",
	"done":     "已完成",
	"closed":   "已关闭",
	"canceled": "已取消",
	"refunded": "已退款",
	"failed":   "支付失败",
	"success":  "支付成功",
	"held":     "未使用",
	"used":     "已使用",
	"expired":  "已过期",
}

// StatusText 状态枚举 → 后台表格文案；未知枚举原样回显，便于后台排障。
func StatusText(status string) string {
	if text, ok := statusTexts[status]; ok {
		return text
	}
	return status
}

// Aggregator 聚合编排依赖。
type Aggregator struct {
	Cli   *client.Clients
	Cache *cache.DisplayCache
	Log   logx.Logger
}

// NewAggregator 构造聚合服务。
func NewAggregator(cli *client.Clients, displayCache *cache.DisplayCache, logger logx.Logger) *Aggregator {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Aggregator{Cli: cli, Cache: displayCache, Log: logger}
}

// TableMeta 后台表格分页元信息（字段裁剪面向表格：分页 + 总量）。
type TableMeta struct {
	Page     int32 `json:"page"`
	PageSize int32 `json:"page_size"`
	Total    int32 `json:"total"`
}

// OrderRow 后台表格用订单行（只保留表格列，并把状态枚举翻成文案）。
type OrderRow struct {
	OrderNo       string `json:"order_no"`
	Status        string `json:"status"`
	StatusText    string `json:"status_text"`
	TotalAmount   int64  `json:"total_amount"`
	RefundAmount  int64  `json:"refund_amount"`
	ItemCount     int32  `json:"item_count"`
	StatusChanges int32  `json:"status_change_count"`
}

// DashboardView 运营看板聚合视图；降级分支对应的字段保持 nil。
type DashboardView struct {
	Users       *userpb.GetProfileResp      `json:"users,omitempty"`
	Orders      *orderpb.ReportResp         `json:"orders,omitempty"`
	Pays        *paypb.ListPayFlowsResp     `json:"pays,omitempty"`
	Products    *productpb.ListProductsResp `json:"products,omitempty"`
	Coupons     *couponpb.ListCouponsResp   `json:"coupons,omitempty"`
	Rows        []*OrderRow                 `json:"rows"`
	Table       TableMeta                   `json:"table"`
	Partial     bool                        `json:"partial"`
	Unavailable []string                    `json:"unavailable,omitempty"`
	Degraded    []string                    `json:"degraded,omitempty"`
	CachedAt    int64                       `json:"cached_at"`
	CacheHit    bool                        `json:"cache_hit"`
}

// Dashboard 运营看板聚合：缓存命中直接返回；未命中时并发拉取用户、订单、
// 支付、商品与优惠券五个域，单分支失败只记录降级，不中断其他分支。
func (a *Aggregator) Dashboard(ctx context.Context, userID int64, page, pageSize int32, fromDate, toDate string) (*DashboardView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Dashboard")
	defer span.End()

	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}

	key := fmt.Sprintf(dashboardKeyFmt, userID)
	var view DashboardView
	if a.Cache != nil && a.Cache.Get(ctx, key, &view) {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		view.CacheHit = true
		return &view, nil
	}
	span.SetAttributes(attribute.Bool("cache.hit", false))

	view = DashboardView{
		Table:    TableMeta{Page: page, PageSize: pageSize},
		CachedAt: time.Now().Unix(),
	}
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return a.fetchUsers(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchOrders(groupCtx, userID, fromDate, toDate, &view, &mu) })
	group.Go(func() error { return a.fetchPays(groupCtx, userID, fromDate, toDate, pageSize, &view, &mu) })
	group.Go(func() error { return a.fetchProducts(groupCtx, page, pageSize, &view, &mu) })
	group.Go(func() error { return a.fetchCoupons(groupCtx, userID, &view, &mu) })
	_ = group.Wait()

	mu.Lock()
	view.Rows = trimRows(view.Orders.GetRows())
	view.Table.Total = int32(len(view.Rows))
	view.Partial = len(view.Degraded) > 0
	view.Unavailable = append([]string(nil), view.Degraded...)
	mu.Unlock()

	if a.Cache != nil {
		a.Cache.Set(ctx, key, &view)
	}
	return &view, nil
}

// trimRows 把订单报表行裁成后台表格行（补齐状态枚举文案）。
func trimRows(rows []*orderpb.ReportRow) []*OrderRow {
	out := make([]*OrderRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &OrderRow{
			OrderNo:       row.GetOrderNo(),
			Status:        row.GetStatus(),
			StatusText:    StatusText(row.GetStatus()),
			TotalAmount:   row.GetTotalAmount(),
			RefundAmount:  row.GetRefundAmount(),
			ItemCount:     row.GetItemCount(),
			StatusChanges: row.GetStatusChangeCount(),
		})
	}
	return out
}

// fetchUsers 调用 srv-user GetProfile；失败时降级 users 分支。
func (a *Aggregator) fetchUsers(ctx context.Context, userID int64, view *DashboardView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Dashboard.users")
	defer span.End()
	if a.Cli.User == nil {
		return a.degrade(span, view, mu, "users")
	}
	resp, err := a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "users")
	}
	mu.Lock()
	view.Users = resp
	mu.Unlock()
	return nil
}

// fetchOrders 调用 srv-order Report 拉订单报表。
func (a *Aggregator) fetchOrders(ctx context.Context, userID int64, fromDate, toDate string, view *DashboardView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Dashboard.orders")
	defer span.End()
	if a.Cli.Order == nil {
		return a.degrade(span, view, mu, "orders")
	}
	resp, err := a.Cli.Order.Report(ctx, &orderpb.ReportReq{UserId: userID, FromDate: fromDate, ToDate: toDate})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "orders")
	}
	mu.Lock()
	view.Orders = resp
	mu.Unlock()
	return nil
}

// fetchPays 调用 srv-pay ListPayFlows 拉支付流水。
func (a *Aggregator) fetchPays(ctx context.Context, userID int64, fromDate, toDate string, pageSize int32, view *DashboardView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Dashboard.pays")
	defer span.End()
	if a.Cli.Pay == nil {
		return a.degrade(span, view, mu, "pays")
	}
	resp, err := a.Cli.Pay.ListPayFlows(ctx, &paypb.ListPayFlowsReq{
		UserId: userID, FromDate: fromDate, ToDate: toDate, Limit: pageSize,
	})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "pays")
	}
	mu.Lock()
	view.Pays = resp
	mu.Unlock()
	return nil
}

// fetchProducts 调用 srv-product ListProducts 拉库存（按分页窗口）。
func (a *Aggregator) fetchProducts(ctx context.Context, page, pageSize int32, view *DashboardView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Dashboard.products")
	defer span.End()
	if a.Cli.Product == nil {
		return a.degrade(span, view, mu, "products")
	}
	resp, err := a.Cli.Product.ListProducts(ctx, &productpb.ListProductsReq{
		Limit: pageSize, Offset: (page - 1) * pageSize,
	})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "products")
	}
	mu.Lock()
	view.Products = resp
	mu.Unlock()
	return nil
}

// fetchCoupons 调用 srv-coupon ListCoupons，含已使用券（后台看全量）。
func (a *Aggregator) fetchCoupons(ctx context.Context, userID int64, view *DashboardView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Dashboard.coupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return a.degrade(span, view, mu, "coupons")
	}
	resp, err := a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: true})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "coupons")
	}
	mu.Lock()
	view.Coupons = resp
	mu.Unlock()
	return nil
}

// degrade 标记分支 span，并把分支加入降级清单。
func (a *Aggregator) degrade(span trace.Span, view *DashboardView, mu *sync.Mutex, branch string) error {
	span.SetAttributes(attribute.Bool("degraded", true))
	mu.Lock()
	view.Degraded = append(view.Degraded, branch)
	mu.Unlock()
	return nil
}

// Profile 透传用户资料查询（只读）。
func (a *Aggregator) Profile(ctx context.Context, userID int64) (*userpb.GetProfileResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Profile")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
}

// CheckUser 透传用户活跃校验（只读）。
func (a *Aggregator) CheckUser(ctx context.Context, userID int64) (*userpb.CheckUserResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "CheckUser")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.CheckUser(ctx, &userpb.CheckUserReq{UserId: userID})
}

// ListProducts 透传商品列表查询（库存视图，只读）。
func (a *Aggregator) ListProducts(ctx context.Context, limit, offset int32) (*productpb.ListProductsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "ListProducts")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	return a.Cli.Product.ListProducts(ctx, &productpb.ListProductsReq{Limit: limit, Offset: offset})
}

// GetProduct 透传商品详情查询（只读）。
func (a *Aggregator) GetProduct(ctx context.Context, productID int64) (*productpb.GetProductResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "GetProduct")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	return a.Cli.Product.GetProduct(ctx, &productpb.GetProductReq{ProductId: productID})
}

// ListOrders 透传订单流水查询（只读）。
func (a *Aggregator) ListOrders(ctx context.Context, userID int64, fromDate, toDate string, limit int32) (*orderpb.ListOrdersResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "ListOrders")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.ListOrders(ctx, &orderpb.ListOrdersReq{
		UserId: userID, FromDate: fromDate, ToDate: toDate, Limit: limit,
	})
}

// GetOrder 透传订单详情查询（只读）。
func (a *Aggregator) GetOrder(ctx context.Context, userID, orderID int64) (*orderpb.GetOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "GetOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.GetOrder(ctx, &orderpb.GetOrderReq{UserId: userID, OrderId: orderID})
}

// Report 透传订单报表查询（只读）。
func (a *Aggregator) Report(ctx context.Context, userID int64, fromDate, toDate string) (*orderpb.ReportResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "Report")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.Report(ctx, &orderpb.ReportReq{UserId: userID, FromDate: fromDate, ToDate: toDate})
}

// ListPayFlows 透传支付流水查询（只读）。
func (a *Aggregator) ListPayFlows(ctx context.Context, userID int64, fromDate, toDate string, limit int32) (*paypb.ListPayFlowsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "ListPayFlows")
	defer span.End()
	if a.Cli.Pay == nil {
		return nil, fmt.Errorf("srv-pay unavailable")
	}
	return a.Cli.Pay.ListPayFlows(ctx, &paypb.ListPayFlowsReq{
		UserId: userID, FromDate: fromDate, ToDate: toDate, Limit: limit,
	})
}

// ListCoupons 透传优惠券列表查询（只读）。
func (a *Aggregator) ListCoupons(ctx context.Context, userID int64, includeUsed bool) (*couponpb.ListCouponsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-admin", "ListCoupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return nil, fmt.Errorf("srv-coupon unavailable")
	}
	return a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: includeUsed})
}
