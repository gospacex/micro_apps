// Package service bff-app 业务编排层：errgroup 并发聚合、降级兜底与展示层缓存。
//
// 每个出口方法产生 service.bff-app.<Op> span；Home 的并发分支产生
// service.bff-app.Home.<branch> 子 span，降级时标记 degraded=true。
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

	"hubx/micro_apps/apps/bff-app/internal/cache"
	"hubx/micro_apps/apps/bff-app/internal/client"
	"hubx/micro_apps/infra/telemetry"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

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

// HomeView 首页聚合视图；降级分支对应的字段保持 nil。
type HomeView struct {
	Profile  *userpb.GetProfileResp    `json:"profile,omitempty"`
	Coupons  *couponpb.ListCouponsResp `json:"coupons,omitempty"`
	Recent   *orderpb.ListOrdersResp   `json:"recent_orders,omitempty"`
	Degraded []string                  `json:"degraded,omitempty"`
	CachedAt int64                     `json:"cached_at"`
	CacheHit bool                      `json:"cache_hit"`
}

// Home 首页聚合：缓存命中直接返回；未命中时并发拉取用户、优惠券与订单域，
// 单分支失败只记录降级，不中断其他分支。
func (a *Aggregator) Home(ctx context.Context, userID int64) (*HomeView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home")
	defer span.End()

	key := fmt.Sprintf("home:%d", userID)
	var view HomeView
	if a.Cache != nil && a.Cache.Get(ctx, key, &view) {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		view.CacheHit = true
		return &view, nil
	}
	span.SetAttributes(attribute.Bool("cache.hit", false))

	view = HomeView{CachedAt: time.Now().Unix()}
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return a.fetchProfile(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchCoupons(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchRecent(groupCtx, userID, &view, &mu) })
	_ = group.Wait()

	if a.Cache != nil {
		a.Cache.Set(ctx, key, &view)
	}
	return &view, nil
}

// fetchProfile 调用 srv-user GetProfile；失败时降级 profile 分支。
func (a *Aggregator) fetchProfile(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home.profile")
	defer span.End()
	if a.Cli.User == nil {
		return a.degrade(span, view, mu, "profile")
	}
	resp, err := a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "profile")
	}
	mu.Lock()
	view.Profile = resp
	mu.Unlock()
	return nil
}

// fetchCoupons 调用 srv-coupon ListCoupons，包含已使用优惠券。
func (a *Aggregator) fetchCoupons(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home.coupons")
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

// fetchRecent 调用 srv-order ListOrders 拉取最近五条订单流水。
func (a *Aggregator) fetchRecent(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home.recent")
	defer span.End()
	if a.Cli.Order == nil {
		return a.degrade(span, view, mu, "recent_orders")
	}
	resp, err := a.Cli.Order.ListOrders(ctx, &orderpb.ListOrdersReq{UserId: userID, Limit: 5})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "recent_orders")
	}
	mu.Lock()
	view.Recent = resp
	mu.Unlock()
	return nil
}

// degrade 标记分支 span，并把分支加入降级清单。
func (a *Aggregator) degrade(span trace.Span, view *HomeView, mu *sync.Mutex, branch string) error {
	span.SetAttributes(attribute.Bool("degraded", true))
	mu.Lock()
	view.Degraded = append(view.Degraded, branch)
	mu.Unlock()
	return nil
}

// Register 透传用户注册。
func (a *Aggregator) Register(ctx context.Context, username, password, phone string) (*userpb.RegisterResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Register")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.Register(ctx, &userpb.RegisterReq{Username: username, Password: password, Phone: phone})
}

// Login 透传用户登录。
func (a *Aggregator) Login(ctx context.Context, username, password string) (*userpb.LoginResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Login")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.Login(ctx, &userpb.LoginReq{Username: username, Password: password})
}

// Profile 透传用户资料查询。
func (a *Aggregator) Profile(ctx context.Context, userID int64) (*userpb.GetProfileResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Profile")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
}

// ListProducts 透传商品列表查询。
func (a *Aggregator) ListProducts(ctx context.Context, userID int64, limit, offset int32) (*productpb.ListProductsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "ListProducts")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	return a.Cli.Product.ListProducts(ctx, &productpb.ListProductsReq{UserId: userID, Limit: limit, Offset: offset})
}

// GetProduct 透传商品详情查询。
func (a *Aggregator) GetProduct(ctx context.Context, userID, productID int64) (*productpb.GetProductResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "GetProduct")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	return a.Cli.Product.GetProduct(ctx, &productpb.GetProductReq{UserId: userID, ProductId: productID})
}

// CreateOrder 透传创建订单。
func (a *Aggregator) CreateOrder(ctx context.Context, userID int64, items []*orderpb.OrderItem) (*orderpb.CreateOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "CreateOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.CreateOrder(ctx, &orderpb.CreateOrderReq{UserId: userID, Items: items})
}

// GetOrder 透传订单详情查询。
func (a *Aggregator) GetOrder(ctx context.Context, userID, orderID int64) (*orderpb.GetOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "GetOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.GetOrder(ctx, &orderpb.GetOrderReq{UserId: userID, OrderId: orderID})
}

// Report 透传订单报表查询。
func (a *Aggregator) Report(ctx context.Context, userID int64, fromDate, toDate string) (*orderpb.ReportResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Report")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.Report(ctx, &orderpb.ReportReq{UserId: userID, FromDate: fromDate, ToDate: toDate})
}

// Coupons 透传优惠券列表查询。
func (a *Aggregator) Coupons(ctx context.Context, userID int64, includeUsed bool) (*couponpb.ListCouponsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Coupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return nil, fmt.Errorf("srv-coupon unavailable")
	}
	return a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: includeUsed})
}
