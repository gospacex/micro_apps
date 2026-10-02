// Package service bff-mini 业务编排层：errgroup 并发聚合、降级兜底、展示层缓存
// 与小程序端字段裁剪。
//
// 每个出口方法产生 service.bff-mini.<Op> span；Home 的并发分支产生
// service.bff-mini.Home.<branch> 子 span，降级时标记 degraded=true。
//
// 【端差异】小程序端一律"轻"：DTO 只留首屏渲染必需的字段（商品不带库存/分类、
// 用户不带账号名/城市、订单不带明细数组），另附小程序码/分享位 MiniCode
// （bff-app / bff-web 均无此字段）。
package service

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"hubx/micro_apps/apps/bff-mini/internal/cache"
	"hubx/micro_apps/apps/bff-mini/internal/client"
	"hubx/micro_apps/infra/telemetry"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// 小程序端首页常量（轻量首屏：推荐位 5 条，只取可用券）
const (
	// homeProductLimit 首页推荐商品条数（小程序首屏一屏可看完，不再分页）
	homeProductLimit = 5
	// homePagePath 小程序码落地页路径（wxacode 的 page 字段）
	homePagePath = "pages/home/index"
	// miniCodeURLFmt 小程序码图片地址模板：真实环境由微信 wxacode.getUnlimited
	// 接口换取（本教学案例不出网，用占位模板保持字段自洽）
	miniCodeURLFmt = "https://cdn.hubx.test/wxacode/%s.png"
	// miniShareImage / miniShareTitle 分享卡片（转发好友/群的 5:4 卡片）
	miniShareImage = "https://cdn.hubx.test/share/mini-home.png"
	miniShareTitle = "hubx 商城小程序"
)

// MiniProfile 小程序端用户卡片：只留昵称/头像/等级。
// 裁掉 username（内部账号）与 city（个人中心页才用）——首屏省流量。
type MiniProfile struct {
	Nickname string `json:"nickname"`
	Avatar   string `json:"avatar"`
	Level    int32  `json:"level"`
}

// MiniProduct 小程序端商品卡：只留 id/标题/价格/主图。
// 裁掉 stock（库存只在详情页实时查）与 category_name（分类走筛选页）。
type MiniProduct struct {
	ProductID int64  `json:"product_id"`
	Title     string `json:"title"`
	Price     int64  `json:"price"` // 分
	Cover     string `json:"cover"` // 主图 URL（srv-product 暂不下发图片，空串占位）
}

// MiniCoupon 小程序端券卡：只留 id/名称/面额。
// 裁掉 type/status/used_order_id（"我的券"折叠区只展示可用券）。
type MiniCoupon struct {
	CouponID int64  `json:"coupon_id"`
	Name     string `json:"name"`
	Discount int64  `json:"discount"` // 分
}

// MiniOrder 小程序端订单卡：只留单号/状态/金额/商品数。
// 裁掉 items 明细数组与 paid_amount（明细在订单详情页单独拉）。
type MiniOrder struct {
	OrderNo     string `json:"order_no"`
	Status      string `json:"status"`
	TotalAmount int64  `json:"total_amount"` // 分
	ItemCount   int32  `json:"item_count"`
}

// MiniCode 小程序码 + 分享位（bff-mini 端差异核心，其他端 DTO 无此结构）。
//
// 用途：小程序端首屏要能直接"亮码/转发"，服务端一次性把码与分享素材下发，
// 省去端上再拼一次请求（小程序包体与请求数都受限）。
type MiniCode struct {
	// Scene 小程序码 scene 参数（微信上限 32 字符）：扫码进入后由页面
	// onLoad(options.scene) 取回，用于识别邀请人/来源位。
	Scene string `json:"scene"`
	// QrcodeURL 小程序码图片地址（wxacode.getUnlimited 生成，端上直接 <image src> 渲染）
	QrcodeURL string `json:"qrcode_url"`
	// ShareImage 分享卡片配图（转发给好友/群时的 5:4 卡片图）
	ShareImage string `json:"share_image"`
	// ShareTitle 分享卡片标题
	ShareTitle string `json:"share_title"`
	// PagePath 小程序码落地页路径（如 pages/home/index）
	PagePath string `json:"page_path"`
}

// HomeView 小程序首页聚合视图；降级分支对应的字段保持 nil。
type HomeView struct {
	Profile  *MiniProfile  `json:"profile,omitempty"`
	Products []MiniProduct `json:"products,omitempty"`
	Coupons  []MiniCoupon  `json:"coupons,omitempty"`
	Share    *MiniCode     `json:"share,omitempty"`
	Degraded []string      `json:"degraded,omitempty"`
	CachedAt int64         `json:"cached_at"`
	CacheHit bool          `json:"cache_hit"`
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

// Home 小程序首页聚合：缓存命中直接返回；未命中时并发拉取用户、推荐商品与
// 可用券三个分支，单分支失败只记录降级，不中断其他分支。
//
// 与 bff-app 的端差异：不做订单流水分支（小程序首屏不放流水），改为推荐商品位。
func (a *Aggregator) Home(ctx context.Context, userID int64) (*HomeView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Home")
	defer span.End()

	key := cache.Key("home", userID)
	var view HomeView
	if a.Cache != nil && a.Cache.Get(ctx, key, &view) {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		view.CacheHit = true
		return &view, nil
	}
	span.SetAttributes(attribute.Bool("cache.hit", false))

	view = HomeView{CachedAt: time.Now().Unix(), Share: newMiniCode(userID)}
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return a.fetchProfile(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchProducts(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchCoupons(groupCtx, userID, &view, &mu) })
	_ = group.Wait()

	if a.Cache != nil {
		a.Cache.Set(ctx, key, &view)
	}
	return &view, nil
}

// fetchProfile 调用 srv-user GetProfile 并裁成 MiniProfile。
func (a *Aggregator) fetchProfile(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Home.profile")
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
	view.Profile = &MiniProfile{Nickname: resp.GetNickname(), Avatar: resp.GetAvatar(), Level: resp.GetLevel()}
	mu.Unlock()
	return nil
}

// fetchProducts 调用 srv-product ListProducts 拉首页推荐位（限 5 条，裁成 MiniProduct）。
func (a *Aggregator) fetchProducts(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Home.products")
	defer span.End()
	if a.Cli.Product == nil {
		return a.degrade(span, view, mu, "products")
	}
	resp, err := a.Cli.Product.ListProducts(ctx, &productpb.ListProductsReq{UserId: userID, Limit: homeProductLimit})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "products")
	}
	mu.Lock()
	view.Products = trimProducts(resp.GetProducts())
	mu.Unlock()
	return nil
}

// fetchCoupons 调用 srv-coupon ListCoupons；小程序端只取可用券（IncludeUsed=false）。
func (a *Aggregator) fetchCoupons(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Home.coupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return a.degrade(span, view, mu, "coupons")
	}
	resp, err := a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: false})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "coupons")
	}
	mu.Lock()
	view.Coupons = trimCoupons(resp.GetCoupons())
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

// ---------- 裁剪工具（端差异落点）----------

// trimProducts 把 srv-product 的完整商品裁成小程序端轻量卡片。
func trimProducts(in []*productpb.ProductInfo) []MiniProduct {
	out := make([]MiniProduct, 0, len(in))
	for _, p := range in {
		if p == nil {
			continue
		}
		out = append(out, MiniProduct{
			ProductID: p.GetProductId(),
			Title:     p.GetTitle(),
			Price:     p.GetPrice(),
		})
	}
	return out
}

// trimCoupons 把 srv-coupon 的完整券裁成小程序端轻量券卡。
func trimCoupons(in []*couponpb.CouponInfo) []MiniCoupon {
	out := make([]MiniCoupon, 0, len(in))
	for _, c := range in {
		if c == nil {
			continue
		}
		out = append(out, MiniCoupon{
			CouponID: c.GetCouponId(),
			Name:     c.GetName(),
			Discount: c.GetDiscount(),
		})
	}
	return out
}

// BizError 下游 srv 返回的业务错误（code != 0）。
//
// 裁剪后的 DTO 已不带 code 字段（那是"轻"的一部分），业务码因此改为 error
// 形态上抛；handler 用 errors.As 认领它并归一为 envelope.CodeBizRule，
// 与 bff-app 逐字段透传时"resp.GetCode() != 0 → CodeBizRule"语义一致。
type BizError struct{ Msg string }

func (e *BizError) Error() string { return e.Msg }

// newMiniCode 生成小程序码/分享位素材（scene 只带用户 ID，控制在微信 32 字符上限内）。
func newMiniCode(userID int64) *MiniCode {
	scene := fmt.Sprintf("u=%d", userID)
	return &MiniCode{
		Scene:      scene,
		QrcodeURL:  fmt.Sprintf(miniCodeURLFmt, url.QueryEscape(scene)),
		ShareImage: miniShareImage,
		ShareTitle: miniShareTitle,
		PagePath:   homePagePath,
	}
}

// Register 透传用户注册。
func (a *Aggregator) Register(ctx context.Context, username, password, phone string) (*userpb.RegisterResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Register")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.Register(ctx, &userpb.RegisterReq{Username: username, Password: password, Phone: phone})
}

// Login 透传用户登录。
func (a *Aggregator) Login(ctx context.Context, username, password string) (*userpb.LoginResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Login")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.Login(ctx, &userpb.LoginReq{Username: username, Password: password})
}

// Profile 用户资料（裁成 MiniProfile）。
func (a *Aggregator) Profile(ctx context.Context, userID int64) (*MiniProfile, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Profile")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	resp, err := a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, &BizError{Msg: resp.GetMessage()}
	}
	return &MiniProfile{Nickname: resp.GetNickname(), Avatar: resp.GetAvatar(), Level: resp.GetLevel()}, nil
}

// ListProducts 商品列表（裁成 MiniProduct 列表）；默认分页 10 条，
// 比 app 端的 20 条更小，配合小程序首屏一屏渲染。
func (a *Aggregator) ListProducts(ctx context.Context, userID int64, limit, offset int32) ([]MiniProduct, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "ListProducts")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	resp, err := a.Cli.Product.ListProducts(ctx, &productpb.ListProductsReq{UserId: userID, Limit: limit, Offset: offset})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, &BizError{Msg: resp.GetMessage()}
	}
	return trimProducts(resp.GetProducts()), nil
}

// GetProduct 商品详情（裁成 MiniProduct）。
func (a *Aggregator) GetProduct(ctx context.Context, userID, productID int64) (*MiniProduct, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "GetProduct")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	resp, err := a.Cli.Product.GetProduct(ctx, &productpb.GetProductReq{UserId: userID, ProductId: productID})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, &BizError{Msg: resp.GetMessage()}
	}
	trimmed := trimProducts([]*productpb.ProductInfo{resp.GetProduct()})
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("product %d not found", productID)
	}
	return &trimmed[0], nil
}

// CreateOrder 透传创建订单（下单响应本就只有单号与金额，无需裁剪）。
func (a *Aggregator) CreateOrder(ctx context.Context, userID int64, items []*orderpb.OrderItem) (*orderpb.CreateOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "CreateOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.CreateOrder(ctx, &orderpb.CreateOrderReq{UserId: userID, Items: items})
}

// GetOrder 订单详情（裁成 MiniOrder，不带明细数组）。
func (a *Aggregator) GetOrder(ctx context.Context, userID, orderID int64) (*MiniOrder, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "GetOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	resp, err := a.Cli.Order.GetOrder(ctx, &orderpb.GetOrderReq{UserId: userID, OrderId: orderID})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, &BizError{Msg: resp.GetMessage()}
	}
	return &MiniOrder{
		OrderNo:     resp.GetOrderNo(),
		Status:      resp.GetStatus(),
		TotalAmount: resp.GetTotalAmount(),
		ItemCount:   int32(len(resp.GetItems())),
	}, nil
}

// Report 透传订单报表（ReportRow 字段本就精简，无需二次裁剪）。
func (a *Aggregator) Report(ctx context.Context, userID int64, fromDate, toDate string) (*orderpb.ReportResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Report")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.Report(ctx, &orderpb.ReportReq{UserId: userID, FromDate: fromDate, ToDate: toDate})
}

// Coupons 优惠券列表（裁成 MiniCoupon 列表）。
func (a *Aggregator) Coupons(ctx context.Context, userID int64, includeUsed bool) ([]MiniCoupon, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-mini", "Coupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return nil, fmt.Errorf("srv-coupon unavailable")
	}
	resp, err := a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: includeUsed})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, &BizError{Msg: resp.GetMessage()}
	}
	return trimCoupons(resp.GetCoupons()), nil
}
