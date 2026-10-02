// Package service bff-web 业务编排层：errgroup 并发聚合、降级兜底、展示层缓存
// 与 PC Web 端 dto 裁剪（SEO/SSR 段）。
//
// 每个出口方法产生 service.bff-web.<Op> span；Home 的并发分支产生
// service.bff-web.Home.<branch> 子 span，降级时标记 degraded=true。
//
// 端差异（PC Web）：
//   - dto 带 SEO 段（title / description / keywords / canonical / robots /
//     og_title / og_image / structured_data）与 SSR 段（rendered / rendered_at /
//     locale），供服务端渲染直出 <head> 与 JSON-LD 结构化数据；
//   - 字段按端裁剪：profile 不下发 username，商品不下发精确库存水位（只给
//     in_stock + canonical），订单不下发 user_id；
//   - 首页聚合取「用户卡片 + 商品推荐（结构化数据来源）+ 最近订单」，
//     优惠券不在 PC 首页主链路（走 /coupons 单独取）。
package service

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"hubx/micro_apps/apps/bff-web/internal/cache"
	"hubx/micro_apps/apps/bff-web/internal/client"
	"hubx/micro_apps/infra/telemetry"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// SiteBase PC Web 站点基址：canonical / og:url / JSON-LD 的绝对 URL 由它拼出。
const SiteBase = "https://www.hubx.example"

// homeResource / productResource 展示层缓存的 resource 段（key = bff:web:{resource}:{user_id}）
const (
	homeResource    = "home"
	productResource = "product"
)

// SEOMeta PC Web 页面元信息（SEO/SSR 直出用）。
type SEOMeta struct {
	Title          string         `json:"title"`
	Description    string         `json:"description"`
	Keywords       []string       `json:"keywords,omitempty"`
	Canonical      string         `json:"canonical"`
	Robots         string         `json:"robots"`
	OgTitle        string         `json:"og_title,omitempty"`
	OgImage        string         `json:"og_image,omitempty"`
	StructuredData map[string]any `json:"structured_data,omitempty"`
}

// SSRState 服务端渲染状态（渲染时刻 / 语言 / 边缘缓存策略）。
type SSRState struct {
	Rendered     bool   `json:"rendered"`
	RenderedAt   int64  `json:"rendered_at"`
	Locale       string `json:"locale"`
	CacheControl string `json:"cache_control"`
}

// ProfileBrief 用户卡片（裁剪：不下发 username 与内部码）。
type ProfileBrief struct {
	UserID   int64  `json:"user_id"`
	Nickname string `json:"nickname,omitempty"`
	Avatar   string `json:"avatar,omitempty"`
	City     string `json:"city,omitempty"`
	Level    int32  `json:"level"`
}

// ProductCard 商品卡片（裁剪：不下发精确库存，只给 in_stock + canonical + JSON-LD）。
type ProductCard struct {
	ProductID      int64          `json:"product_id"`
	Title          string         `json:"title"`
	Price          int64          `json:"price"`
	PriceText      string         `json:"price_text"`
	CategoryName   string         `json:"category_name,omitempty"`
	Canonical      string         `json:"canonical"`
	InStock        bool           `json:"in_stock"`
	StructuredData map[string]any `json:"structured_data,omitempty"`
}

// OrderRow 订单行（裁剪：不下发 user_id）。
type OrderRow struct {
	OrderID     int64  `json:"order_id"`
	OrderNo     string `json:"order_no"`
	Status      string `json:"status"`
	TotalAmount int64  `json:"total_amount"`
	CreatedAt   int64  `json:"created_at"`
}

// CouponCard 券卡片（裁剪：不下发 used_order_id）。
type CouponCard struct {
	CouponID int64  `json:"coupon_id"`
	Name     string `json:"name,omitempty"`
	Discount int64  `json:"discount"`
	Type     string `json:"type,omitempty"`
	Status   string `json:"status,omitempty"`
}

// HomeView PC Web 首页聚合视图（含 SEO/SSR 段）；降级分支对应的字段保持 nil。
type HomeView struct {
	Profile  *ProfileBrief `json:"profile,omitempty"`
	Products []ProductCard `json:"products,omitempty"`
	Orders   []OrderRow    `json:"orders,omitempty"`
	Degraded []string      `json:"degraded,omitempty"`
	CachedAt int64         `json:"cached_at"`
	CacheHit bool          `json:"cache_hit"`
	SEO      *SEOMeta      `json:"seo"`
	SSR      *SSRState     `json:"ssr"`
}

// ProductDetailView PC Web 商品详情视图（JSON-LD Product 结构化数据）。
type ProductDetailView struct {
	Product  *ProductCard `json:"product,omitempty"`
	Degraded []string     `json:"degraded,omitempty"`
	CachedAt int64        `json:"cached_at"`
	CacheHit bool         `json:"cache_hit"`
	SEO      *SEOMeta     `json:"seo"`
	SSR      *SSRState    `json:"ssr"`
}

// ProductListView PC Web 商品列表视图（JSON-LD ItemList）。
type ProductListView struct {
	Products []ProductCard `json:"products,omitempty"`
	Degraded []string      `json:"degraded,omitempty"`
	SEO      *SEOMeta      `json:"seo"`
	SSR      *SSRState     `json:"ssr"`
}

// OrderDetailView PC Web 订单详情视图（裁剪：不下发 user_id 与内部码）。
type OrderDetailView struct {
	OrderNo     string          `json:"order_no"`
	Status      string          `json:"status"`
	TotalAmount int64           `json:"total_amount"`
	PaidAmount  int64           `json:"paid_amount"`
	Items       []OrderItemCard `json:"items,omitempty"`
}

// OrderItemCard 订单明细行（裁剪：不下发 product_id 之外的内部字段）。
type OrderItemCard struct {
	ProductID int64  `json:"product_id"`
	Name      string `json:"name,omitempty"`
	Quantity  int32  `json:"quantity"`
	UnitPrice int64  `json:"unit_price"`
}

// CouponListView PC Web 券列表视图。
type CouponListView struct {
	Coupons []CouponCard `json:"coupons,omitempty"`
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

// Home PC Web 首页聚合：缓存命中直接返回；未命中时并发拉取用户、推荐商品与
// 最近订单，单分支失败只记录降级，不中断其他分支。
func (a *Aggregator) Home(ctx context.Context, userID int64) (*HomeView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Home")
	defer span.End()

	key := cache.Key(homeResource, userID)
	var view HomeView
	if a.Cache != nil && a.Cache.Get(ctx, key, &view) {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		view.CacheHit = true
		if view.SEO == nil {
			view.SEO = homeSEO(nil)
		}
		return &view, nil
	}
	span.SetAttributes(attribute.Bool("cache.hit", false))

	view = HomeView{CachedAt: time.Now().Unix()}
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return a.fetchProfile(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchProducts(groupCtx, userID, &view, &mu) })
	group.Go(func() error { return a.fetchRecent(groupCtx, userID, &view, &mu) })
	_ = group.Wait()

	view.SEO = homeSEO(view.Profile)
	view.SSR = newSSR()
	if a.Cache != nil {
		a.Cache.Set(ctx, key, &view)
	}
	return &view, nil
}

// fetchProfile 调用 srv-user GetProfile；失败时降级 profile 分支。
func (a *Aggregator) fetchProfile(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Home.profile")
	defer span.End()
	resp, err := a.Cli.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "profile")
	}
	mu.Lock()
	view.Profile = &ProfileBrief{
		UserID:   resp.GetUserId(),
		Nickname: resp.GetNickname(),
		Avatar:   resp.GetAvatar(),
		City:     resp.GetCity(),
		Level:    resp.GetLevel(),
	}
	mu.Unlock()
	return nil
}

// fetchProducts 调用 srv-product ListProducts 取首页推荐位（SEO 结构化数据来源）。
func (a *Aggregator) fetchProducts(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Home.products")
	defer span.End()
	resp, err := a.Cli.ListProducts(ctx, &productpb.ListProductsReq{UserId: userID, Limit: 8})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "products")
	}
	cards := make([]ProductCard, 0, len(resp.GetProducts()))
	for _, p := range resp.GetProducts() {
		cards = append(cards, newProductCard(p))
	}
	mu.Lock()
	view.Products = cards
	mu.Unlock()
	return nil
}

// fetchRecent 调用 srv-order ListOrders 拉取最近五条订单流水。
func (a *Aggregator) fetchRecent(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Home.recent")
	defer span.End()
	resp, err := a.Cli.ListOrders(ctx, &orderpb.ListOrdersReq{UserId: userID, Limit: 5})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "recent_orders")
	}
	rows := make([]OrderRow, 0, len(resp.GetFlows()))
	for _, f := range resp.GetFlows() {
		rows = append(rows, OrderRow{
			OrderID:     f.GetOrderId(),
			OrderNo:     f.GetOrderNo(),
			Status:      f.GetStatus(),
			TotalAmount: f.GetTotalAmount(),
			CreatedAt:   f.GetCreatedAt(),
		})
	}
	mu.Lock()
	view.Orders = rows
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

// ProductDetail PC Web 商品详情：命中展示层缓存直接返回；未命中取商品并生成
// JSON-LD Product 结构化数据；srv-product 不可用时降级（SEO 转 noindex）。
func (a *Aggregator) ProductDetail(ctx context.Context, userID, productID int64) (*ProductDetailView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "ProductDetail")
	defer span.End()

	key := cache.Key(fmt.Sprintf("%s:%d", productResource, productID), userID)
	view := &ProductDetailView{CachedAt: time.Now().Unix()}
	if a.Cache != nil && a.Cache.Get(ctx, key, view) {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		view.CacheHit = true
		return view, nil
	}
	span.SetAttributes(attribute.Bool("cache.hit", false))

	resp, err := a.Cli.GetProduct(ctx, &productpb.GetProductReq{UserId: userID, ProductId: productID})
	if err != nil || resp.GetCode() != 0 || resp.GetProduct() == nil {
		if err != nil {
			span.RecordError(err)
		}
		span.SetAttributes(attribute.Bool("degraded", true))
		view.Degraded = append(view.Degraded, "product")
		view.SEO = productSEO(nil)
		view.SSR = newSSR()
		return view, nil
	}
	card := newProductCard(resp.GetProduct())
	view.Product = &card
	view.SEO = productSEO(resp.GetProduct())
	view.SSR = newSSR()
	if a.Cache != nil {
		a.Cache.Set(ctx, key, view)
	}
	return view, nil
}

// ListProducts PC Web 商品列表：裁剪后的卡片 + ItemList 结构化数据。
func (a *Aggregator) ListProducts(ctx context.Context, userID int64, limit, offset int32) (*ProductListView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "ListProducts")
	defer span.End()
	if limit <= 0 {
		limit = 20
	}
	resp, err := a.Cli.ListProducts(ctx, &productpb.ListProductsReq{UserId: userID, Limit: limit, Offset: offset})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("srv-product unavailable")
	}
	if resp.GetCode() != 0 {
		return nil, fmt.Errorf("%s", resp.GetMessage())
	}
	cards := make([]ProductCard, 0, len(resp.GetProducts()))
	items := make([]map[string]any, 0, len(resp.GetProducts()))
	for i, p := range resp.GetProducts() {
		card := newProductCard(p)
		cards = append(cards, card)
		items = append(items, map[string]any{
			"@type":    "ListItem",
			"position": i + 1,
			"url":      card.Canonical,
			"name":     card.Title,
		})
	}
	return &ProductListView{
		Products: cards,
		SEO: &SEOMeta{
			Title:       "全部商品 | HubX 商城",
			Description: "HubX 商城商品列表，按类目浏览在售商品。",
			Keywords:    []string{"商城", "商品", "HubX"},
			Canonical:   SiteBase + "/products",
			Robots:      "index,follow",
			StructuredData: map[string]any{
				"@context":        "https://schema.org",
				"@type":           "ItemList",
				"itemListElement": items,
			},
		},
		SSR: newSSR(),
	}, nil
}

// Register 透传用户注册。
func (a *Aggregator) Register(ctx context.Context, username, password, phone string) (*userpb.RegisterResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Register")
	defer span.End()
	return a.Cli.Register(ctx, &userpb.RegisterReq{Username: username, Password: password, Phone: phone})
}

// Login 透传用户登录。
func (a *Aggregator) Login(ctx context.Context, username, password string) (*userpb.LoginResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Login")
	defer span.End()
	return a.Cli.Login(ctx, &userpb.LoginReq{Username: username, Password: password})
}

// Profile PC Web 用户资料（裁剪后卡片）。
func (a *Aggregator) Profile(ctx context.Context, userID int64) (*ProfileBrief, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Profile")
	defer span.End()
	resp, err := a.Cli.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("srv-user unavailable")
	}
	if resp.GetCode() != 0 {
		return nil, fmt.Errorf("%s", resp.GetMessage())
	}
	return &ProfileBrief{
		UserID:   resp.GetUserId(),
		Nickname: resp.GetNickname(),
		Avatar:   resp.GetAvatar(),
		City:     resp.GetCity(),
		Level:    resp.GetLevel(),
	}, nil
}

// CreateOrder 透传创建订单。
func (a *Aggregator) CreateOrder(ctx context.Context, userID int64, items []*orderpb.OrderItem) (*orderpb.CreateOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "CreateOrder")
	defer span.End()
	return a.Cli.CreateOrder(ctx, &orderpb.CreateOrderReq{UserId: userID, Items: items})
}

// GetOrder PC Web 订单详情（裁剪后视图）。
func (a *Aggregator) GetOrder(ctx context.Context, userID, orderID int64) (*OrderDetailView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "GetOrder")
	defer span.End()
	resp, err := a.Cli.GetOrder(ctx, &orderpb.GetOrderReq{UserId: userID, OrderId: orderID})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("srv-order unavailable")
	}
	if resp.GetCode() != 0 {
		return nil, fmt.Errorf("%s", resp.GetMessage())
	}
	items := make([]OrderItemCard, 0, len(resp.GetItems()))
	for _, it := range resp.GetItems() {
		items = append(items, OrderItemCard{
			ProductID: it.GetProductId(),
			Name:      it.GetName(),
			Quantity:  it.GetQuantity(),
			UnitPrice: it.GetUnitPrice(),
		})
	}
	return &OrderDetailView{
		OrderNo:     resp.GetOrderNo(),
		Status:      resp.GetStatus(),
		TotalAmount: resp.GetTotalAmount(),
		PaidAmount:  resp.GetPaidAmount(),
		Items:       items,
	}, nil
}

// Report 透传订单报表查询。
func (a *Aggregator) Report(ctx context.Context, userID int64, fromDate, toDate string) (*orderpb.ReportResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Report")
	defer span.End()
	return a.Cli.Report(ctx, &orderpb.ReportReq{UserId: userID, FromDate: fromDate, ToDate: toDate})
}

// Coupons PC Web 券列表（裁剪后卡片）。
func (a *Aggregator) Coupons(ctx context.Context, userID int64, includeUsed bool) (*CouponListView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-web", "Coupons")
	defer span.End()
	resp, err := a.Cli.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: includeUsed})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("srv-coupon unavailable")
	}
	if resp.GetCode() != 0 {
		return nil, fmt.Errorf("%s", resp.GetMessage())
	}
	cards := make([]CouponCard, 0, len(resp.GetCoupons()))
	for _, c := range resp.GetCoupons() {
		cards = append(cards, CouponCard{
			CouponID: c.GetCouponId(),
			Name:     c.GetName(),
			Discount: c.GetDiscount(),
			Type:     c.GetType(),
			Status:   c.GetStatus(),
		})
	}
	return &CouponListView{Coupons: cards}, nil
}

// ===== SEO / SSR 构造 =====

// newProductCard 按 PC Web 端裁剪策略构造商品卡片。
func newProductCard(p *productpb.ProductInfo) ProductCard {
	price := p.GetPrice()
	return ProductCard{
		ProductID:    p.GetProductId(),
		Title:        p.GetTitle(),
		Price:        price,
		PriceText:    priceText(price),
		CategoryName: p.GetCategoryName(),
		Canonical:    SiteBase + "/product/" + strconv.FormatInt(p.GetProductId(), 10),
		InStock:      p.GetStock() > 0,
		StructuredData: map[string]any{
			"@context": "https://schema.org",
			"@type":    "Product",
			"sku":      strconv.FormatInt(p.GetProductId(), 10),
			"name":     p.GetTitle(),
			"category": p.GetCategoryName(),
			"offers": map[string]any{
				"@type":         "Offer",
				"price":         priceText(price),
				"priceCurrency": "CNY",
				"availability":  availability(p.GetStock()),
			},
		},
	}
}

// homeSEO 首页 SEO：标题按用户个性化（降级无资料时用默认标题）。
func homeSEO(profile *ProfileBrief) *SEOMeta {
	title := "HubX 商城 - 首页"
	description := "HubX 商城：精选商品、优惠券与订单一站式管理。"
	keywords := []string{"HubX", "商城", "购物"}
	if profile != nil && profile.Nickname != "" {
		title = profile.Nickname + " 的个人首页 | HubX 商城"
		description = profile.Nickname + "，欢迎回到 HubX 商城：查看推荐商品与最近订单。"
		keywords = append(keywords, profile.Nickname)
	}
	return &SEOMeta{
		Title:       title,
		Description: description,
		Keywords:    keywords,
		Canonical:   SiteBase + "/",
		Robots:      "index,follow",
		OgTitle:     title,
		StructuredData: map[string]any{
			"@context": "https://schema.org",
			"@type":    "WebSite",
			"name":     "HubX 商城",
			"url":      SiteBase + "/",
		},
	}
}

// productSEO 商品详情 SEO：p 为 nil（降级）时转 noindex，避免空页面被索引。
func productSEO(p *productpb.ProductInfo) *SEOMeta {
	if p == nil {
		return &SEOMeta{
			Title:     "商品暂时不可用 | HubX 商城",
			Canonical: SiteBase + "/products",
			Robots:    "noindex,follow",
		}
	}
	title := p.GetTitle() + " - " + p.GetCategoryName() + " | HubX 商城"
	card := newProductCard(p)
	return &SEOMeta{
		Title:          title,
		Description:    p.GetTitle() + "，仅售 " + card.PriceText + " 元，HubX 商城正品保障。",
		Keywords:       []string{p.GetTitle(), p.GetCategoryName(), "HubX"},
		Canonical:      card.Canonical,
		Robots:         "index,follow",
		OgTitle:        title,
		StructuredData: card.StructuredData,
	}
}

// newSSR PC Web 服务端渲染状态（渲染时刻 / 默认语言 / 边缘缓存策略）。
func newSSR() *SSRState {
	return &SSRState{
		Rendered:     true,
		RenderedAt:   time.Now().Unix(),
		Locale:       "zh-CN",
		CacheControl: "public, max-age=60",
	}
}

// priceText 分 → 元（两位小数字符串，JSON-LD price 要求）。
func priceText(cents int64) string {
	return strconv.FormatFloat(float64(cents)/100, 'f', 2, 64)
}

// availability 库存 → schema.org 可用性。
func availability(stock int32) string {
	if stock > 0 {
		return "https://schema.org/InStock"
	}
	return "https://schema.org/OutOfStock"
}
