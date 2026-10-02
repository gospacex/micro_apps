// Package handler bff-web HTTP 层：Hertz 路由调用 service 编排，并用 envelope
// 输出统一响应。
//
// 身份只读取 gateway 验签后透传的 meta.HeaderUserID；BFF 不二次验签。
//
// 端差异（PC Web）：每个入口额外产出一个 handler.bff-web.<Op> span（webx Otel
// 中间件的 HTTP span 之下），并按 SSR 语义回填响应头 Cache-Control 与渲染语言。
package handler

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/gospacex/logx"
	"github.com/gospacex/webx/envelope"

	"hubx/micro_apps/apps/bff-web/internal/service"
	"hubx/micro_apps/infra/telemetry"
	"hubx/micro_apps/pkg/meta"

	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
)

// API HTTP 处理器集合。
type API struct {
	Svc *service.Aggregator
	Log logx.Logger
}

// NewAPI 构造 HTTP API。
func NewAPI(svc *service.Aggregator, logger logx.Logger) *API {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &API{Svc: svc, Log: logger}
}

// Register 注册路由；Hertz 会优先匹配 /orders/report 静态路由。
func Register(engine *route.Engine, api *API) {
	engine.POST("/users/register", api.Register)
	engine.POST("/users/login", api.Login)
	engine.GET("/users/profile", api.Profile)
	engine.GET("/products", api.ListProducts)
	engine.GET("/products/:id", api.GetProduct)
	engine.POST("/orders", api.CreateOrder)
	engine.GET("/orders/report", api.Report)
	engine.GET("/orders/:id", api.GetOrder)
	engine.GET("/coupons", api.Coupons)
	engine.GET("/home", api.Home)
}

// userIDOf 读取网关透传身份头；缺失或非法时返回 401 envelope 错误。
func userIDOf(c *app.RequestContext) (int64, error) {
	raw := string(c.Request.Header.Get(meta.HeaderUserID))
	if raw == "" {
		return 0, envelope.Errorf(envelope.CodeUnauthorized, "missing identity header")
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, envelope.Errorf(envelope.CodeUnauthorized, "invalid identity header")
	}
	return id, nil
}

// ssrHeader 按 SSR 语义回填响应头：边缘缓存策略 + 渲染语言（PC Web 端差异）。
// ssr 为 nil（缓存里没有 SSR 段的老数据）时跳过。
func ssrHeader(c *app.RequestContext, ssr *service.SSRState) {
	if ssr == nil {
		return
	}
	if ssr.CacheControl != "" {
		c.Response.Header.Set("Cache-Control", ssr.CacheControl)
	}
	if ssr.Locale != "" {
		c.Response.Header.Set("Content-Language", ssr.Locale)
	}
}

// Home 返回 PC Web 首页聚合视图（含 SEO/SSR 段）。
func (a *API) Home(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "Home")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	view, err := a.Svc.Home(ctx, userID)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "home: %v", err))
		return
	}
	ssrHeader(c, view.SSR)
	envelope.Render(c, view, nil)
}

// Register 注册用户。
func (a *API) Register(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "Register")
	defer span.End()
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Phone    string `json:"phone"`
	}
	if err := c.BindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "username/password required"))
		return
	}
	resp, err := a.Svc.Register(ctx, req.Username, req.Password, req.Phone)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "register: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// Login 登录用户。
func (a *API) Login(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "Login")
	defer span.End()
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.BindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "username/password required"))
		return
	}
	resp, err := a.Svc.Login(ctx, req.Username, req.Password)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "login: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// Profile 返回当前用户资料（PC Web 裁剪卡片）。
func (a *API) Profile(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "Profile")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.Profile(ctx, userID)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "profile: %v", err))
		return
	}
	envelope.Render(c, resp, nil)
}

// ListProducts 返回商品列表（含 ItemList 结构化数据）。
func (a *API) ListProducts(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "ListProducts")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 32)
	offset, _ := strconv.ParseInt(c.Query("offset"), 10, 32)
	view, err := a.Svc.ListProducts(ctx, userID, int32(limit), int32(offset))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "products: %v", err))
		return
	}
	ssrHeader(c, view.SSR)
	envelope.Render(c, view, nil)
}

// GetProduct 返回商品详情（含 JSON-LD Product 结构化数据）。
func (a *API) GetProduct(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "GetProduct")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	productID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || productID <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid product id"))
		return
	}
	view, err := a.Svc.ProductDetail(ctx, userID, productID)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "product: %v", err))
		return
	}
	ssrHeader(c, view.SSR)
	envelope.Render(c, view, nil)
}

type createOrderReq struct {
	Items []struct {
		ProductID int64 `json:"product_id"`
		Quantity  int32 `json:"quantity"`
		UnitPrice int64 `json:"unit_price"`
	} `json:"items"`
}

// CreateOrder 创建订单。
func (a *API) CreateOrder(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "CreateOrder")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	var req createOrderReq
	if err := c.BindJSON(&req); err != nil || len(req.Items) == 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "items required"))
		return
	}
	items := make([]*orderpb.OrderItem, 0, len(req.Items))
	for _, item := range req.Items {
		if item.ProductID <= 0 || item.Quantity <= 0 {
			envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid item"))
			return
		}
		items = append(items, &orderpb.OrderItem{
			ProductId: item.ProductID,
			Quantity:  item.Quantity,
			UnitPrice: item.UnitPrice,
		})
	}
	resp, err := a.Svc.CreateOrder(ctx, userID, items)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "create order: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// GetOrder 返回订单详情（裁剪视图）。
func (a *API) GetOrder(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "GetOrder")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	orderID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || orderID <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid order id"))
		return
	}
	view, err := a.Svc.GetOrder(ctx, userID, orderID)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "order: %v", err))
		return
	}
	envelope.Render(c, view, nil)
}

// Report 返回订单报表。
func (a *API) Report(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "Report")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.Report(ctx, userID, c.Query("from_date"), c.Query("to_date"))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "report: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// Coupons 返回优惠券列表（裁剪卡片）。
func (a *API) Coupons(ctx context.Context, c *app.RequestContext) {
	ctx, span := telemetry.Start(ctx, "handler", "bff-web", "Coupons")
	defer span.End()
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	view, err := a.Svc.Coupons(ctx, userID, c.Query("include_used") == "true")
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "coupons: %v", err))
		return
	}
	envelope.Render(c, view, nil)
}
