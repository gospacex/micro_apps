// Package handler bff-admin HTTP 层：Hertz 路由调用 service 编排，并用 envelope
// 输出统一响应。
//
// 管理端隔离：业务路由全部挂在独立路由组 /admin 下（与 C 端 BFF 的 /users、
// /orders 等路径互不相干），组内统一挂 adminGuard 中间件校验网关透传的身份
// 与角色头——BFF 不二次验签，只读 Header。
package handler

import (
	"context"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/gospacex/logx"
	"github.com/gospacex/webx/envelope"

	"hubx/micro_apps/apps/bff-admin/internal/service"
	"hubx/micro_apps/pkg/meta"
)

const (
	// headerRoles 网关验签后透传的角色头（与 gateway RolesHeader 同值）。
	// pkg/meta 只定义身份头，角色头属管理端语义，本服务自持常量（不共享业务代码）。
	headerRoles = "X-User-Roles"
	// roleAdmin 允许访问管理端路由组的角色。
	roleAdmin = "admin"
	// adminPrefix 管理端独立路由组前缀（与 C 端路由物理隔离）。
	adminPrefix = "/admin"
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

// Register 注册路由：只读接口全部收在 /admin 管理端路由组内，组外不暴露任何
// 业务路径（/healthz 等探针由 webx 自挂，与本组无关）。
func Register(engine *route.Engine, api *API) {
	admin := engine.Group(adminPrefix, api.adminGuard)
	admin.GET("/dashboard", api.Dashboard)
	admin.GET("/users/:id", api.UserProfile)
	admin.GET("/users/:id/check", api.CheckUser)
	admin.GET("/products", api.ListProducts)
	admin.GET("/products/:id", api.GetProduct)
	admin.GET("/orders/report", api.Report)
	admin.GET("/orders/:id", api.GetOrder)
	admin.GET("/orders", api.ListOrders)
	admin.GET("/pays", api.ListPayFlows)
	admin.GET("/coupons", api.ListCoupons)
}

// adminGuard 管理端路由组中间件：校验网关透传的身份头与角色头，
// 并把身份写进请求上下文（下游只读，不再验签）。
func (a *API) adminGuard(_ context.Context, c *app.RequestContext) {
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		c.Abort()
		return
	}
	if !hasRole(c, roleAdmin) {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeUnauthorized, "admin role required"))
		c.Abort()
		return
	}
	c.Set("admin_user_id", userID)
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

// hasRole 判断角色头（逗号分隔）是否含目标角色。
func hasRole(c *app.RequestContext, role string) bool {
	for _, r := range strings.Split(string(c.Request.Header.Get(headerRoles)), ",") {
		if strings.EqualFold(strings.TrimSpace(r), role) {
			return true
		}
	}
	return false
}

// queryInt 读取分页类 query 参数，非法或缺省回退 def。
func queryInt(c *app.RequestContext, key string, def int32) int32 {
	v, err := strconv.ParseInt(c.Query(key), 10, 32)
	if err != nil || v <= 0 {
		return def
	}
	return int32(v)
}

// Dashboard 返回运营看板聚合视图。
func (a *API) Dashboard(ctx context.Context, c *app.RequestContext) {
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	view, err := a.Svc.Dashboard(ctx, userID,
		queryInt(c, "page", 1), queryInt(c, "page_size", 20),
		c.Query("from_date"), c.Query("to_date"))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "dashboard: %v", err))
		return
	}
	envelope.Render(c, view, nil)
}

// UserProfile 返回指定用户资料（管理端按 id 查任意用户）。
func (a *API) UserProfile(ctx context.Context, c *app.RequestContext) {
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	target, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || target <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid user id"))
		return
	}
	if target != userID {
		// 管理端允许越权查看他人资料，身份头留痕即可。
		a.Log.InfoContext(ctx, "admin cross query",
			logx.Any("operator", userID), logx.Any("target", target))
	}
	resp, err := a.Svc.Profile(ctx, target)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "profile: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// CheckUser 校验指定用户是否活跃。
func (a *API) CheckUser(ctx context.Context, c *app.RequestContext) {
	_, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	target, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || target <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid user id"))
		return
	}
	resp, err := a.Svc.CheckUser(ctx, target)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "check user: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// ListProducts 返回商品库存列表（分页）。
func (a *API) ListProducts(ctx context.Context, c *app.RequestContext) {
	if _, err := userIDOf(c); err != nil {
		envelope.Render(c, nil, err)
		return
	}
	limit := queryInt(c, "limit", 20)
	resp, err := a.Svc.ListProducts(ctx, limit, queryInt(c, "offset", 0))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "products: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// GetProduct 返回商品详情。
func (a *API) GetProduct(ctx context.Context, c *app.RequestContext) {
	if _, err := userIDOf(c); err != nil {
		envelope.Render(c, nil, err)
		return
	}
	productID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || productID <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid product id"))
		return
	}
	resp, err := a.Svc.GetProduct(ctx, productID)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "product: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// Report 返回订单报表（Hertz 优先匹配 /admin/orders/report 静态路由）。
func (a *API) Report(ctx context.Context, c *app.RequestContext) {
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

// GetOrder 返回订单详情。
func (a *API) GetOrder(ctx context.Context, c *app.RequestContext) {
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
	resp, err := a.Svc.GetOrder(ctx, userID, orderID)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "order: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// ListOrders 返回订单流水（分页）。
func (a *API) ListOrders(ctx context.Context, c *app.RequestContext) {
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.ListOrders(ctx, userID, c.Query("from_date"), c.Query("to_date"), queryInt(c, "limit", 20))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "orders: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// ListPayFlows 返回支付流水。
func (a *API) ListPayFlows(ctx context.Context, c *app.RequestContext) {
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.ListPayFlows(ctx, userID, c.Query("from_date"), c.Query("to_date"), queryInt(c, "limit", 20))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "pays: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// ListCoupons 返回优惠券列表（后台看全量，含已使用）。
func (a *API) ListCoupons(ctx context.Context, c *app.RequestContext) {
	userID, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.ListCoupons(ctx, userID, true)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "coupons: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}
