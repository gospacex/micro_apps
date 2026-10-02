package handler

import (
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"

	"hubx/micro_apps/apps/bff-admin/internal/client"
	"hubx/micro_apps/apps/bff-admin/internal/service"
)

// newEngine 起一台带管理端路由组的 Hertz 引擎（下游客户端全 nil：只读接口
// 走降级兜底，不依赖真实 srv）。
func newEngine() *route.Engine {
	engine := route.NewEngine(config.NewOptions(nil))
	Register(engine, NewAPI(service.NewAggregator(&client.Clients{}, nil, nil), nil))
	return engine
}

func perform(t *testing.T, engine *route.Engine, path string, headers map[string]string) (int, string) {
	t.Helper()
	hdrs := make([]ut.Header, 0, len(headers))
	for k, v := range headers {
		hdrs = append(hdrs, ut.Header{Key: k, Value: v})
	}
	resp := ut.PerformRequest(engine, http.MethodGet, path, nil, hdrs...)
	return resp.Code, resp.Body.String()
}

// TestAdminGroupRequiresIdentity 验证管理端路由组内所有接口都校验网关透传的
// 身份头与角色头：缺头 → 401，非 admin 角色 → 401。
func TestAdminGroupRequiresIdentity(t *testing.T) {
	engine := newEngine()

	if code, _ := perform(t, engine, "/admin/dashboard", nil); code != http.StatusUnauthorized {
		t.Fatalf("no identity header: code = %d, want 401", code)
	}
	if code, _ := perform(t, engine, "/admin/dashboard", map[string]string{
		"X-User-Id":    "42",
		"X-User-Roles": "user",
	}); code != http.StatusUnauthorized {
		t.Fatalf("non-admin role: code = %d, want 401", code)
	}
	code, body := perform(t, engine, "/admin/dashboard", map[string]string{
		"X-User-Id":    "42",
		"X-User-Roles": "ops,admin",
	})
	if code != http.StatusOK {
		t.Fatalf("admin role: code = %d, want 200; body=%s", code, body)
	}
}

// TestAdminGroupIsolatedFromCPath 验证管理端路由组与 C 端路径物理隔离：
// 组外不存在任何业务路由（未加 /admin 前缀的同一路径 → 404）。
func TestAdminGroupIsolatedFromCPath(t *testing.T) {
	engine := newEngine()
	headers := map[string]string{"X-User-Id": "42", "X-User-Roles": "admin"}

	if code, _ := perform(t, engine, "/dashboard", headers); code != http.StatusNotFound {
		t.Fatalf("/dashboard without admin prefix: code = %d, want 404", code)
	}
	if code, _ := perform(t, engine, "/orders", headers); code != http.StatusNotFound {
		t.Fatalf("/orders without admin prefix: code = %d, want 404", code)
	}
}

// TestDashboardDegradesWithoutSrv 验证运营看板聚合在下游全部不可用时仍 200
// （降级兜底，不 500），响应体是统一封装且带"部分数据不可用"语义。
func TestDashboardDegradesWithoutSrv(t *testing.T) {
	engine := newEngine()
	headers := map[string]string{"X-User-Id": "42", "X-User-Roles": "admin"}

	code, body := perform(t, engine, "/admin/dashboard", headers)
	if code != http.StatusOK {
		t.Fatalf("GET /admin/dashboard: code = %d, want 200; body=%s", code, body)
	}
	for _, want := range []string{`"code":0`, `"partial":true`, `"unavailable":`} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard body missing %s: %s", want, body)
		}
	}
}

// TestAdminReadOnlyRoutes 验证管理端单域只读接口在下游未注册时以统一封装的
// 业务错误返回（不是崩溃/裸 500 文本）。
func TestAdminReadOnlyRoutes(t *testing.T) {
	engine := newEngine()
	headers := map[string]string{"X-User-Id": "42", "X-User-Roles": "admin"}

	for _, path := range []string{
		"/admin/orders",
		"/admin/orders/report",
		"/admin/pays",
		"/admin/coupons",
		"/admin/products",
	} {
		_, body := perform(t, engine, path, headers)
		if !strings.Contains(body, `"code":5000`) || !strings.Contains(body, "unavailable") {
			t.Fatalf("GET %s: body = %s, want envelope 5000 + unavailable", path, body)
		}
	}
	if code, _ := perform(t, engine, "/admin/products/abc", headers); code != http.StatusBadRequest {
		t.Fatalf("invalid product id: code = %d, want 400", code)
	}
}
