package service

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/apps/bff-admin/internal/cache"
	"hubx/micro_apps/apps/bff-admin/internal/client"

	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// mockCache 内存 cachex.Cache 实现：只服务展示层缓存命中/未命中两条路径。
type mockCache struct {
	data map[string]string
}

func (m *mockCache) Get(_ context.Context, key string) (string, error) {
	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return "", errors.New("miss")
}

func (m *mockCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	m.data[key] = value
	return nil
}

func (m *mockCache) Delete(context.Context, string) error { return nil }

func (m *mockCache) Exists(context.Context, string) (bool, error) { return false, nil }

func (m *mockCache) Keys(context.Context, string) ([]string, error) { return nil, nil }

func (m *mockCache) Close() error { return nil }

func (m *mockCache) HealthCheck(context.Context) error { return nil }

// failingUserClient 的全部方法都返回错误，用于触发 users 降级分支。
type failingUserClient struct{}

func (f failingUserClient) Register(context.Context, *userpb.RegisterReq, ...callopt.Option) (*userpb.RegisterResp, error) {
	return nil, errors.New("user down")
}

func (f failingUserClient) Login(context.Context, *userpb.LoginReq, ...callopt.Option) (*userpb.LoginResp, error) {
	return nil, errors.New("user down")
}

func (f failingUserClient) GetProfile(context.Context, *userpb.GetProfileReq, ...callopt.Option) (*userpb.GetProfileResp, error) {
	return nil, errors.New("user down")
}

func (f failingUserClient) CheckUser(context.Context, *userpb.CheckUserReq, ...callopt.Option) (*userpb.CheckUserResp, error) {
	return nil, errors.New("user down")
}

// testExporter 全包共享：infra/telemetry 的 current 在包初始化时绑定 otel
// 全局 delegate，只认第一个注册的 provider，故这里统一注册一次、各用例 Reset。
var testExporter *tracetest.InMemoryExporter

func TestMain(m *testing.M) {
	testExporter = tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(testExporter))
	otel.SetTracerProvider(tp)
	code := m.Run()
	_ = tp.Shutdown(context.Background())
	os.Exit(code)
}

// TestDashboardDegradesOnSrvFailure 验证五个 srv 分支全不可用时 Dashboard 仍
// 成功返回（不 500），且响应带"部分数据不可用"语义（partial + unavailable）。
func TestDashboardDegradesOnSrvFailure(t *testing.T) {
	testExporter.Reset()

	agg := NewAggregator(&client.Clients{
		User: failingUserClient{},
		// Product/Order/Pay/Coupon 为 nil，同样应进入降级分支。
	}, nil, nil)

	view, err := agg.Dashboard(context.Background(), 42, 1, 20, "2026-01-01", "2026-01-31")
	if err != nil {
		t.Fatalf("dashboard must not fail on branch failure: %v", err)
	}
	if !view.Partial {
		t.Fatal("partial must be true when some branches degraded")
	}
	want := map[string]bool{
		"users": false, "orders": false, "pays": false, "products": false, "coupons": false,
	}
	for _, branch := range view.Unavailable {
		want[branch] = true
	}
	for branch, seen := range want {
		if !seen {
			t.Fatalf("branch %s not marked unavailable; got %v", branch, view.Unavailable)
		}
	}

	var degradedSpans int
	for _, span := range testExporter.GetSpans() {
		for _, attr := range span.Attributes {
			if string(attr.Key) == "degraded" && attr.Value.AsBool() {
				degradedSpans++
			}
		}
	}
	if degradedSpans != 5 {
		t.Fatalf("expected 5 degraded branch spans, got %d", degradedSpans)
	}
}

// TestDashboardCacheHitSkipsSrv 验证展示层缓存命中时不再打任何 srv：
// 下游全部不可用也不会出现降级分支。
func TestDashboardCacheHitSkipsSrv(t *testing.T) {
	mc := &mockCache{data: map[string]string{
		// 预置一份"完整"看板：下游全部不可用时若仍打了 srv，响应必然带降级清单。
		"bff:admin:dashboard:7": `{"table":{"page":1,"page_size":20,"total":0},"cached_at":1}`,
	}}
	agg := NewAggregator(&client.Clients{User: failingUserClient{}}, cache.New(mc, 60*time.Second), nil)

	view, err := agg.Dashboard(context.Background(), 7, 1, 20, "", "")
	if err != nil {
		t.Fatalf("dashboard on cache hit: %v", err)
	}
	if !view.CacheHit {
		t.Fatal("should hit display cache under bff:admin:dashboard:7")
	}
	if view.Partial || len(view.Unavailable) != 0 || len(view.Degraded) != 0 {
		t.Fatalf("cache hit must not call srv: partial=%v unavailable=%v degraded=%v",
			view.Partial, view.Unavailable, view.Degraded)
	}
}

// TestDashboardCachesUnderAdminKey 验证未命中路径按 bff:admin:{resource}:{user_id}
// 形态回写展示层缓存。
func TestDashboardCachesUnderAdminKey(t *testing.T) {
	mc := &mockCache{data: map[string]string{}}
	agg := NewAggregator(&client.Clients{}, cache.New(mc, 60*time.Second), nil)

	if _, err := agg.Dashboard(context.Background(), 9, 1, 20, "", ""); err != nil {
		t.Fatalf("dashboard: %v", err)
	}
	if mc.data["bff:admin:dashboard:9"] == "" {
		t.Fatalf("dashboard not cached under bff:admin:dashboard:9; keys=%v", mc.data)
	}
}

// TestStatusText 验证状态枚举 → 后台表格中文文案的裁剪映射。
func TestStatusText(t *testing.T) {
	if got := StatusText("created"); got != "待支付" {
		t.Fatalf("StatusText(created) = %q, want 待支付", got)
	}
	if got := StatusText("unknown_enum"); got != "unknown_enum" {
		t.Fatalf("StatusText(unknown) = %q, want passthrough", got)
	}
}
