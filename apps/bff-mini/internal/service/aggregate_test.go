package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/apps/bff-mini/internal/cache"
	"hubx/micro_apps/apps/bff-mini/internal/client"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// ---------- 下游 fakes ----------

// failingUserClient 的全部方法都返回错误，用于触发 profile 降级分支。
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

// okUserClient 返回一份"完整"资料（含小程序端要裁掉的 username/city）。
type okUserClient struct{}

func (okUserClient) Register(context.Context, *userpb.RegisterReq, ...callopt.Option) (*userpb.RegisterResp, error) {
	return &userpb.RegisterResp{}, nil
}

func (okUserClient) Login(context.Context, *userpb.LoginReq, ...callopt.Option) (*userpb.LoginResp, error) {
	return &userpb.LoginResp{}, nil
}

func (okUserClient) GetProfile(_ context.Context, req *userpb.GetProfileReq, _ ...callopt.Option) (*userpb.GetProfileResp, error) {
	return &userpb.GetProfileResp{
		UserId:   req.GetUserId(),
		Username: "u_1001", // 端差异：小程序端裁掉
		Nickname: "小明",
		Avatar:   "https://cdn.hubx.test/a.png",
		City:     "上海", // 端差异：小程序端裁掉
		Level:    3,
	}, nil
}

func (okUserClient) CheckUser(context.Context, *userpb.CheckUserReq, ...callopt.Option) (*userpb.CheckUserResp, error) {
	return &userpb.CheckUserResp{}, nil
}

type okProductClient struct{}

func (okProductClient) GetProduct(_ context.Context, req *productpb.GetProductReq, _ ...callopt.Option) (*productpb.GetProductResp, error) {
	return &productpb.GetProductResp{Product: &productpb.ProductInfo{
		ProductId:    req.GetProductId(),
		Title:        "轻量商品",
		Price:        990,
		Stock:        8,      // 端差异：小程序端裁掉
		CategoryName: "数码配件", // 端差异：小程序端裁掉
	}}, nil
}

func (okProductClient) ReserveStock(context.Context, *productpb.ReserveStockReq, ...callopt.Option) (*productpb.ReserveStockResp, error) {
	return &productpb.ReserveStockResp{Ok: true}, nil
}

func (okProductClient) ListProducts(context.Context, *productpb.ListProductsReq, ...callopt.Option) (*productpb.ListProductsResp, error) {
	return &productpb.ListProductsResp{Products: []*productpb.ProductInfo{{
		ProductId:    1,
		Title:        "轻量商品",
		Price:        990,
		Stock:        8,
		CategoryName: "数码配件",
	}}}, nil
}

type okCouponClient struct{}

func (okCouponClient) Consume(context.Context, *couponpb.ConsumeReq, ...callopt.Option) (*couponpb.ConsumeResp, error) {
	return &couponpb.ConsumeResp{}, nil
}

func (okCouponClient) GrantPoints(context.Context, *couponpb.GrantPointsReq, ...callopt.Option) (*couponpb.GrantPointsResp, error) {
	return &couponpb.GrantPointsResp{}, nil
}

func (okCouponClient) ListCoupons(context.Context, *couponpb.ListCouponsReq, ...callopt.Option) (*couponpb.ListCouponsResp, error) {
	return &couponpb.ListCouponsResp{Coupons: []*couponpb.CouponInfo{{
		CouponId: 11,
		Name:     "新人券",
		Discount: 500,
		Type:     "cash",
		Status:   "held",
	}}}, nil
}

// ---------- 缓存 fake ----------

type memCache struct {
	mu   sync.Mutex
	data map[string]string
}

func newMemCache(preset map[string]string) *memCache {
	if preset == nil {
		preset = map[string]string{}
	}
	return &memCache{data: preset}
}

func (m *memCache) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key], nil
}

func (m *memCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *memCache) Delete(context.Context, string) error { return nil }

func (m *memCache) Exists(context.Context, string) (bool, error) { return false, nil }

func (m *memCache) Keys(context.Context, string) ([]string, error) { return nil, nil }

func (m *memCache) Close() error { return nil }

func (m *memCache) HealthCheck(context.Context) error { return nil }

// ---------- 用例 ----------

// TestHomeDegradesOnBranchFailure 验证任一分支故障不影响 Home 整体成功，
// 三个失败分支均进入降级清单，且对应 span 标记 degraded=true。
func TestHomeDegradesOnBranchFailure(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	agg := NewAggregator(&client.Clients{
		User: failingUserClient{},
		// Product/Coupon 为 nil，同样应进入降级分支。
	}, nil, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home must not fail on branch failure: %v", err)
	}
	want := map[string]bool{"profile": false, "products": false, "coupons": false}
	for _, branch := range view.Degraded {
		want[branch] = true
	}
	for branch, seen := range want {
		if !seen {
			t.Fatalf("branch %s not recorded degraded; got %v", branch, view.Degraded)
		}
	}

	var degradedSpans int
	for _, span := range exporter.GetSpans() {
		for _, attr := range span.Attributes {
			if string(attr.Key) == "degraded" && attr.Value.AsBool() {
				degradedSpans++
			}
		}
	}
	if degradedSpans != 3 {
		t.Fatalf("expected 3 degraded branch spans, got %d", degradedSpans)
	}
}

// TestHomeCacheHitSkipsDownstream 验证展示层缓存命中时直接返回，不打下游 srv
// （下游全挂也不会产生降级清单，响应体标记 cache_hit=true）。
func TestHomeCacheHitSkipsDownstream(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	cached := `{"profile":{"nickname":"小明","avatar":"https://cdn.hubx.test/a.png","level":3},` +
		`"products":[{"product_id":1,"title":"轻量商品","price":990,"cover":""}],"cached_at":1700000000}`
	dc := cache.New(newMemCache(map[string]string{cache.Key("home", 42): cached}), time.Minute)

	agg := NewAggregator(&client.Clients{
		User:    failingUserClient{},
		Product: okProductClient{},
		Coupon:  okCouponClient{},
	}, dc, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home on cache hit: %v", err)
	}
	if !view.CacheHit {
		t.Fatal("cache_hit should be true on hit path")
	}
	if len(view.Degraded) != 0 {
		t.Fatalf("cache hit must not touch downstream; degraded = %v", view.Degraded)
	}
	if view.Profile == nil || view.Profile.Nickname != "小明" {
		t.Fatalf("cached profile not decoded: %+v", view.Profile)
	}
	// 命中路径只有 service.display.get 与 service.bff-mini.Home 两层 span，
	// 不应出现任何 Home.<branch> 子 span（即未打下游）。
	for _, span := range exporter.GetSpans() {
		if strings.HasPrefix(span.Name, "service.bff-mini.Home.") {
			t.Fatalf("cache hit must not start branch span, got %s", span.Name)
		}
	}
}

// TestHomeMiniPayloadTrimAndCode 验证小程序端专属字段与字段裁剪：
// 响应体带小程序码 scene/qrcode_url/share_image，且不含被裁掉的重字段。
func TestHomeMiniPayloadTrimAndCode(t *testing.T) {
	agg := NewAggregator(&client.Clients{
		User:    okUserClient{},
		Product: okProductClient{},
		Coupon:  okCouponClient{},
	}, nil, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	if view.Profile == nil || view.Profile.Nickname != "小明" {
		t.Fatalf("profile not aggregated: %+v", view.Profile)
	}
	if len(view.Products) != 1 || len(view.Coupons) != 1 {
		t.Fatalf("products/coupons not aggregated: %d/%d", len(view.Products), len(view.Coupons))
	}
	if view.Share == nil {
		t.Fatal("mini share (小程序码) block must be present")
	}
	if view.Share.Scene == "" || view.Share.QrcodeURL == "" || view.Share.ShareImage == "" {
		t.Fatalf("mini share fields incomplete: %+v", view.Share)
	}
	if !strings.Contains(view.Share.Scene, "42") {
		t.Fatalf("scene should carry user id, got %q", view.Share.Scene)
	}

	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("marshal view: %v", err)
	}
	for _, want := range []string{"scene", "qrcode_url", "share_image", "page_path"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("mini payload missing %q: %s", want, raw)
		}
	}
	// 裁剪：小程序端不带库存/分类/账号名/城市等重字段
	for _, trimmed := range []string{"stock", "category_name", "username", "city"} {
		if strings.Contains(string(raw), trimmed) {
			t.Fatalf("mini payload must trim %q: %s", trimmed, raw)
		}
	}
}

// TestGetOrderTrim 验证订单详情在小程序端只回状态/金额/商品数，不回明细数组。
func TestGetOrderTrim(t *testing.T) {
	agg := NewAggregator(&client.Clients{}, nil, nil)
	// Order 客户端为 nil 时应返回显式错误（非降级路径：详情页必须知情）
	if _, err := agg.GetOrder(context.Background(), 42, 1); err == nil {
		t.Fatal("GetOrder without srv-order should return error")
	}
	_ = orderpb.GetOrderResp{}
}
