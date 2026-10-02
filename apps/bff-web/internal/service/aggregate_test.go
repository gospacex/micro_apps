package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/apps/bff-web/internal/cache"
	"hubx/micro_apps/apps/bff-web/internal/client"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// ===== 下游客户端 fake =====

type fakeUserClient struct {
	mu      sync.Mutex
	calls   int
	profile *userpb.GetProfileResp
	err     error
}

func (f *fakeUserClient) Register(context.Context, *userpb.RegisterReq, ...callopt.Option) (*userpb.RegisterResp, error) {
	return &userpb.RegisterResp{}, nil
}
func (f *fakeUserClient) Login(context.Context, *userpb.LoginReq, ...callopt.Option) (*userpb.LoginResp, error) {
	return &userpb.LoginResp{}, nil
}
func (f *fakeUserClient) CheckUser(context.Context, *userpb.CheckUserReq, ...callopt.Option) (*userpb.CheckUserResp, error) {
	return &userpb.CheckUserResp{}, nil
}
func (f *fakeUserClient) GetProfile(context.Context, *userpb.GetProfileReq, ...callopt.Option) (*userpb.GetProfileResp, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.profile, nil
}

type fakeProductClient struct {
	mu        sync.Mutex
	calls     int
	list      *productpb.ListProductsResp
	detail    *productpb.GetProductResp
	err       error
	callCount *int
}

func (f *fakeProductClient) mark() {
	f.mu.Lock()
	f.calls++
	if f.callCount != nil {
		*f.callCount++
	}
	f.mu.Unlock()
}

func (f *fakeProductClient) ListProducts(context.Context, *productpb.ListProductsReq, ...callopt.Option) (*productpb.ListProductsResp, error) {
	f.mark()
	if f.err != nil {
		return nil, f.err
	}
	return f.list, nil
}
func (f *fakeProductClient) GetProduct(context.Context, *productpb.GetProductReq, ...callopt.Option) (*productpb.GetProductResp, error) {
	f.mark()
	if f.err != nil {
		return nil, f.err
	}
	return f.detail, nil
}
func (f *fakeProductClient) ReserveStock(context.Context, *productpb.ReserveStockReq, ...callopt.Option) (*productpb.ReserveStockResp, error) {
	return &productpb.ReserveStockResp{}, nil
}

type fakeOrderClient struct {
	mu    sync.Mutex
	calls int
	list  *orderpb.ListOrdersResp
	err   error
}

func (f *fakeOrderClient) mark() {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
}

func (f *fakeOrderClient) ListOrders(context.Context, *orderpb.ListOrdersReq, ...callopt.Option) (*orderpb.ListOrdersResp, error) {
	f.mark()
	if f.err != nil {
		return nil, f.err
	}
	return f.list, nil
}
func (f *fakeOrderClient) CreateOrder(context.Context, *orderpb.CreateOrderReq, ...callopt.Option) (*orderpb.CreateOrderResp, error) {
	return &orderpb.CreateOrderResp{}, nil
}
func (f *fakeOrderClient) GetOrder(context.Context, *orderpb.GetOrderReq, ...callopt.Option) (*orderpb.GetOrderResp, error) {
	return &orderpb.GetOrderResp{}, nil
}
func (f *fakeOrderClient) Report(context.Context, *orderpb.ReportReq, ...callopt.Option) (*orderpb.ReportResp, error) {
	return &orderpb.ReportResp{}, nil
}

type fakeCouponClient struct{}

func (fakeCouponClient) Consume(context.Context, *couponpb.ConsumeReq, ...callopt.Option) (*couponpb.ConsumeResp, error) {
	return &couponpb.ConsumeResp{}, nil
}
func (fakeCouponClient) GrantPoints(context.Context, *couponpb.GrantPointsReq, ...callopt.Option) (*couponpb.GrantPointsResp, error) {
	return &couponpb.GrantPointsResp{}, nil
}
func (fakeCouponClient) ListCoupons(context.Context, *couponpb.ListCouponsReq, ...callopt.Option) (*couponpb.ListCouponsResp, error) {
	return &couponpb.ListCouponsResp{}, nil
}

// memCache 内存 cachex 实现（记录 get/set 的 key 与 TTL）。
type memCache struct {
	mu     sync.Mutex
	data   map[string]string
	ttls   map[string]time.Duration
	getLog []string
	setLog []string
}

func newMemCache() *memCache {
	return &memCache{data: map[string]string{}, ttls: map[string]time.Duration{}}
}

func (m *memCache) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getLog = append(m.getLog, key)
	v, ok := m.data[key]
	if !ok {
		return "", errors.New("miss")
	}
	return v, nil
}

func (m *memCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setLog = append(m.setLog, key)
	m.data[key] = value
	m.ttls[key] = ttl
	return nil
}

func (m *memCache) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}
func (m *memCache) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	return ok, nil
}
func (m *memCache) Keys(context.Context, string) ([]string, error) { return nil, nil }
func (m *memCache) Close() error                                   { return nil }
func (m *memCache) HealthCheck(context.Context) error              { return nil }

func (m *memCache) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.getLog...)
}

// seedHome 预置一份可命中的首页视图。
func (m *memCache) seedHome(t *testing.T, userID int64) {
	t.Helper()
	raw, err := json.Marshal(&HomeView{
		CachedAt: 1,
		SEO:      &SEOMeta{Title: "cached title", Canonical: SiteBase + "/"},
		SSR:      &SSRState{Rendered: true, Locale: "zh-CN"},
	})
	if err != nil {
		t.Fatalf("marshal cached view: %v", err)
	}
	m.mu.Lock()
	m.data[cache.Key("home", userID)] = string(raw)
	m.mu.Unlock()
}

// OTel 全局 TracerProvider 只允许装配一次（第二次 Set 被忽略），
// 因此整包共用同一个内存 exporter，各用例开头 Reset。
var testExporter = tracetest.NewInMemoryExporter()

func TestMain(m *testing.M) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(testExporter))
	otel.SetTracerProvider(tp)
	code := m.Run()
	_ = tp.Shutdown(context.Background())
	os.Exit(code)
}

func withTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	testExporter.Reset()
	return testExporter
}

func countDegradedSpans(spans tracetest.SpanStubs) int {
	var n int
	for _, span := range spans {
		for _, attr := range span.Attributes {
			if string(attr.Key) == "degraded" && attr.Value.AsBool() {
				n++
			}
		}
	}
	return n
}

// TestHomeDegradesOnBranchFailure 验证任一分支故障不影响 Home 整体成功，
// 三个失败分支均进入降级清单，且对应 span 标记 degraded=true。
func TestHomeDegradesOnBranchFailure(t *testing.T) {
	exporter := withTracer(t)

	agg := NewAggregator(&client.Clients{
		User: &fakeUserClient{err: errors.New("user down")},
		// Product / Order 为 nil，同样应进入降级分支。
	}, nil, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home must not fail on branch failure: %v", err)
	}
	want := map[string]bool{"profile": false, "products": false, "recent_orders": false}
	for _, branch := range view.Degraded {
		want[branch] = true
	}
	for branch, seen := range want {
		if !seen {
			t.Fatalf("branch %s not recorded degraded; got %v", branch, view.Degraded)
		}
	}
	if n := countDegradedSpans(exporter.GetSpans()); n != 3 {
		t.Fatalf("expected 3 degraded branch spans, got %d", n)
	}
	// 降级也要有 SEO 兜底（PC Web 页面不能没有 title/canonical）。
	if view.SEO == nil || view.SEO.Title == "" || view.SEO.Canonical == "" {
		t.Fatalf("degraded home must still carry SEO defaults: %+v", view.SEO)
	}
}

// TestHomeEnrichesSEOAndSSR 验证成功路径的 SEO / SSR 段与按端字段裁剪。
func TestHomeEnrichesSEOAndSSR(t *testing.T) {
	withTracer(t)

	agg := NewAggregator(&client.Clients{
		User: &fakeUserClient{profile: &userpb.GetProfileResp{
			UserId:   42,
			Username: "alice",
			Nickname: "爱丽丝",
			Avatar:   "https://cdn/a.png",
			City:     "上海",
			Level:    3,
		}},
		Product: &fakeProductClient{list: &productpb.ListProductsResp{Products: []*productpb.ProductInfo{
			{ProductId: 7, Title: "机械键盘", Price: 39900, Stock: 5, CategoryName: "外设"},
			{ProductId: 8, Title: "显示器", Price: 129900, Stock: 0, CategoryName: "外设"},
		}}},
		Order: &fakeOrderClient{list: &orderpb.ListOrdersResp{Flows: []*orderpb.OrderFlow{
			{OrderId: 99, UserId: 42, OrderNo: "NO99", Status: "paid", TotalAmount: 13900, CreatedAt: 1700000000},
		}}},
	}, nil, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	if view.SEO == nil {
		t.Fatal("home view must carry SEO meta")
	}
	if !strings.Contains(view.SEO.Title, "爱丽丝") {
		t.Fatalf("seo title should be personalized: %q", view.SEO.Title)
	}
	if view.SEO.Description == "" || len(view.SEO.Keywords) == 0 {
		t.Fatalf("seo description/keywords missing: %+v", view.SEO)
	}
	if view.SEO.Canonical != SiteBase+"/" {
		t.Fatalf("canonical = %q, want %q", view.SEO.Canonical, SiteBase+"/")
	}
	if view.SEO.Robots != "index,follow" {
		t.Fatalf("robots = %q, want index,follow", view.SEO.Robots)
	}
	if view.SEO.StructuredData["@type"] != "WebSite" {
		t.Fatalf("structured data @type = %v, want WebSite", view.SEO.StructuredData["@type"])
	}
	if view.SSR == nil || !view.SSR.Rendered || view.SSR.RenderedAt == 0 {
		t.Fatalf("ssr state missing: %+v", view.SSR)
	}

	// 裁剪：profile 只下发展示字段（Nickname/City/Level），不下发 Username。
	if view.Profile == nil || view.Profile.Nickname != "爱丽丝" {
		t.Fatalf("profile brief = %+v", view.Profile)
	}
	if len(view.Products) != 2 {
		t.Fatalf("products = %d, want 2", len(view.Products))
	}
	// 裁剪：PC 端不下发精确库存水位，只下发是否有货 + canonical + 结构化数据。
	if !view.Products[0].InStock || view.Products[1].InStock {
		t.Fatalf("in_stock mapping wrong: %+v", view.Products)
	}
	if view.Products[0].Canonical != SiteBase+"/product/7" {
		t.Fatalf("product canonical = %q", view.Products[0].Canonical)
	}
	if view.Products[0].StructuredData["@type"] != "Product" {
		t.Fatalf("product structured data = %+v", view.Products[0].StructuredData)
	}
	// 裁剪：订单不下发 user_id（同端冗余）。
	if len(view.Orders) != 1 || view.Orders[0].OrderNo != "NO99" || view.Orders[0].Status != "paid" {
		t.Fatalf("orders = %+v", view.Orders)
	}
}

// TestHomeCacheHitSkipsDownstream 验证命中展示层缓存时不再调用任何 srv，
// 且缓存 key 为 bff:web:home:{user_id}。
func TestHomeCacheHitSkipsDownstream(t *testing.T) {
	withTracer(t)

	downstreamCalls := 0
	user := &fakeUserClient{profile: &userpb.GetProfileResp{UserId: 42, Nickname: "爱丽丝"}}
	product := &fakeProductClient{callCount: &downstreamCalls, list: &productpb.ListProductsResp{}}
	order := &fakeOrderClient{list: &orderpb.ListOrdersResp{}}

	mem := newMemCache()
	mem.seedHome(t, 42)
	agg := NewAggregator(&client.Clients{User: user, Product: product, Order: order},
		cache.New(mem, 60*time.Second), nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	if !view.CacheHit {
		t.Fatal("expected cache hit")
	}
	if view.SEO == nil || view.SEO.Title != "cached title" {
		t.Fatalf("cached view should be returned as-is: %+v", view.SEO)
	}
	if downstreamCalls != 0 || product.calls != 0 || order.calls != 0 || user.calls != 0 {
		t.Fatalf("cache hit must not call downstream: product=%d order=%d user=%d",
			product.calls, order.calls, user.calls)
	}
	if got := mem.keys(); len(got) != 1 || got[0] != "bff:web:home:42" {
		t.Fatalf("cache get keys = %v, want [bff:web:home:42]", got)
	}
}

// TestHomeMissWritesCacheWithTTL 验证未命中时把聚合结果按命名空间写回缓存。
func TestHomeMissWritesCacheWithTTL(t *testing.T) {
	withTracer(t)

	mem := newMemCache()
	agg := NewAggregator(&client.Clients{
		User:    &fakeUserClient{profile: &userpb.GetProfileResp{UserId: 42, Nickname: "爱丽丝"}},
		Product: &fakeProductClient{list: &productpb.ListProductsResp{}},
		Order:   &fakeOrderClient{err: errors.New("order down")},
	}, cache.New(mem, 60*time.Second), nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	if view.CacheHit {
		t.Fatal("expected cache miss")
	}
	mem.mu.Lock()
	ttl := mem.ttls["bff:web:home:42"]
	_, written := mem.data["bff:web:home:42"]
	mem.mu.Unlock()
	if !written {
		t.Fatal("miss path must write back cache under bff:web:home:42")
	}
	if ttl != 60*time.Second {
		t.Fatalf("ttl = %v, want 60s", ttl)
	}
}

// TestProductDetailSEOAndDegrade 验证商品详情的 SEO 结构化数据与降级兜底。
func TestProductDetailSEOAndDegrade(t *testing.T) {
	withTracer(t)

	agg := NewAggregator(&client.Clients{
		Product: &fakeProductClient{detail: &productpb.GetProductResp{
			Product: &productpb.ProductInfo{ProductId: 7, Title: "机械键盘", Price: 39900, Stock: 5, CategoryName: "外设"},
		}},
	}, nil, nil)

	view, err := agg.ProductDetail(context.Background(), 42, 7)
	if err != nil {
		t.Fatalf("product detail: %v", err)
	}
	if view.Product == nil || view.Product.Title != "机械键盘" {
		t.Fatalf("product card = %+v", view.Product)
	}
	if view.SEO.Canonical != SiteBase+"/product/7" {
		t.Fatalf("canonical = %q", view.SEO.Canonical)
	}
	if view.SEO.StructuredData["@type"] != "Product" {
		t.Fatalf("structured data = %+v", view.SEO.StructuredData)
	}
	offers, ok := view.SEO.StructuredData["offers"].(map[string]any)
	if !ok || offers["price"] != "399.00" || offers["availability"] != "https://schema.org/InStock" {
		t.Fatalf("offers = %+v", view.SEO.StructuredData["offers"])
	}

	// 降级：srv-product 不可用 → 不报错，SEO 退化为默认标题。
	degraded := NewAggregator(&client.Clients{}, nil, nil)
	dview, err := degraded.ProductDetail(context.Background(), 42, 7)
	if err != nil {
		t.Fatalf("product detail must not fail on degradation: %v", err)
	}
	if len(dview.Degraded) == 0 || dview.Product != nil {
		t.Fatalf("degraded detail = %+v", dview)
	}
	if dview.SEO == nil || dview.SEO.Robots != "noindex,follow" {
		t.Fatalf("degraded seo should be noindex: %+v", dview.SEO)
	}
}
