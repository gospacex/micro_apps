package cache

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

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

type fakeCache struct {
	get func(ctx context.Context, key string) (string, error)
	set func(ctx context.Context, key, value string, ttl time.Duration) error
}

func (f *fakeCache) Get(ctx context.Context, key string) (string, error) {
	return f.get(ctx, key)
}

func (f *fakeCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return f.set(ctx, key, value, ttl)
}

func (f *fakeCache) Delete(context.Context, string) error { return nil }

func (f *fakeCache) Exists(context.Context, string) (bool, error) { return false, nil }

func (f *fakeCache) Keys(context.Context, string) ([]string, error) { return nil, nil }

func (f *fakeCache) Close() error { return nil }

func (f *fakeCache) HealthCheck(context.Context) error { return nil }

// TestGetHitAndMissSpans 验证命中/未命中均产生 service.display.get span，
// 并由 cache.hit 属性区分两条路径。
func TestGetHitAndMissSpans(t *testing.T) {
	testExporter.Reset()

	c := New(&fakeCache{
		get: func(_ context.Context, key string) (string, error) {
			if key == "hit" {
				return `{"v":1}`, nil
			}
			return "", errors.New("miss")
		},
		set: func(context.Context, string, string, time.Duration) error { return nil },
	}, time.Minute)

	var dst struct {
		V int `json:"v"`
	}
	if !c.Get(context.Background(), "hit", &dst) || dst.V != 1 {
		t.Fatal("hit path should unmarshal cached value")
	}
	if c.Get(context.Background(), "miss", &dst) {
		t.Fatal("miss path should return false")
	}

	spans := testExporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans (hit+miss), got %d", len(spans))
	}
	for _, span := range spans {
		if span.Name != "service.display.get" {
			t.Fatalf("span name = %q, want service.display.get", span.Name)
		}
	}
	assertAttrBool(t, spans[0].Attributes, "cache.hit", true)
	assertAttrBool(t, spans[1].Attributes, "cache.hit", false)
}

// TestKeyIsNamespacedByResourceAndUser 验证展示层缓存 key 为
// bff:web:{resource}:{user_id}（PC Web 端命名空间，与 bff-app 互不串 key）。
func TestKeyIsNamespacedByResourceAndUser(t *testing.T) {
	if got := Key("home", 42); got != "bff:web:home:42" {
		t.Fatalf("Key(home,42) = %q, want bff:web:home:42", got)
	}
	if got := Key("product:7", 42); got != "bff:web:product:7:42" {
		t.Fatalf("Key(product:7,42) = %q, want bff:web:product:7:42", got)
	}
	if Key("home", 42) == Key("home", 43) {
		t.Fatal("cache key must be scoped per user")
	}
}

// TestSetPropagatesTTL 验证 TTL 由装配方（main.go 60s）透传到底层 cachex，
// 不在展示层硬编码。
func TestSetPropagatesTTL(t *testing.T) {
	var gotTTL time.Duration
	c := New(&fakeCache{
		get: func(context.Context, string) (string, error) { return "", errors.New("miss") },
		set: func(_ context.Context, _, _ string, ttl time.Duration) error {
			gotTTL = ttl
			return nil
		},
	}, 60*time.Second)
	c.Set(context.Background(), Key("home", 1), map[string]any{"a": 1})
	if gotTTL != 60*time.Second {
		t.Fatalf("ttl = %v, want 60s", gotTTL)
	}
}

// TestGetDisabledWithoutInstance 验证缓存实例不可用时返回未命中（降级无缓存）。
func TestGetDisabledWithoutInstance(t *testing.T) {
	testExporter.Reset()

	c := New(nil, time.Minute)
	var dst map[string]any
	if c.Get(context.Background(), Key("home", 1), &dst) {
		t.Fatal("nil instance must miss")
	}
	spans := testExporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	assertAttrBool(t, spans[0].Attributes, "cache.hit", false)
	assertAttrBool(t, spans[0].Attributes, "cache.disabled", true)
}

func assertAttrBool(t *testing.T, attrs []attribute.KeyValue, key string, want bool) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			if attr.Value.AsBool() != want {
				t.Fatalf("attr %s = %v, want %v", key, attr.Value.AsBool(), want)
			}
			return
		}
	}
	t.Fatalf("attr %s missing", key)
}
