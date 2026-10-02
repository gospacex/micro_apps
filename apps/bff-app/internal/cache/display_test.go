package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

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
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

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

	spans := exporter.GetSpans()
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
