package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gospacex/cachex"
)

// fakeCache 测试桩：内存 map + 可注入错误
type fakeCache struct {
	data map[string]string
	fail bool // Get/Set 一律返回 error（模拟 Redis 挂）
}

func (f *fakeCache) Get(ctx context.Context, key string) (string, error) {
	if f.fail {
		return "", errors.New("cache down")
	}
	return f.data[key], nil
}
func (f *fakeCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if f.fail {
		return errors.New("cache down")
	}
	f.data[key] = value
	return nil
}
func (f *fakeCache) Delete(ctx context.Context, key string) error { delete(f.data, key); return nil }
func (f *fakeCache) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := f.data[key]
	return ok, nil
}
func (f *fakeCache) Keys(ctx context.Context, pattern string) ([]string, error) { return nil, nil }
func (f *fakeCache) Close() error                                               { return nil }
func (f *fakeCache) HealthCheck(ctx context.Context) error                      { return nil }

var _ cachex.Cache = (*fakeCache)(nil)

// TestAllowWithinLimit 窗口内放行、超 limit 拒绝（(ip, route) 维度隔离）
func TestAllowWithinLimit(t *testing.T) {
	c := &fakeCache{data: map[string]string{}}
	l := NewLimiter(c, 2, time.Minute)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if !l.Allow(ctx, "1.1.1.1", "/api/app") {
			t.Fatalf("request %d should pass", i+1)
		}
	}
	if l.Allow(ctx, "1.1.1.1", "/api/app") {
		t.Fatal("third request in window should be rejected")
	}
	if !l.Allow(ctx, "2.2.2.2", "/api/app") {
		t.Fatal("different ip should have independent counter")
	}
	if !l.Allow(ctx, "1.1.1.1", "/api/web") {
		t.Fatal("different route should have independent counter")
	}
}

// TestAllowFailOpen 缓存故障时放行（限流器故障不能放大为网关不可用）
func TestAllowFailOpen(t *testing.T) {
	l := NewLimiter(&fakeCache{fail: true}, 1, time.Minute)
	if !l.Allow(context.Background(), "1.1.1.1", "/api/app") {
		t.Fatal("cache down should fail open")
	}
}

// TestNilSafe nil 形态（门控关闭 / limit 0）恒放行
func TestNilSafe(t *testing.T) {
	var l *Limiter
	if !l.Allow(context.Background(), "1.1.1.1", "/api/app") {
		t.Fatal("nil limiter should allow")
	}
	if !NewLimiter(nil, 10, time.Minute).Allow(context.Background(), "1.1.1.1", "/api/app") {
		t.Fatal("nil cache should allow")
	}
	if !NewLimiter(&fakeCache{data: map[string]string{}}, 0, time.Minute).Allow(context.Background(), "1.1.1.1", "/api/app") {
		t.Fatal("limit 0 should allow")
	}
}
