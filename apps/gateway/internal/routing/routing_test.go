package routing

import (
	"testing"

	"github.com/gospacex/configx"
)

// fakeCfg 测试配置桩：嵌入 nil configx.Config（接口零值），覆写 IsSet/
// Unmarshal 直读 map（gateway 段为嵌套结构，按「key → 子段」两级模拟）。
type fakeCfg struct {
	configx.Config
	gateway map[string]any
}

func (f *fakeCfg) IsSet(key string) bool {
	switch key {
	case "gateway":
		return len(f.gateway) > 0
	case "gateway.rate_limit":
		_, ok := f.gateway["rate_limit"]
		return ok
	default:
		return false
	}
}

func (f *fakeCfg) Unmarshal(key string, out any) error {
	if key == "gateway" {
		return decodeGateway(f.gateway, out)
	}
	if key == "gateway.rate_limit" {
		return decodeRateLimit(f.gateway["rate_limit"], out)
	}
	return nil
}

// 经 interface{} 透传 map 到目标结构（测试桩专用：仅支持 map/[]Route 形态，
// 不引 mapstructure 依赖）
func decodeGateway(m map[string]any, out any) error {
	g, ok := out.(*Gateway)
	if !ok {
		return nil
	}
	if rl, ok := m["rate_limit"].(map[string]any); ok {
		if limit, ok := rl["limit"].(int); ok {
			g.RateLimit.Limit = limit
		}
		if window, ok := rl["window"].(string); ok {
			g.RateLimit.Window = window
		}
	}
	if routes, ok := m["routes"].([]Route); ok {
		g.Routes = routes
	}
	return nil
}

func decodeRateLimit(v any, out any) error {
	rl, ok := out.(*RateLimit)
	if !ok {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		if limit, ok := m["limit"].(int); ok {
			rl.Limit = limit
		}
		if window, ok := m["window"].(string); ok {
			rl.Window = window
		}
	}
	return nil
}

// TestTableMatchLongestPrefix 最长前缀匹配 + 未命中返回 nil
func TestTableMatchLongestPrefix(t *testing.T) {
	g, table, err := Load(&fakeCfg{gateway: map[string]any{
		"routes": []Route{
			{Prefix: "/api", Service: "s1"},
			{Prefix: "/api/app", Service: "s2"},
			{Prefix: "/api/app/v2", Service: "s3"},
		},
	}})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if g.RateLimit.Limit != 0 {
		t.Fatalf("RateLimit.Limit = %d, want 0 (unset)", g.RateLimit.Limit)
	}
	if table.Len() != 3 {
		t.Fatalf("Len = %d, want 3", table.Len())
	}
	if rt := table.Match("/api/app/v2/orders"); rt.Service != "s3" {
		t.Fatalf("Match longest prefix = %s, want s3", rt.Service)
	}
	if rt := table.Match("/api/app/orders"); rt.Service != "s2" {
		t.Fatalf("Match = %s, want s2", rt.Service)
	}
	if rt := table.Match("/api/orders"); rt.Service != "s1" {
		t.Fatalf("Match = %s, want s1", rt.Service)
	}
	if rt := table.Match("/healthz/live"); rt != nil {
		t.Fatalf("Match unrelated path = %v, want nil", rt)
	}
}

// TestLoadRejectsInvalidPrefix 前缀非法 / 缺服务 / 重复前缀 → fail fast
func TestLoadRejectsInvalidPrefix(t *testing.T) {
	cases := []struct {
		name string
		sec  map[string]any
	}{
		{"prefix not slash", map[string]any{"routes": []Route{{Prefix: "api", Service: "s"}}}},
		{"missing service", map[string]any{"routes": []Route{{Prefix: "/a"}}}},
		{"duplicate prefix", map[string]any{"routes": []Route{
			{Prefix: "/a", Service: "s1"}, {Prefix: "/a", Service: "s2"},
		}}},
	}
	for _, c := range cases {
		if _, _, err := Load(&fakeCfg{gateway: c.sec}); err == nil {
			t.Fatalf("%s: expected error", c.name)
		}
	}
}

// TestLoadMissingSection 段缺失 → 空表空配置（非 error）
func TestLoadMissingSection(t *testing.T) {
	g, table, err := Load(&fakeCfg{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if g == nil || table.Len() != 0 {
		t.Fatalf("empty section should give zero config: g=%v len=%d", g, table.Len())
	}
}

// TestRateLimitWindowDuration 窗口解析：显式值生效，空/非法回退缺省 1m
func TestRateLimitWindowDuration(t *testing.T) {
	if d := (RateLimit{Window: "30s"}).WindowDuration(); d != 30*1e9 {
		t.Fatalf("WindowDuration(30s) = %v", d)
	}
	if d := (RateLimit{}).WindowDuration(); d != DefaultWindowDur {
		t.Fatalf("WindowDuration(empty) = %v, want default", d)
	}
	if d := (RateLimit{Window: "bogus"}).WindowDuration(); d != DefaultWindowDur {
		t.Fatalf("WindowDuration(bogus) = %v, want default (fail-open)", d)
	}
}
