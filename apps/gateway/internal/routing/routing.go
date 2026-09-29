// Package routing 网关路由表 + 私有配置段（path 前缀 → 目标服务名，配置驱动）。
//
// 配置源：config.yaml 顶层 gateway 段（与 configs/services/gateway.yaml 逐字
// 同构；灌 Nacos 的 dataId hubx.gateway key 级覆盖）。路由表最长前缀匹配，
// 灰度头（pkg/meta.HeaderGray）由 proxy 层随入站 Header 原样透传（路由表
// 本身不感知灰度——服务寻址走 consul KV，灰度分组由部署侧注册体现）。
package routing

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gospacex/configx"
)

// Route 单条路由（path 前缀 → 目标服务名）
type Route struct {
	// Prefix 请求路径前缀（如 /api/app），须以 / 开头
	Prefix string `mapstructure:"prefix"`
	// Service 目标服务名（consul 服务发现 ID，如 bff-app）
	Service string `mapstructure:"service"`
}

// JWT gateway.jwt 配置段（HS256 验签密钥与期望 issuer）
type JWT struct {
	// Secret HS256 签名密钥（本地演示缺省值；生产经 SecretEnv 注入）
	Secret string `mapstructure:"secret"`
	// SecretEnv 覆盖密钥的环境变量名（如 HUBX_JWT_SECRET），非空时优先
	SecretEnv string `mapstructure:"secret_env"`
	// Issuer 期望签发方（srv-user）；非空时 token iss 不匹配即拒绝
	Issuer string `mapstructure:"issuer"`
}

// RateLimit 全局限流参数（limit 0 = 关）
type RateLimit struct {
	// Limit 窗口内每 (源 IP, 路由前缀) 的请求上限
	Limit int `mapstructure:"limit"`
	// Window 固定窗口时长（字符串 "1m"/"30s"；空则用 DefaultWindow）
	Window string `mapstructure:"window"`
}

// DefaultWindow 限流窗口缺省值（config 未写 window 时）
const DefaultWindow = "1m"

// WindowDuration 解析窗口时长（空/非法回退缺省 1m——fail-open 形态：
// 限流参数配置错误不应放大为网关不可用，装配日志会如实记录生效值）
func (r RateLimit) WindowDuration() time.Duration {
	if r.Window == "" {
		return DefaultWindowDur
	}
	d, err := time.ParseDuration(r.Window)
	if err != nil {
		return DefaultWindowDur
	}
	return d
}

var DefaultWindowDur = time.Minute

// Gateway gateway 服务私有配置段（config.yaml 顶层 gateway 键）
type Gateway struct {
	// JWT 验签配置
	JWT JWT `mapstructure:"jwt"`
	// Routes 路由表（载入后排序进 Table）
	Routes []Route `mapstructure:"routes"`
	// RateLimit 全局限流（零值 = 不限）
	RateLimit RateLimit `mapstructure:"rate_limit"`
}

// Table 已排序路由表（按前缀长度降序，Match 首个命中即最长前缀）
type Table struct {
	routes []Route
}

// Load 读 configx 句柄的 gateway 段。段缺失时返回空表 + 空配置（网关对全部
// 业务路径 404，健康探针不受影响；显式写出了的前缀非法则 fail fast）。
func Load(h configx.Config) (*Gateway, *Table, error) {
	g := &Gateway{}
	if h.IsSet("gateway") {
		if err := h.Unmarshal("gateway", g); err != nil {
			return nil, nil, fmt.Errorf("routing: unmarshal gateway section: %w", err)
		}
	}
	seen := make(map[string]bool, len(g.Routes))
	for _, r := range g.Routes {
		if !strings.HasPrefix(r.Prefix, "/") {
			return nil, nil, fmt.Errorf("routing: prefix %q must start with /", r.Prefix)
		}
		if r.Service == "" {
			return nil, nil, fmt.Errorf("routing: prefix %q missing service", r.Prefix)
		}
		if seen[r.Prefix] {
			return nil, nil, fmt.Errorf("routing: duplicate prefix %q", r.Prefix)
		}
		seen[r.Prefix] = true
	}
	sort.Slice(g.Routes, func(i, j int) bool {
		return len(g.Routes[i].Prefix) > len(g.Routes[j].Prefix)
	})
	return g, &Table{routes: g.Routes}, nil
}

// ResolvedSecret 环境变量优先，回退配置值（与 srv-user 签发侧取值规则一致）
func (j JWT) ResolvedSecret() []byte {
	if j.SecretEnv != "" {
		if v := os.Getenv(j.SecretEnv); v != "" {
			return []byte(v)
		}
	}
	return []byte(j.Secret)
}

// Match 最长前缀匹配（path 形如 /api/app/orders/1）
func (t *Table) Match(path string) *Route {
	for i := range t.routes {
		if strings.HasPrefix(path, t.routes[i].Prefix) {
			return &t.routes[i]
		}
	}
	return nil
}

// Len 路由条数（装配日志用）
func (t *Table) Len() int { return len(t.routes) }
