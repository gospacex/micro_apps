// Package ratelimit 网关全局限流（brief：cachex 固定窗口计数 + On("cache") 门控）。
//
// 与 srv 自保护限流（middleware/ratelimit.go，key 按 user_id）不同，
// 网关按「源 IP × 路由前缀」限流：key = rl:{ip}:{route}，
// 计数超 limit 拒绝（pkg/codes CodeRateLimited → 429）。
//
//   - 门控：main.go 仅在 pkg/config Trace.On("cache") 为真时构造
//     （缓存实例 nil 时 Limiter 为 nil，入口直接放行）；
//   - fail-open：缓存读失败一律放行（限流器故障不能放大为网关不可用）；
//   - 计数用 cachex Get→+1→Set（cachex.Cache 无原子 Incr，deviation 同
//     srv-order 自保护限流已记录：固定窗口场景允许轻微竞态）。
package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/gospacex/cachex"
	"github.com/gospacex/logx"
)

// Limiter 全局限流器（cache 为 nil 时 Allow 恒 true：门控关闭形态）
type Limiter struct {
	cache  cachex.Cache
	limit  int
	window time.Duration
}

// NewLimiter 构造（prefix 固定 "rl"，key 语义见包注释）
func NewLimiter(cache cachex.Cache, limit int, window time.Duration) *Limiter {
	return &Limiter{cache: cache, limit: limit, window: window}
}

// Allow 判定 (ip, route) 在窗口内是否放行（route 传路由表前缀，不带尾部 /）
func (l *Limiter) Allow(ctx context.Context, ip, route string) bool {
	if l == nil || l.cache == nil || l.limit <= 0 {
		return true
	}
	key := "rl:" + ip + ":" + route
	cur, err := l.cache.Get(ctx, key)
	if err != nil {
		// 缓存读失败 → 放行（fail-open）
		return true
	}
	n, _ := strconv.Atoi(cur)
	if n >= l.limit {
		return false
	}
	if err := l.cache.Set(ctx, key, strconv.Itoa(n+1), l.window); err != nil {
		logx.WarnContext(ctx, "ratelimit: counter set failed (fail-open)",
			logx.String("key", key), logx.Err(err))
	}
	return true
}
