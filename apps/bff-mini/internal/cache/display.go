// Package cache 展示层缓存：cachex 实例上的 JSON 读写 + 命中/未命中分层 span。
//
// 键形如 bff:mini:{resource}:{user_id}（端前缀 bff:mini 与其他 BFF 的命名空间
// 隔离：同一个 Redis 库里 bff-app / bff-web / bff-mini 的展示层缓存互不可见）。
//
// 命中/未命中两种路径都产生 service.display.get span，以 cache.hit 属性区分；
// 底层 Redis 命令另由 cachex OTel 回调产生 span，由 Trace.On("cache") 门控。
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gospacex/cachex"
	"go.opentelemetry.io/otel/attribute"

	"hubx/micro_apps/infra/telemetry"
)

// KeyPrefix 小程序端展示层缓存键前缀（bff:mini:{resource}:{user_id}）
const KeyPrefix = "bff:mini"

// Key 拼展示层缓存键：bff:mini:{resource}:{user_id}。
// resource 取业务资源名（home / products / orders ...）。
func Key(resource string, userID int64) string {
	return fmt.Sprintf("%s:%s:%d", KeyPrefix, resource, userID)
}

// DisplayCache 展示层 JSON 缓存。
type DisplayCache struct {
	inst cachex.Cache
	ttl  time.Duration
}

// New 构造展示层缓存；inst 允许为 nil，此时 Get 恒未命中、Set 不执行。
func New(inst cachex.Cache, ttl time.Duration) *DisplayCache {
	return &DisplayCache{inst: inst, ttl: ttl}
}

// Get 读缓存。命中时反序列化进 dest 并返回 true；未命中、缓存不可用或数据损坏
// 均返回 false，并用 span 属性记录具体形态。
func (c *DisplayCache) Get(ctx context.Context, key string, dest any) bool {
	ctx, span := telemetry.Start(ctx, "service", "display", "get")
	defer span.End()
	span.SetAttributes(attribute.String("cache.key", key))
	if c.inst == nil {
		span.SetAttributes(attribute.Bool("cache.hit", false), attribute.Bool("cache.disabled", true))
		return false
	}
	raw, err := c.inst.Get(ctx, key)
	if err != nil || raw == "" {
		span.SetAttributes(attribute.Bool("cache.hit", false))
		return false
	}
	if err := json.Unmarshal([]byte(raw), dest); err != nil {
		span.SetAttributes(attribute.Bool("cache.hit", false), attribute.Bool("cache.corrupt", true))
		return false
	}
	span.SetAttributes(attribute.Bool("cache.hit", true))
	return true
}

// Set 写缓存；序列化或写入失败不阻断主流程。
func (c *DisplayCache) Set(ctx context.Context, key string, value any) {
	if c.inst == nil {
		return
	}
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	_ = c.inst.Set(ctx, key, string(data), c.ttl)
}
