// Package cache 展示层缓存：cachex 实例上的 JSON 读写 + 命中/未命中分层 span。
//
// 命中/未命中两种路径都产生 service.display.get span，以 cache.hit 属性区分；
// 底层 Redis 命令另由 cachex OTel 回调产生 span，由 Trace.On("cache") 门控。
//
// 端差异（PC Web）：key 走 bff:web:{resource}:{user_id} 命名空间，
// 与其余 BFF 的展示层缓存互不覆盖（同一 Redis 库共存）。
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

// KeyPrefix 展示层缓存 key 前缀（端标识，PC Web = bff:web）。
const KeyPrefix = "bff:web"

// Key 生成展示层缓存 key：bff:web:{resource}:{user_id}。
func Key(resource string, userID int64) string {
	return fmt.Sprintf("%s:%s:%d", KeyPrefix, resource, userID)
}

// DisplayCache 展示层 JSON 缓存。
type DisplayCache struct {
	inst cachex.Cache
	ttl  time.Duration
}

// New 构造展示层缓存；inst 允许为 nil，此时 Get 恒未命中、Set 不执行。
// ttl 由装配方（main.go，PC Web 为 60s）透传，本层不硬编码。
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
