// Package config 提供微服务共享的配置契约：统一调用链开关（trace 段）。
//
// 设计（移植 monolith config.go:55-88）：
//   - Enabled 是总开关；db / cache / mq 是三个中间件的细分开关
//   - 取值一律走 Trace.On(kind)，不要在业务代码里直接读 bool 字段——
//     否则容易漏掉总开关（本案例的"唯一开关入口"约束落在这里）
//   - 细分开关最终取值 = trace.<kind> 显式键 ∨ 官方段字段
//     （resolveTraceSwitches 语义，见 Resolve）
//
// 包内无 configx 依赖（纯数据）：官方段覆盖所需的读取能力由 Switches
// 最小接口抽象，configx.Config 天然满足；测试用内存实现。
package config

// DefaultInstance 中间件段的默认实例名，与 configx 企业配置契约保持一致
const DefaultInstance = "default"

// Config 微服务配置根。xmiddleware 段刻意不在此重复定义：
// DB / MQ 走各 SDK 官方 FromConfigx 入口（dbxconfig / mqx），
// 缓存走 cachex.Config（infra/drivers 薄封装），避免两套结构漂移。
type Config struct {
	// Trace 统一调用链开关段（config.yaml 顶层 trace 键）
	Trace Trace `mapstructure:"trace" json:"trace"`
}

// Trace 统一调用链开关（由 configx 读取后逐段 Unmarshal("trace", &cfg.Trace)）
//
// 取值优先级（调用 Resolve 后生效）：
//  1. trace 段显式写出的键（统一覆盖）
//  2. 各中间件段的官方字段（xmiddleware.sqlx.databases.<n>.enable_trace、
//     xmiddleware.cachex.instances.<n>.enable_trace、
//     xmiddleware.mqx.instances.<n>.call_chain_enabled）
//  3. 默认值 true
type Trace struct {
	// Enabled 总开关：关闭时 DB / 缓存 / MQ 与各分层 span 全部降级为 no-op
	Enabled bool `mapstructure:"enabled" json:"enabled"`
	// Exporter 导出目标：stdout / otlp / jaeger
	Exporter string `mapstructure:"exporter" json:"exporter"`
	// Endpoint 导出端点（OTLP/HTTP，对应 Jaeger/collector 的 4318 端口）。
	// 由统一 trace 段（Nacos hubx.trace / 本地 trace 段）供给，
	// 装配方（telemetry.Init）必须读本字段而非硬编码常量。
	Endpoint string `mapstructure:"endpoint" json:"endpoint"`
	// SampleRatio 采样率 0~1
	SampleRatio float64 `mapstructure:"sample_ratio" json:"sample_ratio"`
	// DB 数据库（单表 / 多表事务 / 连表）是否入链
	DB bool `mapstructure:"db" json:"db"`
	// Cache 缓存操作是否入链
	Cache bool `mapstructure:"cache" json:"cache"`
	// MQ 消息生产与消费是否入链
	MQ bool `mapstructure:"mq" json:"mq"`
}

// On 判断某一类中间件是否入链：总开关关闭则一律关闭。
// kind 取值 "db" / "cache" / "mq"；未知 kind 直接跟随总开关。
func (t Trace) On(kind string) bool {
	if !t.Enabled {
		return false
	}
	switch kind {
	case "db":
		return t.DB
	case "cache":
		return t.Cache
	case "mq":
		return t.MQ
	default:
		return true
	}
}

// Switches 配置句柄的最小读取接口（configx.Config 天然满足；
// 只挑 resolveTraceSwitches 用到的两个方法，避免 pkg 层依赖 configx）
type Switches interface {
	// IsSet 判断 key 是否显式写出
	IsSet(key string) bool
	// GetBool 读取布尔值
	GetBool(key string) bool
}

// TraceSource 统一 trace 段的来源句柄（configx.Config 天然满足）。
// 双 dataId 形态下 primary = hubx.trace 句柄，fallback = 服务私有句柄。
//
// 【为什么不用 Unmarshal("trace", &t)】hubx/configx 的 Nacos provider 把
// YAML **展平成点分键**（flattenMap：`trace.enabled` / `trace.endpoint` …），
// 段级 `Get("trace")` 恒为 nil——`Unmarshal("trace")` 直接报 key not found。
// 2026-10-03 实测：11 服务因此从未读到过 hubx.trace，7 组合矩阵逐格同值。
// 改为逐键读取后，JSON 形态（viper/apollo 提供嵌套 map）同样兼容——那些
// provider 上 Get("trace.enabled") 走 getByPath 逐级查找，一样解得出来。
type TraceSource interface {
	Switches
	// GetString 读字符串值（trace.exporter / trace.endpoint）
	GetString(key string) string
	// GetFloat64 读浮点值（trace.sample_ratio）
	GetFloat64(key string) float64
}

// traceKeys 判定「trace 段是否显式存在」的探针键。
//
// 只需命中任一键即算存在：enabled 之外，也可能只写了 endpoint 或某个细分键。
var traceKeys = []string{"enabled", "exporter", "endpoint", "sample_ratio", "db", "cache", "mq"}

// hasTrace 判断句柄上是否显式写了 trace 段。
func hasTrace(s Switches) bool {
	if s == nil {
		return false
	}
	for _, k := range traceKeys {
		if s.IsSet("trace." + k) {
			return true
		}
	}
	return false
}

// ApplyTrace 统一 trace 段裁决（openspec unified-trace-switch 的落点）：
//   - primary（Nacos hubx.trace 句柄，可为 nil）显式含 trace 段 → 单一事实源生效；
//   - 否则 fallback（私有句柄）含 trace 段 → 本地段生效；
//   - 都没有 → 保持 Default() 值。
//
// 三段正交回退（Resolve）始终对「实际提供 trace 段的句柄」做，保证
// 「官方段仅在 trace.<kind> 未显式写出时生效」在两条来源下语义一致。
func (c *Config) ApplyTrace(primary TraceSource, fallback Switches, instance string) {
	var src TraceSource
	switch {
	case primary != nil && hasTrace(primary):
		src = primary
	default:
		if ts, ok := fallback.(TraceSource); ok && hasTrace(ts) {
			src = ts
		}
	}
	if src != nil {
		applyTraceFields(&c.Trace, src)
		c.Trace.Resolve(src, instance)
		return
	}
	// 没有任何 trace 段：只对 fallback 做官方段补位（保持 Default 打底值）
	if fallback != nil {
		c.Trace.Resolve(fallback, instance)
	}
}

// applyTraceFields 逐键覆盖 trace 段字段：键显式写出才覆盖，未写出的保持
// Default() 打底值（避免"配置里没写就被零值冲掉"）。
func applyTraceFields(t *Trace, s TraceSource) {
	if s.IsSet("trace.enabled") {
		t.Enabled = s.GetBool("trace.enabled")
	}
	if s.IsSet("trace.exporter") {
		t.Exporter = s.GetString("trace.exporter")
	}
	if s.IsSet("trace.endpoint") {
		t.Endpoint = s.GetString("trace.endpoint")
	}
	if s.IsSet("trace.sample_ratio") {
		if r := s.GetFloat64("trace.sample_ratio"); r > 0 {
			t.SampleRatio = r
		}
	}
	if s.IsSet("trace.db") {
		t.DB = s.GetBool("trace.db")
	}
	if s.IsSet("trace.cache") {
		t.Cache = s.GetBool("trace.cache")
	}
	if s.IsSet("trace.mq") {
		t.MQ = s.GetBool("trace.mq")
	}
}

// SQLXTraceKey 官方段键：xmiddleware.sqlx.databases.<instance>.enable_trace
func SQLXTraceKey(instance string) string {
	return "xmiddleware.sqlx.databases." + instance + ".enable_trace"
}

// CacheXTraceKey 官方段键：xmiddleware.cachex.instances.<instance>.enable_trace
func CacheXTraceKey(instance string) string {
	return "xmiddleware.cachex.instances." + instance + ".enable_trace"
}

// MQXCallChainKey 官方段键：xmiddleware.mqx.instances.<instance>.call_chain_enabled
func MQXCallChainKey(instance string) string {
	return "xmiddleware.mqx.instances." + instance + ".call_chain_enabled"
}

// Resolve 计算三个细分开关的最终取值（monolith resolveTraceSwitches 等价物）：
//
//	trace.<kind> 显式写出 → 以它为准（统一覆盖，保持 Unmarshal 结果）
//	否则官方段键显式写出 → 取官方字段值
//	再否则                 → 保持默认 true（调用方应先用 Default() 打底）
//
// s 为 nil 时不做覆盖（纯 trace 段语义）；instance 为空按 DefaultInstance。
func (t *Trace) Resolve(s Switches, instance string) {
	if s == nil {
		return
	}
	if instance == "" {
		instance = DefaultInstance
	}
	if !s.IsSet("trace.db") && s.IsSet(SQLXTraceKey(instance)) {
		t.DB = s.GetBool(SQLXTraceKey(instance))
	}
	if !s.IsSet("trace.cache") && s.IsSet(CacheXTraceKey(instance)) {
		t.Cache = s.GetBool(CacheXTraceKey(instance))
	}
	if !s.IsSet("trace.mq") && s.IsSet(MQXCallChainKey(instance)) {
		t.MQ = s.GetBool(MQXCallChainKey(instance))
	}
}

// Default 返回带默认值的配置（三段细分开关默认开，实际取值按优先级重算）
func Default() *Config {
	return &Config{
		Trace: Trace{
			Enabled:     true,
			Exporter:    "otlp",
			Endpoint:    "127.0.0.1:4318",
			SampleRatio: 1,
			DB:          true,
			Cache:       true,
			MQ:          true,
		},
	}
}
