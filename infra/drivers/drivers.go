// Package drivers 封装中间件驱动的装配（装配总公式第 3 步的薄封装）：
// 缓存（cachex/redis）与消息队列（mqx/kafka）。
//
// 驱动自注册：cachex/redis 与 mqx/kafka 的驱动在各自包内 init() 注册
// （RegisterCreator("redis") / RegisterProducerCreator("kafka")），
// 本包 blank import 完成接线——业务代码只调本包工厂，不碰驱动包。
//
// 调用链开关语义（刻意设计，注释写死）：
//   - **trace 开关由调用方以 bool 传入，drivers 不读任何 trace 配置**。
//     调用方负责 pkg/config 的 Trace.On("cache") / Trace.On("mq")
//     （已含官方段 resolveTraceSwitches 裁决），本包只做透传：
//     缓存 → cachex.SetTracingConfig（redis 驱动 startSpan 的全局闸）；
//     MQ → 每份 mqx.Config 的 CallChainEnabled + EnableTrace。
//   - drivers 不自动重建连接池：配置变更后须重启进程重新装配。
package drivers

import (
	"context"
	"fmt"

	"github.com/gospacex/cachex"
	"github.com/gospacex/configx"
	"github.com/gospacex/mqx"
	// 驱动自注册（blank import）
	_ "github.com/gospacex/cachex/redis"
	_ "github.com/gospacex/mqx/kafka"

	"hubx/micro_apps/infra/telemetry"
)

// Cache 缓存资源句柄
type Cache struct {
	Instances cachex.Instances
}

// Close 释放全部缓存实例
func (c *Cache) Close() error {
	if c == nil {
		return nil
	}
	for name, inst := range c.Instances {
		if err := inst.Close(); err != nil {
			return fmt.Errorf("drivers: close cache %q: %w", name, err)
		}
	}
	return nil
}

// NewCache 创建缓存实例并设置调用链全局闸。
//
// h：configx 句柄（读 xmiddleware.cachex.instances.<name>）；
// name：实例名（各服务私有段，如 "bff-app"），空则用 config.DefaultInstance；
// driver：驱动名（redis），空则用 xmiddleware.cachex.driver 段级值；
// traceOn：调用方传入的调用链总开关（= pkg/config Trace.On("cache")），
// 本包不自行裁决。
//
// redis 驱动的连接在 NewClient 时即建立（PING）；失败返回 error。
func NewCache(ctx context.Context, h configx.Config, name, driver string, traceOn bool) (*Cache, error) {
	if name == "" {
		name = "default"
	}
	instKey := "xmiddleware.cachex.instances." + name
	cfg := &cachex.Config{
		Name:           name,
		Driver:         h.GetString(instKey + ".driver"),
		Addr:           h.GetString(instKey + ".addr"),
		Password:       h.GetString(instKey + ".password"),
		DB:             h.GetInt(instKey + ".db"),
		PoolSize:       h.GetInt(instKey + ".pool_size"),
		MinIdleConn:    h.GetInt(instKey + ".min_idle_conns"),
		Timeout:        h.GetDuration(instKey + ".dial_timeout"),
		ReadTimeout:    h.GetDuration(instKey + ".read_timeout"),
		WriteTimeout:   h.GetDuration(instKey + ".write_timeout"),
		RetryCount:     h.GetInt(instKey + ".max_retries"),
		SentinelMaster: h.GetString(instKey + ".sentinel_master"),
		SentinelAddrs:  h.GetString(instKey + ".sentinel_addrs"),
		Cluster:        h.GetString(instKey + ".cluster"),
		TLSEnabled:     h.GetBool(instKey + ".tls_enabled"),
	}
	if driver != "" {
		cfg.Driver = driver
	} else if cfg.Driver == "" {
		cfg.Driver = h.GetString("xmiddleware.cachex.driver")
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("drivers: cache instance %q missing addr", name)
	}
	// 调用链全局闸：redis 驱动 startSpan 先查 IsTracingEnabled()，
	// 不开则缓存 span 静默消失（monolith infra.go openCache 同款注释）
	cachex.SetTracingConfig(cachex.TracingConfig{Enabled: traceOn})

	insts, err := cachex.New([]*cachex.Config{cfg})
	if err != nil {
		return nil, fmt.Errorf("drivers: open cache %q (driver=%s): %w", name, cfg.Driver, err)
	}
	// 【必须】cachex 在 SetTracingConfig(Enabled) 打开时会自建 TracerProvider
	// 并 otel.SetTracerProvider 注册到全局，端点取它的默认值 localhost:4318
	// +TLS —— 把装配层装好的 provider 静默顶掉，凡是走 otel.GetTracerProvider()
	// 取 tracer 的组件（RPC client span、kitex 套件）从此只建不导。
	// 夺回全局的动作放在本包而不是 11 个 main 里，避免漏改。
	telemetry.InstallGlobal()
	return &Cache{Instances: insts}, nil
}

// CacheInstance 取指定名的缓存实例
func (c *Cache) Instance(name string) cachex.Cache {
	if c == nil {
		return nil
	}
	return c.Instances[name]
}

// NewProducer 创建消息生产者（kafka）。
//
// kafka 驱动的消息自带 Topic 优先（resolveTopic），Writer 默认 topic 取
// cfg.Consumer.Topic（官方契约把发布/消费 topic 都挂在 consumer 段）；
// 发布侧 topic 由业务侧在 Message.Topic 填写。traceOn：调用方传入
// （= Trace.On("mq")），写 CallChainEnabled + EnableTrace 两份。
func NewProducer(ctx context.Context, h configx.Config, name string, traceOn bool) (mqx.Producer, error) {
	if h == nil {
		return nil, fmt.Errorf("drivers: producer requires configx handle")
	}
	cfg := mqx.ReadConfigFromConfigx(h, name)
	if cfg.Driver == "" {
		cfg.Driver = "kafka"
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("drivers: mq instance %q missing brokers", orDefault(name))
	}
	applyTrace(cfg, traceOn)
	p, err := mqx.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("drivers: new producer (instance=%s): %w", orDefault(name), err)
	}
	return p, nil
}

// NewConsumer 创建消息消费者（kafka）。
//
// group 非空时覆盖 xmiddleware.mqx.instances.<name>.consumer.group_id
// （job 的 consume/retry 两组消费需要不同 group，官方入口读不到覆盖值）；
// traceOn 同 NewProducer。
func NewConsumer(ctx context.Context, h configx.Config, name, group string, traceOn bool) (mqx.Consumer, error) {
	if h == nil {
		return nil, fmt.Errorf("drivers: consumer requires configx handle")
	}
	cfg := mqx.ReadConfigFromConfigx(h, name)
	if cfg.Driver == "" {
		cfg.Driver = "kafka"
	}
	if group != "" {
		cfg.Consumer.GroupID = group
	}
	if cfg.Consumer.Topic == "" && cfg.Consumer.GroupID == "" {
		return nil, fmt.Errorf("drivers: mq instance %q missing consumer topic/group", orDefault(name))
	}
	applyTrace(cfg, traceOn)
	c, err := mqx.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("drivers: new consumer (instance=%s): %w", orDefault(name), err)
	}
	return c, nil
}

// NewConsumerWithTopic 同 NewConsumer，但显式覆盖订阅 topic。
//
// 存在的理由：mqx 的 ConsumerConfig 只有单个 Topic 字段（配置里的
// consumer.topics 数组官方只取第一个），多 topic 订阅只能靠「一个 topic
// 一个 consumer」。job 的 retry 子命令要消费 `*.dlq`，而这些 topic 绝不能
// 写进配置的 consumer.topics —— 否则 consume 子命令会连死信一起消费，
// 业务消息与死信混在一条循环里，重放语义立刻失控。
func NewConsumerWithTopic(ctx context.Context, h configx.Config, name, group, topic string, traceOn bool) (mqx.Consumer, error) {
	if h == nil {
		return nil, fmt.Errorf("drivers: consumer requires configx handle")
	}
	if topic == "" {
		return nil, fmt.Errorf("drivers: consumer topic required (instance=%s)", orDefault(name))
	}
	cfg := mqx.ReadConfigFromConfigx(h, name)
	if cfg.Driver == "" {
		cfg.Driver = "kafka"
	}
	if group != "" {
		cfg.Consumer.GroupID = group
	}
	cfg.Consumer.Topic = topic
	applyTrace(cfg, traceOn)
	c, err := mqx.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("drivers: new consumer (instance=%s, topic=%s): %w", orDefault(name), topic, err)
	}
	return c, nil
}

// applyTrace 调用链开关透传（本包不读 trace 配置）
func applyTrace(cfg *mqx.Config, on bool) {
	cfg.CallChainEnabled = on
	cfg.EnableTrace = on
}

func orDefault(name string) string {
	if name == "" {
		return "default"
	}
	return name
}
