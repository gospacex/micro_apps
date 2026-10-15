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
	"sync"

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
// 返回值是 topicRouter（见下）：业务侧照旧在 Message.Topic 上写目标 topic，
// 路由层按 topic 懒装配一个底层 producer 并把 Message.Topic 清空后再交给
// 驱动——原因见 topicRouter 的注释（kafka-go 禁止 Writer 与 Message 同时带
// topic，而 mqx 的 Writer 一定自带 topic）。多 topic 场景（outbox 行自带
// topic / job 的 DLQ 回发 / coupon 的 points.granted.v1）因此全部可用。
//
// traceOn：调用方传入（= Trace.On("mq")），写 CallChainEnabled + EnableTrace 两份。
func NewProducer(ctx context.Context, h configx.Config, name string, traceOn bool) (mqx.Producer, error) {
	if h == nil {
		return nil, fmt.Errorf("drivers: producer requires configx handle")
	}
	base, err := readProducerConfig(h, name)
	if err != nil {
		return nil, err
	}
	applyTrace(base, traceOn)
	return &topicRouter{cfg: base, traceOn: traceOn}, nil
}

// NewProducerWithTopic 同 NewProducer，但把发布 topic 固定为 topic
// （返回值只往这一个 topic 发，Message.Topic 会被路由层清空）。
// 与 NewConsumerWithTopic 成对存在：某些调用点需要在装配期就钉死 topic。
func NewProducerWithTopic(ctx context.Context, h configx.Config, name, topic string, traceOn bool) (mqx.Producer, error) {
	p, err := NewProducer(ctx, h, name, traceOn)
	if err != nil {
		return nil, err
	}
	r := p.(*topicRouter)
	r.fixed = topic
	return r, nil
}

// readProducerConfig 读 mqx 实例配置（供 NewProducer / topicRouter 复用）
func readProducerConfig(h configx.Config, name string) (*mqx.Config, error) {
	cfg := mqx.ReadConfigFromConfigx(h, name)
	if cfg.Driver == "" {
		cfg.Driver = "kafka"
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("drivers: mq instance %q missing brokers", orDefault(name))
	}
	return cfg, nil
}

// topicRouter 按 Message.Topic 懒装配底层 producer 的路由层。
//
// 【必须存在的理由（实测坑，勿回退）】mqx 的 kafka producer 把 Writer.Topic
// 取成 cfg.Consumer.Topic，且 mqx 的配置归一化会把空 topic 兜成
// "default-topic"（interface.go:263）——也就是说 Writer.Topic **永不为空**。
// 而 kafka-go 的 Writer 一旦自带 Topic，Message.Topic 再非空就直接报
//
//	"kafka.(*Writer): Topic must not be specified for both Writer and Message"
//
// 症状极隐蔽：调用链日志照打 "call chain: kafka send"、send span 照常产出，
// 实际一条都没发出去——outbox 行静默重试到 retry_count=10 后被放弃
// （status=2），Kafka 里空空如也、下游 job 永不消费。
//
// 于是"一个 producer 只能发一个 topic"成了 SDK 硬约束，而本仓偏偏有 4 处
// 多 topic 发送。路由层把约束收在一处：对外仍按 Message.Topic 语义工作，
// 对内一 topic 一 Writer（send 前把 Message.Topic 清空）。
type topicRouter struct {
	cfg     *mqx.Config
	traceOn bool
	fixed   string // 非空：忽略 Message.Topic，固定发往该 topic

	mu       sync.Mutex
	byTopic  map[string]mqx.Producer
	fallback mqx.Producer // Message.Topic 为空时的兜底（走配置默认 topic）
}

// producerFor 取该 topic 的底层 producer（懒装配 + 缓存）
func (t *topicRouter) producerFor(topic string) (mqx.Producer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topic
	if t.fixed != "" {
		key = t.fixed
	}
	if key == "" {
		if t.fallback == nil {
			var err error
			if t.fallback, err = mqx.NewProducer(t.cfg); err != nil {
				return nil, err
			}
		}
		return t.fallback, nil
	}
	if p, ok := t.byTopic[key]; ok {
		return p, nil
	}
	cfg := *t.cfg // 逐 topic 复制一份：Writer.Topic 是 per-producer 的
	cfg.Consumer.Topic = key
	p, err := mqx.NewProducer(&cfg)
	if err != nil {
		return nil, err
	}
	if t.byTopic == nil {
		t.byTopic = map[string]mqx.Producer{}
	}
	t.byTopic[key] = p
	return p, nil
}

// Send 单条发送：按 Message.Topic 选 producer，并把 Topic 清空后下发
func (t *topicRouter) Send(ctx context.Context, msg *mqx.Message) error {
	if msg == nil {
		return fmt.Errorf("drivers: nil message")
	}
	p, err := t.producerFor(msg.Topic)
	if err != nil {
		return fmt.Errorf("drivers: producer for topic %q: %w", msg.Topic, err)
	}
	out := *msg
	out.Topic = "" // Writer 已绑定 topic，再带就报双 topic 错误
	return p.Send(ctx, &out)
}

// SendBatch 批量发送：先按 topic 分组，再逐组下发（组内同样清空 Topic）
func (t *topicRouter) SendBatch(ctx context.Context, msgs []*mqx.Message) error {
	groups := map[string][]*mqx.Message{}
	for _, m := range msgs {
		if m == nil {
			continue
		}
		groups[m.Topic] = append(groups[m.Topic], m)
	}
	for topic, group := range groups {
		p, err := t.producerFor(topic)
		if err != nil {
			return fmt.Errorf("drivers: producer for topic %q: %w", topic, err)
		}
		outs := make([]*mqx.Message, 0, len(group))
		for _, m := range group {
			out := *m
			out.Topic = ""
			outs = append(outs, &out)
		}
		if err := p.SendBatch(ctx, outs); err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭全部懒装配出来的底层 producer
func (t *topicRouter) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for topic, p := range t.byTopic {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("drivers: close producer %q: %w", topic, err)
		}
	}
	if t.fallback != nil {
		if err := t.fallback.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.byTopic = nil
	t.fallback = nil
	return firstErr
}

// Name 驱动名（取底层配置，与 mqx.Producer 契约一致）
func (t *topicRouter) Name() string { return t.cfg.Driver }

// HealthCheck 全部底层 producer 健康才健康（未装配时视为健康：尚未建连）
func (t *topicRouter) HealthCheck() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.byTopic {
		if !p.HealthCheck() {
			return false
		}
	}
	if t.fallback != nil && !t.fallback.HealthCheck() {
		return false
	}
	return true
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
