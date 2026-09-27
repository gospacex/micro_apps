## ADDED Requirements

### Requirement: gRPC 跨进程 trace 续接
rpcx 的 client 与 server 两侧 SHALL 同时挂载 CompositeTextMapPropagator（TraceContext + Baggage，经 `rpcx.WithTracingMiddleware` 中间件），使 gateway→BFF→SRV→SRV 的调用链在任意一跳都续接同一条 trace。

#### Scenario: 任意一跳不断链
- **WHEN** 一条下单请求穿过 gateway → bff-app → srv-order → srv-user/srv-product/srv-coupon
- **THEN** 全部服务的 span 归属同一 trace_id，Jaeger 中呈现连续的 client/server span 对

#### Scenario: 单侧挂传播器视为缺陷
- **WHEN** 任一侧（仅 client 或仅 server）未挂传播器
- **THEN** 链路断在该跳第一跳，e2e 断言失败（同 trace_id 服务数 < 6）

### Requirement: Kafka traceparent 跨进程续接
生产端 SHALL 在 Kafka message header 写入 traceparent（经 propagator Inject），消费端 SHALL 从 `msg.Headers()` 经 `otel.GetTextMapPropagator().Extract` 重建 ctx，使 `messaging.send` 与 `messaging.receive` 归属同一条 trace。

#### Scenario: 事件收发同 trace
- **WHEN** srv-order 发出 `order.completed` 且 job consume 消费之
- **THEN** Jaeger 中 `messaging.send`（srv-order）与 `messaging.receive`（job）具有相同 trace_id，job 续调 srv-coupon 的 gRPC span 亦挂同一条 trace

### Requirement: mq 开关门控 traceparent 写入
traceparent header 的写入 SHALL 受与 messaging span 同一道 `Trace.On("mq")` 开关门控：mq 关闭时 SHALL NOT 写入 header，消费端 SHALL 退化为自建根 trace。

#### Scenario: mq 关闭时消费端自建根
- **WHEN** `trace.mq: false` 下生产与消费 `order.completed`
- **THEN** 消息 header 中无 traceparent，job 消费 span 为新 trace 的根，且 messaging span 计数为 0
