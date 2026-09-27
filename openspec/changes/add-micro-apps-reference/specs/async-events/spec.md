## ADDED Requirements

### Requirement: outbox 可靠投递
srv-order SHALL 在订单事务内写入本库 outbox 表，并以 3s 轮询补发未投递事件；SHALL NOT 依赖独立生产者服务或分布式事务。

#### Scenario: 事务一致后投递
- **WHEN** CreateOrder 事务提交成功
- **THEN** outbox 记录写入同一事务，轮询任务在 3s 内将 `order.completed.v1` 发往 Kafka；事务回滚时不产生事件

### Requirement: 事件契约与生产消费配对
系统 SHALL 定义三个事件：`order.completed`（srv-order → job）、`coupon.exchanged`（srv-coupon → job）、`points.granted`（job 调 srv-coupon 后由 coupon 服务发出，链路终点）；事件体 SHALL 含 schema 版本号，Kafka SHALL 配置 acks=all。

#### Scenario: 事件体可校验
- **WHEN** 任一事件被消费
- **THEN** 消费端可依据 SchemaVer 字段判断兼容性，消息投递为 acks=all 确认

### Requirement: 消费幂等与 DLQ 重放
Kafka 消费 SHALL 使用固定 consumer group（`job-order-v1`）并幂等消费；消费失败 SHALL 进入对应 `*.dlq` topic，`job retry` 命令 SHALL 能重放 DLQ 消息且以 `job:dlq:{msg_id}` 缓存键防重。

#### Scenario: 失败进 DLQ 并可重放
- **WHEN** job consume 处理某条消息失败
- **THEN** 消息进入 `*.dlq` topic；执行 `job retry` 重放该消息成功后不重复记账（防重锁生效）

### Requirement: job 三命令职责
job SHALL 提供 cobra 三命令：`consume`（Kafka 消费 + traceparent 续接）、`retry`（DLQ 重放）、`schedule`（gocron 宿主：对账、库存日清、报表，任务不直连 DB、自建根 span）。

#### Scenario: schedule 任务走 gRPC 不直连 DB
- **WHEN** `job schedule` 执行对账任务
- **THEN** 任务经 gRPC 拉取流水，job 进程无任何数据库 DSN，每个任务产生独立的 task 根 span
