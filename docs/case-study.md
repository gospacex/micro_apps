# micro_apps 实例案例说明

> 本文档把 `micro_apps` 作为一个**可运行的微服务参考实例**进行拆解说明：它演示了
> hubx 各 x 系列 SDK（cachex / mqx / dbx / webx / otelx / traceflag）在真实多服务
> 架构下的协同方式，以及「统一调用链开关」「outbox 可靠投递」「优雅退出不丢 span」
> 等关键工程实践。
>
> 配套文档：`.backup/micro_apps-review-2026-09-29.md`（首轮评审）、
> `.backup/micro_apps-independent-review-2026-09-29.md`（独立复审 + 修复记录）。
> 验证状态（2026-09-29）：`gofmt -l` 空 / `go build ./...` 通过 / `go vet ./...`
> 通过 / `go test -count=1 -race ./...` 全绿（14 测试包）。

---

## 1. 定位与范围

`micro_apps` 是 gospacex 代码生成器的**输出样例 + 手工打磨参考实现**。它回答一个问题：
「用 hubx 的 SDK 拼一个带网关、域服务、消息、调用链的微服务系统，标准做法是什么？」

- **已实现并可运行**：网关 `gateway` + 5 个域服务（`srv-user` / `srv-product` /
  `srv-order` / `srv-pay` / `srv-coupon`），共 6 个 `main`。
- **规划中（目录存在、尚未实现 `main`）**：聚合层 `bff-app` / `bff-web` / `bff-mini`
  / `bff-admin`，以及消费者 `job`。这一状态在首轮评审中被标记为「demo 未完成」，
  后续仅补齐了可编译性与调用链正确性，未新增 BFF/job 业务。
- **底座**：docker 六件套（mysql / kafka / consul / nacos / redis / jaeger），
  见 `docker/compose.yaml`。

---

## 2. 拓扑与端口

```
                 ┌─────────────────────────────────────────────┐
   HTTP :8080    │  gateway (webx)                              │
  ──────────────▶│   - 配置驱动路由表，经 consul.Resolve 转发    │
                 │   - 限流计数器走 cachex/redis（fail-open）    │
                 │   - 整条 trace 的根 span 诞生地               │
                 └───────────────┬─────────────────────────────┘
                                 │ HTTP → BFF（规划中，暂未实现）
                                 ▼
                 ┌─────────────────────────────────────────────┐
                 │  BFF 聚合层（bff-app/web/mini/admin，规划中） │
                 └───────────────┬─────────────────────────────┘
                                 │ kitex gRPC（经 consul.Target 解析）
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
   srv-user :9011        srv-product :9012        srv-order :9013
                                               （outbox + mqx/kafka 发事件）
         ▼                       ▼                       ▼
   srv-pay :9014         srv-coupon :9015        job（规划中，消费 kafka）
```

| 服务 | 协议 | 端口 | 依赖中间件 |
|---|---|---|---|
| gateway | HTTP (webx) | 127.0.0.1:8080 | cachex/redis、consul、otelx |
| srv-user | kitex gRPC | :9011 | dbx/mysql、consul、otelx |
| srv-product | kitex gRPC | :9012 | dbx/mysql、consul、otelx |
| srv-order | kitex gRPC | :9013 | dbx/mysql、**mqx/kafka(outbox)**、consul、otelx |
| srv-pay | kitex gRPC | :9014 | dbx/mysql、consul、otelx |
| srv-coupon | kitex gRPC | :9015 | dbx/mysql、consul、otelx |

> 所有域服务经 `infra/consul` 注册（`127.0.0.1:8500`），跨调通过
> `consul.Target` 解析地址 + `pkg/rpcx.BuildClientOptions` 装配客户端。

---

## 3. 技术栈（hubx x 系列）

| 能力 | SDK | 入口 |
|---|---|---|
| HTTP 网关 | webx | `webx.New` + `proxy.Register` |
| gRPC 服务/客户端 | kitex（cloudwego） | `pkg/rpcx.BuildServerOptions` / `BuildClientOptions` |
| 缓存 / 限流 | cachex + redis | `infra/drivers.NewCache` |
| 消息 | mqx + kafka | `infra/drivers.NewProducer` / `NewConsumer` |
| 数据库 | dbx（gorm）+ mysql | `openDB`（dbxconfig.LoadDBConfigFromConfigx） |
| 调用链 | otelx + traceflag | `infra/telemetry.Init` |
| 配置 | configx + nacos | `infra/nacos.Load` |
| 注册发现 | consul | `infra/consul.Register` / `Target` |

---

## 4. 装配总公式（6 步，顺序有约束）

每个 `main` 都遵循同一套装配顺序，**`telemetry.Init` 必须先于任何会标 span 的中间件**
（`cachex` / `dbx` / `mqx` 的 OTel 回调从全局 `TracerProvider` 取 tracer，晚装配会拿到
noop tracer）。以 `srv-order` 为例：

1. `infra/nacos.Load` —— 本地 `config.yaml` 兜底 + Nacos key 级裁决，读出 `pkg/config`
   各段（trace 总闸 + 三段细分开关）。
2. `infra/telemetry.Init`（内部 `otelx.New(*otelx.Config)` + `traceflag` 总闸 +
   `Composite(TraceContext, Baggage)` 传播器）。**红线：必须先于第 3 步。**
3. `infra/drivers` —— `dbx`（Open/OpenNoTrace）、`NewCache`（On("cache")）、
   `NewProducer`（On("mq")）。
4. `infra/consul.Register` —— 注册本服务到 `127.0.0.1:8500`。
5. `order.NewServer(impl, rpcx.BuildServerOptions(...))` —— 装配 kitex 服务端。
6. `signal` 优雅退出（**反序**）：`svr.Stop → outbox.Stop → producer/cache/db close →
   consul.Deregister → tracing.Shutdown（≥10s）→ logger close`。

---

## 5. 统一调用链设计（本实例的核心范式）

调用链开关集中在 `pkg/config.Trace`，遵循 **「唯一开关入口」契约**
（`pkg/config/trace.go:56` 的 `On(kind)`）：

- `Trace.On(kind)` 已折 **总闸 `Enabled`** 与 **细分开关 `DB/Cache/MQ`**（含官方段
  `xmiddleware.*.enable_trace` 裁决），业务代码**不要直接读 bool 字段**，否则易漏总闸。
- `infra/telemetry.Init` 用 `otelx.New` 装配 `TracerProvider`；`otelx.New` 内部
  `traceflag.Apply("otelx", cfg.Tracer.Enabled)` 翻全局总闸，启用态 tracing 真实生效
  （非静默 noop，已核实 `otelx/interface.go:129`）。
- 跨服务续链：kitex 套件（`pkg/rpcx` 内 `kitextracing.NewServerSuite/NewClientSuite`）
  经 `Composite(TraceContext, Baggage)` 传播器注入/提取 `traceparent`。**套件仅在
  `TraceEnabled=true` 时装配**——关追踪时连中间件都不装，避免为 noop 链路付固定成本。
- 导出端点 OTLP/HTTP `127.0.0.1:4318` → Jaeger（见 `configs/trace.yaml` 与
  `docker/compose.yaml`）。

> **统一动作（2026-09-29 复审修复 F3）**：`srv-order` 的 db 开关原写为
> `On("db") && (tracing==nil||tracing.Enabled)`，与 cache/mq 的 `On(kind)` 写法不一致；
> 已统一为 `pcfg.Trace.On("db")` 单一入口，符合上述契约，不产生行为分歧。

---

## 6. outbox 模式（srv-order 可靠投递）

`srv-order` 用 **Transactional Outbox** 保证「落库订单」与「发 Kafka 事件」的最终一致：

- 业务事务内写入 `orders` 与 `outbox`（待发事件），同一事务原子提交。
- 后台 `OutboxRunner` 轮询 `pending` 事件，发 Kafka 成功后 `MarkOutboxSent`
  （`WHERE status=pending` 守卫），失败重试 `MarkOutboxRetry`，超阈 `AbandonOutbox`。
- 关键正确性点（首轮评审的「自我纠正」）：早期版本遍历**全量 pending** 计算放弃名单，
  看似会「重复投递」；但 `MarkOutboxSent` / `AbandonOutbox` 均带 `status=pending` 守卫
  （`order.go:336/352/368`），已发事件（status≠pending）更新不到任何行，故**真实危害
  降级为「误导告警 + 多余 UPDATE」，并非数据故障**。修复后 `abandonCandidates(failed)`
  仅统计真正失败的集合（`apps/srv-order/internal/service/outbox.go`）。

---

## 7. 优雅退出红线（不丢尾部 span）

任何会标 span 的资源（cache / db / producer）必须在 `tracing.Shutdown` **之前**关闭：
否则其关闭动作产生的那批 span 还躺在 `BatchSpanProcessor` 缓冲里就被 flush 丢弃，链路
末端永远缺一小段。因此这些资源**刻意不用 `defer`**（defer 在手动 flush 之后才执行），
改为显式反序关闭，最后 `Shutdown(15s)`。

所有 `main` 采用 `run() error` 模式（而非 `log.Fatalf` 直接退出）——`log.Fatalf` 会跳过
`defer`，导致上述反序关闭与 `Shutdown` 被绕过，尾部 span 丢失。

---

## 8. 审查发现并修复的缺陷案例

本实例经历过两轮代码评审，以下为代表性缺陷与修复（文件:行均经独立核实）：

### 案例 A — Trace.On 被误当「是否构造组件」开关（首轮缺陷 1）

- **现象**：旧实现把 `Trace.On(kind)` 当作「开关关 → 不创建 cache/mq 组件」的判据。
  后果：关 cache 追踪 → 网关限流失效；关 mq 追踪 → outbox 不投递事件。
- **修复**：`infra/drivers.NewCache`（drivers.go:88-94）与 `NewProducer`（:122-123）
  **永远构造组件**，只把 `traceOn` 透传给 `cachex.SetTracingConfig` / `applyTrace`
  （`CallChainEnabled + EnableTrace`）。开关只切「是否入链」，不切「组件是否存在」。
- **沉淀**：`pkg/rpcx/options_test.go` 钉住 `TraceEnabled` 开关与 suite 装配关系。

### 案例 B — outbox 放弃名单误算（首轮缺陷 2/3，含降级纠正）

- **现象 / 误判**：初判「遍历全量 pending 会导致重复投递」。
- **纠正**：追到 SQL 层 `status=pending` 守卫后，确认已发事件不可能被改成放弃态，
  危害降级为「误导告警 + 多余 UPDATE」，并补 `TestOutboxAbandonOnlyFailedEvents` /
  `TestAbandonCandidates`（含负向验证）锁住真实语义。

### 案例 C — log.Fatalf 跳过 defer + 退出顺序丢尾部 span（首轮缺陷 4）

- **修复**：6 个 `main` 统一 `run() error` 模式；退出顺序固定为「关资源 →
  `Shutdown(15s)`」；`pkg/rpcx` 的 `BuildServerOptions` 把 `ServiceAddr` 放在 suite 之前
  避免地址被套件传输设置冲掉。

### 案例 D — 复审追加的小修（独立复审 F1/F2/F3）

- **F1（注释）**：`gateway/main.go` 与 `srv-order/main.go` 头部删掉已废弃的
  `rpcx/configx.InitGlobal` / `rpcx/tracing.Init` 引用，改为指向 `infra/telemetry.Init`。
- **F2（测试）**：新增 `pkg/rpcx/options_test.go`（此前该包零覆盖，却支撑 11 个服务）。
- **F3（统一）**：db 的 `traceOn` 统一为 `pcfg.Trace.On("db")` 单一入口。

---

## 9. 已知限制与 Roadmap

- **BFF 聚合层 + job 消费者未实现**：`apps/bff-*` 与 `apps/job` 目录存在但无 `main`，
  当前可运行实例为 gateway + 5 个域服务。
- **F4 端到端冒烟未实跑**：OTLP/HTTP 4318 → Jaeger 真实导出、SQL span 可数、
  gateway→BFF→srv 全链路 curl 验证，需 docker 六件套真起来；代码路径正确但**未实跑≠已验证**。
- **配置接线钩子**：`pkg/config.Trace.Endpoint` 目前未被 main 读取（各服务硬编码
  `127.0.0.1:4318`），属「保留落点、待接线」项，见 `pkg/config/trace.go:38` 注释。
- **全部改动未提交**：含首轮 17 文件修复与本轮 F1/F2/F3，待 review 后提交。

---

## 10. 快速起步

```bash
# 1) 起底座（mysql/kafka/consul/nacos/redis/jaeger）
docker compose -f docker/compose.yaml up -d

# 2) 起服务（每个开一个终端，先 telemetry 依赖的 nacos/consul 已就绪）
go run ./apps/srv-user/cmd/srv-user
go run ./apps/srv-order/cmd/srv-order
# …… srv-product / srv-pay / srv-coupon / gateway

# 3) 验证链路
curl -v http://127.0.0.1:8080/healthz
# Jaeger UI: http://127.0.0.1:16686
```
