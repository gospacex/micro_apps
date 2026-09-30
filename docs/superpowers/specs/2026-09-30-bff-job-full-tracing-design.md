# 设计：BFF×4 + job 补全与全链路 trace 上 Jaeger

日期：2026-09-30
关联 OpenSpec change：`openspec/changes/add-micro-apps-reference/`（本设计为其 delta，冲突时以本文件与 change 内 delta 规格为准）

## 背景与现状结论

调查结论（2026-09-30，基于代码实测）：

**已就绪**：`infra/telemetry`（otelx 装配 + Composite(TraceContext, Baggage) 传播器）、`pkg/config.Trace.On(kind)` 三段正交开关、gateway 与 5 个 srv 的 telemetry 装配、`pkg/rpcx` kitex obs-opentelemetry suite（RPC 双向续链）、dbx/cachex/mqx 自带 OTel 回调、Jaeger all-in-one（compose，`127.0.0.1:4318` OTLP/HTTP，`16686` UI）。

**缺口**：

| # | 缺口 | 位置 | 后果 |
|---|------|------|------|
| 1 | 5 个 srv 的 rpc 桩 `ctx, _ = telemetry.Start(...)`，span 永不 End | `apps/srv-*/internal/rpc/*.go:32-37` | 分层 span 不导出，db span 挂不存在 parent，Jaeger missing-parent |
| 2 | webx `Otel` 中间件不 Extract `traceparent`（只认 `X-Request-Id`） | `../webx/middleware/otel.go:17-50` | HTTP 入站开新 trace，gateway→BFF 必断 |
| 3 | gateway 出站只注入 `traceparent`，丢 `tracestate`/`baggage`；入站旧值漏到下游 | `apps/gateway/internal/proxy/proxy.go:45-52,128-134` | 三元组不一致 |
| 4 | `configs/trace.yaml`（Nacos `hubx.trace`）无任何服务读取；`trace.endpoint`/`sample_ratio` 硬编码 | 全部 6 个 main.go、`pkg/config/trace.go:40-44` | 统一开关空转 |
| 5 | `tx.Exec` 绕过 GORM 回调无 span | `apps/srv-user/internal/repository/user.go:94` | 注册事务 INSERT 不可见 |
| 6 | outbox 补发器/srv-coupon 后台发送无 messaging span，且用 `context.Background()` 丢链 | `apps/srv-order/internal/service/outbox.go:97-205`、`apps/srv-coupon/internal/service/coupon.go:325-364` | send/receive 同 trace 断言无法满足 |
| 7 | 4 个 BFF + job 无代码（仅 `.gitkeep`） | `apps/bff-*/`、`apps/job/` | gateway→bff→srv 中间整段不存在 |
| 8 | 验收三件套脚本、README/PROMPT、trace.stdout.yaml 缺失 | `scripts/`、`README.md`、`configs/trace.stdout.yaml` | tasks 3.1–3.5 未交付 |

## Goals / Non-Goals

**Goals**

- 修复缺口 1–6，使已实现的 gateway/srv 链路在 Jaeger 中完整、无 missing-parent
- 实现 bff-app/web/mini/admin 与 job，达到 gateway → BFF 各层（handler/service/rpc client/cache）→ SRV（rpc server/service/db/cache/mq）全链路同 trace
- 接通统一 trace 配置（Nacos `hubx.trace` 单一事实源，11 服务共读）
- 交付验收三件套（bring-up.sh / e2e-flow.py / trace-matrix.py）与文档，达成 `acceptance-verification` 全部 Requirement

**Non-Goals**

- 不重构已实现的 srv/gateway 业务逻辑（仅修链路相关缺陷）
- 不做配置热更新（沿用"重启生效"裁决）
- 不实现规格外的新业务接口（BFF 聚合面以覆盖 e2e 断言所需的最小集合为准）
- 不改 8 个 SDK 源码；唯一例外是缺口 2（webx 缺陷，按 `implementation-guardrails` 走"说明 + README 记录 + 最小修改"流程）

## 架构与组件

### 1. 链路修复包（缺口 1–6）

**1.1 srv 分层 span 修复**：`telemetry.Start` 的调用点全部改为持有返回的 span 并 `defer span.End()`。落点：5 个 `apps/srv-*/internal/rpc/*.go` 的 guard。同时给 gateway 的 auth/ratelimit 中间件补 `middleware` 层 span（`layers.go` 已定义该层约定）。

**1.2 webx 入站续链（SDK 缺陷，最小修改）**：`../webx/middleware/otel.go` 的 `Otel` 在 `tr.Start` 前先执行：

```go
ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(req.Header))
```

`X-Request-Id` 种子逻辑保留为无 `traceparent` 时的兜底。README 排查清单新增三段式条目（现象：HTTP 入站开新 trace；根因：webx Otel 未 Extract；对策：最小补丁）。

**1.3 gateway 出站注入完整化**：`proxy.go:128-134` 改用 `propagation.HeaderCarrier(outReq.Header)` 整体 Inject；`tracestate`、`baggage` 加入 `hopByHop` 剥离表（`proxy.go:45-52`）。

**1.4 统一 trace 配置接线**：扩展 `infra/nacos` loader 支持双 dataId 合并——`hubx.<svc>`（私有段）+ `hubx.trace`（统一 trace 段），trace 段以 `hubx.trace` 为单一事实源覆盖本地兜底；裁决收敛在 loader，服务代码无 remote/local 分支（符合 `service-assembly`）。`telemetry.Init` 改读 `Trace.Endpoint` 与 `Trace.SampleRatio`，删除各 main 的硬编码 `jaegerEndpoint` 常量；采样器由 `SampleRatio` 决定（`always_on` 为 1.0 的特例）。

**1.5 DB 盲区**：`apps/srv-user/internal/repository/user.go:94` 的 `tx.Exec` 改为 `tx.Create(&Profile{...})`，走 GORM create 回调产出 span。

**1.6 messaging 续链**：srv-order outbox `sendOne` 与 srv-coupon 后台发送：从 outbox 行/事件上下文取 `traceparent`，`Extract` 后以 `telemetry.Start(ctx, "messaging", ...)` 开 send span 并 `End`；`traceparent` header 写入与 messaging span 同受 `Trace.On("mq")` 门控（mq 关时不写头，符合 `trace-propagation`）。

### 2. BFF×4（tasks 2.4–2.5）

先做 **bff-app 模板**并跑通 e2e，再按端独立展开 bff-web / bff-mini / bff-admin。规格红线：四者互不 import、不共享业务代码（仅依赖 `pkg/` + `infra/`）、不直连 DB（无 `xmiddleware.sqlx` 配置、不 import dbx）、不二次验签（消费 gateway 透传的 `X-User-Id` 等身份头）。

每个 BFF 的结构：

```
apps/bff-<x>/
├── cmd/bff-<x>/main.go        # 7 步装配，逐字同构（头部注释顺序约束）
├── internal/handler/          # HTTP 入站；webx Otel 自动出 handler span（修好 1.2 后续链）
├── internal/service/          # errgroup 并发聚合 srv 数据；降级兜底分支；手动 telemetry.Start(ctx,"service",...)
├── internal/client/           # kitex client 桩，rpcx.BuildClientOptions(TraceEnabled: pcfg.Trace.Enabled)
├── internal/cache/            # 展示层缓存（cachex），命中/未命中两种 span（tasks 2.4 验收点）
└── config.yaml                # 本地兜底，无 sqlx 段
```

埋点分层（与 `infra/telemetry/layers.go` 约定一致）：`handler.*`（自动）→ `service.<svc>.<op>`（手动，含 errgroup 每个并发分支的子 span 与降级分支 span）→ rpc client span（kitex suite 自动）→ `redis.*`（cachex 回调自动，`Trace.On("cache")` 门控）。

统一响应格式（code/message/data，复用 `pkg/codes`）；就绪探针 `webx.WithReadyProbes`。

bff-app 聚合面（覆盖 e2e 主链路断言）：用户详情（srv-user）、商品详情（srv-product）、下单（srv-order，触发 srv-order→srv-product 扣库存与 outbox→Kafka→job→srv-coupon 链）、优惠券列表（srv-coupon）。其余三端按端裁剪 DTO 与聚合组合。

### 3. job（tasks 2.6）

cobra 三命令，进程无 DB DSN：

- `consume`：`drivers.NewConsumer`（`CallChainEnabled` 透传）消费 `order.completed`/`coupon.exchanged`；从 `msg.Headers["traceparent"]` Extract 续链开 `messaging.receive` span（mq 关时退化为自建根 trace，符合规格场景）；固定 group `job-order-v1`；幂等消费；失败进 `*.dlq` topic；消费 `order.completed` 后续调 srv-coupon 积分接口，RPC span 挂同 trace（e2e 断言点）。
- `retry`：DLQ 重放，`job:dlq:{msg_id}` 缓存键防重（cachex SETNX 语义）。
- `schedule`：gocron 宿主。对账任务经 gRPC 拉 srv-order/srv-pay 只读流水内存比对（不跨库）；库存日清、报表同理走 gRPC；每个任务 `telemetry.Start(context.Background(), "task", ...)` 自建根 span。

### 4. 数据流（主链路 trace）

```
client
  → gateway  [Otel root span → middleware.auth/ratelimit span → handler.gateway.Proxy span]
      --HTTP traceparent/tracestate/baggage-->
  → bff-app  [handler span(Extract 续链) → service.bff.* 聚合 span(errgroup 子 span/降级 span) → redis span → kitex client span]
      --kitex metainfo traceparent-->
  → srv-order [rpc server span → service.order.CreateOrder span → db.* span(含 4 表 JOIN 锚点) → kitex client span → srv-product]
      --Kafka header traceparent (outbox, Trace.On("mq") 门控)-->
  → job      [messaging.receive span(Extract 续链) → kitex client span → srv-coupon]
```

全程同一 trace_id；e2e 断言 ≥6 服务（gateway、bff-app、srv-order、srv-user/srv-product、srv-coupon、job）。

### 5. 错误处理

- BFF 聚合：errgroup 任一分支失败 → 该分支降级兜底（缓存旧值/默认 DTO），降级路径开独立 span 并记录 `degraded=true` 属性；主流程不因单分支失败整体 5xx。
- job 消费：处理失败 → 消息进 `*.dlq`（ack 原消息防堆积）；重放经防重键幂等。
- 开关关闭语义：`trace.enabled=false` → 全组件 0 span（含分层 span，traceflag 全局熔断）；`trace.<kind>=false` → 仅该类中间件 span 归零，组件照常构造（沿用 2026-09-29 修复的"开关不决定可用性"语义）。
- 优雅退出：反序释放 + ≥10s flush 窗口（沿用现有模式，BFF/job 同构）。

### 6. 测试与验收

- 单测补齐：BFF 降级测试（service 分支失败 → 兜底 DTO + 降级 span 断言）、job traceparent 续接测试（header Extract → 同 trace_id）、loader 双 dataId 合并表驱动测试；`go test -race ./...` 全绿。
- `scripts/bring-up.sh`：依赖序启动（db 就绪 → srv×5 → bff×4 → gateway → job），Consul 健康检查 + 就绪探针门控，`--services`/`--reload` 过滤。
- `scripts/e2e-flow.py`：下单主链路 + Kafka 事件 + job 消费；等 flush ≥10s 后从 Jaeger API 断言同 trace_id ≥6 服务、`messaging.send/receive` 同 trace、job→srv-coupon 同 trace；打印 span 树。
- `scripts/trace-matrix.py`：11×7 矩阵（全开/总闸关/db 关/cache 关/mq 关/省略细分键/官方段关），Nacos 程序化改配置 + 重启 + 固定请求集 + Jaeger 计数，产出 `docs/verify/matrix-report.md`（含 4 表 JOIN SQL span 锚点、组合 5 无 traceparent 头断言）。
- `configs/trace.stdout.yaml`：stdout exporter 零依赖验证路径。
- 文档：`README.md`（拓扑、装配顺序、开关设计、JOIN 红线、SDK 边界、排查清单——继承 monolith 7 条 + 本次新坑：webx Extract 缺陷、span 不 End、tx.Exec 盲区、双 dataId 接线、硬编码 endpoint，逐条现象/根因/对策）、`PROMPT.md`（7 节结构 + 先读源码清单）。

### 7. OpenSpec 补充（在 `add-micro-apps-reference` 上原地 delta）

- `trace-propagation`：修正"gRPC/`rpcx.WithTracingMiddleware`"过时措辞为 kitex obs-opentelemetry suite + 仓内 `pkg/rpcx` 的实际机制；新增 Requirement"HTTP 入站续链"（gateway/BFF 的 web 入站 SHALL Extract traceparent/tracestate/baggage 续链，无 traceparent 时退化为新根）。
- `unified-trace-switch`：补充 loader 双 dataId 合并机制说明与 `trace.endpoint`/`sample_ratio` 接线要求。
- `implementation-guardrails`：README 已知坑清单新增本案例条目（webx 缺陷修复按"说明+记录+最小修改"执行）。
- `tasks.md`：追加本次任务清单（修复包 / bff-app 模板 / BFF×3 展开 / job / loader 接线 / 三件套脚本 / 文档），更新 2.1–2.3 状态备注。

## 实施顺序

1. 修复包 1.1–1.3、1.5（span 上链前提）→ `go build/test` 绿 + 手动冒烟 gateway→srv 同 trace
2. loader 双 dataId + endpoint/sample_ratio 接线（1.4）→ 改 Nacos `hubx.trace` 验证 6 服务同开同关
3. messaging 续链（1.6）→ outbox→Kafka header 可见
4. bff-app 模板 → e2e ≥5 服务同 trace（gateway/bff-app/srv-order/srv-product/srv-user）
5. job（consume/retry/schedule）→ e2e 含 messaging 同 trace ≥6 服务
6. bff-web/mini/admin 展开 → Jaeger 11 服务名齐全
7. 三件套脚本 + stdout 变体 + 文档 + 矩阵报表
8. openspec delta 落定 + tasks 勾选 + 收尾（gofmt/vet/禁令扫描）

## 风险

| 风险 | 对策 |
|------|------|
| webx 补丁影响 gateway 现有行为 | Extract 仅在存在 traceparent 时生效，无头时走原 X-Request-Id 兜底；gateway 冒烟回归 |
| 双 dataId 合并改变现有配置裁决语义 | 仅在 `hubx.trace` 存在时覆盖 trace 段；loader 表驱动单测覆盖优先级矩阵 |
| BFF×3 机械复制引入不一致 | 以 bff-app 冒烟通过的模板为准逐字展开，差异仅限 DTO/聚合组合/端口 |
| 矩阵 7×11 耗时 | `bring-up.sh --services` 过滤重启，不重起 compose（沿用 design.md 裁决） |
| kitex metainfo 传播依赖 TTHeader | 不切传输协议；如未来切换需重验续链（记入 README 排查清单） |
