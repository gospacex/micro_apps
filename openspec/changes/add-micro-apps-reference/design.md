# add-micro-apps-reference 设计

## Context

hubx 需要一个"完整微服务拓扑下的全家桶参考案例"，与既有 `hubx/monolith` 单体案例同族。完整设计规格已评审并记录于 `docs/superpowers/specs/2026-09-27-micro-apps-design.md`（本文件为其 OpenSpec 决策摘要，冲突时以设计规格为准）。

约束：
- 单 go.mod + `apps/` 目录隔离；`replace` 指向本地 hubx SDK（未发布）
- SDK 未发布，禁止改 SDK 源码实现功能
- 本地 Docker 运行时底座（MySQL/Kafka/Consul/Nacos/Redis/Jaeger）
- configx 无热更新（watch 未实现），配置变更必须重启生效

## Goals / Non-Goals

**Goals:**
- 11 个可部署单元从 Gateway 到存储全链路串进同一条 trace，统一开关一次改、11 进程同开同关
- 跨进程 trace 续接：gRPC（rpcx）与 Kafka（header traceparent）两条路径
- 架构红线可运行示范：库内 JOIN、BFF 按端独立、网关零业务、最深 3 层调用
- 全量矩阵（11×7）脚本化验收，从零可无人值守复跑

**Non-Goals:**
- 不做配置热更新（按重启生效设计）
- 不自研调度（gocron 收编进 job）
- 不服务容器化部署（11 服务本地 `go run`，Dockerfile 仅可选路径）
- 不实现二级 srv 互调、跨库 JOIN、BFF 直连 DB
- 不改 SDK 源码

## Decisions

- **运行时底座：本地 Docker compose** — 备选：kind/远程环境。理由：用户环境已有 Docker，最快可复跑。
- **RPC：rpcx（kitex 栈）** — 备选：原生 grpc-go。理由：hubx 生态统一，避免双轨传播器/插桩实现。
- **服务发现：Consul + 自研 resolver** — rpcx 无内置 Consul resolver，挂 `pkg/rpcx/resolver` 扩展点（`Register` + `WithResolver` 中间件）；动手前先读源码确认，若已内置则薄封装。
- **配置：configx 双通道（本地字段优先）+ Nacos 远程** — 远程/本地裁决收敛在 configx/`infra/nacos` loader，服务代码不写分支；统一 trace 段为单一 Nacos dataId，11 服务共读。
- **统一开关：`pkg/config` 封装 `Trace.On(kind)`** — 生效值 = `trace.enabled ∧ trace.<kind> ∧ 官方段 enable_trace`，三细分正交；业务/装配代码禁止直读 bool 字段。依据：configx.Trace() 只返回 Switches，无 On 方法，monolith 的等价封装落在本仓库 `pkg/config`。
- **传播器：`rpcx.WithTracingMiddleware` 中间件，client/server 两侧都挂** — 备选：otelgrpc 自动插桩。理由：SDK 实际机制即中间件；只挂一侧会静默断在第一跳。
- **Kafka traceparent 门控与 messaging span 同闸（`Trace.On("mq")`）** — mq 关时不写头，job 退化自建根 trace，保证矩阵组合 5 语义自洽。
- **跨域数据：gRPC 只读接口 + job 内存比对** — 备选：跨库 JOIN/共享库。理由：违背微服务红线；对账（order vs pay）拉回内存比对。
- **可靠事件：outbox 表 + 3s 轮询补发** — 备选：分布式事务/独立 producer 服务。理由：库内事务保证一致且不起额外部署单元。
- **telemetry 必须早于 dbx/cachex/mqx 装配** — 三个 SDK 从全局 TracerProvider 取 tracer，晚装 = noop = 静默断链；11 份 main.go 装配顺序逐字同构。
- **kitex_gen 提交入库** — 验收不依赖 kitex CLI。
- **服务不进 compose** — `scripts/bring-up.sh` 本地 `go run` 按依赖序起，改配置重启单服务快。

## Risks / Trade-offs

- [rpcx 无 Consul resolver] → 动手前读 `pkg/rpcx/resolver/resolver.go` 确认扩展点签名；自研 resolver 用 httptest 单测覆盖
- [传播器/Kafka 续接静默断链，编译通过但 trace 断] → e2e-flow.py 强制断言同 trace_id 跨 ≥6 服务、messaging.send/receive 同 trace
- [cachex/dbx tracing 开关与 monolith 认知不一致] → 以当前源码为准，写入 README 排查清单；SetTracingConfig 显式确认
- [期望配置热生效] → 文档与脚本明确"重启生效"，不承诺热更新
- [矩阵 7 组合 × 11 服务耗时（30~40 min）] → `bring-up.sh --services` 过滤重启，不重起 compose
- [GORM Scan 不产 SQL span] → 一律 Find；4 表 JOIN 锚点在验收脚本计数
- [4 个 BFF 无共享代码导致 80% 重复] → 接受重复（按端隔离是裁决，不是缺陷）

## Migration Plan

全新仓库目录，无存量迁移。回滚 = 删除 `micro_apps/` 目录；docker 底座 `docker compose down -v`。

## Open Questions

- （无——关键裁决已在设计评审中拍板，见设计规格 §1 关键裁决表）
