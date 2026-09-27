# add-micro-apps-reference

## Why

hubx 已有 `monolith` 单体参考案例，但缺少"完整微服务拓扑下 hubx 全家桶如何落地"的参考实现——尤其是跨进程 gRPC/Kafka 的全链路 trace 续接、统一开关经 Nacos 下发的多进程一致性，以及微服务架构红线（数据库私有、BFF 按端独立、网关零业务）的可运行示范。本变更在 `hubx/micro_apps/` 从零搭建该参考案例。

## What Changes

- 新建单 go.mod + `apps/` 目录的 11 个可部署单元：gateway、4 个 BFF（app/web/mini/admin）、5 个 SRV（user/product/order/pay/coupon）、job（cobra CLI）
- 统一调用链开关：`configs/trace.yaml` 单一 Nacos dataId，11 服务共读；`pkg/config` 封装 `Trace.On(kind)` 唯一开关入口（总闸 ∧ 细分 ∧ 官方段三者正交）
- 跨进程 trace 续接：rpcx client/server 两侧挂 CompositeTextMapPropagator；Kafka message header 传递 traceparent，job 消费端 Extract 重建 ctx
- 数据私有：MySQL 五库五账号，JOIN 只发生在本 srv 库内（4 表 JOIN 为矩阵锚点），跨域数据走 gRPC 只读接口，对账在 job 内存比对
- 异步事件链：srv-order outbox 表 + 3s 轮询补发 → Kafka（order.completed / coupon.exchanged / points.granted）→ job consume，DLQ + `job retry` 重放，gocron 收编进 `job schedule`
- 验收脚本三件套：`scripts/bring-up.sh`（依赖序启动 11 服务）、`scripts/e2e-flow.py`（主链路跨进程 trace 证据）、`scripts/trace-matrix.py`（11×7 全量开关矩阵），产出 `docs/verify/matrix-report.md`
- docker 底座：MySQL/Kafka(KRaft)/Consul/Nacos/Redis/Jaeger compose 编排 + Nacos 配置灌入
- 文档：`README.md`（拓扑、装配顺序、红线、排查清单）+ `PROMPT.md`（案例生成提示词）

## Capabilities

### New Capabilities

- `service-topology`: 11 服务拓扑、调用深度红线（最深 3 层）、gateway/BFF/SRV/job 各层职责边界（网关零业务、BFF 不直连 DB、BFF 按端独立无共享代码）
- `service-assembly`: 各服务 main.go 装配顺序硬约束（configx → telemetry → logx → infra → rpc → 入口 → 优雅退出反序），11 份逐字同构
- `unified-trace-switch`: 统一 trace 段开关语义（三细分正交、总闸覆盖分层 span、唯一入口 `Trace.On(kind)`）、Nacos 单 dataId 下发、11×7 全量矩阵验收
- `trace-propagation`: 跨进程 trace 续接（rpcx 两侧传播器、Kafka header traceparent 读写受 `Trace.On("mq")` 门控、job 消费端 ctx 重建）
- `data-isolation`: 五库私有数据模型、库内 JOIN 落点、跨域 gRPC 只读接口、禁跨库 JOIN/事务红线
- `async-events`: outbox 可靠投递、Kafka 事件语义（acks=all、幂等消费组、DLQ）、job consume/retry/schedule 三命令
- `acceptance-verification`: 脚本化验收（静态检查、主链路证据、分层 span 清点、11×7 矩阵、Jaeger 复核、三件套可复跑）
- `implementation-guardrails`: 实现前 SDK 源码核对门禁与禁止事项（不改 SDK 源码、不留占位、入链须有实测 span 证据、已知坑补录 README）

### Modified Capabilities

（无——`openspec/specs/` 当前为空，本变更为全新增）

## Impact

- 受影响 specs：上述 8 个新 capability（全部为新增）
- 受影响代码：`hubx/micro_apps/` 全部新建（go.mod、apps/×11、pkg/、infra/、configs/、docker/、scripts/、docs/verify/、README.md、PROMPT.md）
- 依赖：本地 hubx SDK ×8（rpcx/configx/logx/dbx/cachex/mqx/webx/otelx，`replace` 指向本地）、kitex、golang-jwt、gocron、consul/api；运行时依赖 docker 底座（MySQL/Kafka/Consul/Nacos/Redis/Jaeger）
- 不改动任何现有仓库代码与 SDK 源码；无既有 spec 受影响
