## ADDED Requirements

### Requirement: 统一装配顺序
每个服务的 main.go SHALL 按以下固定顺序装配：1) configx.Init（本地优先 + Nacos 远程）→ 2) telemetry.Init（读统一 trace 段，创建全局 TracerProvider）→ 3) logx → 4) infra（dbx → cachex → mqx → Consul 注册）→ 5) 注册 rpc server/client 桩 → 6) 起 webx（gateway/BFF）或 cobra 命令（job）→ 7) 优雅退出反序 + OTel flush。11 份装配 SHALL 逐字同构，且 main.go 头部 SHALL 注释该顺序约束。

#### Scenario: telemetry 先于数据组件
- **WHEN** 任一服务启动并完成装配
- **THEN** telemetry.Init 在 dbx/cachex/mqx 装配之前完成，数据组件 span 走全局 TracerProvider 而非 noop

#### Scenario: 顺序违反即失败
- **WHEN** 任一服务将 telemetry.Init 移至 dbx/cachex/mqx 之后
- **THEN** 该服务的 SQL/缓存/MQ span 静默消失（验收矩阵计数为 0）

### Requirement: 配置企业契约对齐
配置 SHALL 逐字对齐 configx 企业契约键：`xmiddleware.sqlx.databases.<name>`、`xmiddleware.cachex.instances.<name>`、`xmiddleware.mqx.instances.<name>`；DB 与 MQ SHALL 走官方 loader（`dbxconfig.LoadDBConfigFromConfigx` / `mqx.ReadConfigFromConfigx`），SHALL NOT 发明第二套配置结构；远程/本地裁决 SHALL 交由 configx/`infra/nacos` loader 统一处理，服务代码 SHALL NOT 自写 `if remote else local` 分支；本地文件路径 SHALL 只经 `HUBX_CONFIG` 指定（configx 无环境变量覆盖键机制）。

#### Scenario: 契约键逐字匹配
- **WHEN** 对照 11 份服务配置与 configx 企业契约键
- **THEN** 三类 xmiddleware 键路径与字段名逐字一致，无自造别名

#### Scenario: 无本地远程分支
- **WHEN** 审查服务代码中的配置读取路径
- **THEN** 不存在应用层 remote/local 判断分支，裁决集中在 loader 一处

### Requirement: 可用性探针
gateway 与 4 个 BFF SHALL 暴露就绪探针（webx WithReadyProbes）；`scripts/bring-up.sh` SHALL 以 Consul 健康检查与就绪探针作为服务间启动门控。

#### Scenario: 依赖序启动门控
- **WHEN** 执行 `scripts/bring-up.sh`
- **THEN** 启动顺序为 db 就绪 → srv×5 → bff×4 → gateway → job，每步等待上一步健康检查通过后才继续

### Requirement: 优雅退出
服务收到退出信号时 SHALL 按装配反序释放资源，并在退出前 flush OTel BatchSpanProcessor（批超时 5s，退出等待窗口 ≥ 10s），保证已产生 span 不丢失。

#### Scenario: 退出不丢 span
- **WHEN** 服务在处理请求后收到 SIGTERM
- **THEN** 反序关闭 rpc/web/DB/MQ 连接，flush span 导出后进程才退出
