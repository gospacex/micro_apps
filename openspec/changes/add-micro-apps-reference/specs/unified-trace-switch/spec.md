## ADDED Requirements

### Requirement: 统一 trace 配置单一事实源
11 个服务 SHALL 从同一个 Nacos dataId（`configs/trace.yaml`，dataId `hubx.trace`，group `HUBX`）读取统一 trace 段；trace 段字段 SHALL 为 `enabled`（总闸）、`exporter`（otlp/stdout）、`endpoint`、`db`、`cache`、`mq`。

#### Scenario: 改一处 11 服务共读
- **WHEN** 通过 Nacos OpenAPI 修改 `hubx.trace` dataId 并重启 11 个服务
- **THEN** 11 个服务全部读到同一份 trace 配置，无需逐服务改配置文件

### Requirement: 开关生效值三段正交
任一 kind（db/cache/mq）的 span 生效值 SHALL 为 `trace.enabled ∧ trace.<kind> ∧ 官方段 enable_trace` 三者按逻辑与计算；省略 trace 段细分键时该 kind SHALL 取官方段（xmiddleware.sqlx/cachex/mqx 的 enable_trace）的值。

#### Scenario: 细分键省略回退官方段
- **WHEN** trace 段仅保留 `enabled: true` 而省略 `db/cache/mq` 键
- **THEN** 三类 span 按官方段 enable_trace 默认值输出（默认全开）

#### Scenario: 官方段关闭压过 trace 段
- **WHEN** trace 段全部为 true 但 `sqlx.enable_trace: false`
- **THEN** SQL span 输出为 0，其余 span 照旧（证明逻辑与关系）

### Requirement: 总闸覆盖分层 span
总闸 `trace.enabled=false` SHALL 比中间件开关管得更宽：除 SQL/缓存/MQ span 外，分层 span（handler/service/repository/middleware）SHALL 一并归零，全部 11 服务 0 span。

#### Scenario: 总闸关全归零
- **WHEN** `enabled: false` 下执行固定请求集
- **THEN** Jaeger 收到的 span 数为 0（含分层 span）

### Requirement: 唯一开关入口
业务与装配代码 SHALL 只通过 `pkg/config` 封装的 `Trace.On(kind)` 判断开关，SHALL NOT 直读 `trace.enabled` 等 bool 字段（防止漏总闸）。

#### Scenario: 直读 bool 被约束禁止
- **WHEN** 审查代码中对 trace 段的读取路径
- **THEN** 所有开关判断经由 `cfg.Trace.On(kind)`，无散落的 `.Enabled`/`.DB` 等字段直读（configx 读取与封装层除外）

### Requirement: 11×7 全量矩阵验收
系统 SHALL 提供 `scripts/trace-matrix.py`，对 11 个服务 × 7 种开关组合（全开/总闸关/db 关/cache 关/mq 关/省略细分键/官方段关）逐一执行并生成 `docs/verify/matrix-report.md`，报表 SHALL 逐格记录各类 span 计数并给出三点结论。

#### Scenario: 矩阵可复跑
- **WHEN** docker 底座就绪后执行 `python3 scripts/trace-matrix.py`
- **THEN** 脚本无人值守完成 7 轮（Nacos 程序化改配置 → 重启 11 服务 → 固定请求集 → 等 flush → Jaeger 计数）并输出 11×7 报表
