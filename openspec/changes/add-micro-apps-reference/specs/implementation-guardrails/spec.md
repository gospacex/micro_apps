## ADDED Requirements

### Requirement: 实现前 SDK 源码核对门禁
实现任何服务代码之前，SHALL 逐项核对 hubx 本地 SDK（rpcx/configx/logx/dbx/cachex/mqx/webx/otelx）的真实 API 源码，至少覆盖：rpcx 的 resolver 扩展点与 WithTracingMiddleware 传播器、configx 的 Nacos 通道契约（dataId/group）与 trace 段读取入口、mqx 的 Header 存取 API、cachex 的 SetTracingConfig 门、dbx 的 Open/OpenNoTrace 差异与 SQL span 属性名、otelx/webx 的装配入口。SHALL NOT 凭 kitex/OpenTelemetry 通用印象写调用；核对结论 SHALL 以 file:line 形式记录在 PROMPT.md 先读清单中。

#### Scenario: 先读清单有源码依据
- **WHEN** 审查任一 SDK 调用点
- **THEN** 该调用与 PROMPT.md 先读清单中的源码依据（file:line）一致，无凭通用惯例臆造的 API 用法

### Requirement: 禁止修改 SDK 源码
实现 SHALL NOT 为完成功能而修改 `../{rpcx,configx,logx,dbx,cachex,mqx,webx,otelx}` 下任何 SDK 源码；确属 SDK 缺陷时 SHALL 先说明、记录到 README，再做最小修改。

#### Scenario: SDK 目录零改动
- **WHEN** 检查本变更的全部改动文件
- **THEN** 改动范围仅在 `micro_apps/` 内，8 个 SDK 本地目录无任何 diff；若确需修缺陷，README 中有对应记录条目

### Requirement: 禁止占位实现
代码 SHALL NOT 留有 TODO、占位实现或 `panic("not implemented")`；所有任务完成态必须是可运行的真实实现。

#### Scenario: 无占位标记
- **WHEN** 在 `micro_apps/` 执行占位标记扫描（TODO/FIXME/panic("not implemented")）
- **THEN** 业务与装配代码中无匹配结果

### Requirement: 入链须有实测 span 证据
任何"已入链/已串进 trace"的结论 SHALL 有实测 span 计数或 trace_id 证据支撑；无证据 SHALL NOT 宣称入链。

#### Scenario: 结论附证据
- **WHEN** README 验证记录或 matrix-report.md 声称某类 span 已入链
- **THEN** 同一文档给出该类 span 的实测数量（或 trace_id），与脚本输出一致

### Requirement: 已知坑补录
发现的新坑（现象/根因/对策三段）SHALL 及时补进 README 排查清单；已知坑清单 SHALL 同时包含继承 monolith 的 7 条与本案例新增条目。

#### Scenario: 新坑三段式补录
- **WHEN** 实现或验收中出现未记录的静默失败/断链问题
- **THEN** README 排查清单新增一行，含现象、根因、对策，且实现中已有对应规避措施
