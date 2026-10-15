## ADDED Requirements

### Requirement: 三层门禁分级与本裁决规则
验收标准 SHALL 将每一项判定归入 P0 红线 / P1 功能 / P2 增强 三级，并规定：P0 存在任一 FAIL 即不得进入 `/sddflow close`；P1 未过允许带例外登记限期补齐；P2 仅记录不阻断。

#### Scenario: P0 未过阻断收尾
- **WHEN** 判定完成时存在任意一项 P0 标记为 FAIL
- **THEN** 裁决结果为「不满足收尾门槛」，并输出 P0 未过项的补齐清单与执行排序

#### Scenario: P1 带例外放行
- **WHEN** P0 全部 PASS 且存在 P1 未过项，但该未过项已在例外登记表登记
- **THEN** 该 P1 项记为 EXEMPT 而非 FAIL，不阻断收尾，且在判定表中显式列出

### Requirement: 六类判定覆盖
验收标准 SHALL 覆盖且仅覆盖以下六类：静态质量门、服务可运行性、架构红线、统一 trace 开关、跨进程续链、交付物与文档；新增判定类别 MUST 归入这六类之一或经变更修订本规格。

#### Scenario: 六类齐备
- **WHEN** 查阅 `docs/verify/ACCEPTANCE.md`
- **THEN** 六类判定各自成节，无一类缺失，且无六类之外的游离判定项

### Requirement: 判定项五字段结构
标准中的每一项判定 SHALL 包含五个字段：级别、判定命令、通过阈值、证据落点、现状；现状取值 MUST 为 PASS / FAIL / EXEMPT / NOT-RUN 之一。

#### Scenario: 字段齐全可复现
- **WHEN** 任取标准中一项判定
- **THEN** 该项含可直接在仓库根目录执行的判定命令、明确的通过阈值、指向 `docs/verify/` 下具体文件的证据落点，以及四值之一的现状

### Requirement: 例外登记四段式与理由收口
例外登记表 SHALL 为每项 EXEMPT 写明现象、根因、影响范围、处置四段，且登记理由 MUST 限于「SDK 固有限制且本仓不可绕过」；本仓代码可控的未达标项 MUST 判为 FAIL，不得登记为例外。

#### Scenario: SDK 限制登记
- **WHEN** `mqx` SDK 自带 `messaging.kafka.*` span 不随 `Trace.On("mq")` 细分键关闭
- **THEN** 该项登记为 EXEMPT 并写全四段，同时注明只有 traceflag 全局闸能掐断，且本仓 `messaging.outbox.*` 的归零断言仍按 P0 判定

#### Scenario: 本仓可控项不得豁免
- **WHEN** 某项未达标源于本仓脚本或配置（如 e2e 脚本只打 `/api/app` 导致 bff-web/mini/admin 零 span）
- **THEN** 该项判为 FAIL 并进入补齐清单，不得以例外登记绕过

### Requirement: 阈值沿用现有脚本断言且不抬高
标准中的通过阈值 SHALL 沿用三件套现有断言：主链路同 trace 服务数门槛 ≥6、`bring-up.sh` 11 服务全绿、矩阵 C2 总闸归零 / C3 db 归零 / C4 cache 归零 / C6 缺省回退 true / C7 官方段压灭；11 服务同 trace 全覆盖 MUST 列为 P2 而非 P0。

#### Scenario: 门槛不被抬高
- **WHEN** 对照标准与 `scripts/e2e-flow.py`、`scripts/trace-matrix.py` 中的断言
- **THEN** 每个阈值均能在脚本中找到同名断言，且无脚本未实现的更严门槛

#### Scenario: 全覆盖列为增强项
- **WHEN** 判定「11 个服务是否全部出现在同一 trace」
- **THEN** 该项级别为 P2，未达成时记录但不断阻断收尾

### Requirement: C5 组合按产出方拆分判定
矩阵 C5「mq 关」组合 SHALL 拆为两行判定：本仓自产出 `messaging.outbox.*` 归零为 P0；SDK 自带 `messaging.kafka.*` 残留转入例外登记，两行 MUST 在报表中并列呈现。

#### Scenario: 拆分后并行呈现
- **WHEN** 执行 `scripts/trace-matrix.py` 并检查 `docs/verify/matrix-report.md`
- **THEN** 报表同时给出本仓 messaging span 数与 SDK messaging span 数，且前者为 0 时该 P0 项判 PASS

### Requirement: 现状判定与补齐清单产出
标准 SHALL 配套产出 `docs/verify/acceptance-status.md`，对每一项给出判定结果，并在存在 P0 FAIL 时输出按执行依赖排序的补齐清单。

#### Scenario: 判定表覆盖全部项
- **WHEN** 打开 `docs/verify/acceptance-status.md`
- **THEN** 标准中的每一项均有一行判定结果，无遗漏项，且 P0 FAIL 项对应补齐清单中的条目

#### Scenario: 补齐清单按依赖排序
- **WHEN** 存在多条 P0 未过项
- **THEN** 清单按「先补脚本覆盖面、再重跑证据、最后复核文档」的依赖顺序排列，而非按功能模块排列

### Requirement: 判定结论可随三件套复跑刷新
每一项判定的结论 SHALL 由三件套脚本产出或由脚本产出的证据支撑，`docs/verify/acceptance-status.md` MUST 标注证据生成时间，禁止无脚本支撑的人工结论。

#### Scenario: 复跑刷新结论
- **WHEN** 从干净环境依次执行 `docker compose up` → seed-nacos → `scripts/bring-up.sh` → `scripts/e2e-flow.py` → `scripts/trace-matrix.py`
- **THEN** 全部判定项的现状可被刷新，且刷新后的结论与证据文件一致

### Requirement: 收尾裁决规则
验收标准 SHALL 明确：仅当全部 P0 项 PASS（或登记为例外且经确认）时方可进入 `/sddflow close`；任一 P0 FAIL 时 MUST 输出补齐清单并在补齐后重跑判定。

#### Scenario: 准入通过
- **WHEN** 判定表中 P0 项全为 PASS 或 EXEMPT
- **THEN** 裁决为「满足收尾门槛」，允许进入 close 归档

#### Scenario: 准入未通过
- **WHEN** 判定表中存在 P0 FAIL
- **THEN** 裁决为「不满足收尾门槛」，先执行补齐清单并重跑判定，不得直接归档
