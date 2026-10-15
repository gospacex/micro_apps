## Context

`hubx/micro_apps` 是微服务参考案例：11 个可部署单元（gateway、4 个 BFF、5 个 SRV、job），配三件套验收脚本（`scripts/bring-up.sh`、`scripts/e2e-flow.py`、`scripts/trace-matrix.py`）。当前状态：

- 静态门全绿（`go build` / `go vet` / `gofmt -l` 零输出），`go test -race ./...` 尚未在本轮跑
- 11 服务可启动；矩阵 7 组合已跑完，C1/C2/C3/C4/C6/C7 结论成立，C5「mq 关」断言 FAIL
- e2e 主链路 6 服务（gateway、bff-app、srv-user、srv-product、srv-order、srv-coupon）70 span 同 trace，门槛 6 已过
- 证据缺口：bff-web / bff-mini / bff-admin / srv-pay / job 五者 span 数为 0；messaging 续链标记 SKIP；Jaeger `/api/services` 11 服务名未复核
- 网关路由表已配齐 `/api/app` `/api/web` `/api/mini` `/api/admin` 四个前缀，故 BFF 零 span 的根因在脚本只打 `/api/app`，不在实现

约束：不改 8 个本地 SDK 源码；不改服务业务代码；判定必须可由脚本重跑刷新。

## Goals / Non-Goals

**Goals:**

- 产出成文、可裁决的验收标准，让 `/sddflow close` 有明确放行条件
- 把「SDK 固有限制」与「真实缺陷」在文档层面分开，禁止静默放过任何一项
- 让每项判定都绑定具体命令与阈值，随三件套重跑即可刷新结论

**Non-Goals:**

- 不新增比现有脚本更严格的门槛（标准追不上实现会立刻失真）
- 不重构三件套的内部结构，只补覆盖面与断言
- 不改动任何服务业务代码与 SDK 源码
- 不把标准写成自动化硬门禁（阻断式 hook），本轮保持文档 + 脚本可复跑

## Decisions

### 决策 1：三层门禁（P0 红线 / P1 功能 / P2 增强），而非平铺 checklist

- **取舍**：平铺 checklist 写起来快，但无法回答「哪些没做完也能放行」，收尾必扯皮；指标化 SLO 数值好看，但本案例核心是架构红线与续链证据，多数项不可量化。
- **规则**：P0 未过 → 不得进入 `/sddflow close`；P1 未过 → 允许带例外登记并限期补齐；P2 只记录不阻断。
- **理由**：参考案例的价值在于「红线可示范」，红线必须硬，增强项不能绑架收尾。

### 决策 2：每项五字段（级别 / 判定命令 / 通过阈值 / 证据落点 / 现状），而非纯人工复核表

- **取舍**：人工复核表最省事，但每次结论不可复现；脚本硬门禁最严格，但要改三件套的退出语义，本轮成本过高。
- **规则**：每项必须写明由哪条命令判定、阈值是什么、证据落在哪个文件；现状列填 PASS / FAIL / EXEMPT / NOT-RUN。
- **理由**：结论能随脚本重跑刷新，才有长期价值。

### 决策 3：例外登记显式化——凡 SDK 不可控行为登记为 EXEMPT，其余一律 FAIL

- **取舍**：一律红线会导致 SDK 限制永远无法收尾；只管仓内代码则会放过「本仓可控但未做」的缺陷。
- **判定规则**：本仓代码可控 → FAIL；SDK 固有限制且不可绕过 → EXEMPT，但必须写全现象 / 根因 / 影响范围 / 处置四段。
- **已知登记项**：`mqx` SDK 自带 `messaging.kafka.*` 无条件建 span，不受细分键门控（MQ 关时残留 4 个 span）；e2e 注入外部 traceparent 产生 5 个孤儿 span（预期行为）。

### 决策 4：阈值沿用现脚本已断言的门槛，不抬高

- 主链路同 trace 服务数门槛保持 ≥6（P0）；11 服务全覆盖列为 P2，不阻断
- 矩阵组合断言沿用 C2 总闸归零、C3 db 归零、C4 cache 归零、C6 缺省回退 true、C7 官方段压灭
- C5 改判：本仓 `messaging.outbox.*` 归零为 P0；SDK `messaging.kafka.*` 残留转入例外登记

### 决策 5：标准文档落 `docs/verify/`，`openspec/specs/` 只写契约

- 数值门槛、现状判定表、例外登记表属于易变事实，放 `docs/verify/`；OpenSpec delta 只固化「必须有标准、必须有例外登记、必须可复跑」这类不变量。

## Risks / Trade-offs

- **[标准与实现漂移]** 脚本改了阈值忘了改文档 → 缓解：每项绑定具体命令，重跑即可发现不一致；现状列注明证据生成时间。
- **[例外登记被滥用]** 把真实缺陷登记成 EXEMPT 就能绕过红线 → 缓解：登记项必须写全四段，且仅允许「SDK 不可控」这一条理由；本仓可控项一律不得登记。
- **[覆盖补齐引入新缺口]** 扩展 e2e 到另外 3 个 BFF 后可能暴露新的断链 → 缓解：补齐即重跑，新暴露项按同一标准判定，不再另设临时豁免。
- **[C5 断言改判被视为放水]** 拆分本仓/SDK 两部分可能被质疑降低标准 → 缓解：在标准文档中并列两行，SDK 部分显式标 EXEMPT 并注明只有 traceflag 全局闸能掐。

## Open Questions

- `go test -race ./...` 是否在本轮补跑后直接计入 P0，还是先跑一次确认基线再定级别（暂按 P0 列入）。
- job 侧的 gocron 定时任务是否纳入 e2e 覆盖面（暂列 P2，只覆盖 `job consume` 的 Kafka 续链）。
