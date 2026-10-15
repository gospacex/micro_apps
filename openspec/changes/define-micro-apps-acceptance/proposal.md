## Why

micro_apps 微服务参考案例已实装 11 个服务与三件套验收脚本，但「什么算验收通过」始终没有成文标准：判定散落在 `tasks.md` 备注、`docs/verify/` 两份证据和脚本断言里，且留下无法自解释的缺口——矩阵 C5「mq 关」断言 FAIL、bff-web/mini/admin/srv-pay/job 证据为 0 span、messaging 续链标记 SKIP。没有标准就无法裁决 `/sddflow close` 能否放行，也无法区分「SDK 固有限制」与「真实缺陷」。本变更产出成文的验收标准（三层门禁 + 六类判定 + 例外登记），并对当前证据逐条判定，输出补齐清单。

## What Changes

- 新增 `docs/verify/ACCEPTANCE.md`：三层门禁（P0 红线 / P1 功能 / P2 增强）× 六类判定的验收标准表，每项含「级别 / 判定命令 / 通过阈值 / 证据落点 / 现状」五字段
- 同一文档内置例外登记表：记录 SDK 固有限制与预期行为（现象 / 根因 / 影响范围 / 处置），登记项不计 FAIL 但必须显式列出，禁止静默放过
- 新增 `docs/verify/acceptance-status.md`：对照标准对当前证据逐条判定（PASS / FAIL / EXEMPT / NOT-RUN），附 P0 未过项的补齐清单与执行排序
- 补齐三件套覆盖缺口：`scripts/e2e-flow.py` 增加 `/api/web`、`/api/mini`、`/api/admin` 三端与 srv-pay、job 路径；补 Kafka 事件链触发，使 `messaging.send` / `messaging.receive` 同 trace 可断言
- 判定结论可复跑刷新：结论由脚本产出或由脚本证据支撑，不由人手写
- 收尾裁决规则：P0 全部 PASS 方可进入 `/sddflow close`；存在 P0 FAIL 则输出补齐清单并重跑

## Capabilities

### New Capabilities

- `acceptance-standard`: 微服务案例的三层门禁验收标准——六类判定、每项五字段结构、例外登记规则、收尾裁决规则

### Modified Capabilities

（无——`openspec/specs/` 为空，且本变更不改动 `add-micro-apps-reference` 的既有 delta）

## Impact

- 新增文档：`docs/verify/ACCEPTANCE.md`、`docs/verify/acceptance-status.md`
- 修改脚本（build 阶段执行）：`scripts/e2e-flow.py`、`scripts/trace-matrix.py`（扩展覆盖面与断言）
- 受影响流程：`add-micro-apps-reference` 的收尾裁决改以本标准为准
- 不改动任何服务业务代码；不改 8 个本地 SDK 源码
