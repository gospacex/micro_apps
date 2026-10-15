# 实现计划：define-micro-apps-acceptance

## 来源
- 提案：openspec/changes/define-micro-apps-acceptance/proposal.md
- 设计：openspec/changes/define-micro-apps-acceptance/design.md
- 规格：openspec/changes/define-micro-apps-acceptance/specs/acceptance-standard/spec.md
- 任务：openspec/changes/define-micro-apps-acceptance/tasks.md

## 实现步骤

### Task 1: 编写三层门禁规则与六类判定表
- [ ] **任务完成**（与 superpowers plan `Task 1`、`tasks.md` 对应条目同步勾选）
- 目标：在 `docs/verify/ACCEPTANCE.md` 中固化三层门禁（P0 红线阻断 close / P1 带例外放行 / P2 只记录）与六类判定（静态质量门、服务可运行性、架构红线、统一 trace 开关、跨进程续链、交付物与文档），每项含级别 / 判定命令 / 通过阈值 / 证据落点 / 现状五字段
- 改动文件：`docs/verify/ACCEPTANCE.md`（新建）
- 验证方式：逐类检查六类是否齐备；逐项检查五字段是否齐全、判定命令可在仓库根目录直接执行

### Task 2: 编写例外登记表与裁决规则
- [ ] **任务完成**（与 superpowers plan `Task 2`、`tasks.md` 对应条目同步勾选）
- 目标：在同一文档追加例外登记表（mqx SDK `messaging.kafka.*` 不受细分键门控、e2e 注入外部 traceparent 产生孤儿 span），每项写全现象 / 根因 / 影响范围 / 处置四段；追加收尾裁决规则（P0 全过方可 close）与三件套复跑说明
- 改动文件：`docs/verify/ACCEPTANCE.md`
- 验证方式：每条登记项四段齐全且理由限于「SDK 不可控」；裁决规则明确 P0 FAIL 时的处置路径

### Task 3: 补跑 race 测试并落证据
- [ ] **任务完成**（与 superpowers plan `Task 3`、`tasks.md` 对应条目同步勾选）
- 目标：执行 `go test -race ./...`，将结果落入 `docs/verify/`，作为静态质量门 P0 项的证据
- 改动文件：`docs/verify/`（新增证据文件）
- 验证方式：`go test -race ./...` 退出码 0，证据文件记录包名与用例数

### Task 4: 采集 Jaeger 服务名清单
- [ ] **任务完成**（与 superpowers plan `Task 4`、`tasks.md` 对应条目同步勾选）
- 目标：从 Jaeger `/api/services` 拉取服务名清单，确认 11 个服务名齐全并落证据
- 改动文件：`docs/verify/`（新增证据文件）
- 验证方式：返回的 11 个服务名与 `apps/` 下 11 个可部署单元一一对应，无缺失

### Task 5: 产出现状逐条判定表
- [ ] **任务完成**（与 superpowers plan `Task 5`、`tasks.md` 对应条目同步勾选）
- 目标：产出 `docs/verify/acceptance-status.md`，对照标准逐条判定，现状取 PASS / FAIL / EXEMPT / NOT-RUN，并标注证据生成时间
- 改动文件：`docs/verify/acceptance-status.md`（新建）
- 验证方式：标准中每一项均有一行判定，无遗漏；现状取值合法；标注证据时间

### Task 6: 产出 P0 补齐清单
- [ ] **任务完成**（与 superpowers plan `Task 6`、`tasks.md` 对应条目同步勾选）
- 目标：在同一文档输出 P0 未过项补齐清单，按「先补脚本覆盖面 → 再重跑证据 → 最后复核文档」排序
- 改动文件：`docs/verify/acceptance-status.md`
- 验证方式：清单条目与判定表中 P0 FAIL 项一一对应，顺序符合依赖排序

### Task 7: e2e 补三端 BFF 路径
- [ ] **任务完成**（与 superpowers plan `Task 7`、`tasks.md` 对应条目同步勾选）
- 目标：扩展 `scripts/e2e-flow.py`，增加 `/api/web`、`/api/mini`、`/api/admin` 三端请求，使 bff-web / bff-mini / bff-admin 产生 span 证据
- 改动文件：`scripts/e2e-flow.py`
- 验证方式：重跑后这三个 BFF 在 Jaeger 中出现非零 span，且 span 树挂入主 trace

### Task 8: e2e 补 srv-pay 与 job 路径
- [ ] **任务完成**（与 superpowers plan `Task 8`、`tasks.md` 对应条目同步勾选）
- 目标：扩展 `scripts/e2e-flow.py`，覆盖 srv-pay 与 job 调用路径（job 走 `job consume` 消费链）
- 改动文件：`scripts/e2e-flow.py`
- 验证方式：重跑后 srv-pay 与 job 在 Jaeger 中出现非零 span，job 侧 span 挂入同 trace

### Task 9: e2e 触发 Kafka 事件链并断言
- [ ] **任务完成**（与 superpowers plan `Task 9`、`tasks.md` 对应条目同步勾选）
- 目标：让 `scripts/e2e-flow.py` 触发下单 → outbox → Kafka → job consume 全链，断言 `messaging.send` 与 `messaging.receive` 同 trace_id、job 续调 srv-coupon 挂同 trace
- 改动文件：`scripts/e2e-flow.py`
- 验证方式：脚本 messaging 段不再 SKIP；send/receive span 的 trace_id 一致；断言失败非零退出

### Task 10: 矩阵脚本拆分 messaging 计数
- [ ] **任务完成**（与 superpowers plan `Task 10`、`tasks.md` 对应条目同步勾选）
- 目标：`scripts/trace-matrix.py` 将 messaging 计数拆为本仓 `messaging.outbox.*` 与 SDK `messaging.kafka.*` 两行并列写入 `docs/verify/matrix-report.md`
- 改动文件：`scripts/trace-matrix.py`、`docs/verify/matrix-report.md`
- 验证方式：报表同时给出两行计数；C5 组合下本仓行为 0，P0 项判 PASS，SDK 残留按例外登记呈现

### Task 11: 重跑三件套刷新证据
- [ ] **任务完成**（与 superpowers plan `Task 11`、`tasks.md` 对应条目同步勾选）
- 目标：依次执行 bring-up → e2e → 矩阵，刷新全部证据文件，同步刷新 `acceptance-status.md` 现状列与补齐清单
- 改动文件：`docs/verify/` 下证据文件、`docs/verify/acceptance-status.md`
- 验证方式：全部判定项的现状可被刷新，且与证据文件内容一致

### Task 12: 复核 P0 并给出收尾裁决
- [ ] **任务完成**（与 superpowers plan `Task 12`、`tasks.md` 对应条目同步勾选）
- 目标：复核 P0 项是否全部 PASS / EXEMPT，在 `acceptance-status.md` 给出收尾裁决结论（满足 / 不满足 close 门槛）
- 改动文件：`docs/verify/acceptance-status.md`
- 验证方式：裁决结论与判定表中 P0 项现状一致；存在 P0 FAIL 时明确列出剩余补齐项
