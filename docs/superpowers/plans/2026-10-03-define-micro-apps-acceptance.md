# 微服务案例验收标准 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为 `hubx/micro_apps` 微服务参考案例产出成文的验收标准（三层门禁 + 六类判定 + 例外登记），对现有证据逐条判定，并补齐三件套脚本的覆盖缺口。

**架构：** 标准文档落在 `docs/verify/ACCEPTANCE.md`（六类判定表，每项五字段 + 例外登记 + 裁决规则），现状判定落在 `docs/verify/acceptance-status.md`。判定结论全部由 `scripts/` 三件套产出或支撑，可重跑刷新。脚本侧补两处缺口：e2e 覆盖面（另外 3 个 BFF + srv-pay + job + Kafka 事件链）与矩阵脚本的 messaging 分桶断言。

**技术栈：** Python 3 标准库（urllib / hmac / json，零依赖）、Go 工具链（go build / vet / gofmt / test -race）、OpenTelemetry + Jaeger API、Nacos 配置下发、OpenSpec CLI 1.4.1

---

**Traceability (sddflow):**
- plan-ready: `openspec/changes/define-micro-apps-acceptance/plan-ready.md`
- tasks: `openspec/changes/define-micro-apps-acceptance/tasks.md`
- plan: `docs/superpowers/plans/2026-10-03-define-micro-apps-acceptance.md`

---

### 任务 1：编写三层门禁规则与六类判定表

**文件：**
- 创建：`docs/verify/ACCEPTANCE.md`

> **trace:** plan-ready.md → `### Task 1: 编写三层门禁规则与六类判定表` | tasks.md → `- [ ] 1.1 编写 `docs/verify/ACCEPTANCE.md`：三层门禁规则（P0/P1/P2）与六类判定表，每项含级别 / 判定命令 / 通过阈值 / 证据落点 / 现状五字段`
> **sync:** tasks.md → `- [ ] 1.1 编写 `docs/verify/ACCEPTANCE.md`：三层门禁规则（P0/P1/P2）与六类判定表，每项含级别 / 判定命令 / 通过阈值 / 证据落点 / 现状五字段` | plan-ready.md → `### Task 1: 编写三层门禁规则与六类判定表`

- [ ] **步骤 1：写入文档头与三层门禁定义**

创建 `docs/verify/ACCEPTANCE.md`，先写适用范围与分级规则：

```markdown
# 微服务案例验收标准（hubx/micro_apps）

> 本标准由 `/sddflow` 变更 `define-micro-apps-acceptance` 产出。
> 判定结论随 `scripts/` 三件套重跑刷新，禁止无脚本支撑的人工结论。

## 三层门禁

| 级别 | 含义 | 裁决 |
| --- | --- | --- |
| P0 红线 | 架构红线与核心证据 | 任一 FAIL 不得进入 `/sddflow close` |
| P1 功能 | 功能完整性与交付物 | 未过可带例外登记限期补齐，不阻断 |
| P2 增强 | 覆盖广度与体验项 | 仅记录，不阻断 |

现状取值：`PASS` / `FAIL` / `EXEMPT`（已登记例外）/ `NOT-RUN`（本轮未执行）。
```

- [ ] **步骤 2：写入 A/B/C 三类判定表（静态质量门 / 服务可运行性 / 架构红线）**

```markdown
## A 静态质量门

| 项 | 级别 | 判定命令 | 通过阈值 | 证据落点 | 现状 |
| --- | --- | --- | --- | --- | --- |
| A1 编译 | P0 | `go build ./...` | 退出码 0 | acceptance-status.md | NOT-RUN |
| A2 静态检查 | P0 | `go vet ./...` | 退出码 0 | acceptance-status.md | NOT-RUN |
| A3 格式 | P0 | `gofmt -l .` | 输出为空 | acceptance-status.md | NOT-RUN |
| A4 race 测试 | P0 | `go test -race ./...` | 退出码 0 | docs/verify/race-evidence.md | NOT-RUN |

## B 服务可运行性

| 项 | 级别 | 判定命令 | 通过阈值 | 证据落点 | 现状 |
| --- | --- | --- | --- | --- | --- |
| B1 依赖序启动 | P0 | `bash scripts/bring-up.sh up` | 11 个服务全部 healthy | .hubx-bin/bring-up.log | NOT-RUN |
| B2 优雅退出不丢尾部 span | P0 | `bash scripts/bring-up.sh down` 后查 Jaeger | 停止前请求产生的 span 仍可查 | acceptance-status.md | NOT-RUN |

## C 架构红线

| 项 | 级别 | 判定命令 | 通过阈值 | 证据落点 | 现状 |
| --- | --- | --- | --- | --- | --- |
| C1 数据私有（BFF 不直连 DB） | P0 | `grep -rn "sqlx" apps/bff-*/config.yaml \| grep -v "#"` | 零输出 | configs/services/ | NOT-RUN |
| C2 跨 BFF 零共享代码 | P0 | `for d in app web mini admin; do grep -rn "micro_apps/apps/bff-" apps/bff-$d/ --include="*.go" \| grep -v "apps/bff-$d/"; done` | 零输出 | apps/bff-*/ | NOT-RUN |
| C3 网关零业务（无 DB） | P0 | `grep -n "sqlx" configs/services/gateway.yaml \| grep -v "#"` | 零输出 | configs/services/gateway.yaml | NOT-RUN |
| C4 调用深度 ≤3 | P0 | 查 `docs/verify/e2e-evidence.md` span 树最大深度 | ≤3 | docs/verify/e2e-evidence.md | NOT-RUN |
```

- [ ] **步骤 3：写入 D/E/F 三类判定表（统一 trace 开关 / 跨进程续链 / 交付物与文档）**

```markdown
## D 统一 trace 开关（11×7 矩阵）

| 项 | 级别 | 判定命令 | 通过阈值 | 证据落点 | 现状 |
| --- | --- | --- | --- | --- | --- |
| D1 C2 总闸归零 | P0 | `python3 scripts/trace-matrix.py --combos 2` | C2 全服务 span 总数 = 0 | docs/verify/matrix-report.md | NOT-RUN |
| D2 C3 db 细分归零 | P0 | `python3 scripts/trace-matrix.py --combos 3` | db.* = 0 且 service.* > 0 | docs/verify/matrix-report.md | NOT-RUN |
| D3 C4 cache 细分归零 | P0 | `python3 scripts/trace-matrix.py --combos 4` | cache.* = 0 | docs/verify/matrix-report.md | NOT-RUN |
| D4 C6 缺省回退 true | P0 | `python3 scripts/trace-matrix.py --combos 6` | 计数与 C1 一致 | docs/verify/matrix-report.md | NOT-RUN |
| D5 C7 官方段压灭 | P0 | `python3 scripts/trace-matrix.py --combos 7` | db/mq 归零 | docs/verify/matrix-report.md | NOT-RUN |
| D6 C5 本仓 messaging 归零 | P0 | `python3 scripts/trace-matrix.py --combos 5` | `messaging_own` = 0 | docs/verify/matrix-report.md | NOT-RUN |
| D7 C5 SDK messaging 残留 | EXEMPT | 同上，读 `messaging` 桶计数 | 记录数值，不判定 | 例外登记表 | NOT-RUN |

## E 跨进程续链

| 项 | 级别 | 判定命令 | 通过阈值 | 证据落点 | 现状 |
| --- | --- | --- | --- | --- | --- |
| E1 主链路 ≥6 服务同 trace | P0 | `python3 scripts/e2e-flow.py --min-services 6` | 同 trace 服务数 ≥6 | docs/verify/e2e-evidence.md | NOT-RUN |
| E2 messaging send/receive 同 trace | P1 | 同上（Kafka 触发后） | send 与 receive 同 trace_id | docs/verify/e2e-evidence.md | NOT-RUN |
| E3 job 续调挂同 trace | P1 | 同上 | job span 的父 span 在本 trace 内 | docs/verify/e2e-evidence.md | NOT-RUN |
| E4 11 服务全覆盖 | P2 | 同上（四端路径补齐后） | 11 个服务名均出现 | docs/verify/e2e-evidence.md | NOT-RUN |

## F 交付物与文档

| 项 | 级别 | 判定命令 | 通过阈值 | 证据落点 | 现状 |
| --- | --- | --- | --- | --- | --- |
| F1 README 排查清单三段式 | P1 | 人工核对 README「排查清单」每条目 | 每条含现象 / 根因 / 对策 | README.md | NOT-RUN |
| F2 PROMPT 结构齐备 | P1 | 核对 `PROMPT.md` 章节 | 复刻 monolith 7 节结构 | PROMPT.md | NOT-RUN |
| F3 三件套无人值守复跑 | P1 | compose up → seed-nacos → bring-up → e2e → matrix | 全流程无人工干预 | docs/verify/ | NOT-RUN |
| F4 证据齐备 | P1 | `ls docs/verify/` | e2e-evidence.md / matrix-report.md / acceptance-status.md 存在 | docs/verify/ | NOT-RUN |
```

- [ ] **步骤 4：校验六类齐备与五字段齐全**

运行：`grep -c "^## " docs/verify/ACCEPTANCE.md`
预期：≥8（六类判定节 + 三层门禁节 + 后续例外登记节）；每一判定行的表格列数为 6（项 / 级别 / 判定命令 / 通过阈值 / 证据落点 / 现状）

- [ ] **步骤 5：Commit**

```bash
git add docs/verify/ACCEPTANCE.md
git commit -m "docs(verify): 微服务案例验收标准——三层门禁 + 六类判定表"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 2：编写例外登记表与裁决规则

**文件：**
- 修改：`docs/verify/ACCEPTANCE.md`

> **trace:** plan-ready.md → `### Task 2: 编写例外登记表与裁决规则` | tasks.md → `- [ ] 1.2 在同一文档编写例外登记表（现象 / 根因 / 影响范围 / 处置四段式）与收尾裁决规则、可复跑说明`
> **sync:** tasks.md → `- [ ] 1.2 在同一文档编写例外登记表（现象 / 根因 / 影响范围 / 处置四段式）与收尾裁决规则、可复跑说明` | plan-ready.md → `### Task 2: 编写例外登记表与裁决规则`

- [ ] **步骤 1：追加例外登记表**

在 `docs/verify/ACCEPTANCE.md` 末尾追加：

```markdown
## 例外登记表

登记理由仅限于「SDK 固有限制且本仓不可绕过」。本仓代码可控的未达标项一律判 FAIL，不得登记。

| 编号 | 项 | 现象 | 根因 | 影响范围 | 处置 |
| --- | --- | --- | --- | --- | --- |
| X1 | D7 C5 SDK messaging 残留 | `mq` 细分键关闭后 Jaeger 仍有 4 个 `messaging.kafka.*` span | mqx SDK 无条件建 span，不受 `Trace.On("mq")` 门控（见 `../mqx/FEATURE_MATRIX.md`） | 仅 SDK 自带 span；本仓 `messaging.outbox.*` 仍受控归零 | 记录数值不判定；需要彻底掐断时用 traceflag 全局闸 |
| X2 | 注入外部 traceparent 产生孤儿 span | e2e 注入 `00-<trace>-00f067aa0ba902b7-01`，span 树显示 5 个 `<MISSING 00f067aa>` | 父 span id 由脚本编造，Jaeger 中不存在该父 span | 仅影响 span 树展示，不影响 trace_id 连续性 | 预期行为，不计缺陷 |
```

- [ ] **步骤 2：追加收尾裁决规则与复跑说明**

````markdown
## 收尾裁决规则

1. P0 项全部为 `PASS` 或 `EXEMPT` → 满足 `/sddflow close` 门槛
2. 存在任一 P0 `FAIL` → 不满足门槛，输出补齐清单，补齐后重跑判定
3. P1 未过允许带例外登记放行，但必须在 `docs/verify/acceptance-status.md` 中列出

## 可复跑说明

从干净环境依次执行：

```bash
docker compose -f docker/docker-compose.yml up -d
bash configs/seed-nacos.sh
bash scripts/bring-up.sh up
python3 scripts/e2e-flow.py
python3 scripts/trace-matrix.py
```

判定项的现状随之刷新；`docs/verify/acceptance-status.md` 须标注本轮证据生成时间。
````

- [ ] **步骤 3：校验四段式完整**

运行：`grep -n "^| X" docs/verify/ACCEPTANCE.md`
预期：每行 6 列（编号 / 项 / 现象 / 根因 / 影响范围 / 处置），无空列

- [ ] **步骤 4：Commit**

```bash
git add docs/verify/ACCEPTANCE.md
git commit -m "docs(verify): 例外登记表 + 收尾裁决规则 + 可复跑说明"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 3：补跑 race 测试并落证据

**文件：**
- 创建：`docs/verify/race-evidence.md`

> **trace:** plan-ready.md → `### Task 3: 补跑 race 测试并落证据` | tasks.md → `- [ ] 2.1 执行 `go test -race ./...` 并将结果落入 `docs/verify/``
> **sync:** tasks.md → `- [ ] 2.1 执行 `go test -race ./...` 并将结果落入 `docs/verify/`` | plan-ready.md → `### Task 3: 补跑 race 测试并落证据`

- [ ] **步骤 1：执行 race 测试并收集结果**

运行：`go test -race ./... 2>&1 | tee /tmp/race.txt; echo "exit=${PIPESTATUS[0]}"`
预期：全部 `ok` 或无 `FAIL`；若出现 `DATA RACE` 则记为 A4 FAIL 并摘录堆栈

- [ ] **步骤 2：落证据文件**

将结果写入 `docs/verify/race-evidence.md`：

```bash
{ echo "# go test -race 证据"; echo; echo "- 生成时间：$(date '+%Y-%m-%d %H:%M:%S')"; echo "- 命令：`go test -race ./...`"; echo; echo '```'; cat /tmp/race.txt; echo '```'; } > docs/verify/race-evidence.md
```

- [ ] **步骤 3：把 A4 现状回填标准文档**

将 `docs/verify/ACCEPTANCE.md` 中 A4 行的现状列由 `NOT-RUN` 改为 `PASS`（或 `FAIL` 并附失败包名）

- [ ] **步骤 4：Commit**

```bash
git add docs/verify/race-evidence.md docs/verify/ACCEPTANCE.md
git commit -m "docs(verify): 补 go test -race 证据并回填 A4 现状"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 4：采集 Jaeger 服务名清单

**文件：**
- 创建：`docs/verify/jaeger-services.md`

> **trace:** plan-ready.md → `### Task 4: 采集 Jaeger 服务名清单` | tasks.md → `- [ ] 2.2 采集 Jaeger `/api/services` 11 服务名清单证据`
> **sync:** tasks.md → `- [ ] 2.2 采集 Jaeger `/api/services` 11 服务名清单证据` | plan-ready.md → `### Task 4: 采集 Jaeger 服务名清单`

- [ ] **步骤 1：拉取服务名并比对 11 个可部署单元**

```bash
curl -s --noproxy '*' http://127.0.0.1:16686/api/services \
  | python3 -c "import sys,json;print('\n'.join(sorted(json.load(sys.stdin)['data'])))"
```

预期：输出包含且仅包含 `bff-admin bff-app bff-mini bff-web gateway job srv-coupon srv-order srv-pay srv-product srv-user`（11 个）

- [ ] **步骤 2：落证据文件**

```bash
{ echo "# Jaeger 服务注册清单"; echo; echo "- 生成时间：$(date '+%Y-%m-%d %H:%M:%S')"; echo "- 接口：`GET /api/services`"; echo; echo '```'; curl -s --noproxy '*' http://127.0.0.1:16686/api/services | python3 -c "import sys,json;print('\n'.join(sorted(json.load(sys.stdin)['data'])))"; echo '```'; } > docs/verify/jaeger-services.md
```

- [ ] **步骤 3：Commit**

```bash
git add docs/verify/jaeger-services.md
git commit -m "docs(verify): Jaeger 11 服务名注册清单证据"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 5：产出现状逐条判定表

**文件：**
- 创建：`docs/verify/acceptance-status.md`

> **trace:** plan-ready.md → `### Task 5: 产出现状逐条判定表` | tasks.md → `- [ ] 3.1 产出 `docs/verify/acceptance-status.md`：对照标准逐条判定，现状取 PASS / FAIL / EXEMPT / NOT-RUN`
> **sync:** tasks.md → `- [ ] 3.1 产出 `docs/verify/acceptance-status.md`：对照标准逐条判定，现状取 PASS / FAIL / EXEMPT / NOT-RUN` | plan-ready.md → `### Task 5: 产出现状逐条判定表`

- [ ] **步骤 1：建立判定表骨架**

创建 `docs/verify/acceptance-status.md`，表头固定为「项 / 级别 / 现状 / 依据」：

```markdown
# 验收现状判定（acceptance-status）

> 证据生成时间：<重跑后回填>
> 对照 `ACCEPTANCE.md` 逐条判定；现状取值 PASS / FAIL / EXEMPT / NOT-RUN。

| 项 | 级别 | 现状 | 依据 |
| --- | --- | --- | --- |
```

- [ ] **步骤 2：按现有证据回填 A/B/C/D（一）各项**

对照 `docs/verify/e2e-evidence.md`（6 服务 / 70 span）、`docs/verify/matrix-report.md`（C1 106 span、C2 0、db 28→0、本仓 messaging 5→0）、`bring-up.log`（11 服务全绿）回填，例如：

```markdown
| B1 依赖序启动 | P0 | PASS | .hubx-bin/bring-up.log：11 服务 healthy |
| D1 C2 总闸归零 | P0 | PASS | matrix-report.md：C2 全服务 0 span |
| D6 C5 本仓 messaging 归零 | P0 | PASS | matrix-report.md：messaging.outbox 5→0 |
| D7 C5 SDK messaging 残留 | EXEMPT | EXEMPT | 例外登记 X1：SDK 残留 4 个 |
| E1 主链路 ≥6 服务同 trace | P0 | PASS | e2e-evidence.md：6 服务 / 70 span |
| E2 messaging send/receive 同 trace | P1 | NOT-RUN | e2e 未触发 Kafka，标记 SKIP |
| E4 11 服务全覆盖 | P2 | FAIL | bff-web/mini/admin、srv-pay、job 为 0 span |
```

- [ ] **步骤 3：回填剩余项并标注证据时间**

运行：`date '+%Y-%m-%d %H:%M:%S'`
预期：把该时间写入文档头部「证据生成时间」

- [ ] **步骤 4：Commit**

```bash
git add docs/verify/acceptance-status.md
git commit -m "docs(verify): 验收现状逐条判定表"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 6：产出 P0 补齐清单

**文件：**
- 修改：`docs/verify/acceptance-status.md`

> **trace:** plan-ready.md → `### Task 6: 产出 P0 补齐清单` | tasks.md → `- [ ] 3.2 产出 P0 未过项补齐清单，按「先补脚本覆盖面 → 再重跑证据 → 最后复核文档」的依赖顺序排列`
> **sync:** tasks.md → `- [ ] 3.2 产出 P0 未过项补齐清单，按「先补脚本覆盖面 → 再重跑证据 → 最后复核文档」的依赖顺序排列` | plan-ready.md → `### Task 6: 产出 P0 补齐清单`

- [ ] **步骤 1：追加按依赖排序的补齐清单**

```markdown
## P0 补齐清单

按执行依赖排序（先补脚本覆盖面 → 再重跑证据 → 最后复核文档）：

1. 【覆盖面】`scripts/e2e-flow.py` 增加 `/api/web`、`/api/mini`、`/api/admin` 三端路径 —— 对应 E4
2. 【覆盖面】`scripts/e2e-flow.py` 增加 srv-pay（`/api/admin/pays`）与 job 路径 —— 对应 E4
3. 【覆盖面】`scripts/e2e-flow.py` 触发 Kafka 事件链（下单 → outbox → job consume）—— 对应 E2/E3
4. 【断言】`scripts/trace-matrix.py` 组合 5 断言改用 `messaging_own` 桶 —— 对应 D6/D7
5. 【证据】重跑三件套刷新 e2e-evidence.md 与 matrix-report.md
6. 【文档】复核 README 排查清单三段式与 PROMPT 结构 —— 对应 F1/F2
```

- [ ] **步骤 2：校验清单与判定表一致**

运行：`grep -c "^[0-9]\. 【" docs/verify/acceptance-status.md`
预期：条目数与判定表中 P0 `FAIL` / `NOT-RUN` 项一一对应

- [ ] **步骤 3：Commit**

```bash
git add docs/verify/acceptance-status.md
git commit -m "docs(verify): P0 补齐清单（按依赖排序）"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 7：e2e 补三端 BFF 路径

**文件：**
- 修改：`scripts/e2e-flow.py:46-52`（`REQUESTS` 列表）

> **trace:** plan-ready.md → `### Task 7: e2e 补三端 BFF 路径` | tasks.md → `- [ ] 4.1 `scripts/e2e-flow.py` 增加 `/api/web`、`/api/mini`、`/api/admin` 三端路径，使 bff-web / bff-mini / bff-admin 有 span 证据`
> **sync:** tasks.md → `- [ ] 4.1 `scripts/e2e-flow.py` 增加 `/api/web`、`/api/mini`、`/api/admin` 三端路径，使 bff-web / bff-mini / bff-admin 有 span 证据` | plan-ready.md → `### Task 7: e2e 补三端 BFF 路径`

- [ ] **步骤 1：扩展 REQUESTS 列表**

将 `scripts/e2e-flow.py` 的 `REQUESTS` 替换为（保留原 5 条，追加三端 + srv-pay + 下单）：

```python
REQUESTS = [
    ("GET", "/api/app/home", True),             # 聚合：profile + 近期订单 + 券
    ("GET", "/api/app/users/profile", False),   # 单 srv-user
    ("GET", "/api/app/products", False),        # 单 srv-product
    ("GET", "/api/app/coupons", False),         # 单 srv-coupon
    ("GET", "/api/app/orders/report", False),   # srv-order 4 表 JOIN（矩阵锚点）
    ("GET", "/api/web/home", False),            # bff-web 聚合面
    ("GET", "/api/web/products", False),        # bff-web 单 srv
    ("GET", "/api/mini/home", False),           # bff-mini 聚合面
    ("GET", "/api/mini/coupons", False),        # bff-mini 单 srv
    ("GET", "/api/admin/dashboard", False),     # bff-admin 运营看板（5 srv 只读聚合）
    ("GET", "/api/admin/pays", False),          # bff-admin → srv-pay（当前覆盖缺口）
]
```

bff-admin 路由组前缀为 `/`（`apps/bff-admin/internal/handler/handler.go:37`），网关剥掉 `/api/admin` 后组内路径即 `/dashboard`、`/pays`。

- [ ] **步骤 2：让脚本支持 POST 下单请求（为任务 9 铺路）**

在 `REQUESTS` 之后增加带 body 的请求集，并在 `main()` 的请求循环中支持四元组之外的 body：

```python
# 带 body 的请求：(方法, 路径, body, 是否必须成功)
POSTS = [
    ("POST", "/api/app/orders",
     '{"items":[{"product_id":1,"quantity":1,"unit_price":100}]}', False),
]
```

在 `main()` 中 `for method, path, required in REQUESTS:` 循环之后追加：

```python
    for method, path, body, required in POSTS:
        code, resp = http_call(method, GATEWAY + path, headers={
            "Authorization": "Bearer " + token,
            "traceparent": tp,
            "tracestate": "hubx=e2e",
            "X-User-Id": "1",
            "Content-Type": "application/json",
        }, body=body)
        snippet = resp[:160].replace("\n", " ")
        log(f"      {method} {path} -> HTTP {code}  {snippet}")
        results.append((method, path, code, required, snippet))
```

- [ ] **步骤 3：跑脚本确认三端 BFF 有 span**

运行：`python3 scripts/e2e-flow.py --no-up --min-services 6`（服务已起时）
预期：参与服务列表出现 `bff-web`、`bff-mini`、`bff-admin`；同 trace 服务数 ≥6

- [ ] **步骤 4：Commit**

```bash
git add scripts/e2e-flow.py
git commit -m "feat(scripts): e2e 补 web/mini/admin 三端路径与 POST 下单"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 8：e2e 补 srv-pay 与 job 路径

**文件：**
- 修改：`scripts/e2e-flow.py`（`REQUESTS` 与 job 覆盖）

> **trace:** plan-ready.md → `### Task 8: e2e 补 srv-pay 与 job 路径` | tasks.md → `- [ ] 4.2 `scripts/e2e-flow.py` 增加 srv-pay 与 job 路径覆盖`
> **sync:** tasks.md → `- [ ] 4.2 `scripts/e2e-flow.py` 增加 srv-pay 与 job 路径覆盖` | plan-ready.md → `### Task 8: e2e 补 srv-pay 与 job 路径`

- [ ] **步骤 1：确认 srv-pay 与 job 的入口**

运行：`grep -rn "ListPayFlows\|job consume" apps/bff-admin/ apps/job/ --include="*.go" | head -20`
预期：确认 `/api/admin/pays` → bff-admin `ListPayFlows` → srv-pay；job 侧入口为 `job consume` 子命令（Kafka 消费，非 HTTP）

- [ ] **步骤 2：为 job 侧补充非 HTTP 覆盖说明**

job 不经 gateway，需在脚本注释与证据中明确其覆盖方式——在 `scripts/e2e-flow.py` 头部注释「做什么」段落追加：

```python
#   2b. job 不走 HTTP（无网关入口），其 span 由 Kafka 消费链产生：
#       下单 → srv-order outbox → Kafka → job consume，见 POSTS 与任务 9 断言
```

并在 `main()` 的结论段落把 job 覆盖情况计入日志：

```python
    job_spans = [r for r in rows if r[0] == "job"]
    log(f"      job span 数：{len(job_spans)}"
        + ("" if job_spans else "（未出现：检查 job consume 是否在跑）"))
```

- [ ] **步骤 3：跑脚本确认 srv-pay 与 job 出现**

运行：`python3 scripts/e2e-flow.py --no-up --min-services 6`
预期：`srv-pay` 出现在参与服务中；job span 数 ≥1（Kafka 触发后，见任务 9）

- [ ] **步骤 4：Commit**

```bash
git add scripts/e2e-flow.py
git commit -m "feat(scripts): e2e 补 srv-pay 路径与 job span 计数"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 9：e2e 触发 Kafka 事件链并断言

**文件：**
- 修改：`scripts/e2e-flow.py:239-248`（messaging 断言段）

> **trace:** plan-ready.md → `### Task 9: e2e 触发 Kafka 事件链并断言` | tasks.md → `- [ ] 4.3 `scripts/e2e-flow.py` 触发 Kafka 事件链，断言 `messaging.send` 与 `messaging.receive` 同 trace_id 且 job 续调挂同 trace`
> **sync:** tasks.md → `- [ ] 4.3 `scripts/e2e-flow.py` 触发 Kafka 事件链，断言 `messaging.send` 与 `messaging.receive` 同 trace_id 且 job 续调挂同 trace` | plan-ready.md → `### Task 9: e2e 触发 Kafka 事件链并断言`

- [ ] **步骤 1：把 flush 等待改为覆盖 outbox 轮询周期**

outbox 为 3s 轮询补发，Kafka 投递 + job 消费需更长的等待窗口。在 `main()` 的参数定义后追加提示，并把默认 flush 由 14 提到 25：

```python
    ap.add_argument("--flush", type=int, default=25,
                    help="等 span flush 的秒数（默认 25，需覆盖 outbox 3s 轮询 + 投递 + 消费）")
```

- [ ] **步骤 2：把 messaging 断言升级为硬断言**

将 `scripts/e2e-flow.py` 中 `msg_note = "（本轮未触发 Kafka，标记 SKIP）"` 一段替换为：

```python
    msgs = [s for s in trace.get("spans", [])
            if s["operationName"].startswith("messaging.")]
    msg_ok = False
    if msgs:
        names = sorted({s["operationName"] for s in msgs})
        send = [n for n in names if ".send" in n or "publish" in n]
        recv = [n for n in names if ".receive" in n or ".process" in n or "consume" in n]
        msg_ok = bool(send and recv)
        msg_note = "PASS（send+receive 同 trace）" if msg_ok else \
                   f"FAIL（只有 {names}）"
    else:
        msg_note = "FAIL（未触发 Kafka：检查 outbox runner 与 job consume 是否在跑）"
```

并在结论返回处把 messaging 计入退出码：

```python
    ok = len(services) >= args.min_services and (msg_ok or args.allow_no_mq)
```

同时新增开关：

```python
    ap.add_argument("--allow-no-mq", action="store_true",
                    help="允许 messaging 段未触发（调试用，验收时不得加）")
```

- [ ] **步骤 3：跑脚本验证 messaging 不再 SKIP**

运行：`python3 scripts/e2e-flow.py --no-up --min-services 6`
预期：日志出现 `messaging 续链：PASS（send+receive 同 trace）`；退出码 0

- [ ] **步骤 4：Commit**

```bash
git add scripts/e2e-flow.py
git commit -m "feat(scripts): e2e 触发 Kafka 事件链，messaging 续链改硬断言"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 10：矩阵脚本拆分 messaging 计数

**文件：**
- 修改：`scripts/trace-matrix.py:307-312`（组合 5 断言）、`scripts/trace-matrix.py:329`（报表单元格）

> **trace:** plan-ready.md → `### Task 10: 矩阵脚本拆分 messaging 计数` | tasks.md → `- [ ] 4.4 `scripts/trace-matrix.py` 将 messaging 计数拆为本仓 `messaging.outbox.*` 与 SDK `messaging.kafka.*` 两行并列输出`
> **sync:** tasks.md → `- [ ] 4.4 `scripts/trace-matrix.py` 将 messaging 计数拆为本仓 `messaging.outbox.*` 与 SDK `messaging.kafka.*` 两行并列输出` | plan-ready.md → `### Task 10: 矩阵脚本拆分 messaging 计数`

- [ ] **步骤 1：组合 5 断言改用本仓桶**

`scripts/trace-matrix.py:307-312` 当前按 `messaging` 桶（含 SDK 无条件 span）判零，导致该格永远不为 0。替换为：

```python
        # 组合 5 附加断言：mq 关时本仓自产出 messaging.outbox.* 必须为 0
        # （SDK 自带 messaging.kafka.* 不受细分键门控，单独记录不参与判定）
        if no == 5:
            own_total = sum((r or {}).get("messaging_own", 0) for r in row.values())
            sdk_total = sum((r or {}).get("messaging", 0) for r in row.values())
            expects_zero = own_total == 0
            notes[no] = (name, desc + f"；mq 关断言 messaging.outbox 总数={own_total} → "
                                      f"{'PASS' if expects_zero else 'FAIL'}"
                                      f"（SDK messaging.kafka 残留 {sdk_total}，见例外登记 X1）", reqs)
```

- [ ] **步骤 2：报表单元格并列两个桶**

`scripts/trace-matrix.py:329` 由：

```python
                cells.append(f"{c['total']} (db {c['db']} / msg {c['messaging']})")
```

改为：

```python
                cells.append(f"{c['total']} (db {c['db']} / msg {c['messaging']} "
                             f"[own {c['messaging_own']}])")
```

并把表头说明同步更新（`scripts/trace-matrix.py:319`）：

```python
             "", "## 矩阵：每格 = 该服务该组合下的 span 总数（括号为 db / messaging 细分，own = 本仓自产出）", "",
```

- [ ] **步骤 3：跑组合 5 验证断言转为 PASS**

运行：`python3 scripts/trace-matrix.py --combos 5`
预期：`mq 关断言 messaging.outbox 总数=0 → PASS`；`docs/verify/matrix-report.md` 每格含 `own N`

- [ ] **步骤 4：Commit**

```bash
git add scripts/trace-matrix.py docs/verify/matrix-report.md
git commit -m "fix(scripts): 组合 5 断言改用本仓 messaging_own 桶，SDK 残留单独记录"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 11：重跑三件套刷新证据

**文件：**
- 修改：`docs/verify/e2e-evidence.md`、`docs/verify/matrix-report.md`、`docs/verify/acceptance-status.md`

> **trace:** plan-ready.md → `### Task 11: 重跑三件套刷新证据` | tasks.md → `- [ ] 5.1 重跑三件套刷新全部证据，同步刷新 `acceptance-status.md` 现状列与补齐清单`
> **sync:** tasks.md → `- [ ] 5.1 重跑三件套刷新全部证据，同步刷新 `acceptance-status.md` 现状列与补齐清单` | plan-ready.md → `### Task 11: 重跑三件套刷新证据`

- [ ] **步骤 1：按顺序重跑三件套**

```bash
bash scripts/bring-up.sh up
python3 scripts/e2e-flow.py
python3 scripts/trace-matrix.py
```

预期：e2e 退出码 0 且 messaging 段 PASS；矩阵三点结论全部「成立」

- [ ] **步骤 2：刷新判定表现状列**

按新证据更新 `docs/verify/acceptance-status.md`：E2 / E3 由 `NOT-RUN` → `PASS`；E4 视 11 服务是否齐备更新；D6 由 `NOT-RUN` → `PASS`

- [ ] **步骤 3：更新证据生成时间**

运行：`date '+%Y-%m-%d %H:%M:%S'`
预期：写入 `acceptance-status.md` 头部「证据生成时间」

- [ ] **步骤 4：Commit**

```bash
git add docs/verify/
git commit -m "docs(verify): 重跑三件套刷新全部验收证据"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

### 任务 12：复核 P0 并给出收尾裁决

**文件：**
- 修改：`docs/verify/acceptance-status.md`

> **trace:** plan-ready.md → `### Task 12: 复核 P0 并给出收尾裁决` | tasks.md → `- [ ] 5.2 复核 P0 项是否全部 PASS / EXEMPT，给出收尾裁决结论`
> **sync:** tasks.md → `- [ ] 5.2 复核 P0 项是否全部 PASS / EXEMPT，给出收尾裁决结论` | plan-ready.md → `### Task 12: 复核 P0 并给出收尾裁决`

- [ ] **步骤 1：统计 P0 项现状**

运行：`grep "^| [A-F][0-9]" docs/verify/acceptance-status.md | grep "| P0 |" | grep -vc "PASS\|EXEMPT"`
预期：`0` 表示 P0 全过；非 0 则该数即剩余 P0 未过项数

- [ ] **步骤 2：写入裁决结论**

在 `docs/verify/acceptance-status.md` 末尾追加（按上一步实际结果二选一）：

```markdown
## 收尾裁决

P0 项：全部 PASS / EXEMPT → **满足 `/sddflow close` 门槛**。
```

或：

```markdown
## 收尾裁决

P0 项存在 <N> 条未过（见上表 FAIL / NOT-RUN 行）→ **不满足 `/sddflow close` 门槛**，
按「P0 补齐清单」执行后重跑判定。
```

- [ ] **步骤 3：Commit**

```bash
git add docs/verify/acceptance-status.md
git commit -m "docs(verify): 收尾裁决结论"
```

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）
