# 实现计划：add-micro-apps-reference

## 来源
- 提案：openspec/changes/add-micro-apps-reference/proposal.md
- 设计：openspec/changes/add-micro-apps-reference/design.md（完整设计规格 docs/superpowers/specs/2026-09-27-micro-apps-design.md）
- 规格：openspec/changes/add-micro-apps-reference/specs/（8 个 capability）
- 任务：openspec/changes/add-micro-apps-reference/tasks.md

## 实现步骤

### Task 1: 仓库脚手架 + go.mod
- [ ] **任务完成**（与 superpowers plan `Task 1`、`tasks.md` 对应条目同步勾选）
- 目标：建立单模块仓库骨架，锁定本地 SDK replace 清单
- 步骤：
  1. 写 `go.mod`（module hubx/micro_apps，go 1.24，`replace` 指向 ../rpcx ../configx ../webx ../logx ../dbx ../cachex ../mqx ../otelx，参照 monolith go.mod 清单）— 文件：go.mod — 验证：`go mod edit -json` 显示 8 条 replace
  2. `go mod tidy` 拉取 kitex/gin/viper 等传递依赖 — 文件：go.sum — 验证：tidy 退出码 0
  3. 创建 11 个 `apps/*/cmd/*/` 目录占位 — 文件：apps/gateway、bff-×4、srv-×5、job — 验证：`go build ./...` 空仓库通过
- 改动文件：go.mod、go.sum、apps/*/cmd/*/
- 验证方式：`go build ./...` 通过；commit `chore: scaffold micro_apps module`

### Task 2: pkg 数据层（纯数据，无外部依赖）
- [ ] **任务完成**（与 superpowers plan `Task 2`、`tasks.md` 对应条目同步勾选）
- 目标：落 proto 单一事实源、事件体、错误码、身份常量、五库种子 SQL
- 步骤：
  1. `pkg/meta` Header 常量 + `pkg/codes` 统一错误码（按 5 个 srv 错误域分组，只定一次）— 文件：pkg/meta/meta.go、pkg/codes/codes.go — 验证：`go vet ./pkg/meta ./pkg/codes`
  2. `pkg/events` 三事件体（OrderCompleted/CouponExchanged/PointsGranted，含 SchemaVer、TraceHeaderKey）— 文件：pkg/events/*.go — 验证：MarshalJSON 往返单测
  3. `pkg/proto` 五服务 proto + `pkg/seedsql` 五库 SQL — 文件：pkg/proto/*.proto、pkg/seedsql/*.sql — 验证：proto 语法可被 kitex CLI 解析（Task 3 干跑）
- 改动文件：pkg/{codes,meta,events,proto,seedsql}/
- 验证方式：`go vet ./pkg/...`；commit `feat: pkg codes/meta/events/seedsql/proto`

### Task 3: 生成 kitex_gen（RPC 桩）
- [ ] **任务完成**（与 superpowers plan `Task 3`、`tasks.md` 对应条目同步勾选）
- 目标：生成 5 个 srv 的 RPC 桩并提交入库（验收不依赖 kitex CLI）
- 步骤：
  1. 安装 kitex CLI 并读 `hubx/rpcx/example/` 生成命令 — 文件：无（环境） — 验证：`kitex --version`
  2. 先对 user.proto 跑通一条生成命令，再批 5 个 — 文件：pkg/kitex_gen/{user,product,order,pay,coupon}/ — 验证：`go build ./pkg/kitex_gen/...` 通过
- 改动文件：pkg/kitex_gen/**
- 验证方式：`go build ./pkg/kitex_gen/...`；commit `feat: generate rpcx stubs for 5 services`

### Task 4: infra/telemetry（移植 monolith）
- [ ] **任务完成**（与 superpowers plan `Task 4`、`tasks.md` 对应条目同步勾选）
- 目标：分层 span 助手 + 全局 TracerProvider + 总闸 noop 行为
- 步骤：
  1. 读 monolith `internal/telemetry/` 全部文件并移植（Init/Start，层命名 handler/service/repository/middleware 对齐）— 文件：infra/telemetry/provider.go、layers.go — 验证：编译过、签名与 monolith 同构
  2. ServiceName 参数化；总闸关返回 noop provider 并关 logx.EnableTrace — 文件：infra/telemetry/provider.go — 验证：单测断言总闸关时 Start 返回无 parent span
  3. TDD：stdout exporter 捕获 3 类 span + 总闸 noop 测试 — 文件：infra/telemetry/provider_test.go — 验证：`go test ./infra/telemetry/` 红→绿
- 改动文件：infra/telemetry/{provider,layers,provider_test}.go
- 验证方式：`go test ./infra/telemetry/`；commit `feat: infra/telemetry (port monolith layered tracing)`

### Task 5: infra/nacos + infra/drivers + pkg/config
- [ ] **任务完成**（与 superpowers plan `Task 5`、`tasks.md` 对应条目同步勾选）
- 目标：`Trace.On(kind)` 唯一开关入口 + configx loader + 驱动薄封装
- 步骤：
  1. `pkg/config/trace.go`：读三官方段 enable_trace + configx.Trace() 段，封装 `On(kind)`（enabled ∧ kind ∧ 官方段三段正交）— 文件：pkg/config/trace.go — 验证：7 组合表驱动单测（直接对应矩阵 7 组合）
  2. `infra/nacos/loader.go`：LoadFromBytes 远程 + LoadFromLocal 本地优先，注释写明本地优先与无热更新 — 文件：infra/nacos/loader.go — 验证：loader 单测（远程失败回退本地）
  3. `infra/drivers`：blank import redis/kafka 驱动 + NewCache/NewProducer/NewConsumer 薄封装 — 文件：infra/drivers/drivers.go — 验证：`go build ./infra/...`
- 改动文件：pkg/config/trace.go、infra/nacos/loader.go、infra/drivers/drivers.go
- 验证方式：`go test ./pkg/config/` 红→绿；commit `feat: pkg/config trace switch + infra nacos loader & drivers`

### Task 6: infra/consul（自研 resolver + 注册）
- [ ] **任务完成**（与 superpowers plan `Task 6`、`tasks.md` 对应条目同步勾选）
- 目标：在 rpcx resolver 扩展点挂 Consul 服务发现 + 服务注册/注销
- 步骤：
  1. 读 `hubx/rpcx/pkg/rpcx/resolver/resolver.go` 确认 Resolver 接口与 Register 签名 — 文件：无（读源码，先读门禁） — 验证：实现签名与源码逐字一致
  2. ConsulResolver 实现 Resolve（KV `hubx/svc/<serviceID>` 读 host:port）+ register.go Register/Deregister — 文件：infra/consul/{resolver,register}.go — 验证：`go build ./infra/consul/`
  3. TDD：httptest 模拟 Consul KV 断言地址列表与空 KV 行为 — 文件：infra/consul/resolver_test.go — 验证：`go test ./infra/consul/` 红→绿
- 改动文件：infra/consul/{resolver,register,resolver_test}.go
- 验证方式：`go test ./infra/consul/`；commit `feat: infra/consul custom rpcx resolver + registry`

### Task 7: docker 底座 + configs 灌入
- [ ] **任务完成**（与 superpowers plan `Task 7`、`tasks.md` 对应条目同步勾选）
- 目标：六件套中间件底座 + 11 份服务配置 + Nacos 灌入脚本
- 步骤：
  1. compose.yaml（mysql/kafka KRaft 自动建 topic/consul/nacos/redis/jaeger 4317+16686，各带健康检查）— 文件：docker/compose.yaml — 验证：`docker compose up -d && docker compose ps` 全绿
  2. init.sql 五库五账号 + 表结构（外键不建）— 文件：docker/mysql/init.sql — 验证：5 库可分别用各自账号连接
  3. 11 份 `configs/services/*.yaml`（xmiddleware 三键逐字对齐企业契约）+ `configs/trace.yaml` — 文件：configs/ — 验证：键路径与契约逐字对照
  4. seed-nacos.sh 逐个 publish dataId（hubx.trace / hubx.<svc>，group HUBX）— 文件：configs/seed-nacos.sh — 验证：Nacos 配置列表出现全部 dataId
- 改动文件：docker/**、configs/**
- 验证方式：compose 全绿 + 5 库可连 + Nacos 灌入成功；commit `feat: docker base + nacos config seed`

### Task 8: srv 模板服务（srv-order 先行，作为 5 个 srv 的模板）
- [ ] **任务完成**（与 superpowers plan `Task 8`、`tasks.md` 对应条目同步勾选）
- 目标：定型 11 份同构装配 + 库内事务/跨域 gRPC 协作 + outbox 事件 + 中间件
- 步骤：
  1. main.go 按七步装配顺序（configx→telemetry→logx→infra→rpc→入口→优雅退出），头部注释写死顺序约束 — 文件：apps/srv-order/cmd/srv-order/main.go — 验证：telemetry.Init 行号早于 dbx/cachex/mqx
  2. repository：CreateOrderTx（本地事务 + 库存走 srv-product gRPC，失败回滚）、GetOrder 2 表 JOIN、Report 4 表 JOIN、ListOrders 只读流水（全 Find 禁 Scan）— 文件：apps/srv-order/internal/repository/ — 验证：stdout 数出 4 表 JOIN SQL span
  3. rpc handler（4 个 gRPC 方法 + service 分层 span）+ service（outbox 表 + 3s 轮询 + traceparent 受 On("mq") 门控写入）+ middleware（限流/身份）— 文件：apps/srv-order/internal/{rpc,service,middleware}/ — 验证：手动冒烟 CreateOrder，stdout 见分层 span 与 messaging.send
- 改动文件：apps/srv-order/**
- 验证方式：`go build ./apps/srv-order/...` + 冒烟 span 计数；commit `feat: srv-order template service (join/outbox/mq)`

### Task 9: 其余 4 个 srv（user/product/pay/coupon，照 srv-order 模板展开）
- [ ] **任务完成**（与 superpowers plan `Task 9`、`tasks.md` 对应条目同步勾选）
- 目标：按模板扩出 4 个 srv，各自库内 JOIN 落点 + 领域方法
- 步骤：
  1. srv-user：users⋈user_profiles JOIN、JWT 签发校验、auth_sessions — 文件：apps/srv-user/** — 验证：登录/详情冒烟 + 2 表 JOIN span
  2. srv-product：3 表 JOIN、product:hot 缓存（On("cache") 门控）、ReserveStock 行锁 gRPC — 文件：apps/srv-product/** — 验证：缓存命中/未命中 span 各一条
  3. srv-pay：Pay 事务 + ListPayFlows 对账流水 — 文件：apps/srv-pay/** — 验证：Pay 冒烟 SQL span
  4. srv-coupon：Consume 幂等核销 + GrantPoints + points.granted 事件 — 文件：apps/srv-coupon/** — 验证：核销防重单测/冒烟
- 改动文件：apps/srv-{user,product,pay,coupon}/**
- 验证方式：5 个 srv `go build ./apps/...` 全过 + 各核心方法冒烟；commit `feat: srv user/product/pay/coupon`

### Task 10: gateway（根 span + JWT + 限流 + 路由）
- [ ] **任务完成**（与 superpowers plan `Task 10`、`tasks.md` 对应条目同步勾选）
- 目标：网关零业务——一次验签、限流、配置路由、根 span 转发
- 步骤：
  1. auth 验签一次写身份 Header 透传；ratelimit 用 cachex Set/Get+TTL（On("cache") 门控）— 文件：apps/gateway/internal/{auth,ratelimit}/ — 验证：带 JWT/不带 JWT 两种响应
  2. routing 配置驱动路由表 + 灰度头透传；proxy HTTP→gRPC 转发 + 根 span `handler.gateway.Proxy` — 文件：apps/gateway/internal/{routing,proxy}/ — 验证：curl 全链路 stdout 见根 span 串下游
  3. main.go 同构装配（无 DB，只装缓存+限流）— 文件：apps/gateway/cmd/gateway/main.go — 验证：无 sqlx 配置段
- 改动文件：apps/gateway/**
- 验证方式：起 gateway+bff+srv curl 全链路冒烟；commit `feat: gateway (root span/jwt/ratelimit/routing)`

### Task 11: BFF 模板（bff-app 先行）
- [ ] **任务完成**（与 superpowers plan `Task 11`、`tasks.md` 对应条目同步勾选）
- 目标：errgroup 并发聚合 + 降级兜底 + 展示层缓存 + 统一响应
- 步骤：
  1. aggregate errgroup 并发调 srv + 降级兜底 + dto 字段裁剪 — 文件：apps/bff-app/internal/aggregate/、dto/ — 验证：单 srv 挂时返回默认值不 500
  2. cache `bff:app:{resource}:{user_id}` TTL 60s（命中不打 srv）— 文件：apps/bff-app/internal/cache/ — 验证：命中/未命中两类 span
  3. handler webx 统一响应 + 就绪探针 + main 同构装配（无 DB）— 文件：apps/bff-app/internal/handler/、cmd/ — 验证：`WithReadyProbes` 探针 200
- 改动文件：apps/bff-app/**
- 验证方式：聚合接口冒烟 + 缓存两类 span；commit `feat: bff-app template (errgroup aggregate/degrade/cache)`

### Task 12: 其余 3 个 BFF（web/mini/admin，照 bff-app 展开）
- [ ] **任务完成**（与 superpowers plan `Task 12`、`tasks.md` 对应条目同步勾选）
- 目标：按端独立展开 3 个 BFF，零共享业务代码
- 步骤：
  1. bff-web（SEO/SSR dto 裁剪）、bff-mini（小程序码字段）、bff-admin（5 srv 只读 + 独立管理路由组）— 文件：apps/bff-{web,mini,admin}/** — 验证：各聚合接口冒烟
  2. 四 BFF aggregate/handler/cache 各写一份（允许重复，禁止互 import）— 文件：apps/bff-*/internal/** — 验证：import 关系扫描无跨 BFF 依赖
- 改动文件：apps/bff-{web,mini,admin}/**
- 验证方式：4 个 BFF `go build` + 聚合冒烟 + 无共享代码扫描；commit `feat: bff web/mini/admin`

### Task 13: job（cobra + Kafka 消费续 traceparent + gocron 调度）
- [ ] **任务完成**（与 superpowers plan `Task 13`、`tasks.md` 对应条目同步勾选）
- 目标：consume/retry/schedule 三命令，跨进程 trace 续接与对账
- 步骤：
  1. cobra 三命令骨架 + schedule 注册 gocron 任务（对账走 gRPC 拉流水、库存日清、报表；task 自建根 span，无 DB DSN）— 文件：apps/job/cmd/job/main.go、internal/schedule/ — 验证：`job schedule` 跑一轮出 task 根 span
  2. consumer 从 msg.Headers() Extract traceparent 重建 ctx 续接 messaging.receive，失败进 *.dlq — 文件：apps/job/internal/consumer/ — 验证：send/receive 同 trace_id 冒烟
  3. handler：order.completed → 调 srv-coupon GrantPoints（gRPC 同 trace）；retry：DLQ 重放 + `job:dlq:{msg_id}` 防重 — 文件：apps/job/internal/{handler,retry}/ — 验证：重放不重复记账
- 改动文件：apps/job/**
- 验证方式：同 trace 冒烟 + schedule 一轮；commit `feat: job (kafka consumer + gocron schedule)`

### Task 14: README + PROMPT
- [ ] **任务完成**（与 superpowers plan `Task 14`、`tasks.md` 对应条目同步勾选）
- 目标：交付两份文档（含 6 维度：先读清单/硬约束/验收/已知坑/交付物/禁令）
- 步骤：
  1. README：拓扑图、装配顺序约束、开关设计、JOIN 红线、SDK 边界、矩阵结论占位、Jaeger 接入、排查清单（monolith 7 条 + 本案例新坑，逐条现象/根因/对策）— 文件：README.md — 验证：拓扑/顺序与代码一致
  2. PROMPT：复刻 monolith 7 节结构，第 1 节先读源码清单与 plan 第 0 节逐条对应 — 文件：PROMPT.md — 验证：先读清单 file:line 依据齐全
- 改动文件：README.md、PROMPT.md
- 验证方式：人工对照；commit `docs: README + PROMPT`

### Task 15: bring-up.sh（按依赖序起 11 服务 + 配置重载）
- [ ] **任务完成**（与 superpowers plan `Task 15`、`tasks.md` 对应条目同步勾选）
- 目标：依赖序启动 + 单服务/批量重启（矩阵复用）
- 步骤：
  1. 依赖序 db→srv×5→bff×4→gateway→job，每步等 Consul 健康 + 就绪探针；支持 `--services <csv>`、`--reload <svc>` — 文件：scripts/bring-up.sh — 验证：从零启动 11 服务全绿
  2. 过滤重启验证 — 文件：scripts/bring-up.sh — 验证：`--services srv-order` 只重启一个
- 改动文件：scripts/bring-up.sh
- 验证方式：从零 bring-up 全绿；commit `feat: bring-up.sh dependency-ordered startup`

### Task 16: e2e-flow.py（主链路跨进程 trace 证据）
- [ ] **任务完成**（与 superpowers plan `Task 16`、`tasks.md` 对应条目同步勾选）
- 目标：主链路 ≥6 服务同 trace + messaging 同 trace 的可复跑证据
- 步骤：
  1. 打全链路请求（JWT 下单 + Kafka 事件 + job 消费 + 报表），等 flush ≥10s，Jaeger API 断言同 trace_id ≥6 服务、send/receive 同 trace、job 续调同 trace，打印 trace_id 与 span 树 — 文件：scripts/e2e-flow.py — 验证：`python3 scripts/e2e-flow.py` 退出码 0
- 改动文件：scripts/e2e-flow.py
- 验证方式：Jaeger 就绪下脚本通过；commit `feat: e2e cross-process trace evidence`

### Task 17: trace-matrix.py（11×7 全量矩阵）
- [ ] **任务完成**（与 superpowers plan `Task 17`、`tasks.md` 对应条目同步勾选）
- 目标：7 组合 × 11 服务全量开关矩阵 + 三点结论报表
- 步骤：
  1. Nacos 程序化改 trace dataId → bring-up 重启（不重起 compose）→ 固定请求集 → 等 flush → Jaeger 按 service×span 前缀计数；组合 5 断言消息无 traceparent 头 + job 自建根；落 matrix-report.md — 文件：scripts/trace-matrix.py — 验证：全跑 7 轮（30~40 min）报表生成
- 改动文件：scripts/trace-matrix.py、docs/verify/matrix-report.md
- 验证方式：矩阵三点结论成立；commit `feat: full trace switch matrix (11×7)`

### Task 18: config.stdout.yaml 零依赖验证路径
- [ ] **任务完成**（与 superpowers plan `Task 18`、`tasks.md` 对应条目同步勾选）
- 目标：Jaeger 不可用时的本地验证变体
- 步骤：
  1. `configs/trace.stdout.yaml`（exporter: stdout）+ README 写"先 stdout 后 Jaeger"两步走 — 文件：configs/trace.stdout.yaml、README.md — 验证：`HUBX_CONFIG=configs/trace.stdout.yaml go run ./apps/srv-order` 终端逐行见分层 span
- 改动文件：configs/trace.stdout.yaml、README.md
- 验证方式：stdout 终端 span 可数；commit `feat: stdout exporter config variant`

### Task 19: go test -race ./... 全绿
- [ ] **任务完成**（与 superpowers plan `Task 19`、`tasks.md` 对应条目同步勾选）
- 目标：补齐各层单测至 race 全绿
- 步骤：
  1. srv service 层 SQLite 内存库单测、BFF aggregate 降级测试、job traceparent 续接测试 — 文件：**/ *_test.go — 验证：`go test -race ./...` 全绿
  2. 覆盖率确认 — 文件：无 — 验证：`go test -cover ./...` ≥80% 关键包
- 改动文件：各 *_test.go
- 验证方式：race + cover；commit `test: race + coverage`

### Task 20: Jaeger 全链路人肉复核 + 排查清单
- [ ] **任务完成**（与 superpowers plan `Task 20`、`tasks.md` 对应条目同步勾选）
- 目标：服务名齐全 + 3 条抽查 trace + 新坑补录
- 步骤：
  1. `/api/services` 11 服务名齐全；抽查主链路/Kafka 续接/4 表 JOIN 三条 trace 逐 span 核对 — 文件：无（Jaeger UI/API） — 验证：截图或记录
  2. 新坑按现象/根因/对策补录 README — 文件：README.md — 验证：清单含全部实测新坑
- 改动文件：README.md
- 验证方式：Jaeger 复核记录；commit `docs: jaeger manual verification + pitfall addendum`

### Task 21: git 收尾 + 复跑三件套
- [ ] **任务完成**（与 superpowers plan `Task 21`、`tasks.md` 对应条目同步勾选）
- 目标：清仓 + 禁令扫描 + 从零无人值守复跑全部结论
- 步骤：
  1. `git status` 清仓、`gofmt -l .` 空、`go vet ./...` 绿；禁令扫描（无 TODO/占位、8 个 SDK 目录零改动、入链结论均附实测证据）— 文件：全仓 — 验证：扫描零命中
  2. 从零复跑 compose up → seed-nacos → bring-up → e2e-flow → trace-matrix — 文件：无（执行三件套） — 验证：无人值守完成、结论与上次一致
- 改动文件：无新文件（清理）
- 验证方式：三件套复跑通过；commit `chore: final verification pass`
