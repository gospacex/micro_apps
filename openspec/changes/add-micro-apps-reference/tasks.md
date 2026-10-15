# Tasks — add-micro-apps-reference

## 1. 基础设施与共享层

- [x]1.1 仓库脚手架：单 go.mod（module hubx/micro_apps，replace 本地 SDK ×8）+ `go mod tidy` + 11 个 apps 目录占位，`go build ./...` 通过（对应 Task 1）
- [x]1.2 pkg 数据层：meta Header 常量、codes 统一错误码、events 三事件体（含 SchemaVer/traceparent 常量）、seedsql 五库 SQL、proto 五服务定义，`go vet ./pkg/...` 通过（对应 Task 2）
- [x]1.3 生成 kitex_gen RPC 桩（提交入库），`go build ./pkg/kitex_gen/...` 通过（对应 Task 3）
- [x]1.4 infra/telemetry：移植 monolith 分层 span 助手 + Init 顺序 + 总闸 noop 行为，stdout exporter 单测红→绿（对应 Task 4）
- [x]1.5 pkg/config `Trace.On(kind)` 三段正交封装 + infra/nacos loader + infra/drivers 薄封装，7 组合表驱动单测红→绿（对应 Task 5）
- [x]1.6 infra/consul：自研 rpcx resolver（Resolve + Register/Deregister），httptest 模拟 KV 单测红→绿（对应 Task 6）
- [x] 1.7 docker 底座 + configs：compose 六件套（mysql 五库/kafka/consul/nacos/redis/jaeger）、init.sql、11 份服务配置、trace.yaml、seed-nacos.sh，`docker compose ps` 全绿（对应 Task 7）<!-- 已实现: compose 六件套全 healthy；五库五账号建齐（user/product/order/pay/coupon_db）；seed-nacos.sh 灌入 hubx.trace + hubx.<svc> ×11 共 12 个 dataId 并回读逐字一致 -->

## 2. 服务实现

> 状态备注（2026-09-29）：2.1/2.2/2.3 的**代码已全部实装**，且 `go build ./...`
> 与 `go vet ./...` 全绿；仍未勾选的原因是验收标准里含「手动冒烟 SQL span 可数」
> 「全链路 curl 冒烟」，这两条依赖 docker 六件套真起来后才能执行，当前未跑。
> 2.4–2.6（BFF ×4 / job）仍只有目录占位。
>
> 状态更新（2026-10-02 build）：docker 六件套已全 healthy（五库五账号建齐、Nacos 12 个
> dataId 灌入并回读一致）。本轮实装并提交了：srv rpc 桩 span 泄漏修复、srv-user 注册事务
> SQL span 盲区、outbox/coupon messaging 续链、gateway 出站 W3C 头完整注入、统一 trace
> 配置接线（双 dataId + ApplyTrace + Endpoint/SampleRatio/Exporter）、webx 入站 Extract
> 补丁（SDK 特许修改，用户已批准）、bff-app 模板与 bff-web/mini/admin 三个 BFF。
> `go build ./... && go vet ./... && gofmt -l . && go test ./...` 全绿，跨 BFF import 扫描零命中。
> 多进程冒烟证据见 `docs/verify/bff-app-smoke.md`：**webx 补丁确认生效**（bff-app 根 span
> 续接入站 traceparent，trace_id=4bf92f3577b34da6a3ce929d0e0e4736），分层 span 与降级兜底
> 均符合设计，srv-user 同 trace_id；**待修缺陷**：bff-app 侧无 RPC client span 上报，
> srv 侧 span 父级解析为 ROOT，span 树在 bff→srv 边界断开。

- [ ] 2.1 srv-order 模板服务：装配顺序 main + 库内事务/跨域 gRPC 协作 repository + gRPC handler + outbox 事件 + 限流/身份中间件，手动冒烟 SQL span 可数（对应 Task 8）
- [ ] 2.2 其余 4 个 srv（user/product/pay/coupon）照模板展开，各库 JOIN 落点与核心方法冒烟通过（对应 Task 9）
- [ ] 2.3 gateway：JWT 一次验签透传 + cachex 限流 + 配置路由 + 根 span 转发，全链路 curl 冒烟通过（对应 Task 10）
- [ ] 2.4 BFF 模板 bff-app：errgroup 并发聚合 + 降级兜底 + 展示层缓存 + 统一响应，缓存命中/未命中两种 span 冒烟通过（对应 Task 11）
- [ ] 2.5 其余 3 个 BFF（web/mini/admin）按端独立展开（无共享业务代码），4 个 BFF 聚合接口冒烟通过（对应 Task 12）
- [ ] 2.6 job：cobra 三命令 + Kafka 消费 traceparent 续接 + DLQ 重放 + gocron 调度（对账走 gRPC），send/receive 同 trace 冒烟通过（对应 Task 13）

## 3. 文档与验收脚本

- [x] 3.1 README + PROMPT：拓扑图、装配顺序、开关设计、JOIN 红线、SDK 边界、排查清单（现象/根因/对策）、8 节结构 PROMPT（对应 Task 14）— 2026-10-03 已落 README.md / PROMPT.md
- [x] 3.2 scripts/bring-up.sh：依赖序启动 11 服务 + `--services`/`--reload` 过滤（对应 Task 15）— 2026-10-03 落脚本并实测 11 服务全绿；重载坑（pid 记到 wrapper + 端口未释放）已修
- [x] 3.3 scripts/e2e-flow.py：主链路 ≥6 服务同 trace + span 树输出（对应 Task 16）— 2026-10-03 实测 6 服务 / 70 span 同 trace，证据 docs/verify/e2e-evidence.md；messaging 段本轮未触发 Kafka，标记 SKIP
- [x] 3.4 scripts/trace-matrix.py：11×7 全量矩阵 + 组合 5 断言 + matrix-report.md 落盘（对应 Task 17）— 2026-10-03 全跑 7 轮：C1 全开 106 span / C2 总闸关 0 / db 28→0 / 本仓 messaging.outbox 5→0；mqx SDK 自带 messaging.kafka.* 不受细分键门控（SDK 无条件建 span），报表分桶记录
- [x] 3.5 configs/trace.stdout.yaml 零依赖验证路径：exporter stdout 下终端可逐行数 span（对应 Task 18）— 2026-10-03 落配置 + README 两步走说明

## 4. 验证与收尾

- [ ] 4.1 `go test -race ./...` 全绿：service 层 SQLite 单测、BFF 降级测试、job traceparent 续接测试补齐（对应 Task 19）
      - 进度（2026-09-29）：`go test -race ./...` **已全绿**（14 个包），覆盖 gateway auth/proxy/ratelimit/routing、五个 srv 的 repository/service、infra/consul、infra/telemetry、pkg/config。
      - 仍缺：BFF 降级测试与 job traceparent 续接测试 —— 对应服务尚未实现（见第 2 节备注），故这两条用例不存在。
- [ ] 4.2 Jaeger 人肉复核：/api/services 11 服务名齐全、3 条抽查 trace 逐 span 核对、新坑补录 README（对应 Task 20）
- [ ] 4.3 git 收尾 + 三件套从零无人值守复跑：gofmt/vet 全绿、清仓、复放全部验收结论；禁令扫描通过（无 TODO/占位、8 个 SDK 目录零改动、README 入链结论均附实测证据）（对应 Task 21）
      - 进度（2026-09-29）：`gofmt -l .` 零输出、`go vet ./...` 通过、`go build ./...` 通过。

## 变更记录

### 2026-09-29 依赖迁移：kitex-framework → 仓内实现 + 缺陷修复

**背景**：本地工作树 `../../rpcx`（外部模块 `github.com/kitexx/kitex-framework`）被删除且全仓无副本，导致本仓库 `go build ./...` 全量失败 —— 1.4、1.5、1.6、2.1–2.3、4.x 全部无法验证。

**依赖替换**（语义一一对应，无外部隐式依赖）：

| 原依赖 | 现实现 | 落点 |
| :--- | :--- | :--- |
| `rpcx/tracing.Init(Options{...})` | `otelx.New(*otelx.Config)` | `infra/telemetry/provider.go` |
| `rpcx/logx.TraceEnabled()` | `traceflag.EnabledFor("otelx")` | 同上 |
| `rpcx/logx.DisableTrace()` | `traceflag.SetEnabled(false)` | 同上 |
| `rpcx/configx.InitGlobal` | 删除（原本只用于给 tracing 喂全局 viper） | 各服务 main.go |
| `rpcx.BuildServerOptions/BuildClientOptions` | 仓内 `pkg/rpcx`（kitex 官方 API + obs-opentelemetry suite） | `pkg/rpcx/options.go` |

配套变更：OTLP 端点由 gRPC 4317 改为 **HTTP 4318**（otelx 走 HTTP 通道，Jaeger 1.35+ 原生支持，compose 两端口均已映射）；`configs/trace.yaml`、`pkg/config/trace.go` 默认值同步。

**同源修出的三个缺陷**：

1. **观测开关反向决定可用性** —— `Trace.On("cache"/"mq")` 语义是"是否入链"，却被当成"是否构造组件"。关掉 cache 追踪会让网关限流器静默变 nil；关掉 MQ 追踪会让 `OutboxRunner.Start` 静默 return，outbox 无限堆积。已改为组件照常构造、开关只透传给 drivers。
2. **退出顺序丢尾部 span** —— 会标 span 的资源原先部分走 `defer`，而 defer 要在手动 `tracing.Shutdown` **之后**才执行，关闭动作产生的 span 会被 flush 漏掉。已全部改为显式关闭并前置（gateway 与五个 srv 一致修正）。
3. **`log.Fatalf` 跳过 defer** —— 装配顺序上任一环节失败即 `os.Exit(1)`，已打开的 configx 句柄（内含 Nacos 长轮询 goroutine）与 logger 都来不及收尾。六个服务的 `main()` 统一改为 `run() error` 模式。

另修 `outbox.go` 放弃名单遍历全量 `pending` 而非失败集合的问题（误导告警 + 每轮多余 UPDATE），抽出纯函数 `abandonCandidates`，并补了「临时改回旧实现会红」的回归用例。
