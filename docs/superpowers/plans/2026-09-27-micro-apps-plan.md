# micro_apps 微服务参考案例 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 按 `docs/superpowers/specs/2026-09-27-micro-apps-design.md` 在 `hubx/micro_apps/` 从零搭建 11 个可部署单元的 hubx 微服务案例，统一调用链开关（configx trace 段，Nacos 统一下发）让 Gateway→BFF→SRV→存储全链路串进同一条 trace，并以脚本化矩阵验收。

**架构：** 单 go.mod + `apps/` 目录隔离（11 服务），`replace` 指向本地 SDK。服务间走 rpcx（kitex，JSON transport）+ 自研 Consul resolver；配置走 configx 双通道（本地 yaml 优先 + Nacos 远程）；DB 走 dbx（MySQL 五库）、缓存走 cachex redis driver、MQ 走 mqx Kafka driver。

**Traceability (sddflow):**
- plan-ready: `openspec/changes/add-micro-apps-reference/plan-ready.md`
- tasks: `openspec/changes/add-micro-apps-reference/tasks.md`
- plan: `docs/superpowers/plans/2026-09-27-micro-apps-plan.md`

**技术栈：** Go 1.24，kitexx/kitex（经 hubx/rpcx），gopkg.in/yaml.v3（经 hubx/configx），hubx 本地 SDK ×8，gin（经 hubx/webx），cobra，github.com/ouqiang/gocron，SQLite/MySQL/Redis/Kafka/Consul/Nacos/Jaeger（docker compose）。

---

## 0. 写代码前必须读（硬要求，违反即返工）

本仓库 API 与社区惯例有多处出入，**照惯例写会"编译通过、运行无错、但链路是断的"**。逐文件读：

1. `hubx/rpcx/docs/API.md` — 全文。关键：
   - **无 Consul 内置**：resolver 机制在 `pkg/rpcx/resolver/resolver.go`（`type Resolver interface { Resolve(ctx, serviceID, targets []string, cfg config.ResolverConfig) }` + `Register(name, resolver)` 返回 `WithResolver` 中间件）。**自研 Consul resolver 挂这里**
   - **传播器是 rpcx 中间件**：`rpcx.WithTracingMiddleware(serviceName)`（`pkg/rpcx/middleware/tracing.go`，TraceContext，client+server 同用；`WithMetricsMiddleware`、`WithLoggingMiddleware` 同模式）
   - **OTel 装配**：`rpcx.OTel.InitShutdown(opts)`（`pkg/rpcx/otel.go`，OTLP **gRPC** 4317、stdout、Jaeger gRPC 三种 exporter；默认开、`log.enable_trace` 关时降级 stdout）
   - 客户端寻址：endpoint 字符串直连（`"host:port"`）+ `WithResolver` 服务名寻址（默认 `default` = 配置中心寻址，`configx.RegisterAddressProvider` 注入）
2. `hubx/rpcx/rpcx/rpcx.go` — server 启动：`rpcx.NewServer(cfg, m)` → `m.BeforeStartup(server)` 注册 → `m.BuildClient()` 取 client → `m.Start(ctx)`；`kitex.NewClient(configclient.NewConfiger(opts), target, kopts...)`；`kitex.NewServer(configserver.NewConfiger(opts), kopts...)` + `server.RegisterService(...)`
3. `hubx/configx/docs/API.md` + `hubx/configx/examples/example.go` — 关键：
   - **本地 + 远程同时加载，本地字段优先**（不是"远程优先"！）
   - `configx.LoadFromBytes(data, dataId, groupId)` 读远程、`configx.LoadFromLocal(cfgPath)` 读本地；`HUBX_CONFIG` 环境变量
   - **`xmiddleware` 各段在 configx 是通用 map，不自动装配；DB/MQ 要走官方 loader**：`dbxconfig.LoadDBConfigFromConfigx(ctx, "xmiddleware.sqlx")`、`mqx.ReadConfigFromConfigx(ctx, "xmiddleware.mqx")`（与 monolith 同契约）
   - `configx.Trace()` 返回 `pkg/configx/trace.Switches{Enabled, DB, Cache, MQ}`（**无 On(kind) 方法**——monolith 的 `cfg.Trace.On` 是 monolith 自己在 `config/` 封装的；本案例在 `pkg/config` 里封装同一语义）
   - **无热更新**（watch 未实现）——配置变更必须重启服务生效（脚本按此设计）
4. `hubx/mqx/interface.go` + `hubx/mqx/config/read_config.go` + `hubx/mqx/examples/kafka/example.go` — 关键：
   - **header 是专用类型** `mqx.Header`（`pkg/mqx/header/header.go`，带 `With(key, val)` / `Get(key)`），不是 `map[string]string`；生产：`producer.Send(ctx, topic, headers, body)` / `SendAsync`；消费：`msg.Headers()` 返回 `*mqx.Header`
   - 驱动注册（blank import 驱动包即自动注册，`RegisterProducerCreator/ConsumerCreator` 在驱动 init 里做）；Kafka producer **幂等**（`Idempotent: true` 默认、acks=all），consumer **可配幂等**
   - **topic 自动创建由 broker 配置**（`auto.create.topics.enable=true`，示例 docker 里设了；bitnami kafka 需加环境变量 `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`）
   - configx 契约键：`xmiddleware.mqx.instances.<name>` → `mqx.ReadConfigFromConfigx` → `mqx.NewProducer(inst)` / `mqx.NewConsumer(inst, groupID)`
5. `hubx/cachex/interface.go` + `hubx/cachex/examples/redis/example.go` — 关键：
   - redis driver **已内置并自带 OTel 插桩**（`cachex.RegisterCreator` 由驱动包 init 注册，blank import `hubx/cachex/redis` 即可）
   - `SetTracingConfig(cfg)` 全局开关，**默认 `tracing.NewConfig()` 即默认开**（与 monolith 的"默认 false"认知相反——以当前源码为准，monolith 提示词里那条坑可能基于旧版）；**切驱动后必须显式确认 tracing 开**
   - Cache 接口完整清单（**没有 Incr**——限流计数用 `Set(key, val, TTL)` + 应用层判断，或直接用 Redis 原生 client 的 IncrExpire 走 cachex 的底层连接；本案例限流用 `Set NX EX` 语义的原子占位：`Set` 带 TTL + `Get` 判断，够用）
6. `hubx/dbx/docs/API.md` + `hubx/dbx/examples/gorm/example.go` — `dbxconfig.LoadDBConfigFromConfigx(ctx, "xmiddleware.sqlx")` → `dbx.Open(ctx, cfg)` / `OpenNoTrace`；GORM `Scan` 不产 span（用 `Find`）；SQL span 属性名 `db.query.text`
7. `hubx/otelx/interface.go` — 薄门面，OTel 实际装配在 monolith `internal/telemetry`（读全文件后移植到 `infra/telemetry`）
8. `hubx/webx/docs/API.md` — `webx.NewRouter` / `NewServiceWithResponse`（统一响应封装）/ `WithTraceMiddleware` / `WithReadyProbes` / `WithCORS`
9. `hubx/rpcx/example/server/main.go` + `example/client/main.go` + `example/user.proto` — 可抄的最小样例

**monolith 案例（hubx/monolith）整体读一遍**——装配顺序、分层 span 命名、PROMPT.md 七节结构、scripts/trace-matrix.py 风格，本案例逐字对齐。

---

## 0.1 已核实 API 修订（源码实证，优先级高于 0 节与规格）

> 2026-09-27 两个调研子代理逐文件核过 SDK 源码，以下结论覆盖 0 节与规格中的旧假设。**实施时以本节为准。**

### rpcx（`github.com/kitexx/kitex-framework`，本地目录 `../rpcx`）

- **无 Consul resolver、无注册中心集成**（全 rpcx/ grep `consul|resolver|discovery|registry` 零命中）。寻址靠 `ClientOptions.Target`（逗号分隔多实例 `ip1:port,ip2:port`，kitex 内置多副本负载+故障转移）
- **Consul 的用法因此是**：`infra/consul` 做 KV 注册表（每服务写 `hubx/svc/<serviceID>` → `host:port`），**客户端在启动/周期刷新时拉 KV 拼 Target**，而非挂 resolver 中间件。这就是"服务发现降级"，README 记录偏差
- 服务端：`<svc>.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{ServiceName, Addr, EnableTrace...})...)` → `go svr.Run()` → `svr.Stop()`（signal 优雅退出）；**健康探针 SDK 没有，各服务自行加 webx 探针或外部探针**
- 客户端：`<svc>.NewClient(destService, rpcx.BuildClientOptions(rpcx.ClientOptions{ServiceName, Target, EnableTrace...})...)`
- **OTel 传播器由 `obs-opentelemetry` suite 自动挂**：`BuildServerOptions`/`BuildClientOptions` 在 trace 开时追加 `WithSuite(tracing.NewServerSuite()/NewClientSuite())`，client 侧注入 header、server 侧续链，无需手写传播中间件。**但全局 TracerProvider 必须先由 `rpcx/tracing.Init(Options{ServiceName, Endpoint, Insecure})` 装好**（它读**rpcx 内置 configx** 的 `log.enable_trace`，OTLP gRPC 4317，TraceContext+Baggage 传播器）。**每个服务自己的 hubx/configx trace 段 → 映射到 `log.enable_trace` 写到本地 config.yaml**，两侧开关联动
- **rpcx 栈的配置键是 `kitex.server.*` / `kitex.client.*` / `log.*`**（rpcx/configx 的 viper 全局，`InitGlobal`/`MustLoad` 读本地文件），**不是 `xmiddleware.*`**
- span 命名（obs-opentelemetry）：`$package.$service/$method`（如 `api.OrderService/CreateOrder`），semconv v1.12 属性
- **kitex_gen 用 prutal**（`github.com/cloudwego/prutal`，纯 Go protobuf 替代）：`.pb.go` 由 `go run github.com/cloudwego/prutal/prutalgen` 生成，kitex 桩（client/server）用 kitex CLI；本 SDK `kitex_gen/echo/` 是完整可抄模板（含 `server.go` 的 `NewServer(handler, opts...)`、`client.go` 的 `NewClient(destService, opts...)`）

### hubx/configx（`github.com/gospacex/configx`，本地目录 `../configx`）

- **真实入口：`configx.New(&ConfigOptions{ConfigFile, ConfigType, Remote})` 或 `configx.ReadConfig(opts)`**；`0 节说的 `LoadFromBytes/LoadFromLocal` 不存在
- Nacos 通道：**key 级裁决**（远程有的 key 用远程，没有逐 key 回本地 viper）；**单实例单 dataId**（"11 服务共享 trace" = 每个服务 `data_id` 都指向共享 dataId `hubx.trace`，服务私有段放本地 config.yaml 兜底）；**远程开启但 Nacos 不可用 → `New` 直接报错**（不降级）
- **支持 Watch 热更新**（Nacos Listener 长轮询；回调值是 key 级；SDK 不自动重建 DB/MQ 连接池——本案例仍按"重启生效"设计，README 说明）
- 结构体 tag **必须 mapstructure + json 双写**；远程 YAML 数组读成 `[]string`、JSON 数字读成 `float64`
- **`configx.Trace()` 不存在**；trace 段照 monolith `config/` 模式逐段 `Unmarshal("trace", &c.Trace)` + `Trace.On(kind)` 方法（monolith config.go:55-88 有完整可抄实现，含 `resolveTraceSwitches` 官方段覆盖逻辑）
- 各 SDK 官方 loader：`dbxconfig.LoadDBConfigFromConfigx(cc, driver, name)`（注意首参是 `configx.Config` 句柄）、`mqx.ReadConfigFromConfigx(cfg, instance)`

### 装配总公式（每服务 main.go，替换 0 节旧描述）

```
1. hubx/configx.New（Nacos dataId=hubx.trace + 本地 config.yaml 兜底）→ Unmarshal 各段 + Trace.On 封装
2. 本地 config.yaml 写 kitex.server.*/kitex.client.*/log.enable_trace（= hubx trace 段 enabled）→ rpcx/configx.MustLoad + rpcx/tracing.Init
   ⚠ 步骤 2 先于 3：rpcx BuildServerOptions/BuildClientOptions 从全局 TracerProvider 取 suite
3. dbx.Open（经 LoadDBConfigFromConfigx，Trace.On("db") 门控）→ cachex（redis driver，Trace.On("cache") 门控）→ mqx（kafka driver，Trace.On("mq") 门控）
4. infra/consul 注册本服务；client 侧拉 KV 拼 Target
5. 起 kitex server（srv/job）/ webx（gateway/BFF）/ cobra（job）
6. 优雅退出反序 + rpcx/tracing 返回的 shutdown（OTel flush，≥10s 批超时已知坑）
```

---

## 1. 文件结构（创建前先锁定）

```
micro_apps/
├── go.mod / go.sum                  # module hubx/micro_apps；replace 本地 SDK ×8
├── pkg/
│   ├── proto/{user,product,order,pay,coupon}/*.proto
│   ├── kitex_gen/…                  # 生成产物（提交入库；生成命令见任务 3）
│   ├── events/{order_completed,coupon_exchanged,points_granted}.go
│   ├── codes/codes.go
│   ├── meta/meta.go                 # Header 常量（X-User-Id/X-Tenant/X-Gray）
│   ├── config/trace.go             # pkgconfigx: 从 configx 读 xmiddleware+trace 段，封装 Trace.On(kind) 等
│   └── seedsql/{user_db,product_db,order_db,pay_db,coupon_db}.sql
├── infra/
│   ├── telemetry/{provider,layers}.go   # 移植 monolith internal/telemetry（含 SetTracingConfig 门）
│   ├── consul/{resolver,register}.go     # 自研 Consul resolver（rpcx 扩展点）+ 服务注册
│   ├── nacos/loader.go                   # configx 双通道装配助手
│   └── drivers/                            # blank import 驱动包 + 统一驱动注册入口
├── apps/
│   ├── gateway/
│   │   ├── cmd/gateway/main.go
│   │   ├── internal/{auth, ratelimit, routing, proxy}/
│   │   └── config.yaml
│   ├── bff-{app,web,mini,admin}/       # 每个自包含（任务 12 用模板展开）
│   │   ├── cmd/bff-*/main.go
│   │   ├── internal/{handler, aggregate, dto, cache}/
│   │   └── config.yaml
│   ├── srv-{user,product,order,pay,coupon}/   # 每个自包含（任务 8/9）
│   │   ├── cmd/srv-*/main.go
│   │   ├── internal/{service, repository, rpc, middleware}/
│   │   └── config.yaml
│   └── job/
│       ├── cmd/job/main.go            # cobra: consume / retry / schedule
│       ├── internal/{consumer, handler, retry, schedule}/
│       └── config.yaml
├── configs/
│   ├── trace.yaml                     # 11 服务共读的 Nacos dataId（hubx.trace）
│   ├── services/*.yaml                # 各服务 xmiddleware 段
│   └── seed-nacos.sh
├── docker/{compose.yaml, mysql/init.sql, Dockerfile}
├── scripts/{bring-up.sh, e2e-flow.py, trace-matrix.py}
├── docs/verify/matrix-report.md      # 矩阵跑完生成
├── README.md
└── PROMPT.md
```

---

## 2. 任务分解（21 个任务，按依赖序）

### 任务 1：仓库脚手架 + go.mod
> **trace:** plan-ready.md → `### Task 1: 仓库脚手架 + go.mod` | tasks.md → `- [ ] 1.1 仓库脚手架：单 go.mod（module hubx/micro_apps，replace 本地 SDK ×8）+ `go mod tidy` + 11 个 apps 目录占位，`go build ./...` 通过（对应 Task 1）`
> **sync:** tasks.md → `- [ ] 1.1 仓库脚手架：单 go.mod（module hubx/micro_apps，replace 本地 SDK ×8）+ `go mod tidy` + 11 个 apps 目录占位，`go build ./...` 通过（对应 Task 1）` | plan-ready.md → `### Task 1: 仓库脚手架 + go.mod`

**文件：** 创建 `go.mod`、`go.sum`；确认 11 个 `apps/*/cmd/*/` 空目录占位

- [x]写 `go.mod`：`module hubx/micro_apps`，go 1.24；`require` + `replace` 指向 `../rpcx`、`../configx`、`../webx`、`../logx`、`../dbx`、`../cachex`、`../mqx`、`../otelx`（逐个 `go mod edit -require`/`-replace`，或参照 monolith go.mod 的 replace 清单复制）
- [x]`go mod tidy`（会拉 kitex、gin、viper 等进 go.sum；失败则逐个 SDK 的 go.mod 确认 version 号）
- [x]验证：`go build ./...`（空仓库过）；commit `chore: scaffold micro_apps module`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 2：pkg 数据层（纯数据，无外部依赖）
> **trace:** plan-ready.md → `### Task 2: pkg 数据层（纯数据，无外部依赖）` | tasks.md → `- [ ] 1.2 pkg 数据层：meta Header 常量、codes 统一错误码、events 三事件体（含 SchemaVer/traceparent 常量）、seedsql 五库 SQL、proto 五服务定义，`go vet ./pkg/...` 通过（对应 Task 2）`
> **sync:** tasks.md → `- [ ] 1.2 pkg 数据层：meta Header 常量、codes 统一错误码、events 三事件体（含 SchemaVer/traceparent 常量）、seedsql 五库 SQL、proto 五服务定义，`go vet ./pkg/...` 通过（对应 Task 2）` | plan-ready.md → `### Task 2: pkg 数据层（纯数据，无外部依赖）`

**文件：** 创建 `pkg/codes/codes.go`、`pkg/meta/meta.go`、`pkg/events/order_completed.go`、`pkg/events/coupon_exchanged.go`、`pkg/events/points_granted.go`、5 个 `pkg/seedsql/*.sql`、5 个 `pkg/proto/*.proto`

- [x]`pkg/meta`：`const HeaderUserID = "X-User-Id"; HeaderTenant = "X-Tenant"; HeaderGray = "X-Gray"`
- [x]`pkg/codes`：`type Code struct{Code int64; Message string}` + 统一错误码常量（`CodeOK=0`、`CodeInvalidParam`、`CodeNotFound`、`CodeStockNotEnough`、`CodeCouponUsed`、`CodePayFailed`…按 5 个 srv 的错误域分组，具体数值在写时定，**只定一次**）
- [x]`pkg/events`：三个事件体 struct（`OrderCompleted{OrderID int64; UserID int64; Amount int64; Items []Item; SchemaVer int; TraceID string}` 等，字段名对齐 proto 消息），`MarshalJSON/UnmarshalJSON`；`TraceHeaderKey = "traceparent"` 常量
- [x]`pkg/proto/*.proto`：5 个文件，按规格第 3 节的服务清单定 RPC 方法（srv-user: `Register/Login/GetProfile/CheckUser`；srv-product: `GetProduct/ReserveStock/ListProducts`；srv-order: `CreateOrder/GetOrder/Report`（4 表 JOIN）/`ListOrders`（只读流水，对账用）；srv-pay: `Pay/Callback/ListPayFlows`；srv-coupon: `Consume/GrantPoints/ListCoupons`）。每个 message 带 `int64 user_id`（跨域透传身份），**trace 走 gRPC metadata，不塞进消息体**
- [x]验证：`go vet ./pkg/...`；proto 语法用 `protoc --proto_path=pkg/proto --go_out=... 干跑`或 kitex CLI 校验（下一步）；commit `feat: pkg codes/meta/events/seedsql/proto`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 3：生成 kitex_gen（RPC 桩）
> **trace:** plan-ready.md → `### Task 3: 生成 kitex_gen（RPC 桩）` | tasks.md → `- [ ] 1.3 生成 kitex_gen RPC 桩（提交入库），`go build ./pkg/kitex_gen/...` 通过（对应 Task 3）`
> **sync:** tasks.md → `- [ ] 1.3 生成 kitex_gen RPC 桩（提交入库），`go build ./pkg/kitex_gen/...` 通过（对应 Task 3）` | plan-ready.md → `### Task 3: 生成 kitex_gen（RPC 桩）`

**文件：** 生成 `pkg/kitex_gen/{user,product,order,pay,coupon}/`

- [x]安装 kitex CLI（`go install github.com/kitexx/kitex/kitex@latest`，或 hubx/rpcx/scripts 里若有封装脚本用它的）
- [x]按 `hubx/rpcx/example/` 的生成命令逐 proto 生成到 `pkg/kitex_gen/`（monolith/monolith 的 PROMPT 无此步，本任务新增；生成命令格式：`kitex -module hubx/micro_apps -namespace hubx.micro_apps -service user -output pkg/kitex_gen/user pkg/proto/user/user.proto`，以 SDK 实际参数为准——先跑一个成功的再批 5 个）
- [x]验证：`go build ./pkg/kitex_gen/...` 过（生成代码依赖 go.mod 里的 kitex 依赖）；commit `feat: generate rpcx stubs for 5 services`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 4：infra/telemetry（移植 monolith）
> **trace:** plan-ready.md → `### Task 4: infra/telemetry（移植 monolith）` | tasks.md → `- [ ] 1.4 infra/telemetry：移植 monolith 分层 span 助手 + Init 顺序 + 总闸 noop 行为，stdout exporter 单测红→绿（对应 Task 4）`
> **sync:** tasks.md → `- [ ] 1.4 infra/telemetry：移植 monolith 分层 span 助手 + Init 顺序 + 总闸 noop 行为，stdout exporter 单测红→绿（对应 Task 4）` | plan-ready.md → `### Task 4: infra/telemetry（移植 monolith）`

**文件：** 创建 `infra/telemetry/provider.go`、`infra/telemetry/layers.go`、测试 `infra/telemetry/provider_test.go`

- [x]读 `hubx/monolith/internal/telemetry/` 全部文件，移植到 `infra/telemetry`：`Init(ctx, service, trace)`（读 `configx.Trace()` 总闸 → OTLP gRPC 4317 / stdout exporter）、`Start(ctx, layer, entity, op)` 分层 span 助手（命名 `handler.* / service.* / repository.* / middleware.*`，与 monolith 逐字对齐）
- [x]微调：`ServiceName` 参数化（11 服务各自传入）；**总闸关时返回 noop provider + 关闭 logx.EnableTrace**（对齐 monolith 矩阵"总闸比中间件管得宽"结论）
- [x]TDD：`provider_test.go` 先写——stdout exporter 捕获 3 类 span（`service.order.CreateOrder` 父 + `repository.orders.InsertTx` 子 + 属性层命名正确）；总闸关时 `Start` 返回 span 无 parent（noop）；运行 `go test ./infra/telemetry/` 红→绿
- [x]commit `feat: infra/telemetry (port monolith layered tracing)`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 5：infra/nacos + infra/drivers + pkg/config
> **trace:** plan-ready.md → `### Task 5: infra/nacos + infra/drivers + pkg/config` | tasks.md → `- [ ] 1.5 pkg/config `Trace.On(kind)` 三段正交封装 + infra/nacos loader + infra/drivers 薄封装，7 组合表驱动单测红→绿（对应 Task 5）`
> **sync:** tasks.md → `- [ ] 1.5 pkg/config `Trace.On(kind)` 三段正交封装 + infra/nacos loader + infra/drivers 薄封装，7 组合表驱动单测红→绿（对应 Task 5）` | plan-ready.md → `### Task 5: infra/nacos + infra/drivers + pkg/config`

**文件：** 创建 `infra/nacos/loader.go`、`infra/drivers/drivers.go`、`pkg/config/trace.go`（+ 各自测试）

- [x]`pkg/config/trace.go`：`type Config struct{ ... }` 从 configx 读 `xmiddleware.sqlx / cachex / mqx` 三段的官方 `enable_trace` 字段 + `configx.Trace()` 段，封装 `On(kind string) bool`（`enabled ∧ <kind> ∧ 官方段`，三细分正交——**这就是 monolith `cfg.Trace.On` 的等价物，本案例的"唯一开关入口"约束落在这里**）
- [x]`infra/nacos/loader.go`：`Load(ctx, dataID, group, localPath)` = `configx.LoadFromBytes`（远程，失败回退）+ `configx.LoadFromLocal(localPath)`（本地优先）；**文档注释写清"本地字段优先"语义 + "无热更新，改配置须重启"**
- [x]`infra/drivers/drivers.go`：blank import `hubx/cachex/redis`、`hubx/mqx/kafka`（驱动自注册）；导出 `NewCache(ctx, name)` / `NewProducer(name)` / `NewConsumer(name, group)` 薄封装（内部走 `cachex.Open(ctx, nil, name)` + `mqx.ReadConfigFromConfigx` + `mqx.NewProducer/NewConsumer`）
- [x]TDD：`pkg/config/trace_test.go` 写 7 种组合表驱动（全开/总闸关/db=off/cache=off/mq=off/省略细分/官方段关）断言 `On(kind)` 输出——**这份表驱动直接就是矩阵 7 组合的单元测试版**；运行红→绿
- [x]commit `feat: pkg/config trace switch + infra nacos loader & drivers`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 6：infra/consul（自研 resolver + 注册）
> **trace:** plan-ready.md → `### Task 6: infra/consul（自研 resolver + 注册）` | tasks.md → `- [ ] 1.6 infra/consul：自研 rpcx resolver（Resolve + Register/Deregister），httptest 模拟 KV 单测红→绿（对应 Task 6）`
> **sync:** tasks.md → `- [ ] 1.6 infra/consul：自研 rpcx resolver（Resolve + Register/Deregister），httptest 模拟 KV 单测红→绿（对应 Task 6）` | plan-ready.md → `### Task 6: infra/consul（自研 resolver + 注册）`

**文件：** 创建 `infra/consul/resolver.go`、`infra/consul/register.go`、测试 `infra/consul/resolver_test.go`

- [x]读 `hubx/rpcx/pkg/rpcx/resolver/resolver.go` 确认 `Resolver` 接口与 `Register` 签名
- [x]`resolver.go`：`type ConsulResolver struct{Client *consulapi.Client}` 实现 `Resolve(ctx, serviceID, targets, cfg)`——调 Consul KV `hubx/svc/<serviceID>` 读 `host:port` 列表；`Register("consul", &ConsulResolver{})` 导出 `WithConsulResolver` 中间件别名
- [x]`register.go`：`Register(agentAddr, serviceID, host, port int)` + `Deregister`（Consul KV 写/删）
- [x]TDD：`resolver_test.go` 用 httptest 模拟 Consul KV API，断言 `Resolve` 返回的地址列表正确、空 KV 返回空；红→绿
- [x]依赖：`github.com/hashicorp/consul/api`（`go get`）
- [x]commit `feat: infra/consul custom rpcx resolver + registry`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 7：docker 底座 + configs 灌入
> **trace:** plan-ready.md → `### Task 7: docker 底座 + configs 灌入` | tasks.md → `- [ ] 1.7 docker 底座 + configs：compose 六件套（mysql 五库/kafka/consul/nacos/redis/jaeger）、init.sql、11 份服务配置、trace.yaml、seed-nacos.sh，`docker compose ps` 全绿（对应 Task 7）`
> **sync:** tasks.md → `- [ ] 1.7 docker 底座 + configs：compose 六件套（mysql 五库/kafka/consul/nacos/redis/jaeger）、init.sql、11 份服务配置、trace.yaml、seed-nacos.sh，`docker compose ps` 全绿（对应 Task 7）` | plan-ready.md → `### Task 7: docker 底座 + configs 灌入`

**文件：** 创建 `docker/compose.yaml`、`docker/mysql/init.sql`、`docker/Dockerfile`、`configs/trace.yaml`、`configs/services/*.yaml` ×11、`configs/seed-nacos.sh`

- [x] `compose.yaml`：mysql8（`init.sql` 挂载进 `/docker-entrypoint-initdb.d`）/ bitnami kafka KRaft（加 `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`）/ consul:1.18+（server+http 8500+dns 8600）/ nacos standalone（8848）/ redis:7 / jaeger all-in-one（4317 gRPC + 16686 UI）；健康检查各容器
- [x] `init.sql`：建 5 库 5 账号（`user_rw@%` 等，`GRANT` 各自库），表结构与规格第 3 节一致（外键不建——微服务惯例不依赖跨库 FK）
- [x] `configs/services/*.yaml` ×11：每个服务 `xmiddleware.sqlx.databases.<self>`（连自己那个库的 DSN，密码走 env 占位）+ `xmiddleware.cachex.instances.<self>`（redis）+ `xmiddleware.mqx.instances.<self>`（kafka，按该服务是否生产/消费事件选择性声明）
- [x] `configs/trace.yaml`：规格第 5 节那份（enabled/exporter/endpoint/db/cache/mq）
- [x] `seed-nacos.sh`：`curl` Nacos OpenAPI 逐个 publish `trace.yaml` + `services/*.yaml`（dataId 命名规则：`trace.yaml` → dataId `hubx.trace` group `HUBX`；`services/<svc>.yaml` → dataId `hubx.<svc>`）
- [x] 验证：`docker compose up -d && docker compose ps` 全绿；`mysql -h127.0.0.1 -uuser_rw -p<pw> -e "show tables" user_db` 5 库可连；commit `feat: docker base + nacos config seed`

- [x]**Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 8：srv 模板服务（srv-order 先行，作为 5 个 srv 的模板）
> **trace:** plan-ready.md → `### Task 8: srv 模板服务（srv-order 先行，作为 5 个 srv 的模板）` | tasks.md → `- [ ] 2.1 srv-order 模板服务：装配顺序 main + 库内事务/跨域 gRPC 协作 repository + gRPC handler + outbox 事件 + 限流/身份中间件，手动冒烟 SQL span 可数（对应 Task 8）`
> **sync:** tasks.md → `- [ ] 2.1 srv-order 模板服务：装配顺序 main + 库内事务/跨域 gRPC 协作 repository + gRPC handler + outbox 事件 + 限流/身份中间件，手动冒烟 SQL span 可数（对应 Task 8）` | plan-ready.md → `### Task 8: srv 模板服务（srv-order 先行，作为 5 个 srv 的模板）`

**文件：** 创建 `apps/srv-order/{cmd/srv-order/main.go, internal/{service,repository,rpc,middleware}/, config.yaml}`

- [x] `main.go` 按规格第 5 节装配顺序（configx→telemetry→logx→infra→rpc server→webx 健康探针→优雅退出）；**头部注释逐字写装配顺序约束（11 份同构）**
- [x] `repository/`：GORM `dbx.Open`（经 `dbxconfig.LoadDBConfigFromConfigx`）；`CreateOrderTx`（orders+order_items 事务 + 扣 inventories——注意 inventories 在 product_db，**不能跨库 JOIN/事务**，改为：事务内写本地 order_db，库存扣减走 srv-product 的 gRPC `ReserveStock`，失败回滚本地事务——这是跨库铁律的代码示范）；`GetOrder`（2 表 JOIN，`Find`）；`Report`（4 表 JOIN，`Find`）；`ListOrders`（只读流水）
- [x] `rpc/`：实现 `pkg/kitex_gen/order` 生成的 `Server` 接口（`CreateOrder/GetOrder/Report/ListOrders` 四个 gRPC handler）；每个 handler 里 `telemetry.Start(ctx,"service","order","CreateOrder")` + 调 service 层
- [x] `service/`：业务逻辑 + 发 `OrderCompleted` 事件（`infra/drivers.NewProducer("order")` → `Send(ctx, "order.completed.v1", mqx.NewHeader().With("traceparent", tp), json)`；traceparent 从 `otel.GetTextMapPropagator().Inject` 取；**traceparent 写入受 `pkgconfigx.On("mq")` 门控**——mq 关时不写头）；outbox 表（order_db 内 `outbox` 表）+ 3s 轮询补发（`time.Ticker` goroutine）
- [x] `middleware/`：自保护限流（cachex `Set/Get` + TTL，`pkgconfigx.On("cache")` 门控）+ 内网身份校验（读 `meta.HeaderUserID`）
- [ ] 验证：`go build ./apps/srv-order/...` 过；起 mysql + `go run` 手动调 `CreateOrder`（curl 走 rpcx 不行，用 `pkg/kitex_gen/order` 的 client + `rpcx.OTel.InitShutdown` 小测试程序跑通，或直接写一个 `_smoke` 的 main）；SQL span 数（stdout exporter 数）；commit `feat: srv-order template service (join/outbox/mq)`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 9：其余 4 个 srv（user/product/pay/coupon，照 srv-order 模板展开）
> **trace:** plan-ready.md → `### Task 9: 其余 4 个 srv（user/product/pay/coupon，照 srv-order 模板展开）` | tasks.md → `- [ ] 2.2 其余 4 个 srv（user/product/pay/coupon）照模板展开，各库 JOIN 落点与核心方法冒烟通过（对应 Task 9）`
> **sync:** tasks.md → `- [ ] 2.2 其余 4 个 srv（user/product/pay/coupon）照模板展开，各库 JOIN 落点与核心方法冒烟通过（对应 Task 9）` | plan-ready.md → `### Task 9: 其余 4 个 srv（user/product/pay/coupon，照 srv-order 模板展开）`

**文件：** 创建 `apps/srv-{user,product,pay,coupon}/`（各自 cmd/internal/config.yaml，结构同任务 8）

- [x] srv-user：`users ⋈ user_profiles` 连表（`Find`）；JWT 签发/校验（`golang-jwt/jwt/v5`，HS256，密钥走配置）；`auth_sessions` 表写登录态
- [x] srv-product：`products ⋈ categories ⋈ inventories` 连表（`Find`）；`product:hot:{id}` 缓存（TTL 30s，cachex redis）；`ReserveStock` gRPC 扣库存（`FOR UPDATE` 行锁 + `pkgconfigx.On("db")` 门控的 SQL span）
- [x] srv-pay：`pay_orders` / `pay_callbacks` 两表；`Pay` 事务（insert + 更新 state）；`ListPayFlows` 只读流水（对账数据源）
- [x] srv-coupon：`coupons` / `user_coupons` / `points_log` 三表；`Consume`（核销，幂等：`user_coupons` 唯一约束防重）；`GrantPoints`（积分，job 调用入口，发 `PointsGranted` 事件）
- [ ] 验证：5 个 srv `go build ./apps/...` 全过；各起一个（mysql 已建库）跑通核心方法；commit `feat: srv user/product/pay/coupon`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 10：gateway（根 span + JWT + 限流 + 路由）
> **trace:** plan-ready.md → `### Task 10: gateway（根 span + JWT + 限流 + 路由）` | tasks.md → `- [ ] 2.3 gateway：JWT 一次验签透传 + cachex 限流 + 配置路由 + 根 span 转发，全链路 curl 冒烟通过（对应 Task 10）`
> **sync:** tasks.md → `- [ ] 2.3 gateway：JWT 一次验签透传 + cachex 限流 + 配置路由 + 根 span 转发，全链路 curl 冒烟通过（对应 Task 10）` | plan-ready.md → `### Task 10: gateway（根 span + JWT + 限流 + 路由）`

**文件：** 创建 `apps/gateway/{cmd/gateway/main.go, internal/{auth,ratelimit,routing,proxy}/, config.yaml}`

- [x] `auth/`：JWT 验签一次（`golang-jwt`），解出 `user_id/roles` 写 `meta.HeaderUserID` 等 Header，透传给下游 BFF（**不再二次验签**）
- [x] `ratelimit/`：全局限流（cachex `Set NX EX` 计数器，`rl:{ip}:{route}`，`pkgconfigx.On("cache")` 门控）
- [x] `routing/`：路由表 `path → 目标 BFF/SRV 服务名`（配置驱动，不硬编码）+ 灰度头透传（`meta.HeaderGray`）
- [x] `proxy/`：HTTP→gRPC 转发（rpcx client 桩，`WithTracingMiddleware` 自动注入 traceparent；**根 span 在 gateway 诞生**——`telemetry.Start(ctx,"handler","gateway","Proxy:"+path)` 作为整条 trace 的根）
- [x] `main.go` 装配同任务 8 模板（gateway 无 DB，infra 只装缓存+限流）
- [ ] 验证：`go build`；起 gateway + 一个 bff + 一个 srv，curl 带 JWT 打全链路，stdout exporter 看到 `handler.gateway.Proxy` 根 span 串着下游；commit `feat: gateway (root span/jwt/ratelimit/routing)`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 11：BFF 模板（bff-app 先行）
> **trace:** plan-ready.md → `### Task 11: BFF 模板（bff-app 先行）` | tasks.md → `- [ ] 2.4 BFF 模板 bff-app：errgroup 并发聚合 + 降级兜底 + 展示层缓存 + 统一响应，缓存命中/未命中两种 span 冒烟通过（对应 Task 11）`
> **sync:** tasks.md → `- [ ] 2.4 BFF 模板 bff-app：errgroup 并发聚合 + 降级兜底 + 展示层缓存 + 统一响应，缓存命中/未命中两种 span 冒烟通过（对应 Task 11）` | plan-ready.md → `### Task 11: BFF 模板（bff-app 先行）`

**文件：** 创建 `apps/bff-app/{cmd/bff-app/main.go, internal/{handler,aggregate,dto,cache}/, config.yaml}`

- [x] `aggregate/`：`errgroup` 并发调多 srv（`pkg/kitex_gen` 的 client 桩 + `rpcx.WithTracingMiddleware`）+ 降级兜底（某 srv 挂返回默认值，`gofail` 或返回 sentinel error）+ 字段裁剪（`dto/` 按端裁）
- [x] `cache/`：`bff:app:{resource}:{user_id}` 展示层缓存（命中直接返回不打 srv；miss 走 errgroup；TTL 60s，cachex redis）
- [x] `handler/`：webx `NewServiceWithResponse`（统一响应封装）+ 就绪探针（`WithReadyProbes`）
- [x] `main.go` 装配同任务 8 模板（BFF 无 DB，infra 只装缓存+MQ 消费）
- [ ] 验证：`go build`；起 bff-app + 依赖的 srv，curl 聚合接口，stdout 看到 errgroup 并发 + 缓存命中/未命中两种 span；commit `feat: bff-app template (errgroup aggregate/degrade/cache)`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 12：其余 3 个 BFF（web/mini/admin，照 bff-app 展开）
> **trace:** plan-ready.md → `### Task 12: 其余 3 个 BFF（web/mini/admin，照 bff-app 展开）` | tasks.md → `- [ ] 2.5 其余 3 个 BFF（web/mini/admin）按端独立展开（无共享业务代码），4 个 BFF 聚合接口冒烟通过（对应 Task 12）`
> **sync:** tasks.md → `- [ ] 2.5 其余 3 个 BFF（web/mini/admin）按端独立展开（无共享业务代码），4 个 BFF 聚合接口冒烟通过（对应 Task 12）` | plan-ready.md → `### Task 12: 其余 3 个 BFF（web/mini/admin，照 bff-app 展开）`

**文件：** 创建 `apps/bff-{web,mini,admin}/`（各自 cmd/internal/config.yaml，结构同任务 11）

- [x] 差异按端隔离：bff-web 多了 SEO/SSR 相关字段（`dto` 裁剪不同）；bff-mini 多了小程序码字段；bff-admin 接全部 5 个 srv 的只读接口 + 管理端路由（与 C 端隔离，单独路由组）
- [x] 4 个 BFF 的 `aggregate`/`handler`/`cache` 按端参数化（不共享代码，各写一份，允许 80% 重复——规格裁决"独立目录、无共享代码"）
- [ ] 验证：4 个 BFF `go build`；各起一个打聚合接口；commit `feat: bff web/mini/admin`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 13：job（cobra + Kafka 消费续 traceparent + gocron 调度）
> **trace:** plan-ready.md → `### Task 13: job（cobra + Kafka 消费续 traceparent + gocron 调度）` | tasks.md → `- [ ] 2.6 job：cobra 三命令 + Kafka 消费 traceparent 续接 + DLQ 重放 + gocron 调度（对账走 gRPC），send/receive 同 trace 冒烟通过（对应 Task 13）`
> **sync:** tasks.md → `- [ ] 2.6 job：cobra 三命令 + Kafka 消费 traceparent 续接 + DLQ 重放 + gocron 调度（对账走 gRPC），send/receive 同 trace 冒烟通过（对应 Task 13）` | plan-ready.md → `### Task 13: job（cobra + Kafka 消费续 traceparent + gocron 调度）`

**文件：** 创建 `apps/job/{cmd/job/main.go, internal/{consumer,handler,retry,schedule}/, config.yaml}`

- [ ] `cmd/job/main.go`：cobra 三命令 `consume`（Kafka 消费者，`mqx.NewConsumer("job", "job-order-v1")` → `Consume(ctx, "order.completed.v1")`）、`retry`（DLQ 重放，`*.dlq` topic）、`schedule`（gocron 宿主：`github.com/ouqiang/gocron` 注册 cron 任务——对账 order vs pay 流水（各 srv gRPC 拉数，task 自身不直连 DB）、库存日清、报表聚合；每个任务 `telemetry.Start(ctx,"task","reconcile","Run")` 自建根 span）
- [ ] `consumer/`：解析 `msg.Headers()` 的 `traceparent` → `otel.GetTextMapPropagator().Extract` 重建 ctx → `telemetry.Start(ctx,"messaging","order.completed","Receive")` 续接 srv-order 的 trace；消费失败进 `*.dlq`（`mqx` 消费侧重投）
- [ ] `handler/`：`order.completed` → 调 srv-coupon `GrantPoints`（gRPC，trace 续段）
- [ ] 验证：`go build`；起 srv-order（发事件）+ job consume，stdout/Jaeger 看到 `messaging.send`（srv-order）与 `messaging.receive`（job）同 trace_id；`job schedule` 起来跑一轮对账，stdout 看到 `task.*` 根 span；commit `feat: job (kafka consumer + gocron schedule)`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 14：README + PROMPT
> **trace:** plan-ready.md → `### Task 14: README + PROMPT` | tasks.md → `- [ ] 3.1 README + PROMPT：拓扑图、装配顺序、开关设计、JOIN 红线、SDK 边界、排查清单（现象/根因/对策）、7 节结构 PROMPT（对应 Task 14）`
> **sync:** tasks.md → `- [ ] 3.1 README + PROMPT：拓扑图、装配顺序、开关设计、JOIN 红线、SDK 边界、排查清单（现象/根因/对策）、7 节结构 PROMPT（对应 Task 14）` | plan-ready.md → `### Task 14: README + PROMPT`

**文件：** 创建 `README.md`、`PROMPT.md`

- [ ] `README.md`：拓扑图（ASCII，11 服务 + docker 底座）、装配顺序约束（11 份 main 头部注释摘录）、统一开关设计（trace 段裁决规则 + 矩阵结论占位）、JOIN 红线（库内 JOIN 表 + 对账走 gRPC 说明）、SDK 使用边界（哪些 SDK 用在哪个服务）、Jaeger 接入与排查清单（继承 monolith 7 条 + 本案例 7 条新坑，**逐条写：现象/根因/对策**）
- [ ] `PROMPT.md`：复刻 monolith `PROMPT.md` 7 节结构（先读源码/硬性约束/功能范围/验收标准/已知坑/交付物/禁止事项），把本案例的"先读源码"清单（计划第 0 节）原样搬进去
- [ ] 验证：人工读一遍 README 的拓扑图与装配顺序是否与代码一致；PROMPT 的"先读源码"清单与第 0 节逐条对应；commit `docs: README + PROMPT`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 15：bring-up.sh（按依赖序起 11 服务 + 配置重载）
> **trace:** plan-ready.md → `### Task 15: bring-up.sh（按依赖序起 11 服务 + 配置重载）` | tasks.md → `- [ ] 3.2 scripts/bring-up.sh：依赖序启动 11 服务 + `--services`/`--reload` 过滤，从零启动全绿（对应 Task 15）`
> **sync:** tasks.md → `- [ ] 3.2 scripts/bring-up.sh：依赖序启动 11 服务 + `--services`/`--reload` 过滤，从零启动全绿（对应 Task 15）` | plan-ready.md → `### Task 15: bring-up.sh（按依赖序起 11 服务 + 配置重载）`

**文件：** 创建 `scripts/bring-up.sh`

- [ ] 依赖序：`db 就绪 → srv×5 → bff×4 → gateway → job`（每步 `wait` Consul 健康检查 + 各服务 `/healthz/ready`）；支持 `--services <csv>` 过滤（矩阵用：改 Nacos 后只重启受影响服务）；支持 `--reload <svc>` 单服务重启
- [ ] 验证：从零 `bash scripts/bring-up.sh`（docker compose up 后）11 服务全绿；`--services srv-order` 只重启一个；commit `feat: bring-up.sh dependency-ordered startup`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 16：e2e-flow.py（主链路跨进程 trace 证据）
> **trace:** plan-ready.md → `### Task 16: e2e-flow.py（主链路跨进程 trace 证据）` | tasks.md → `- [ ] 3.3 scripts/e2e-flow.py：主链路 ≥6 服务同 trace + messaging 同 trace 断言与 span 树输出（对应 Task 16）`
> **sync:** tasks.md → `- [ ] 3.3 scripts/e2e-flow.py：主链路 ≥6 服务同 trace + messaging 同 trace 断言与 span 树输出（对应 Task 16）` | plan-ready.md → `### Task 16: e2e-flow.py（主链路跨进程 trace 证据）`

**文件：** 创建 `scripts/e2e-flow.py`

- [ ] 打一条下单请求（带 JWT 的完整链）+ 触发 `order.completed` Kafka 事件 + job 消费 + 报表接口；等 BatchSpanProcessor flush（≥10s，已知坑）；走 Jaeger API（`/api/services` + `/api/traces?service=...`）拉出主 trace，断言同 `trace_id` 出现在 ≥6 个服务、`messaging.send`（srv-order）与 `messaging.receive`（job）同 trace、job 续调 srv-coupon 挂同 trace；打印该 `trace_id` 与逐服务 span 树
- [ ] 验证：`python3 scripts/e2e-flow.py`（Jaeger 已起）过；commit `feat: e2e cross-process trace evidence`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 17：trace-matrix.py（11×7 全量矩阵）
> **trace:** plan-ready.md → `### Task 17: trace-matrix.py（11×7 全量矩阵）` | tasks.md → `- [ ] 3.4 scripts/trace-matrix.py：11×7 全量矩阵 + 组合 5 无 traceparent 头断言 + matrix-report.md 落盘（对应 Task 17）`
> **sync:** tasks.md → `- [ ] 3.4 scripts/trace-matrix.py：11×7 全量矩阵 + 组合 5 无 traceparent 头断言 + matrix-report.md 落盘（对应 Task 17）` | plan-ready.md → `### Task 17: trace-matrix.py（11×7 全量矩阵）`

**文件：** 创建 `scripts/trace-matrix.py`、`docs/verify/matrix-report.md`（脚本生成）

- [ ] 7 组合（规格第 6 节表格）× 11 服务：Nacos 程序化改 `trace.yaml` dataId → `bring-up.sh --services all`（重启全 11 服务，**不重起 compose**）→ 打固定请求集（users CRUD + 下单 + 报表 + 缓存命中 + Kafka 全流程）→ 等 flush → 走 Jaeger API 按 `service × span.name 前缀` 计数（`handler.*/service.*/repository.*/rpc client/server/db.*/缓存/messaging.*`）→ 组合 5 额外断言 Kafka 消息无 `traceparent` 头、job 侧为自建根 trace；落 `docs/verify/matrix-report.md`（11×7 表格 + 三点结论）
- [ ] 验证：`python3 scripts/trace-matrix.py` 全跑（约 7 轮 × 重启 + 请求集，预留 30~40 min）；矩阵报表三点结论成立；commit `feat: full trace switch matrix (11×7)`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 18：config.stdout.yaml 零依赖验证路径
> **trace:** plan-ready.md → `### Task 18: config.stdout.yaml 零依赖验证路径` | tasks.md → `- [ ] 3.5 config.stdout.yaml 零依赖验证路径：exporter stdout 下终端可逐行数 span（对应 Task 18）`
> **sync:** tasks.md → `- [ ] 3.5 config.stdout.yaml 零依赖验证路径：exporter stdout 下终端可逐行数 span（对应 Task 18）` | plan-ready.md → `### Task 18: config.stdout.yaml 零依赖验证路径`

**文件：** 创建 `configs/trace.stdout.yaml`（变体）、各 `apps/*/config.stdout.yaml`（可选，或统一在 `trace.stdout.yaml` 里 `exporter: stdout`）

- [ ] Jaeger 不可用时的本地验证路径：`exporter: stdout`，`go run` 各服务直接终端数 span；README 写"先 stdout 验证逻辑、再切 Jaeger 验收"两步走
- [ ] 验证：`HUBX_CONFIG=configs/trace.stdout.yaml go run ./apps/srv-order` 起服务 + 打请求，终端逐行看到分层 span；commit `feat: stdout exporter config variant`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 19：go test -race ./... 全绿
> **trace:** plan-ready.md → `### Task 19: go test -race ./... 全绿` | tasks.md → `- [ ] 4.1 `go test -race ./...` 全绿：service 层 SQLite 单测、BFF 降级测试、job traceparent 续接测试补齐（对应 Task 19）`
> **sync:** tasks.md → `- [ ] 4.1 `go test -race ./...` 全绿：service 层 SQLite 单测、BFF 降级测试、job traceparent 续接测试补齐（对应 Task 19）` | plan-ready.md → `### Task 19: go test -race ./... 全绿`

**文件：** 各 `*_test.go` 补齐（任务 4/5/6/8/9 已写一部分）

- [ ] 补：各 srv 的 service 层单测（GORM 用 SQLite 内存库替代 MySQL——`dbx` 的 SQLite 驱动在 monolith 已验证）、BFF 的 aggregate 降级兜底测试（`gofail` 模拟 srv 挂）、job 的 traceparent 续接测试
- [ ] 验证：`go test -race ./...` 全绿（至少 80% 行覆盖，`go test -cover ./...`）；commit `test: race + coverage`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 20：Jaeger 全链路人肉复核 + 排查清单
> **trace:** plan-ready.md → `### Task 20: Jaeger 全链路人肉复核 + 排查清单` | tasks.md → `- [ ] 4.2 Jaeger 人肉复核：/api/services 11 服务名齐全、3 条抽查 trace 逐 span 核对、新坑补录 README（对应 Task 20）`
> **sync:** tasks.md → `- [ ] 4.2 Jaeger 人肉复核：/api/services 11 服务名齐全、3 条抽查 trace 逐 span 核对、新坑补录 README（对应 Task 20）` | plan-ready.md → `### Task 20: Jaeger 全链路人肉复核 + 排查清单`

**文件：** 无新文件，`README.md` 排查清单章节补充实测

- [ ] `docker compose up -d jaeger` → Jaeger UI（:16686）查 `/api/services` 出现全部 11 个服务名；抽查 3 条 trace（主链路 / Kafka 续接 / 报表 4 表 JOIN）逐 span 核对命名与层级
- [ ] 把复核中发现的新坑补进 README 排查清单（已知坑表末行"遇新坑补录"）；commit `docs: jaeger manual verification + pitfall addendum`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

### 任务 21：git 收尾 + 复跑三件套
> **trace:** plan-ready.md → `### Task 21: git 收尾 + 复跑三件套` | tasks.md → `- [ ] 4.3 git 收尾 + 三件套从零无人值守复跑：gofmt/vet 全绿、清仓、复放全部验收结论；禁令扫描通过（无 TODO/占位、8 个 SDK 目录零改动、README 入链结论均附实测证据）（对应 Task 21）`
> **sync:** tasks.md → `- [ ] 4.3 git 收尾 + 三件套从零无人值守复跑：gofmt/vet 全绿、清仓、复放全部验收结论；禁令扫描通过（无 TODO/占位、8 个 SDK 目录零改动、README 入链结论均附实测证据）（对应 Task 21）` | plan-ready.md → `### Task 21: git 收尾 + 复跑三件套`

**文件：** 无新文件

- [ ] `git status` 清仓（无 `*.pid` / `*.log` / 临时文件）；`gofmt -l .` 空；`go vet ./...` 全绿
- [ ] 从零复跑：`docker compose up -d` → `bash configs/seed-nacos.sh` → `bash scripts/bring-up.sh` → `python3 scripts/e2e-flow.py` → `python3 scripts/trace-matrix.py`；确认三件套无人值守重放全部结论；commit `chore: final verification pass`

- [ ] **Task complete**（本 Task 全部 Step 为 `[x]` 后勾选；与 plan-ready **任务完成**、tasks.md 对应行同步）

---

## 3. 自检（计划对照规格）

| 规格章节 | 对应任务 | 备注 |
|---|---|---|
| §1 拓扑与调用深度 | 8~13 | 最深 3 层（gateway→bff→srv→srv）|
| §2 目录结构 | 1~14 | 逐目录对应 |
| §3 数据模型 + JOIN 落点 | 8~9 | 4 表 JOIN 锚点在 srv-order Report |
| §4 缓存/MQ 入链点 | 4~9,13 | outbox 补发在 srv-order（任务 8）；Kafka 门控在 pkg/config（任务 5）|
| §5 装配 + 统一开关 | 4,5,8~13 | 11 份 main 同构（任务 8 定模板）|
| §6 验收 | 16,17,18,20,21 | 主证据 + 矩阵 + stdout 变体 + 人肉复核 + 复跑 |
| §7 已知坑 | 14,20 | README 排查清单 |
| §8 交付物 | 全部 | 含 PROMPT.md（任务 14）|

**一致性检查：** `pkgconfigx.On(kind)` 语义（任务 5 表驱动）与 `trace-matrix.py` 7 组合（任务 17）逐条对应——矩阵组合 5（mq 关）同时验证"messaging span 不出 + Kafka 不写 traceparent 头 + job 退化自建根"三件事，任务 17 已写明。

**与 monolith 的偏差（已在计划第 0 节明示，非 bug）：**
1. configx 是"本地优先"不是"远程优先"（monolith 提示词措辞"远程优先"可能基于旧版）
2. 传播器是 `rpcx.WithTracingMiddleware` 中间件，不是 otelgrpc 自动插桩
3. OTel 装配推荐 `rpcx.OTel.InitShutdown()`（读 `log.enable_trace`）；若要走统一 trace 段则用 `infra/telemetry` 自装（读 `configx.Trace()`）——**本计划定死走 `infra/telemetry` 自装**，`rpcx.OTel` 仅用于 rpcx 内部插桩不冲突时用
4. cachex tracing 默认开（当前源码），但仍须显式 `SetTracingConfig` 门控（与 monolith 行为对齐）
