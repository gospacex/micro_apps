# micro_apps — hubx 微服务参考案例设计规格

> 状态：已评审（2026-09-27）。与 `hubx/monolith` 案例同族，定位是"hubx 全家桶在完整微服务拓扑下的落地形态 + 跨进程全链路 trace"。

## 0. 目标与定位

在 `hubx/micro_apps/` 下从零搭建一套**生产级完整微服务参考案例**：横向按流量走向分层（Gateway → BFF → SRV → 存储），纵向覆盖注册发现（Consul）/ 配置（Nacos）/ 调用链（OTel + Jaeger）/ 事件（Kafka）/ 缓存（Redis）。

核心演示目标（对齐 monolith）：**一个统一调用链开关（configx 的 trace 段），让全部 11 个服务从 Gateway 到 BFF、SRV 到各存储中间件的操作全部串进同一条 trace，开关改一次、11 个进程同开同关。**

与 monolith 案例的独有增量知识点：

1. **跨进程 gRPC trace 续接**（TraceContext 传播器，rpcx client/server 两侧）
2. **Kafka 事件 traceparent 跨进程续接**（message header 传递，job 消费端 Extract 重建 ctx）
3. **统一开关经 Nacos 下发，多进程行为一致性**（全量矩阵验证）
4. **架构红线示范**：数据库私有（JOIN 只发生在本 srv 库内，跨域数据走 gRPC 不 JOIN）、BFF 按端独立、网关零业务

## 1. 服务拓扑（11 个可部署单元）

```
客户端
  │ HTTP
gateway          根 span 诞生地：JWT 验一次 → 身份 Header 透传 / 全局限流 / 路由 / TraceID 注入
  │ gRPC（rpcx，Consul 服务名寻址）
┌─────────────── BFF 层（按端独立、各自目录、无共享业务代码）
│  bff-app · bff-web · bff-mini · bff-admin(兼 admin 后台管理)
└──────────────────────────────────────────────────────────
  │ gRPC
┌─────────────── SRV 层（领域原子服务，库表私有）
│  srv-user · srv-product · srv-order · srv-pay · srv-coupon
└──────────────────────────────────────────────────────────
  │ MySQL(五库) · Redis · Kafka
job（cobra CLI）：consume（Kafka 消费者，续 traceparent）/ retry（DLQ 重放）/
                  schedule（gocron 宿主：对账、库存日清、报表）
```

### 关键裁决（已拍板）

| 决策 | 结论 |
|---|---|
| 运行时底座 | 本地 Docker（用户环境已有）：MySQL / Kafka / Consul / Nacos / Redis / Jaeger |
| 服务间 RPC | **rpcx（hubx 的 kitex 栈）**，不引入原生 grpc-go 双轨 |
| 服务发现 | **Consul KV 注册 + 客户端拉取组装 Target**（rpcx 无 resolver 扩展点，寻址走 kitex 内置多实例机制 `ClientOptions.Target` 逗号分隔；`infra/consul` 负责 KV 注册/拉取/周期刷新，README 记录该偏差） |
| 配置中心 | **Nacos**（configx 远程通道；本地 `config.yaml` viper 兜底；远程/本地裁决交给 configx，服务不写分支） |
| BFF 组织 | 四个 BFF **独立目录、无共享代码**；差异按端隔离（DTO/聚合/路由） |
| 共享边界 | 仅 `pkg/`（纯数据：proto 桩、事件体、错误码、身份 Header 常量）+ `infra/`（纯装配助手） |
| 定时任务 | **不自研调度**，gocron（`github.com/ouqiang/gocron`）收编进 job 的 `job schedule` 命令；不设独立 task 服务 |
| CLI | job 用 **cobra**（consume / retry / schedule） |
| 代码组织 | 单 go.mod + `apps/` 目录隔离，`replace` 指向本地 SDK 目录（SDK 未发布） |
| kitex_gen | 代码生成产物**提交入库**（验收不依赖 kitex CLI） |
| JOIN 红线 | **各 srv 库内 JOIN**；跨域数据走 srv gRPC 只读接口。对账（order vs pay）在 job 内存比对，不跨库 JOIN |
| 缓存 | 真接 **Redis**（cachex 切 redis driver）：BFF 展示层缓存、srv 热点、gateway 限流计数器 |
| 验收 | **全量矩阵**：11 服务 × 7 组合（B 方案），Nacos 程序化改配置 + 重启服务 |
| 服务运行方式 | 11 个服务不进 compose，`scripts/bring-up.sh` 本地 `go run` 按依赖序起（改配置重启单服务快） |

### 调用深度（防雪崩铁律）

gateway → bff → srv（2 层 gRPC）；srv-order → srv-user / srv-product / srv-coupon（+1，共 3 层）。**最深 3 层，无 srv 二级互调**（srv-user 不再调别人）。跨域异步协作走 Kafka 事件。

## 2. 目录结构

```
micro_apps/
├── go.mod  go.sum           # 单模块；replace ../{rpcx,configx,logx,dbx,cachex,mqx,otelx,…}
├── apps/
│   ├── gateway/             # cmd/gateway/main.go + internal/{auth, ratelimit, routing, proxy} + config.yaml
│   ├── bff-app/  bff-web/  bff-mini/  bff-admin/
│   │                      # 各自 cmd/ + internal/{handler, aggregate, dto, cache} + config.yaml
│   ├── srv-user/ srv-product/ srv-order/ srv-pay/ srv-coupon/
│   │                      # 各自 cmd/ + internal/{service, repository, rpc, middleware} + config.yaml
│   └── job/                 # cmd/job/main.go(cobra: consume/retry/schedule) + internal/{consumer, handler, retry, schedule(gocron)} + config.yaml
├── pkg/
│   ├── proto/               # 5 个 srv 的 .proto（单一事实源）
│   ├── kitex_gen/           # rpcx 生成产物（提交入库）
│   ├── events/              # Kafka 事件体（order_completed / coupon_exchanged / points_granted，带 schema 版本）
│   ├── codes/               # 统一错误码
│   └── meta/                # 身份透传 Header 常量（X-User-Id / X-Tenant / X-Gray…）
├── infra/
│   ├── telemetry/           # 全局 TracerProvider + OTLP 导出 + 分层 span 助手 Start(layer,entity,op)
│   ├── nacos/               # configx 远程通道接入（dataId/group 对齐企业契约）
│   ├── consul/              # rpcx 自定义 resolver（若 SDK 内置则薄封装）
│   └── drivers/             # 缓存/MQ 驱动注册（Redis、Kafka；切 driver 只改配置）
├── configs/
│   ├── trace.yaml           # 统一 trace 段（11 服务读同一 Nacos dataId）
│   ├── services/*.yaml      # 各服务 xmiddleware.{sqlx,cachex,mqx} 段（字段名对齐 monolith 契约）
│   └── seed-nacos.sh        # 灌入 Nacos（compose 起完执行一次）
├── docker/
│   ├── compose.yaml         # mysql(五库) / kafka(KRaft) / consul / nacos / redis / jaeger
│   ├── mysql/init.sql       # 五库五账号 + 全量表
│   └── Dockerfile           # 可选：服务容器化路径（默认 go run）
├── scripts/
│   ├── bring-up.sh          # docker compose up + seed-nacos + 按依赖序起 11 服务
│   ├── e2e-flow.py          # 主链路跨进程 trace 证据
│   └── trace-matrix.py      # 11×7 全量开关矩阵
├── docs/verify/matrix-report.md
├── README.md
└── PROMPT.md                # 本案例生成提示词（复刻 monolith 7 节结构）
```

## 3. 数据模型（一库一 srv 一账号）

| 库 | 归属 | 表 |
|---|---|---|
| user_db | srv-user | users；user_profiles(1:1)；auth_sessions |
| product_db | srv-product | products；categories；inventories(1:1)；product_tags + tag_relations(M:N) |
| order_db | srv-order | orders；order_items；order_status_log；refund_records |
| pay_db | srv-pay | pay_orders；pay_callbacks |
| coupon_db | srv-coupon | coupons；user_coupons；points_log |

### JOIN 落点（验收 SQL span 计数锚点；一律 GORM `Find`，禁 `Scan`——Scan 不产 SQL span，monolith 已知坑）

| 所在 srv | 查询 | 表数 |
|---|---|---|
| srv-user | 用户详情 users ⋈ user_profiles | 2 |
| srv-product | 商品详情 products ⋈ categories ⋈ inventories | 3 |
| srv-order | 订单详情 orders ⋈ order_items | 2 |
| srv-order | 报表 orders ⋈ order_items ⋈ order_status_log ⋈ refund_records | **4（矩阵锚点）** |
| job | 对账：srv-order/srv-pay 各给 gRPC 只读流水接口，拉回内存比对 | 跨库不 JOIN（铁律演示） |

## 4. 缓存 / MQ 入链点

### 缓存（cachex → Redis driver；**必须显式 SetTracingConfig**，monolith 已知坑：默认关，切 Redis 后 span 静默消失）

| 位置 | 键 | 演示点 |
|---|---|---|
| 4×BFF | `bff:{app}:{resource}:{user_id}` | 展示层缓存：命中不再打 srv；miss 走 errgroup 并发聚合 + 降级兜底 |
| srv-product | `product:hot:{id}` | srv 侧热点，TTL 30s |
| gateway | `rl:{ip}:{route}` | 限流计数器复用 cachex 同一 Cache 接口（不引第二套客户端） |
| job retry | `job:dlq:{msg_id}` | 重放防重锁 |

### MQ（mqx → Kafka driver，bitnami KRaft 单节点）

| 事件 | 生产 | 消费 | traceparent |
|---|---|---|---|
| order.completed | srv-order（事务提交后 outbox 表 + 内置 3s 轮询补发，不起独立生产者服务） | job consume | producer 写 header（受 `Trace.On("mq")` 门控）；job 解析 header 续接 → `messaging.receive` 与 `messaging.send` **同 trace** |
| coupon.exchanged | srv-coupon | job | 同上 |
| points.granted | job 调 srv-coupon gRPC 后由 coupon 服务发出（链路终点事件） | 无 | 消费端 trace 续段 |

Kafka 语义：acks=all；consumer group `job-order-v1` 幂等；失败进 `*.dlq` topic，`job retry` 重放。

## 5. 装配与统一调用链开关

### 每个服务 main.go 装配顺序（11 份逐字同构，顺序为硬约束）

```
1. configx.Init        远程 Nacos 优先、本地 config.yaml 兜底（裁决在 configx）
2. telemetry.Init      读统一 trace 段 → 全局 TracerProvider + OTLP(Jaeger 4317)
   ⚠ 必须早于第 4 步：dbx/cachex/mqx 从全局 TracerProvider 取 tracer，晚装 = noop = 静默断链
3. logx                默认日志；EnableTrace 后日志自动带 trace_id/span_id
4. infra: dbx → cachex(Redis) → mqx(Kafka) → Consul 注册
5. 注册 rpc server + client 桩（按 Consul 服务名寻址）
6. 起 webx（gateway/BFF）或 cobra 命令（job）
7. 优雅退出反序 + OTel flush（BatchSpanProcessor 5s 批超时，已知坑）
```

分层 span 助手与 monolith 同签名：`telemetry.Start(ctx, "service", "order", "CreateOrder")`，层级命名对齐 monolith：`handler.* / service.* / repository.* / middleware.*`；跨进程新增 rpcx client/server span 对，靠 CompositeTextMapPropagator（TraceContext + Baggage）续接——**client 与 server 两侧都要挂传播器**。

### 统一 trace 段（`configs/trace.yaml`，Nacos 单一 dataId，11 服务共读）

```yaml
trace:
  enabled: true       # 总闸：关则 11 服务 0 span（含分层 span）
  exporter: otlp      # otlp / stdout
  endpoint: jaeger:4317
  db: true
  cache: true
  mq: true
```

裁决规则（与 monolith 完全同构，矩阵要证的三点）：

1. 生效值 = `trace.enabled ∧ trace.<kind> ∧ 官方段 enable_trace` —— 三细分开关**正交**，官方段（sqlx/cachex/mqx）为默认、trace 段覆盖
2. 总闸 `enabled=false` 比中间件管得宽：分层 span 一起灭，全服务归零
3. 省略细分键 → 该 kind 取官方段值

各服务唯一开关入口：`cfg.Trace.On(kind)`；**业务/装配代码禁止直读 bool 字段**（漏总闸）。

### gRPC 传播器 / Kafka 续接

- rpcx client/server 两侧挂 `propagation.TraceContext + Baggage`（不配 = 断在 BFF→SRV 第一跳，静默坑）
- Kafka 消费端：`otel.GetTextMapPropagator().Extract(ctx, header carrier)` 重建 ctx；`messaging.receive` span 挂进调用方 trace
- traceparent 头写入受 `Trace.On("mq")` 门控——mq 关时不写头，job 侧退化为自建根 trace（矩阵组合 5 的验证点）

## 6. 验收标准（实测证据，"数出来的 span 才算数"）

**静态**：`go build ./... && go vet ./... && gofmt -l .` 全绿；`go test -race ./...` 通过。

**① 主链路（scripts/e2e-flow.py）**：

- 一条下单请求同 trace_id 出现在 ≥6 个服务（gateway / bff-app / srv-order / srv-user / srv-product / srv-coupon），脚本从 Jaeger API 拉 trace 逐服务列 span 树
- `order.completed` 的 `messaging.send`（srv-order）与 `messaging.receive`（job）同 trace_id，脚本打印 trace_id
- job 续调 srv-coupon gRPC 的 span 挂同一条 trace

**② 分层 span 逐类清点**：固定请求集（users CRUD + 下单 + 报表 + 缓存命中 + Kafka 全流程）下按 `handler.*/service.*/repository.*/rpc client/server/db.*（含 4 表 JOIN SQL span）/缓存 span/messaging.*` 逐类计数，落 `docs/verify/matrix-report.md`。

**③ 全量开关矩阵（scripts/trace-matrix.py，11 服务 × 7 组合）**：

| # | 组合 | 改动 | 期望 |
|---|---|---|---|
| 1 | 全开 | 默认 | 全出 |
| 2 | 总闸关 | `enabled: false` | 0 span（分层也灭） |
| 3 | db 关 | `db: false` | SQL span 0，其余照旧 |
| 4 | cache 关 | `cache: false` | Redis span 0 |
| 5 | mq 关 | `mq: false` | messaging span 0 **且 Kafka 不写 traceparent 头**，job 退化自建根 trace |
| 6 | 省略细分键 | 只留 `enabled` | 细分取官方段默认（全出） |
| 7 | 官方段关 | trace 全开 + `sqlx.enable_trace: false` | SQL span 0（证逻辑与） |

流程：Nacos 程序化改 dataId → bring-up.sh 重启 11 服务（不重起 compose）→ 固定请求集 → 等 flush（≥10s）→ Jaeger API 按 service × span 名前缀计数 → 11×7 报表。

**④ Jaeger 实测**：`/api/services` 出现全部 11 个服务名；矩阵与主证据可人肉复核。

**⑤ 可复跑**：`bring-up.sh + e2e-flow.py + trace-matrix.py` 三件套从零（docker compose up 后）无人值守重放全部结论。

## 7. 已知坑（README 排查清单；实现时逐一绕开，遇新坑补录）

继承 monolith 7 条：cachex tracing 默认 false（切驱动后静默丢 span，须 SetTracingConfig）；GORM Scan 不产 span（用 Find）；企业契约 instances 是 map（逐字段 Get，Unmarshal 静默失败）；本地 viper 通道多词字段缺 mapstructure tag 静默丢值；OTLP 端口看 docker 实际映射；BatchSpanProcessor 5s 批超时（等 5~10s）；configx 不支持 HUBX_TRACE_XXX 环境变量覆盖键。

微服务新增：

| 坑 | 表现 | 对策 |
|---|---|---|
| rpcx 可能无 Consul resolver（**动手前读源码确认**） | 服务发现编不过 | `infra/consul` 自研 resolver 挂 rpcx 扩展点；SDK 已内置则薄封装 |
| 传播器只挂一侧 | 跨进程断在第一跳 | client/server 两侧都挂 CompositeTextMapPropagator |
| Kafka 消费端不续接 | job span 成孤儿 trace | Extract header 重建 ctx |
| mq 关/开时 traceparent 头门控不一致 | 矩阵组合 5 矛盾 | 头写入与 messaging span 同一道 `Trace.On("mq")` 闸 |
| 期望 Nacos 改配置热生效 | "改了没效果" | 本案例**不承诺热更新**（configx 能力先读源码）；脚本按"重启生效"设计并写明 |
| 服务起错序 | 启动期调用全挂 | bring-up.sh 依赖序 + Consul 健康门控 |
| telemetry 顺序写反 | SQL span 静默消失 | main.go 头部模板注释，11 份逐字同构 |

## 8. 交付物

- 11 个可运行服务（apps/）+ pkg/ + infra/（全部开箱可跑：docker compose up + bring-up.sh）
- `configs/trace.yaml`（默认 OTLP 全开）+ `config.stdout.yaml` 变体（exporter: stdout，验证 Jaeger 前的零依赖路径）
- `scripts/{bring-up.sh, e2e-flow.py, trace-matrix.py}`
- `README.md`：拓扑图、装配顺序约束、统一开关设计、JOIN 红线、SDK 使用边界、矩阵结论、Jaeger 接入与排查清单
- `PROMPT.md`：本案例的生成提示词（复刻 monolith 7 节结构，含"先读源码/硬性约束/功能范围/验收/已知坑/交付物/禁止事项"）
- `docs/verify/matrix-report.md`：矩阵实测报表

### 实现前必做（PROMPT 第 1 节"先读源码"）

逐个核对真实 API，禁止凭 kitex/OTel 通用印象：rpcx 的 resolver 扩展点与 client 桩调用、configx 的 Nacos 通道契约（dataId/group）与 trace 段读取入口、mqx Kafka driver 的 header 存取 API、cachex Redis 的 SetTracingConfig 门、dbx Open/OpenNoTrace、otelx 的 OTLP gRPC 导出装配。

## 9. 禁止事项

- 不改 SDK 源码实现功能（确属缺陷：先说明、记录、再最小修改）
- 不留 TODO / 占位 / `panic("not implemented")`
- 无实测 span 证据不得宣称"已入链"
- 验证必须脚本化，不是一次性手工操作
- 数据库私有：不跨库 JOIN、不直连他人 DB；BFF 不直连 DB
- 网关零业务逻辑；srv→srv 不超过 1 层
