# hubx micro_apps —— 11 服务微服务参考实现（教学案例）

一套**能跑起来、能在 Jaeger 上看见全链路**的 Go 微服务参考仓。它不是框架，是一份
"照着做就能复现"的样板：网关 → BFF → 领域服务 → 异步任务，Kitex RPC + Hertz/HTTP +
OTel 全链路 trace + Nacos 统一开关。

- 11 个可部署单元：gateway ×1、BFF ×4、srv ×5、job ×1
- 数据私有：5 个 MySQL 库各归一个 srv，跨域只读走 gRPC，**禁止跨库 JOIN**
- 统一调用链：一个 `hubx.trace` dataId 管 11 个服务的开关，Jaeger 同 trace 可查
- 脚本化验收：`bring-up.sh` / `e2e-flow.py` / `trace-matrix.py` 三条命令复跑

---

## 1. 架构

```
                      ┌──────────────────────────────────────┐
   客户端 ──HTTP──▶   │ gateway :8080  (Hertz)               │
                      │  JWT 验签 · 限流 · 路由 · 出站注入     │
                      └───────────────┬──────────────────────┘
                                      │  /api/{app,web,mini,admin}  去前缀转发
              ┌───────────┬───────────┼───────────┬────────────┐
              ▼           ▼           ▼           ▼            │
        bff-app:8091 bff-web:8092 bff-mini:8093 bff-admin:8094 │
        (移动端)      (PC/SEO)     (小程序)      (运营后台)      │
              │           │           │           │            │
              │  errgroup 并发聚合 + 降级兜底 + 展示层缓存(redis) │
              └───────────┴───────────┴───────────┘            │
                          │ Kitex RPC                          │
        ┌─────────┬───────┴────┬─────────┬─────────┐           │
        ▼         ▼            ▼         ▼         ▼           │
   srv-user   srv-product  srv-order  srv-pay  srv-coupon      │
    :9011       :9012        :9013     :9014     :9015          │
    user_db   product_db   order_db   pay_db    coupon_db       │
        │                      │                               │
        │                      └── Kafka ──▶ job（无端口）────────┘
        │                                   消费 · DLQ 重放 · 定时任务
        └── gRPC 只读接口（job 对账用，job 不直连业务库）

  依赖：MySQL 3306 · Redis 6379 · Kafka 19093 · Consul 8500 · Nacos 8848 · Jaeger 4317/4318 + UI 16686
```

**端口唯一事实源**：`docs/superpowers/plans/local-docker-env.md`。改端口先改它，再改各
`apps/*/config.yaml`、Consul 注册与三个脚本，不要各起炉灶。

| 服务 | 端口 | 类型 | 数据 |
|---|---|---|---|
| gateway | :8080 | HTTP 入口 | 无 DB，只挂限流缓存 |
| bff-app / web / mini / admin | :8091~:8094 | HTTP→RPC 聚合 | 无 DB，展示层缓存 |
| srv-user / product / order / pay / coupon | :9011~:9015 | Kitex RPC | 各独占一库 |
| job | 无 | cobra CLI | 不直连业务库，走 gRPC 只读 |

---

## 2. 目录

```
apps/<svc>/cmd/<svc>/main.go   装配入口（6 步，顺序有约束）
apps/<svc>/config.yaml          服务私有配置（= configs/services/<svc>.yaml）
apps/<svc>/internal/{handler,service,client,cache,repository,rpc}/
pkg/rpcx/       kitex client/server option 装配 + RPC client span 中间件
pkg/config/     统一 trace 开关裁决 Trace.On(kind)
pkg/kitex_gen/  kitex 生成代码
pkg/events/     消息载荷 + traceparent 头键（生产/消费同源）
infra/telemetry/ OTel 装配 + 分层 span 助手
infra/nacos/    双 dataId 加载（hubx.trace + hubx.<svc>）
infra/drivers/  cachex / mqx 工厂薄封装
infra/consul/   服务注册
configs/        12 份配置 + seed-nacos.sh
scripts/        bring-up.sh / e2e-flow.py / trace-matrix.py
docs/verify/    验收证据（冒烟、e2e、矩阵）
```

四个 BFF **独立目录、无共享业务代码**（允许约 80% 重复）。这是刻意的：BFF 的价值就在
"按端裁剪"，抽公共层会把四端拖回同一个形状。跨端 import 是被禁止的。

---

## 3. 跑起来

### 3.1 起依赖（docker 六件套）

```bash
cd docker && docker compose up -d && docker compose ps   # 六个全 healthy
```

> macOS 上 `docker` 常不在默认 PATH：用 `/usr/local/bin/docker` 或 `export PATH=/usr/local/bin:$PATH`。

### 3.2 灌配置与种子数据

```bash
bash configs/seed-nacos.sh        # hubx.trace + hubx.<svc> ×11，group HUBX
                                  # 回读：curl 'http://127.0.0.1:8848/nacos/v1/cs/configs?dataId=hubx.trace&group=HUBX'
```

种子数据（可选，冒烟/e2e 需要）：

```bash
for db in user_db product_db order_db pay_db coupon_db; do
  docker exec -i hubx-ma-mysql mysql -uroot -prootpass $db < pkg/seedsql/$db.sql
done
```

### 3.3 起 11 个服务

```bash
export PATH=/Users/hyx/work/gowork/go/bin:$PATH   # go 常不在默认 PATH
export HUBX_DB_PW=hubx123
bash scripts/bring-up.sh up          # 按依赖序：srv×5 → bff×4 → gateway → job
bash scripts/bring-up.sh status      # 一眼看出谁活着谁挂了
bash scripts/bring-up.sh --help      # up/down/status/restart + 过滤
```

两个**踩过的坑**，脚本已经处理，手动起服务时务必照做：

1. **必须从 `apps/<svc>` 目录启动**。本地 `config.yaml` 是相对路径读取的，从仓库根
   启动会静默走 fallback，排障时极难定位。
2. **服务进程需要 `HUBX_DB_PW=hubx123`**（与 `docker/mysql/init.sql` 一致）。

### 3.4 验证

```bash
python3 scripts/e2e-flow.py --no-up --min-services 5   # 打请求 + 拉 Jaeger + 落证据
python3 scripts/trace-matrix.py --combos 1,2,5         # 开关矩阵（全跑 7 轮约 20~40 min）
```

产物：`docs/verify/e2e-evidence.md`、`docs/verify/matrix-report.md`。

> 宿主有 HTTP 代理时访问 127.0.0.1 会被代理劫持：curl 必须带 `--noproxy '*'`，
> urllib 必须装 `ProxyHandler({})`。两个脚本内部已处理。

---

## 4. 配置体系：Nacos 双 dataId

| dataId | 内容 | 谁读 |
|---|---|---|
| `hubx.trace` | 统一 trace 段（enabled / exporter / endpoint / db / cache / mq） | 11 个服务共读，**单一事实源** |
| `hubx.<svc>` | 服务私有段（DB / Redis / Kafka / 端口 / 路由） | 该服务自己 |

裁决在 `pkg/config.ApplyTrace`：`hubx.trace` 显式含 `trace` 段则以它为准，否则回退私有段，
都没有则保持 `Default()`。细分开关 `db/cache/mq` 省略时回退各中间件**官方段**
（`xmiddleware.sqlx.databases.<inst>.enable_trace` 等）。

**业务代码只准经 `cfg.Trace.On("db"/"cache"/"mq")` 读开关，禁止直读 bool 字段**——
否则会漏掉总闸。

不用 Jaeger 也能验证（零依赖路径）：

```bash
curl -X POST 'http://127.0.0.1:8848/nacos/v1/cs/configs' \
  --data-urlencode 'dataId=hubx.trace' --data-urlencode 'group=HUBX' \
  --data-urlencode content@configs/trace.stdout.yaml
bash scripts/bring-up.sh up --reload     # span 直接打进程 stdout，终端逐行数
```

建议**两步走**：先 stdout 验证"span 命名/父子/分层对不对"，再切回
`configs/trace.yaml`（`exporter: otlp`）走 Jaeger 做可视化验收。

---

## 5. 装配红线（改代码前必读）

每个 `cmd/<svc>/main.go` 头部都写了这 6 步，顺序本身有约束：

1. `infra/nacos.LoadUnified(ctx, "config.yaml", "hubx.<svc>", "HUBX")`
2. `infra/telemetry.Init`（**必须早于第 3 步**：cachex/dbx 的埋点从全局 TracerProvider 取 tracer）
3. `infra/drivers.*`（缓存 / MQ / DB）
4. 业务装配（kitex client + 分层 handler/service/repository）+ Consul 注册
5. `webx.New` / kitex server：挂路由与就绪探针
6. signal 优雅退出（反序，OTel flush 给 ≥10s）

三条硬约束：

- **telemetry 必须先于 infra**，否则 span 全落到 noop provider。
- **开关一律走 `Trace.On(kind)`**，禁止直读 bool。
- **驱动装配完必须 `telemetry.InstallGlobal()`**。理由见第 7 节已知限制第 2 条——
  这是本项目最难查的一个坑。

---

## 6. trace 怎么在 Jaeger 上看

打开 <http://127.0.0.1:16686>，选服务 → Find Traces。一条主链路应看到类似：

```
gateway     GET /api/app/home                  ← 根
bff-app     GET /home                          ← 父 = gateway（webx 入站 Extract）
bff-app     service.bff-app.Home
bff-app     service.bff-app.Home.profile
bff-app     client.User.GetProfile             ← pkg/rpcx 产出的 RPC client span
srv-user    user.User/GetProfile               ← 父 = client.User.GetProfile
srv-user    service.user.GetProfile
srv-user    db.query
```

分层命名约定：`handler.*` / `service.*` / `repository.*` / `client.<svc>.<method>` /
`db.query` / `messaging.*` / `task.*`。Jaeger 上一眼能看出"谁调谁"。

---

## 7. 已知限制与特许改动

按"读者会不会被坑"排序，都如实列出：

1. **唯一特许 SDK 改动**：`../webx/middleware/otel.go` 加了入站 `propagator.Extract`。
   原实现只认 `X-Request-Id` 做 trace-id 种子，不续 `traceparent`；且 `webx.WithMiddleware`
   排在默认链之后，仓内中间件无法抢在它前面 Extract——不改 SDK 则 gateway→BFF **必断**。
   补丁只在存在 traceparent 时改变行为，无头时原逻辑逐字不变。除此之外 8 个 SDK 目录零改动。

2. **SDK 会抢走全局 TracerProvider（已修，勿回退）**：cachex（`NewCache`）与 dbx（`openDB`）
   在开启 trace 时各自 `otel.SetTracerProvider()` 注册**它们自己**的 provider，端点取 SDK
   默认 `localhost:4318` + TLS，把装配层装好的 otelx provider 静默顶掉。后果极隐蔽：
   业务分层 span 照常落地，但所有走 `otel.GetTracerProvider()` 的组件（RPC client span、
   kitex 套件）建得出 span、注入得出 traceparent，却永远导不出去——下游同 trace_id、
   父 span id 指向一个不存在的 span，Jaeger 上断成两棵子树。
   对策：`telemetry.InstallGlobal()` 在驱动装配后夺回全局，已落在 `drivers.NewCache`
   与 5 个 srv 的 `openDB` 里，并有回归测试 `TestInstallGlobalRecoversFromSDKOverride`。

3. **cachex 自带 provider 的端点不可配**：它会按默认端点发一批 span 并失败，进程日志里
   会出现一条 `traces export: Post "https://localhost:4318/..."`。这是 SDK 侧限制，
   不影响本仓的缓存 span（`service.display.get` 由本仓自产出，同 trace 正常落地）。

4. **mqx SDK 自带 `messaging.kafka.*` span 是无条件创建的**：`EnableTrace` /
   `CallChainEnabled` 对它不生效（见 `../mqx/FEATURE_MATRIX.md`），只有 traceflag 全局闸
   能掐。因此矩阵里「mq 关」那格的 messaging 计数不为 0——真正受 `Trace.On("mq")` 门控
   的是本仓自产出的 `messaging.outbox.send`，它归零。两者在报表里分桶记录。

5. **job 的 Kafka 端到端（send→receive 同 trace）尚未在真实环境跑通**：单测已覆盖
   （`consumer_test.go` / `replay_test.go` 用内存 exporter 断言续链），但 job 的
   `consume` 子命令需要 topic 就绪 + 消费组，本轮未纳入矩阵请求集。

6. **跨库 JOIN 禁止**：4 表 JOIN（srv-order 报表）只发生在 order_db 内。跨域数据一律走
   gRPC 只读接口，对账在 job 内存比对。

7. **Nacos 配置在进程里是"点分键"，不是嵌套段（已修，勿回退）**：hubx/configx 的
   Nacos provider 把 YAML **展平**成 `trace.enabled` / `trace.db` 这类点分键，
   段级 `Unmarshal("trace", &t)` 恒 key not found。症状极具迷惑性：`hubx.trace`
   看着是单一事实源，实则**从不生效**——7 组合开关矩阵逐格同值、总闸关那一轮照样
   55 个 span 就是这个坑。对策：`pkg/config` 改逐键 `GetString/GetFloat64` 覆盖
   （`hasTrace` / `applyTraceFields`），`TraceSource` 接口不再需要 `Unmarshal`。

8. **producer 的 Writer 不能自带 topic（已修，勿回退）**：mqx 的 kafka producer
   把 `Writer.Topic` 取成 `cfg.Consumer.Topic`（SDK 侧 producer/consumer 共用
   consumer 段的 topic，未配 consumer 段时还拿 `producer.topic` 兜底），而
   kafka-go 的 Writer 一旦自带 Topic，`Message.Topic` 再非空就直接报
   `Topic must not be specified for both Writer and Message`。症状同样隐蔽：
   调用链日志照打 "call chain: kafka send"、send span 照常产出，实际一条都没发
   出去——outbox 行静默重试到 `retry_count=10` 后被放弃（`status=2`），Kafka 里
   空空如也、下游 job 永不消费。对策：`drivers.NewProducer` 里显式
   `cfg.Consumer.Topic = ""`，topic 一律由业务侧在 `Message.Topic` 上逐条指定
   （多 topic 场景必需：outbox 行自带 topic / job 的 DLQ 回发 / coupon 的
   points.granted.v1）。

9. **起服务的进程会被调用方回收，起停与请求必须在同一次调用内完成**：
   `bring-up.sh` 起的进程在启动它的 shell / 脚本退出后随进程组被回收。分成两次
   调用（先 up、再 curl）只会得到 `Connection refused`。这就是 `e2e-flow.py` /
   `trace-matrix.py` 都在脚本内部自己调 `bring-up.sh` 的原因。

10. **Jaeger 的 `/api/services` 会混入非应用服务名**：`cachex`（SDK 自带 provider
    的 service.name，见第 3 条）、`jaeger-all-in-one`（Jaeger 自身）、以及外部
    注入 traceparent 时冒出来的外部服务名（如 `e2e-helper`）。11 个应用服务名
    之外多出这几个是预期的，不要当成"多起了一个服务"。

11. **`/api/traces?service=X` 返回的是整条 trace，不是"该服务的 span"**：按 service
    过滤后仍会拿到同 trace 的兄弟 span。按服务计数必须自己按
    `processes[pid].serviceName` 归属（`trace-matrix.py` 的 `count_spans`）。

12. **容器内连不上 Kafka**：compose 里 Kafka 的 advertised listener 指向宿主端口
    （容器内 9092、宿主 19093），所以 `docker exec ... kafka-topics.sh` 一律
    `Node may not be available`。要探 topic 得从宿主侧连 `127.0.0.1:19093`。

## 8. 排查清单

| 现象 | 根因 | 对策 |
|---|---|---|
| Jaeger 上只有 gateway，BFF 另起一条 trace | 入站未 Extract | 见第 7 节第 1 条（webx 补丁） |
| 下游同 trace_id，父 span 指向不存在的 span | 全局 provider 被 SDK 顶掉 | `telemetry.InstallGlobal()`（第 7 节第 2 条） |
| 一个 span 都没有 | 总闸或采样 | 查 Nacos `hubx.trace` 的 `enabled`；`sample_ratio` 缺省按 1 |
| 服务起了但连不上 DB | 工作目录不对 / 缺口令 | 从 `apps/<svc>` 启动；`export HUBX_DB_PW=hubx123` |
| curl 127.0.0.1 超时 | 宿主代理劫持 | `curl --noproxy '*'` |
| 改了 Nacos 配置没生效 | 进程内不热更新 | `bash scripts/bring-up.sh up --reload`（drivers 刻意不重建连接池） |
| span 树缺最后几个 | 没等 flush | BatchSpanProcessor 5s 一批，脚本默认等 14s |

---

## 9. 验收状态

| 项 | 状态 |
|---|---|
| `go build ./...` / `go vet ./...` / `gofmt -l .` / `go test ./...` | 全绿 |
| bff-app 多进程冒烟（同 trace + 分层 span + 降级） | 通过，见 `docs/verify/bff-app-smoke.md` |
| 主链路 e2e（≥N 服务同 trace） | 见 `docs/verify/e2e-evidence.md` |
| 11×7 开关矩阵 | 见 `docs/verify/matrix-report.md` |
| job（Kafka 续链 / DLQ 重放 / 定时任务） | 已实装，端到端待补 Kafkor topic 验证 |
