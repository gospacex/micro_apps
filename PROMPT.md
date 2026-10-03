# PROMPT.md —— 改这个仓库时必须遵守的规格契约

> 给 AI（也给人）的硬约束清单。动手前先读，违反任何一条都会造成"编译得过、
> 跑得起来、但链路断了/数据越界"的隐性故障。每条都写清了**为什么**，
> 因为不理解原因的约束会被绕过。

---

## 0. 先读这三份，再动手

- `README.md` —— 架构、端口表、跑法、已知限制
- `docs/superpowers/plans/local-docker-env.md` —— **端口与依赖的唯一事实源**
- `docs/superpowers/plans/2026-09-27-micro-apps-plan.md` —— 21 个任务的验收项

不确定就标「待核」，**禁止编造 API 名、端口、目录**。

---

## 1. 装配顺序红线（每个 `cmd/<svc>/main.go` 都要逐字同构 6 步）

```
1. infra/nacos.LoadUnified(ctx, "config.yaml", "hubx.<svc>", "HUBX")
2. infra/telemetry.Init        ← 必须先于第 3 步
3. infra/drivers.*（缓存 / MQ / DB）+ telemetry.InstallGlobal()
4. 业务装配（kitex client + handler/service/repository）+ Consul 注册
5. webx.New / kitex server：路由 + /healthz/ready
6. signal 优雅退出（反序，OTel flush ≥10s）
```

**必须**：

- telemetry 早于任何 infra 装配。cachex / dbx 的埋点从**全局 TracerProvider** 取 tracer，
  先装配它们则其 span 全落 noop。
- 驱动装配完成后调 `telemetry.InstallGlobal()`。cachex 与 dbx 开启 trace 时会各自
  `otel.SetTracerProvider()` 注册自己的 provider（端点取 SDK 默认 `localhost:4318` + TLS），
  把装配层装好的 provider 顶掉。被顶掉的后果：**RPC client span 建得出、注入得出
  traceparent，却永不导出**——下游同 trace_id、父 span id 指向不存在的 span。
- 头部注释照抄模板（`apps/srv-order/cmd/srv-order/main.go`），把 6 步与红线写进去。
- 新增服务照模板同构展开，不要自创装配形状。

---

## 2. 开关入口红线

**业务代码一律经 `cfg.Trace.On("db" / "cache" / "mq")` 读开关，禁止直读 bool 字段。**

理由：细分开关最终值 = `trace.<kind>` 显式键 ∨ 官方段 `enable_trace` ∨ 默认 true，
三段正交裁决封装在 `pkg/config`。直读 bool 会漏掉总闸与官方段回退。

统一 trace 段走 Nacos 双 dataId：`hubx.trace`（单一事实源）+ `hubx.<svc>`（私有）。
**不要另造配置读取路径**，一律 `infra/nacos.LoadUnified` + `pkg/config.ApplyTrace`。

---

## 3. 服务边界（越界即设计失败）

| 层 | 可以有 | 禁止有 |
|---|---|---|
| gateway | JWT 验签、限流、路由、出站注入 | 业务逻辑、DB |
| BFF ×4 | errgroup 并发聚合、降级兜底、展示层缓存 | DB、MQ、跨端共享代码 |
| srv ×5 | 各自独占一个库的读写、事务 | 跨库 JOIN、跨库事务、调别的 srv 写接口 |
| job | Kafka 消费、DLQ 重放、定时任务、gRPC 只读 | 监听端口、直连业务库 |

- **禁止跨库 JOIN / 跨库事务**。4 表 JOIN（order 报表）只发生在 `order_db` 内。
  跨域数据一律走 gRPC 只读接口，对账在 job 内存比对。
- **4 个 BFF 独立目录、无共享业务代码**（允许约 80% 重复）。跨端 import 是禁止的——
  BFF 的价值就在"按端裁剪"，抽公共层会把四端拖回同一个形状。
- job 不起监听端口，只做 cobra 子命令：`consume` / `retry` / `schedule`。

---

## 4. SDK 边界

**禁止改动仓库外的 8 个 SDK 目录**：`../webx ../rpcx ../configx ../dbx ../cachex
../mqx ../otelx ../logx`（以及 `../traceflag`）。

**唯一特许**：`../webx/middleware/otel.go` 已加入站 `propagator.Extract`。
原实现只认 `X-Request-Id`、不续 `traceparent`，且 `WithMiddleware` 排在默认链之后，
仓内中间件抢不到前面——不改 SDK 则 gateway→BFF 必断。已改，勿回退。
除此之外再动任何 SDK 文件，必须先说明理由并写进 `README.md` 第 7 节。

**依赖**：不要 `go mod tidy`（会与其他并行改动冲突）。确实缺依赖就单独
`go get <pkg>` 并单独 commit。

---

## 5. 调用链埋点约定

span 命名沿用分层约定，Jaeger 上一眼能看出"谁调谁"：

```
handler.*    入站 HTTP
service.*    业务分层
repository.* 数据访问
client.<svc>.<method>   出站 RPC（pkg/rpcx 产出）
db.query     SQL
messaging.*  Kafka 生产/消费
task.*       job 定时任务
```

- 新增出站 RPC 不要手写 span，`rpcx.BuildClientOptions` 已挂 `ClientSpanMiddleware`。
- 消费侧从消息头 Extract `traceparent` 续链，键与生产侧同源（`pkg/events.TraceHeaderKey`）。
- mq 开关关时不写头，job 侧退化为自建根 trace——这是矩阵组合 5 的预期语义，不是 bug。

---

## 6. 测试与提交

- **TDD**：先写失败测试 → 跑一次看到 FAIL → 再写实现 → 转绿。不许跳过"先看到 FAIL"。
- 每个逻辑子任务单独 commit，`git add` 只加自己改的文件。message 用中文，
  正文写清**为什么**（尤其是绕过某个坑或做了某个裁决）。
- 提交前必跑：
  ```bash
  export PATH=/Users/hyx/work/gowork/go/bin:$PATH
  go build ./... && go vet ./... && gofmt -l . && go test ./...
  ```
- 若某个包失败但不在你负责范围内：**只报告，不要顺手改**（可能是别人在并行改）。

---

## 7. 本地环境事实（命令找不到就看这里）

- Go 不在默认 PATH：`export PATH=/Users/hyx/work/gowork/go/bin:$PATH`
- docker 不在默认 PATH：`export PATH=/usr/local/bin:$PATH`
- 服务**必须从 `apps/<svc>` 目录启动**，否则读不到 `config.yaml`（静默走 fallback）
- 服务进程需要 `HUBX_DB_PW=hubx123`
- 宿主有 HTTP 代理：curl 加 `--noproxy '*'`，python 用 `ProxyHandler({})`
- 六件套端口：MySQL 3306 / Redis 6379 / Kafka 19093 / Consul 8500 / Nacos 8848 /
  Jaeger 4317-4318 + UI 16686
- **不要 `docker compose down`**（容器是共用的）

---

## 8. 改完自查

- [ ] 装配 6 步顺序没变，telemetry 仍在 infra 之前
- [ ] 驱动装配后有 `telemetry.InstallGlobal()`
- [ ] 开关全走 `Trace.On(kind)`，没有直读 bool
- [ ] 没有跨库 JOIN、没有跨端 BFF import
- [ ] 没有改 SDK（webx 特许项除外）
- [ ] 没有动 go.mod / go.sum（或已单独 commit 说明）
- [ ] build / vet / gofmt / test 全绿，commit 已拆分子任务
