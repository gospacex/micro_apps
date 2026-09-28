# 任务 8 批 1：5 个 srv 服务（自包含目录，srv-order 当模板先行）

实现 apps/srv-user、srv-product、srv-order、srv-pay、srv-coupon。做完 commit 结束。

## 环境
- 所有 go 命令前先 `export PATH=/Users/hyx/work/gowork/go/bin:$PATH`（默认 go 是 claudegate 包装器会挂起）
- 验证：`go build ./... && go vet ./... && gofmt -l .` 全绿 + `go test -race ./...`
- 不起 docker、不跑服务

## 读（按序）
1. 本文件 + plan 的 0.1 节（已核实 API 修订，最高优先级）
2. configs/services/srv-*.yaml（端口/库/账号/实例名已定死）
3. 模板参考：hubx/rpcx/example/server/main.go、hubx/rpcx/rpcx/rpcx.go、hubx/rpcx/tracing/tracing.go
4. 本仓已有：infra/telemetry、infra/nacos、infra/drivers、infra/consul、pkg/config、pkg/kitex_gen/{user,product,order,pay,coupon}、pkg/events、pkg/codes、pkg/meta、pkg/seedsql
5. monolith：main.go、config/、internal/infra/infra.go（Trace.On 门控写法）

## 每服务结构（目录自包含，禁止跨 app import）
apps/srv-<x>/
- cmd/srv-<x>/main.go（头部注释写装配总公式 6 步 + telemetry 早于 infra 红线）
- internal/{service,repository,rpc,middleware}/
- config.yaml（= configs/services/srv-<x>.yaml 拷贝）

main.go 装配（逐字同构模板）：
1. infra/nacos.Load（本地 config.yaml 兜底）→ pkg/config 各段 + Trace 封装
2. rpcx/configx.MustLoad 本地 yaml + rpcx/tracing.Init(Options{ServiceName:"srv-x", Endpoint:"127.0.0.1:4317", Insecure:true})
   ⚠ 先于第 3 步：全局 TracerProvider 就绪后 BuildServerOptions 才挂 obs-opentelemetry suite
3. infra/drivers：NewCache（On("cache") 门控）、NewProducer/NewConsumer（On("mq") 门控）、dbx.Open（经 LoadDBConfigFromConfigx，On("db") 门控）
4. consul.Register("127.0.0.1:8500","srv-x","127.0.0.1:90xx")
5. svc.NewServer(impl, rpcx.BuildServerOptions(rpcx.ServerOptions{ServiceName,Addr:":90xx"})...) 起
6. signal 优雅退出：svr.Stop() + tracing 返回的 shutdown（注释：OTel flush 批超时 ≥10s 已知坑）

## srv-order（模板）
- repository：GetOrder（orders⋈order_items，2 表 JOIN，**Find 禁 Scan**）；Report（orders⋈order_items⋈order_status_log⋈refund_records，4 表 JOIN Find）；CreateOrderTx（事务：orders+order_items+outbox，outbox 表在 order_db）；ListOrders（只读流水，对账用）
- service.CreateOrder：本地事务 + rpcx client 调 srv-product.ReserveStock（Target=infra/consul.Resolve 拼）失败→回滚；成功→事务写 outbox；后台 3s ticker 补发（outbox.sent 标记幂等）
- 发 OrderCompleted（pkg/events）：mqx.Header 写 traceparent——仅当 On("mq") 为真（值=otel.GetTextMapPropagator().Inject 到 W3C TextMap carrier）
- rpc：实现 kitex_gen/order 桩 4 方法，每方法 telemetry.Start(ctx,"service","order","<Method>") + pkg/codes 错误码
- middleware：cachex 自保护限流（On("cache") 门控，读不到缓存放行）+ metadata 读 X-User-Id 校验（pkg/meta）

## 其余 4 个
- srv-user：Register/Login（golang-jwt/jwt/v5 HS256，密钥读 config；Login 写 auth_sessions）/GetProfile（users⋈user_profiles 2 表 JOIN Find）/CheckUser
- srv-product：GetProduct（products⋈categories⋈inventories 3 表 JOIN Find）+ product:hot:{id} TTL 30s 缓存；ReserveStock（FOR UPDATE 行锁扣 inventories.stock/locked_stock，事务）；ListProducts
- srv-pay：Pay（pay_orders 插入+state 事务）/Callback（pay_callbacks 幂等：pay_order_id 唯一约束）/ListPayFlows
- srv-coupon：Consume（user_coupons 唯一约束幂等，used_order_id 回填）/GrantPoints（points_log + 发 PointsGranted，outbox 模式同 srv-order；**outbox 表加到 coupon_db：pkg/seedsql/coupon_db.sql 与 docker/mysql/init.sql 两处同步改**）/ListCoupons

## 纪律
不许 TODO/panic；GORM JOIN 一律 Find；错误 %w 包装；分 2 commit：① srv-order ② 其余 4 个（含 coupon outbox 表 DDL 同步）。

## 报告
① 2 个 commit hash+文件清单 ② build/vet/test 摘要 ③ 偏差（mqx.Header 构造/consul.Resolve 拼 Target/outbox ticker 实际写法）④ 留给批 2/3 的调用约定：各 srv client 构造签名 + config.yaml 位置 + coupon outbox 表结构。

## 批 2 修订（2026-09-28，覆盖上文冲突处）
**进度：** srv-order 的 repository 层已完成（`apps/srv-order/internal/repository/order.go`，未提交，随批 2 提交①入库）；`tools/tools.go`、`go.mod/go.sum` 已修好（gorm 1.31.2）。
**端口（唯一事实源 local-docker-env.md，本文旧写法作废）：** srv-user **:9011**、srv-product **:9012**、srv-order **:9013**、srv-pay **:9014**、srv-coupon **:9015**（9001 被宿主 hubx-minio 占用，整段上移）。consul Register 的 hostPort 与 rpcx ServerOptions.Addr 一律用 901x。
**已核实 API（前批实测，直接采用，不必重查）：**
1. mqx 没有 Header 类型；traceparent 走 `Message.Headers map[string]string`（key 名自定，如 "traceparent"）
2. consul 拼 rpcx client Target 用 `infra/consul.Target(agentAddr, serviceID) (string, error)`（逗号分隔多实例）；`Resolve` 返回 `[]string`
3. infra/drivers 实际签名：`NewCache(ctx, h configx.Config, name, driver string, traceOn bool)`；`NewProducer(ctx, h, name, traceOn bool)`；`NewConsumer(ctx, h, name, group, traceOn bool)`
4. infra/telemetry：`Start(ctx, layer, entity, op string) (context.Context, trace.Span)`
5. pkg/config：`Trace.On(kind)`（kind ∈ db/cache/mq）+ `(*Trace).Resolve(s Switches, instance string)` + `SQLXTraceKey/CacheXTraceKey/MQXCallChainKey`
6. kitex 无 metadata 透传约定——X-User-Id 由 handler 内读 `req.GetUserId()` 校验（pkg/meta 常量），不是中间件从 metadata 取
7. dbx 的 Open 驱动参是 "mysql"（xmiddleware 段 driver: gorm 是契约值，别混用）
