# bff-app 全链路冒烟记录（2026-10-02）

> 产出阶段：sddflow build。用于 Task 11（BFF 模板）/ Task 12（其余 3 个 BFF）的验收证据，
> 以及 webx 入站 Extract 补丁是否生效的判据。脚本化验收（e2e-flow.py / trace-matrix.py）
> 落地前，本文件是唯一的多进程实测记录。

## 1. 环境

- docker 六件套全 healthy（mysql 3306 / redis 6379 / kafka 19093 / consul 8500 / nacos 8848 / jaeger 4317-4318 + UI 16686）
- 五库五账号已建，种子数据经 `pkg/seedsql/{user_db,product_db}.sql` 灌入（users 46 行、products 1 行）
- Nacos 已灌入 `hubx.trace` + `hubx.<svc>` ×11（group HUBX），回读逐字一致
- 起进程：`srv-user`（:9011）、`srv-product`（:9012）、`bff-app`（:8091）；**gateway / srv-order / srv-coupon 未起**

## 2. 请求

```
GET http://127.0.0.1:8091/home
X-User-Id: 1
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

响应 `HTTP 200`：

```json
{"code":0,"message":"ok","data":{
  "profile":{"user_id":1,"username":"e2e_9e612c96","level":1},
  "degraded":["coupons","recent_orders"],
  "cache_hit":false},
 "request_id":"4bf92f3577b34da6a3ce929d0e0e4736"}
```

## 3. Jaeger 证据（trace_id = 4bf92f3577b34da6a3ce929d0e0e4736）

| 服务 | span | parent | 关键 tag |
|---|---|---|---|
| bff-app | `GET /home` | `<ROOT>` | http.route=/home |
| bff-app | `service.bff-app.Home` | `GET /home` | cache.hit=false |
| bff-app | `service.display.get` | `service.bff-app.Home` | cache.hit=false |
| bff-app | `service.bff-app.Home.profile` | `service.bff-app.Home` | — |
| bff-app | `service.bff-app.Home.recent` | `service.bff-app.Home` | degraded=true |
| bff-app | `service.bff-app.Home.coupons` | `service.bff-app.Home` | degraded=true |
| srv-user | `service.user.GetProfile` | **`<ROOT>`（父级断，见第 5 节）** | — |

Jaeger `/api/services` 共 14 个服务名，含本案例 11 个可部署单元
（gateway、bff-app/web/mini/admin、srv-user/product/order/pay/coupon、job）。

## 4. 结论（成立的部分）

1. **webx 入站 Extract 补丁生效** —— 入站 `traceparent` 被 webx `Otel` 中间件采纳，
   bff-app 根 span 的 trace_id = 注入值，且回写 `X-Request-Id` 与响应体 `request_id` 一致。
   补丁前该 trace 必然在 HTTP 入站处断开。
2. **分层 span 命名与层级符合设计** —— handler → service → {分支 span、展示层缓存 span}，
   `service.display.get` 带 `cache.hit` tag，命中/未命中可分。
3. **降级兜底生效** —— 未起的 `srv-coupon` / `srv-order` 两个分支返回默认值并打 `degraded=true`，
   主接口仍 200（不 500）。
4. **跨进程同 trace 成立** —— srv-user 的 `service.user.GetProfile` 与 bff-app 同属
   trace_id `4bf92f35...`，说明 RPC 侧传播器把 traceparent 传到了下游。

## 5. 待修缺陷：srv 侧 span 父级断链

- 现象：`srv-user` 的 `service.user.GetProfile` 虽同 trace_id，但 parent 解析为 `<ROOT>`，
  bff-app 侧也**没有**出现 RPC client span。
- 推论：bff-app 的 kitex client 侧未上报 span（obs-opentelemetry client suite 未挂，或 span 未 End），
  导致下游 span 的 parent span id 指向一个不存在的 span。
- 影响：span 树在 bff→srv 边界断开，Jaeger 上呈现为两棵子树；不阻断"同 trace_id"验收，
  但阻断"逐 span 父子核对"（Task 20）与"≥6 服务 span 树"证据。
- 下一步：核对 `pkg/rpcx/options.go` 的 `BuildClientOptions` 是否在 trace 开时挂
  `tracing.NewClientSuite()`，并确认 bff-app `internal/client` 的 client span 有 End。

## 6. 本次冒烟未覆盖

- gateway（:8080）未起 —— `handler.gateway.Proxy` 根 span 与 JWT/限流链路未实测
- srv-order / srv-coupon 未起 —— outbox、Kafka、4 表 JOIN 未实测
- job 未实现 —— messaging send/receive 同 trace 未实测
- 统一 trace 开关矩阵（11×7）未跑 —— 依赖 `scripts/trace-matrix.py`（Task 17）
