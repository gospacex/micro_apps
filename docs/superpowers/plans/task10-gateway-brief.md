# 任务 10-11：gateway（根 span）+ bff-app（BFF 模板）

完成 plan 任务 10、11，做完 commit 结束。srv 侧 5 服务已就绪（本批之前已 commit）。

## 环境
- 所有 go 命令前先 `export PATH=/Users/hyx/work/gowork/go/bin:$PATH`
- 验证：`go build ./... && go vet ./... && gofmt -l .` 全绿 + `go test -race ./...`
- 不起 docker、不跑服务（冒烟留给任务 15 脚本）

## 读（按序）
1. 本文件 + plan 任务 10/11 两节（docs/superpowers/plans/2026-09-27-micro-apps-plan.md）
2. 端口事实源：docs/superpowers/plans/local-docker-env.md（gateway :8080、bff-app :8091、srv 9011~9015）
3. 装配模板：apps/srv-order/cmd/srv-order/main.go（逐字同构，注释同样 6 步 + telemetry 早于 infra 红线）
4. configs/services/gateway.yaml、bff-app.yaml（已 commit，拷贝到 apps/*/config.yaml）
5. 参考：hubx/rpcx/example/client、hubx/webx 示例（路由/响应封装/就绪探针）、pkg/kitex_gen、pkg/meta、pkg/codes、infra/{telemetry,drivers,consul,nacos}

## 已核实 API（直接采用）
- rpcx client：`rpcx.BuildClientOptions(...)` + kitex client 桩（pkg/kitex_gen/<svc> 下 NewClient 签名以桩为准）；寻址 `infra/consul.Target(agentAddr, serviceID)` 拼逗号多实例 Target
- 网关根 span：`telemetry.Start(ctx, "handler", "gateway", "Proxy:"+path)`——**gateway 是整条 trace 根**，下游 span 自动挂其下（obs-opentelemetry suite 自动注入 traceparent）
- 限流：cachex `Set NX EX` 计数 key `rl:{ip}:{route}`，`On("cache")` 门控（读不到缓存放行）
- webx 统一响应封装 + 就绪探针：按 monolith/main.go 的 webx 用法（读 ../monolith/main.go 核对真实 API，别凭印象）

## gateway（apps/gateway/，自包含目录）
- internal/auth：golang-jwt/jwt/v5 HS256 验一次（密钥读 config.yaml jwt 段，configs/services/gateway.yaml 有则用之、没有就补上并同步 configs 侧两份），解出 user_id/roles 写 `pkg/meta.HeaderUserID` 等 Header 透传下游，不再二次验签
- internal/ratelimit：全局限流（缓存计数器 + On("cache") 门控）
- internal/routing：路由表 path→目标服务名，**配置驱动**（config.yaml routes 段），灰度头 `pkg/meta.HeaderGray` 透传
- internal/proxy：HTTP→gRPC 转发；根 span 在 gateway 诞生（telemetry.Start 见上）
- main.go：装配同 srv-order 模板（gateway 无 DB：infra 只装缓存）；consul.Register("127.0.0.1:8500","gateway","127.0.0.1:8080")

## bff-app（apps/bff-app/，BFF 模板）
- internal/aggregate：errgroup 并发调多 srv（kitex_gen client + consul.Target 寻址）+ 降级兜底（某 srv 挂返回 sentinel 默认值）
- internal/cache：`bff:app:{resource}:{user_id}` 展示层缓存，TTL 60s，cachex redis，On("cache") 门控；命中直接返回不打 srv
- internal/dto：按端字段裁剪
- internal/handler：webx `NewServiceWithResponse` 统一响应 + 就绪探针（API 以 monolith 实际用法为准）
- main.go：装配同模板（BFF 无 DB，infra 只装缓存）；consul.Register(...,"bff-app","127.0.0.1:8091")

## 纪律
不许 TODO/panic；错误 %w；2 个 commit：① `feat: gateway (root span/jwt/ratelimit/routing)` ② `feat: bff-app template (errgroup aggregate/degrade/cache)`；尾部 Co-Authored-By 行。

## 报告
2 个 commit hash+文件清单、build/vet/fmt/test 摘要、偏差（webx API/rpcx client 构造实际写法）、留给任务 12（其余 BFF）与 13（job）的调用约定。
