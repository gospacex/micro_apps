# Tasks — add-micro-apps-reference

## 1. 基础设施与共享层

- [ ] 1.1 仓库脚手架：单 go.mod（module hubx/micro_apps，replace 本地 SDK ×8）+ `go mod tidy` + 11 个 apps 目录占位，`go build ./...` 通过（对应 Task 1）
- [ ] 1.2 pkg 数据层：meta Header 常量、codes 统一错误码、events 三事件体（含 SchemaVer/traceparent 常量）、seedsql 五库 SQL、proto 五服务定义，`go vet ./pkg/...` 通过（对应 Task 2）
- [ ] 1.3 生成 kitex_gen RPC 桩（提交入库），`go build ./pkg/kitex_gen/...` 通过（对应 Task 3）
- [ ] 1.4 infra/telemetry：移植 monolith 分层 span 助手 + Init 顺序 + 总闸 noop 行为，stdout exporter 单测红→绿（对应 Task 4）
- [ ] 1.5 pkg/config `Trace.On(kind)` 三段正交封装 + infra/nacos loader + infra/drivers 薄封装，7 组合表驱动单测红→绿（对应 Task 5）
- [ ] 1.6 infra/consul：自研 rpcx resolver（Resolve + Register/Deregister），httptest 模拟 KV 单测红→绿（对应 Task 6）
- [ ] 1.7 docker 底座 + configs：compose 六件套（mysql 五库/kafka/consul/nacos/redis/jaeger）、init.sql、11 份服务配置、trace.yaml、seed-nacos.sh，`docker compose ps` 全绿（对应 Task 7）

## 2. 服务实现

- [ ] 2.1 srv-order 模板服务：装配顺序 main + 库内事务/跨域 gRPC 协作 repository + gRPC handler + outbox 事件 + 限流/身份中间件，手动冒烟 SQL span 可数（对应 Task 8）
- [ ] 2.2 其余 4 个 srv（user/product/pay/coupon）照模板展开，各库 JOIN 落点与核心方法冒烟通过（对应 Task 9）
- [ ] 2.3 gateway：JWT 一次验签透传 + cachex 限流 + 配置路由 + 根 span 转发，全链路 curl 冒烟通过（对应 Task 10）
- [ ] 2.4 BFF 模板 bff-app：errgroup 并发聚合 + 降级兜底 + 展示层缓存 + 统一响应，缓存命中/未命中两种 span 冒烟通过（对应 Task 11）
- [ ] 2.5 其余 3 个 BFF（web/mini/admin）按端独立展开（无共享业务代码），4 个 BFF 聚合接口冒烟通过（对应 Task 12）
- [ ] 2.6 job：cobra 三命令 + Kafka 消费 traceparent 续接 + DLQ 重放 + gocron 调度（对账走 gRPC），send/receive 同 trace 冒烟通过（对应 Task 13）

## 3. 文档与验收脚本

- [ ] 3.1 README + PROMPT：拓扑图、装配顺序、开关设计、JOIN 红线、SDK 边界、排查清单（现象/根因/对策）、7 节结构 PROMPT（对应 Task 14）
- [ ] 3.2 scripts/bring-up.sh：依赖序启动 11 服务 + `--services`/`--reload` 过滤，从零启动全绿（对应 Task 15）
- [ ] 3.3 scripts/e2e-flow.py：主链路 ≥6 服务同 trace + messaging 同 trace 断言与 span 树输出（对应 Task 16）
- [ ] 3.4 scripts/trace-matrix.py：11×7 全量矩阵 + 组合 5 无 traceparent 头断言 + matrix-report.md 落盘（对应 Task 17）
- [ ] 3.5 config.stdout.yaml 零依赖验证路径：exporter stdout 下终端可逐行数 span（对应 Task 18）

## 4. 验证与收尾

- [ ] 4.1 `go test -race ./...` 全绿：service 层 SQLite 单测、BFF 降级测试、job traceparent 续接测试补齐（对应 Task 19）
- [ ] 4.2 Jaeger 人肉复核：/api/services 11 服务名齐全、3 条抽查 trace 逐 span 核对、新坑补录 README（对应 Task 20）
- [ ] 4.3 git 收尾 + 三件套从零无人值守复跑：gofmt/vet 全绿、清仓、复放全部验收结论；禁令扫描通过（无 TODO/占位、8 个 SDK 目录零改动、README 入链结论均附实测证据）（对应 Task 21）
