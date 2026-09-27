## ADDED Requirements

### Requirement: 静态质量门
仓库 SHALL 满足 `go build ./... && go vet ./... && gofmt -l .` 全绿（gofmt 无输出），且 `go test -race ./...` 通过。

#### Scenario: 静态检查全绿
- **WHEN** 执行静态检查命令集
- **THEN** build/vet 退出码为 0，gofmt 输出为空，race 测试全部通过

### Requirement: 主链路跨进程 trace 证据
系统 SHALL 提供 `scripts/e2e-flow.py`：打一条完整下单链路请求并触发 Kafka 事件与 job 消费，等待 flush（≥10s）后从 Jaeger API 拉取 trace，断言同一 trace_id 出现在 ≥6 个服务（gateway、bff-app、srv-order、srv-user、srv-product、srv-coupon），且 `messaging.send` 与 `messaging.receive` 同 trace_id、job 续调 srv-coupon 的 span 挂同 trace；脚本 SHALL 打印该 trace_id 与逐服务 span 树。

#### Scenario: 六服务同 trace
- **WHEN** Jaeger 就绪且 11 服务运行时执行 `python3 scripts/e2e-flow.py`
- **THEN** 脚本断言通过并输出主 trace_id 与 span 树，任何断言失败则非零退出

### Requirement: 分层 span 逐类清点
在固定请求集（users CRUD + 下单 + 报表 + 缓存命中 + Kafka 全流程）下，系统 SHALL 按 handler/service/repository/middleware/rpc client/server/db（含 4 表 JOIN SQL span）/缓存/messaging 逐类计数，并落盘 `docs/verify/matrix-report.md`。

#### Scenario: 报表含逐类计数
- **WHEN** 矩阵脚本完成一轮执行
- **THEN** `docs/verify/matrix-report.md` 含服务 × span 类别的计数表与 4 表 JOIN SQL span 记录

### Requirement: Jaeger 服务注册完整性
Jaeger `/api/services` SHALL 返回全部 11 个服务名，且矩阵与主链路证据可供人肉在 Jaeger UI 复核。

#### Scenario: 服务名齐全
- **WHEN** 11 服务全部运行且 flush 完成后查询 `/api/services`
- **THEN** 返回列表包含全部 11 个服务名，无缺失

### Requirement: 三件套从零可复跑
`docker compose up` 后，`scripts/bring-up.sh`、`scripts/e2e-flow.py`、`scripts/trace-matrix.py` 三件套 SHALL 支持无人值守重放全部验收结论（含配置灌入）。

#### Scenario: 无人值守复跑
- **WHEN** 从干净环境依次执行 compose up → seed-nacos → bring-up.sh → e2e-flow.py → trace-matrix.py
- **THEN** 全流程无需人工干预完成，产出与上次一致的结论

### Requirement: 交付物清单完整
变更 SHALL 交付：11 个可运行服务（apps/）与 pkg/、infra/；`configs/trace.yaml`（OTLP 全开默认）与 `configs/trace.stdout.yaml`（stdout 零依赖变体）；`scripts/{bring-up.sh, e2e-flow.py, trace-matrix.py}`；`docker/` 底座（compose、mysql/init.sql）；`README.md`、`PROMPT.md`；验收后产出 `docs/verify/matrix-report.md`。

#### Scenario: 交付物齐备
- **WHEN** 对照交付清单逐项检查仓库
- **THEN** 上述文件/目录全部存在且非空，矩阵报表由脚本生成而非手写

### Requirement: 文档交付
SHALL 交付 `README.md`（拓扑图、装配顺序约束、统一开关设计、JOIN 红线、SDK 使用边界、矩阵结论、Jaeger 接入与排查清单——含继承 monolith 7 条与本案例新增坑，逐条写现象/根因/对策）与 `PROMPT.md`（复刻 monolith 7 节结构的案例生成提示词，含先读源码清单）。

#### Scenario: 排查清单可定位坑
- **WHEN** 开发者按 README 排查清单逐条核对
- **THEN** 每条已知坑均有现象、根因、对策三段，且与实现中的规避措施一一对应
