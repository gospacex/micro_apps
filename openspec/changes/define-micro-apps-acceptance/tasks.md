## 1. 验收标准文档

- [ ] 1.1 编写 `docs/verify/ACCEPTANCE.md`：三层门禁规则（P0/P1/P2）与六类判定表，每项含级别 / 判定命令 / 通过阈值 / 证据落点 / 现状五字段
- [ ] 1.2 在同一文档编写例外登记表（现象 / 根因 / 影响范围 / 处置四段式）与收尾裁决规则、可复跑说明

## 2. 证据补采

- [ ] 2.1 执行 `go test -race ./...` 并将结果落入 `docs/verify/`
- [ ] 2.2 采集 Jaeger `/api/services` 11 服务名清单证据

## 3. 现状判定

- [ ] 3.1 产出 `docs/verify/acceptance-status.md`：对照标准逐条判定，现状取 PASS / FAIL / EXEMPT / NOT-RUN
- [ ] 3.2 产出 P0 未过项补齐清单，按「先补脚本覆盖面 → 再重跑证据 → 最后复核文档」的依赖顺序排列

## 4. 脚本覆盖补齐

- [ ] 4.1 `scripts/e2e-flow.py` 增加 `/api/web`、`/api/mini`、`/api/admin` 三端路径，使 bff-web / bff-mini / bff-admin 有 span 证据
- [ ] 4.2 `scripts/e2e-flow.py` 增加 srv-pay 与 job 路径覆盖
- [ ] 4.3 `scripts/e2e-flow.py` 触发 Kafka 事件链，断言 `messaging.send` 与 `messaging.receive` 同 trace_id 且 job 续调挂同 trace
- [ ] 4.4 `scripts/trace-matrix.py` 将 messaging 计数拆为本仓 `messaging.outbox.*` 与 SDK `messaging.kafka.*` 两行并列输出

## 5. 复跑与收尾裁决

- [ ] 5.1 重跑三件套刷新全部证据，同步刷新 `acceptance-status.md` 现状列与补齐清单
- [ ] 5.2 复核 P0 项是否全部 PASS / EXEMPT，给出收尾裁决结论
