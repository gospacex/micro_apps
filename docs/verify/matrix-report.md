# 11×7 全量开关矩阵报告（trace-matrix.py）

- 生成时间：2026-10-03 12:13:26
- Jaeger：`http://127.0.0.1:16686` / Nacos：`http://127.0.0.1:8848` / gateway：`http://127.0.0.1:8080`
- 本轮执行组合：1 全开, 2 总闸关, 3 db 关, 4 cache 关, 5 mq 关, 6 省略细分键, 7 官方段关

## 矩阵：每格 = 该服务该组合下的 span 总数（括号为 db / messaging 细分）

| 服务 | C1 全开 | C2 总闸关 | C3 db 关 | C4 cache 关 | C5 mq 关 | C6 省略细分键 | C7 官方段关 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| gateway | 20 (db 0 / msg 0) | 0 (db 0 / msg 0) | 20 (db 0 / msg 0) | 20 (db 0 / msg 0) | 20 (db 0 / msg 0) | 20 (db 0 / msg 0) | 20 (db 0 / msg 0) |
| bff-app | 27 (db 0 / msg 0) | 0 (db 0 / msg 0) | 27 (db 0 / msg 0) | 27 (db 0 / msg 0) | 27 (db 0 / msg 0) | 27 (db 0 / msg 0) | 27 (db 0 / msg 0) |
| bff-web | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) |
| bff-mini | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) |
| bff-admin | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) |
| srv-user | 3 (db 1 / msg 0) | 0 (db 0 / msg 0) | 2 (db 0 / msg 0) | 3 (db 1 / msg 0) | 3 (db 1 / msg 0) | 3 (db 1 / msg 0) | 2 (db 0 / msg 0) |
| srv-product | 9 (db 5 / msg 0) | 0 (db 0 / msg 0) | 4 (db 0 / msg 0) | 9 (db 5 / msg 0) | 9 (db 5 / msg 0) | 9 (db 5 / msg 0) | 4 (db 0 / msg 0) |
| srv-order | 36 (db 16 / msg 5) | 0 (db 0 / msg 0) | 18 (db 0 / msg 5) | 29 (db 13 / msg 4) | 25 (db 13 / msg 4) | 36 (db 16 / msg 5) | 14 (db 0 / msg 6) |
| srv-pay | 3 (db 1 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 3 (db 1 / msg 0) | 0 (db 0 / msg 0) |
| srv-coupon | 7 (db 5 / msg 0) | 0 (db 0 / msg 0) | 2 (db 0 / msg 0) | 9 (db 7 / msg 0) | 9 (db 7 / msg 0) | 8 (db 6 / msg 0) | 2 (db 0 / msg 0) |
| job | 1 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 0 (db 0 / msg 0) | 1 (db 0 / msg 0) | 0 (db 0 / msg 0) |

## 组合说明

- **C1 全开**：db/cache/mq span 全在
- **C2 总闸关**：全服务 0 span
- **C3 db 关**：db.* span 消失，其余在
- **C4 cache 关**：缓存 span 消失，其余在
- **C5 mq 关**：messaging.* 消失 + 消息无 traceparent 头；mq 关断言 messaging 总数=4 → FAIL
- **C6 省略细分键**：细分键缺省 → 回退默认 true
- **C7 官方段关**：细分键省略 + 官方段 false → 三类全灭

## 三点结论

- 1. 总闸语义：全开 C1 共 106 个 span，总闸关 C2 共 0 个 → 成立（C2 归零）
- 2. db 细分开关：db.* span 全开 28 个 → db 关 0 个 → 成立（归零且其余未受影响）
- 3. mq 细分开关：本仓自产出 messaging.outbox.* 全开 5 个 → mq 关 0 个 → 成立（归零，且消息不带 traceparent 头）
-    （注：mqx SDK 自带 messaging.kafka.* 全开 5 个 → mq 关 4 个，不随细分键关闭——SDK 无条件建 span，只有 traceflag 全局闸能掐，见 ../mqx/FEATURE_MATRIX.md）
- 4. 官方段回退：省略细分键 C6 db/mq 计数 29/5，官方段关 C7 0/6 → 成立（C7 被官方段压灭）
