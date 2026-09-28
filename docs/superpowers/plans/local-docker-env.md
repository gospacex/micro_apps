# 本地 Docker 环境事实（实施任务 7 起生效，优先级高于计划文档中的镜像描述）

本机 Docker 走华为云镜像仓库，compose 里镜像**必须用完整前缀** `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/<image>`（镜像以本地已有为准，bitnami/kafka:3.9.0 与 redis:7 首次 `up` 时经此前缀拉取）：

| 角色 | 镜像（完整名） | 备注 |
|---|---|---|
| MySQL | `.../mysql:8.0` | 挂 init.sql 进 /docker-entrypoint-initdb.d，五库五账号 |
| Kafka | `.../bitnami/kafka:3.9.0` | **bitnami KRaft 单节点（无 zookeeper 容器/依赖）**；KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19093；KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true 自动建 topic（order.completed.v1 / coupon.exchanged.v1 / points.granted.v1 + 各自 .dlq） |
| Consul | `.../hashicorp/consul:1.22.4` | `server agent -client 0.0.0.0`，8500(http)/8600(dns) |
| Nacos | `.../nacos/nacos-server:v2.4.2` | SPRING_DATASOURCE_PLATFORM=nacos 单机 derby，8848 |
| Redis | `.../redis:7` | 无密码（compose 按计划用 redis:7；本机原有 redis:8.4 可作回退） |
| Jaeger | `.../jaegertracing/all-in-one:1.63.0` | OTLP gRPC 4317 + UI 16686 |

**最终端口裁决（2026-09-28 lsof 实测）：**
- 宿主 free：3306 / 8500 / 8600 / 8848 / 6379 / 4317 / 14317 / 16686
- 宿主 BUSY：13306 / 9092 / 19092 / 18848 / 16379 / 12181（2181 的宿主映射）/ 29092
- **Kafka 宿主映射用 19093→9092**；用 bitnami KRaft（**无 zookeeper 容器/依赖**——宿主 2181 被既有 hubx-logx-zookeeper 占着，但 KRaft 方案不需要 zk，与本 compose 无关）
- MySQL 3306 / Consul 8500+8600 / Nacos 8848 / Redis 6379 / Jaeger 4317+16686 均直接用默认端口
- `configs/services/*.yaml` 里所有地址写宿主侧值：MySQL 127.0.0.1:3306、Kafka 127.0.0.1:19093、Consul 127.0.0.1:8500、Redis 127.0.0.1:6379、Nacos 127.0.0.1:8848、Jaeger OTLP 127.0.0.1:4317


已知在跑的宿主容器（勿 kill）：hubx-logx-kafka / hubx-logx-jaeger / hubx-logx-zookeeper / hubx-mongo* / hubx-minio / hubx-rustfs / hubx-rocketmq-* / chroma-server / vector-admin-db / freellmapi-freellmapi-1。
