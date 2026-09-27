# 本地 Docker 环境事实（实施任务 7 起生效，优先级高于计划文档中的镜像描述）

本机 Docker 走华为云镜像仓库，compose 里镜像**必须用完整前缀** `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/<image>`（以下镜像本地已有）：

| 角色 | 镜像（完整名） | 备注 |
|---|---|---|
| MySQL | `.../mysql:8.0` | 挂 init.sql 进 /docker-entrypoint-initdb.d，五库五账号 |
| Kafka | `.../wurstmeister/kafka:2.13-2.8.1` + `.../zookeeper:3.9.2` | **zookeeper 方案（不是 bitnami KRaft）**；KAFKA_ADVERTISED_CLIENT_IP=127.0.0.1；topic 用 KAFKA_CREATE_TOPICS 显式建（order.completed.v1 / coupon.exchanged.v1 / points.granted.v1 + 各自 .dlq） |
| Consul | `.../hashicorp/consul:1.22.4` | `server agent -client 0.0.0.0`，8500(http)/8600(dns) |
| Nacos | `.../nacos/nacos-server:v2.4.2` | SPRING_DATASOURCE_PLATFORM=nacos 单机 derby，8848 |
| Redis | `.../redis:8.4` | 无密码 |
| Jaeger | `.../jaegertracing/all-in-one:1.63.0` | OTLP gRPC 4317 + UI 16686 |

**端口冲突注意**：本机已有多个 6 小时前起的容器（hubx-logx-kafka、hubx-mongo*、hubx-minio 等）占用部分宿主端口。compose 起前自检 3306/9092/2181/8500/8600/8848/6379/4317/16686 的占用情况；冲突就换**宿主侧映射端口**（容器内端口不动），并把实际映射值同步进 `configs/services/*.yaml` 的 DSN/地址，把连通性自检写进 `scripts/bring-up.sh`。

已知在跑的宿主容器（勿 kill）：hubx-logx-kafka / hubx-logx-jaeger / hubx-logx-zookeeper / hubx-mongo* / hubx-minio / hubx-rustfs / hubx-rocketmq-* / chroma-server / vector-admin-db / freellmapi-freellmapi-1。
