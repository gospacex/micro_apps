# syntax=docker/dockerfile:1
# ============================================================
# hubx/micro_apps 服务容器化路径（可选；默认用 scripts/bring-up.sh 本地 go run）
#
# go.mod 的 replace 指向 ../{rpcx,configx,logx,dbx,cachex,mqx,otelx,webx} 兄弟
# 目录，因此构建上下文必须是 hubx/ 仓库根，而不是 micro_apps/：
#
#   cd ..                                        # hubx/ 仓库根
#   docker build -f micro_apps/docker/Dockerfile \
#     --build-arg SVC=srv-order -t hubx-ma/srv-order .
#
# SVC 取 apps/ 下的可部署单元名（gateway / bff-app / bff-web / bff-mini /
# bff-admin / srv-user / srv-product / srv-order / srv-pay / srv-coupon / job）。
#
# 运行示例（宿主网络，直连 compose 暴露的 3306/19093/8500/8848/6379/4317）：
#   docker run --rm --network host \
#     -e HUBX_DB_PW=hubx123 -e HUBX_CONFIG=config.yaml \
#     -v $(pwd)/micro_apps/configs:/configs \
#     hubx-ma/srv-order
#
# 镜像统一走本机华为云前缀（与 docker/compose.yaml 同一约定）。
# ============================================================

# ---------- 构建阶段 ----------
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/golang:1.27-bookworm AS build
ARG SVC=srv-order

WORKDIR /src

# go.mod 的 replace 目标（SDK 本地目录）先拷，供 go mod download 解析
COPY webx/    ./webx/
COPY configx/ ./configx/
COPY logx/    ./logx/
COPY dbx/     ./dbx/
COPY cachex/  ./cachex/
COPY mqx/     ./mqx/
COPY otelx/   ./otelx/
COPY rpcx/    ./rpcx/

# 再拷模块定义预热依赖，最后拷业务源码（利用层缓存）
COPY micro_apps/go.mod micro_apps/go.sum ./micro_apps/
WORKDIR /src/micro_apps
RUN go mod download

COPY micro_apps/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /out/service ./apps/${SVC}/cmd/${SVC}

# ---------- 运行阶段 ----------
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 svc
COPY --from=build /out/service /service
USER svc
# 配置经 HUBX_CONFIG / HUBX_NACOS_SERVER / HUBX_DB_PW 等环境变量注入
ENTRYPOINT ["/service"]
