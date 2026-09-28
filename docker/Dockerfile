# hubx/micro_apps 多 app 共用 Dockerfile
#
# 用法（go.mod 的 replace 指向 ../rpcx、../webx 等 SDK 目录，构建上下文必须在 hubx/ 根）：
#   cd hubx && docker build -f micro_apps/docker/Dockerfile -t hubx-ma:gateway --build-arg app=gateway .
#   cd hubx && docker build -f micro_apps/docker/Dockerfile -t hubx-ma:srv-order --build-arg app=srv-order .
#
# 说明：
#   - builder 阶段编译 apps/<app>/cmd/<app>（11 个 app：gateway、bff-app、bff-web、
#     bff-mini、bff-admin、srv-user、srv-product、srv-order、srv-pay、srv-coupon、job）
#   - 运行时镜像只带二进制（配置走 Nacos hubx.trace / 本地 configs，不进镜像）
#   - 端口定死值（见 configs/services/<svc>.yaml）：gateway 8080、bff-app 8081、
#     bff-web 8082、bff-mini 8083、bff-admin 8084、srv-user 9001、srv-product 9002、
#     srv-order 9003、srv-pay 9004、srv-coupon 9005、job 无监听（cobra 命令）

ARG app

# ---------- builder ----------
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/golang:1.24-bookworm AS builder
ARG app
WORKDIR /src/micro_apps

# 先拷 go.mod/go.sum + 本地 SDK（replace 目标）预拉依赖（缓存层）
COPY micro_apps/go.mod micro_apps/go.sum ./
COPY cachex/ ./cachex/
COPY configx/ ./configx/
COPY logx/ ./logx/
COPY dbx/ ./dbx/
COPY mqx/ ./mqx/
COPY otelx/ ./otelx/
COPY webx/ ./webx/
COPY rpcx/ ./rpcx/
RUN go mod download

# 再拷业务源码（build 期 apps/ 尚未实现全 11 个时，可单独拷目标 app 目录）
COPY micro_apps/ .
RUN if [ -n "$app" ]; then \
      go build -o /out/app ./apps/${app}/cmd/${app}; \
    fi

# ---------- runtime ----------
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/alpine:3.20
ARG app
LABEL hubx.app=${app}
RUN apk add --no-cache ca-certificates tzdata \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=builder /out/app /usr/local/bin/app
# 运行时挂载/生成 config.yaml（HUBX_CONFIG 指定）；默认按 11 服务同构装配读本地 + Nacos
ENTRYPOINT ["/usr/local/bin/app"]
