## ADDED Requirements

### Requirement: 服务拓扑分层
系统 SHALL 由 11 个可部署单元组成，按流量走向分四层：gateway（1）→ BFF 层（bff-app、bff-web、bff-mini、bff-admin 共 4）→ SRV 层（srv-user、srv-product、srv-order、srv-pay、srv-coupon 共 5）→ job（cobra CLI，含 consume/retry/schedule 三命令），存储层为 MySQL 五库、Redis、Kafka。

#### Scenario: 拓扑完整性
- **WHEN** 执行 `scripts/bring-up.sh` 按依赖序启动全部服务
- **THEN** 11 个服务全部就绪，Jaeger `/api/services` 出现全部 11 个服务名

### Requirement: 调用深度红线
系统 SHALL 限制调用深度最深为 3 层：gateway → BFF → SRV（2 层 gRPC），srv-order → srv-user/srv-product/srv-coupon（+1 层）。SRV 之间 SHALL NOT 存在二级互调（srv-user 不再调用其他 srv）；跨域异步协作 SHALL 通过 Kafka 事件完成。

#### Scenario: 无二级互调
- **WHEN** 审查 5 个 srv 的 rpc 客户端依赖
- **THEN** 仅 srv-order 持有对其他 srv 的 client 桩，其余 srv 不发起 srv→srv 调用

### Requirement: 网关零业务
gateway SHALL 只承担：JWT 验签一次并透传身份 Header（X-User-Id 等）、全局限流、路由转发、根 span 创建。gateway SHALL NOT 包含业务逻辑、SHALL NOT 直连数据库。

#### Scenario: 身份一次验签透传
- **WHEN** 客户端携带 JWT 请求经 gateway 转发至 BFF
- **THEN** gateway 完成验签并写入身份 Header，BFF/SRV 不再二次验签，gateway 进程无 DB 连接

### Requirement: BFF 按端独立
4 个 BFF SHALL 各自拥有独立目录（cmd/internal/config.yaml），SHALL NOT 共享业务代码；DTO、聚合、路由差异按端隔离，允许重复实现。

#### Scenario: 无共享业务代码
- **WHEN** 检查 `apps/bff-app`、`apps/bff-web`、`apps/bff-mini`、`apps/bff-admin` 的 import 关系
- **THEN** 四者之间不存在互相 import，也不依赖共享的业务包（仅允许依赖 `pkg/` 与 `infra/`）

### Requirement: 共享边界收敛
跨服务共享 SHALL 仅限 `pkg/`（proto 桩、事件体、错误码、身份 Header 常量等纯数据）与 `infra/`（纯装配助手）；BFF SHALL NOT 直连数据库。

#### Scenario: BFF 无 DB 依赖
- **WHEN** 检查 4 个 BFF 的依赖与配置
- **THEN** BFF 不包含 xmiddleware.sqlx 数据库配置、不 import dbx
