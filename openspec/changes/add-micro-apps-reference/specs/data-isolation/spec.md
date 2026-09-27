## ADDED Requirements

### Requirement: 一库一 srv 一账号
数据 SHALL 按领域拆为五个库：user_db（srv-user）、product_db（srv-product）、order_db（srv-order）、pay_db（srv-pay）、coupon_db（srv-coupon）；每库使用独立账号授权，服务只连接自己的库。

#### Scenario: 账号隔离
- **WHEN** 以 srv-order 的账号尝试连接 user_db
- **THEN** 连接被拒绝（无该库权限）

### Requirement: JOIN 限定本库
跨表查询 SHALL 只发生在本 srv 私有库内；SHALL NOT 存在跨库 JOIN 或跨库事务。已知 JOIN 落点：srv-user 用户详情（2 表）、srv-product 商品详情（3 表）、srv-order 订单详情（2 表）、srv-order 报表（4 表，矩阵锚点）。

#### Scenario: 4 表 JOIN 锚点
- **WHEN** 调用 srv-order 的 Report 接口
- **THEN** 单次查询在 order_db 内完成 orders ⋈ order_items ⋈ order_status_log ⋈ refund_records 四表 JOIN，产出一条 4 表 SQL span，且不访问其他库

#### Scenario: JOIN 用 Find 产出 SQL span
- **WHEN** 执行任一 JOIN 查询
- **THEN** 使用 GORM `Find`（非 `Scan`），db.* span 可被计数

### Requirement: 跨域数据走 gRPC 只读接口
需要他人数据时 SHALL 调用对方 srv 的 gRPC 只读接口（如对账：srv-order/srv-pay 各提供 ListOrders/ListPayFlows 流水），SHALL NOT 直连他人数据库。

#### Scenario: 对账不跨库
- **WHEN** job schedule 执行 order vs pay 对账
- **THEN** 经 gRPC 拉取两侧只读流水后在 job 内存比对，job 与两侧 srv 均无跨库 SQL

### Requirement: 库内跨实体事务与跨域协作分离
单 srv 内的多实体写入 SHALL 用本库事务（如 srv-order 订单+明细）；跨库写入协作 SHALL 拆为"本地事务 + gRPC 调用 + 失败回滚本地事务"（如扣库存调 srv-product 的 ReserveStock，不发起跨库事务）。

#### Scenario: 扣库存跨域协作
- **WHEN** CreateOrder 扣减商品库存
- **THEN** 事务只写 order_db，库存扣减经 srv-product gRPC 完成，gRPC 失败则回滚本地订单事务
