# 主链路跨进程 trace 证据（e2e-flow.py）

- 生成时间：2026-10-03 12:15:07
- gateway：`http://127.0.0.1:8080` / Jaeger：`http://127.0.0.1:16686`
- trace_id：`97c2dface4540831a25b80f8c78ad81e`（注入 traceparent `00-97c2dface4540831a25b80f8c78ad81e-00f067aa0ba902b7-01`）
- 同 trace 服务数：**6**（门槛 6）→ **PASS**
- span 总数：70；孤儿 span（父不在本 trace）：5
- messaging 续链：（本轮未触发 Kafka，标记 SKIP）

## 请求与响应

| 方法 | 路径 | HTTP | 响应摘要 |
| --- | --- | --- | --- |
| GET | `/api/app/home` | 200 | {"code":0,"message":"ok","data":{"profile":{"user_id":1,"username":"e2e_9e612c96 |
| GET | `/api/app/users/profile` | 200 | {"code":0,"message":"ok","data":{"user_id":1,"username":"e2e_9e612c96","level":1 |
| GET | `/api/app/products` | 200 | {"code":0,"message":"ok","data":{"user_id":1,"products":[{"product_id":1,"title" |
| GET | `/api/app/coupons` | 200 | {"code":0,"message":"ok","data":{"user_id":1},"request_id":"97c2dface4540831a25b |
| GET | `/api/app/orders/report` | 200 | {"code":0,"message":"ok","data":{"user_id":1,"rows":[{"order_no":"O2026100218554 |

## span 树

| 服务 | span | 父 span |
| --- | --- | --- |
| gateway | `GET /api/app/home` | `<MISSING 00f067aa>` |
| gateway | `middleware.gateway.ratelimit` | `GET /api/app/home` |
| gateway | `middleware.gateway.auth` | `GET /api/app/home` |
| gateway | `handler.gateway.Proxy:/api/app` | `GET /api/app/home` |
| bff-app | `GET /home` | `handler.gateway.Proxy:/api/app` |
| bff-app | `service.bff-app.Home` | `GET /home` |
| bff-app | `service.display.get` | `service.bff-app.Home` |
| bff-app | `service.bff-app.Home.profile` | `service.bff-app.Home` |
| bff-app | `service.bff-app.Home.recent` | `service.bff-app.Home` |
| bff-app | `service.bff-app.Home.coupons` | `service.bff-app.Home` |
| bff-app | `order.Order/ListOrders` | `service.bff-app.Home.recent` |
| bff-app | `coupon.Coupon/ListCoupons` | `service.bff-app.Home.coupons` |
| bff-app | `user.User/GetProfile` | `service.bff-app.Home.profile` |
| bff-app | `client.Order.ListOrders` | `service.bff-app.Home.recent` |
| bff-app | `client.User.GetProfile` | `service.bff-app.Home.profile` |
| bff-app | `client.Coupon.ListCoupons` | `service.bff-app.Home.coupons` |
| srv-order | `order.Order/ListOrders` | `client.Order.ListOrders` |
| srv-coupon | `coupon.Coupon/ListCoupons` | `client.Coupon.ListCoupons` |
| srv-user | `user.User/GetProfile` | `client.User.GetProfile` |
| srv-coupon | `service.coupon.ListCoupons` | `coupon.Coupon/ListCoupons` |
| srv-order | `service.order.ListOrders` | `order.Order/ListOrders` |
| srv-user | `service.user.GetProfile` | `user.User/GetProfile` |
| srv-coupon | `db.query` | `service.coupon.ListCoupons` |
| srv-user | `db.query` | `service.user.GetProfile` |
| srv-order | `db.query` | `service.order.ListOrders` |
| gateway | `GET /api/app/users/profile` | `<MISSING 00f067aa>` |
| gateway | `middleware.gateway.ratelimit` | `GET /api/app/users/profile` |
| gateway | `middleware.gateway.auth` | `GET /api/app/users/profile` |
| gateway | `handler.gateway.Proxy:/api/app` | `GET /api/app/users/profile` |
| bff-app | `GET /users/profile` | `handler.gateway.Proxy:/api/app` |
| bff-app | `service.bff-app.Profile` | `GET /users/profile` |
| bff-app | `user.User/GetProfile` | `service.bff-app.Profile` |
| bff-app | `client.User.GetProfile` | `service.bff-app.Profile` |
| srv-user | `user.User/GetProfile` | `client.User.GetProfile` |
| srv-user | `service.user.GetProfile` | `user.User/GetProfile` |
| srv-user | `db.query` | `service.user.GetProfile` |
| gateway | `GET /api/app/products` | `<MISSING 00f067aa>` |
| gateway | `middleware.gateway.ratelimit` | `GET /api/app/products` |
| gateway | `middleware.gateway.auth` | `GET /api/app/products` |
| gateway | `handler.gateway.Proxy:/api/app` | `GET /api/app/products` |
| bff-app | `GET /products` | `handler.gateway.Proxy:/api/app` |
| bff-app | `service.bff-app.ListProducts` | `GET /products` |
| bff-app | `product.Product/ListProducts` | `service.bff-app.ListProducts` |
| bff-app | `client.Product.ListProducts` | `service.bff-app.ListProducts` |
| srv-product | `product.Product/ListProducts` | `client.Product.ListProducts` |
| srv-product | `service.product.ListProducts` | `product.Product/ListProducts` |
| srv-product | `db.query` | `service.product.ListProducts` |
| srv-product | `db.query` | `service.product.ListProducts` |
| gateway | `GET /api/app/coupons` | `<MISSING 00f067aa>` |
| gateway | `middleware.gateway.ratelimit` | `GET /api/app/coupons` |
| gateway | `middleware.gateway.auth` | `GET /api/app/coupons` |
| gateway | `handler.gateway.Proxy:/api/app` | `GET /api/app/coupons` |
| bff-app | `GET /coupons` | `handler.gateway.Proxy:/api/app` |
| bff-app | `service.bff-app.Coupons` | `GET /coupons` |
| bff-app | `coupon.Coupon/ListCoupons` | `service.bff-app.Coupons` |
| bff-app | `client.Coupon.ListCoupons` | `service.bff-app.Coupons` |
| srv-coupon | `coupon.Coupon/ListCoupons` | `client.Coupon.ListCoupons` |
| srv-coupon | `service.coupon.ListCoupons` | `coupon.Coupon/ListCoupons` |
| srv-coupon | `db.query` | `service.coupon.ListCoupons` |
| gateway | `GET /api/app/orders/report` | `<MISSING 00f067aa>` |
| gateway | `middleware.gateway.ratelimit` | `GET /api/app/orders/report` |
| gateway | `middleware.gateway.auth` | `GET /api/app/orders/report` |
| gateway | `handler.gateway.Proxy:/api/app` | `GET /api/app/orders/report` |
| bff-app | `GET /orders/report` | `handler.gateway.Proxy:/api/app` |
| bff-app | `service.bff-app.Report` | `GET /orders/report` |
| bff-app | `order.Order/Report` | `service.bff-app.Report` |
| bff-app | `client.Order.Report` | `service.bff-app.Report` |
| srv-order | `order.Order/Report` | `client.Order.Report` |
| srv-order | `service.order.Report` | `order.Order/Report` |
| srv-order | `db.query` | `service.order.Report` |

## 参与服务

- bff-app
- gateway
- srv-coupon
- srv-order
- srv-product
- srv-user
