// Package client bff-web 的 kitex 下游客户端装配（srv-user / srv-product /
// srv-order / srv-coupon）。
//
// 目标地址经 infra/consul.Target 读注册中心 KV（hubx/svc/<svc>）；续链由
// rpcx.BuildClientOptions 挂的 obs-opentelemetry ClientSuite 完成（traceparent
// 经 kitex metainfo 自动注入，无需手动）。
//
// 降级语义：任一目标未注册 → 对应客户端为 nil，包装方法直接返回不可用错误，
// service 层据此走降级兜底，不阻断其余分支。
//
// 分层埋点：每个下游调用额外包一层 client.<srv>.<Method> span（在 kitex
// obs 客户端套件产生的 RPC span 之外，给出 BFF 视角的下游调用耗时）。
package client

import (
	"context"
	"fmt"

	"github.com/gospacex/logx"

	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/telemetry"
	"hubx/micro_apps/pkg/rpcx"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	couponcli "hubx/micro_apps/pkg/kitex_gen/coupon/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	ordercli "hubx/micro_apps/pkg/kitex_gen/order/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	productcli "hubx/micro_apps/pkg/kitex_gen/product/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
	usercli "hubx/micro_apps/pkg/kitex_gen/user/user"
)

// Clients 四个下游 srv 客户端集合（nil 字段表示该下游未注册，调用方须降级）。
type Clients struct {
	User    usercli.Client
	Product productcli.Client
	Order   ordercli.Client
	Coupon  couponcli.Client
}

// New 装配四个客户端；单个失败仅告警，整体不返回 error。
func New(self, consulAgent string, traceOn bool, logger logx.Logger) *Clients {
	c := &Clients{}
	if cli, err := dial(consulAgent, "srv-user", func(target string) (any, error) {
		return usercli.NewClient("User", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-web: srv-user unavailable (degraded)", logx.Err(err))
	} else {
		c.User = cli.(usercli.Client)
	}
	if cli, err := dial(consulAgent, "srv-product", func(target string) (any, error) {
		return productcli.NewClient("Product", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-web: srv-product unavailable (degraded)", logx.Err(err))
	} else {
		c.Product = cli.(productcli.Client)
	}
	if cli, err := dial(consulAgent, "srv-order", func(target string) (any, error) {
		return ordercli.NewClient("Order", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-web: srv-order unavailable (degraded)", logx.Err(err))
	} else {
		c.Order = cli.(ordercli.Client)
	}
	if cli, err := dial(consulAgent, "srv-coupon", func(target string) (any, error) {
		return couponcli.NewClient("Coupon", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-web: srv-coupon unavailable (degraded)", logx.Err(err))
	} else {
		c.Coupon = cli.(couponcli.Client)
	}
	return c
}

// dial 解析目标并建连；错误向上返回，由调用方决定降级策略。
func dial(consulAgent, service string, factory func(target string) (any, error)) (any, error) {
	target, err := consul.Target(consulAgent, service)
	if err != nil {
		return nil, err
	}
	return factory(target)
}

// call 统一的下游调用壳：客户端为 nil → 不可用错误（service 层据此降级）；
// 否则包一层 client.<srv>.<Method> span 后发起调用。
func call[R any](ctx context.Context, svc, method string, available bool, invoke func(context.Context) (R, error)) (R, error) {
	var zero R
	if !available {
		return zero, fmt.Errorf("%s unavailable", svc)
	}
	ctx, span := telemetry.Start(ctx, "client", svc, method)
	defer span.End()
	resp, err := invoke(ctx)
	if err != nil {
		span.RecordError(err)
	}
	return resp, err
}

func (c *Clients) available() bool { return c != nil }

// GetProfile 调 srv-user GetProfile。
func (c *Clients) GetProfile(ctx context.Context, req *userpb.GetProfileReq) (*userpb.GetProfileResp, error) {
	return call(ctx, "srv-user", "GetProfile", c.available() && c.User != nil, func(ctx context.Context) (*userpb.GetProfileResp, error) {
		return c.User.GetProfile(ctx, req)
	})
}

// Register 调 srv-user Register。
func (c *Clients) Register(ctx context.Context, req *userpb.RegisterReq) (*userpb.RegisterResp, error) {
	return call(ctx, "srv-user", "Register", c.available() && c.User != nil, func(ctx context.Context) (*userpb.RegisterResp, error) {
		return c.User.Register(ctx, req)
	})
}

// Login 调 srv-user Login。
func (c *Clients) Login(ctx context.Context, req *userpb.LoginReq) (*userpb.LoginResp, error) {
	return call(ctx, "srv-user", "Login", c.available() && c.User != nil, func(ctx context.Context) (*userpb.LoginResp, error) {
		return c.User.Login(ctx, req)
	})
}

// ListProducts 调 srv-product ListProducts。
func (c *Clients) ListProducts(ctx context.Context, req *productpb.ListProductsReq) (*productpb.ListProductsResp, error) {
	return call(ctx, "srv-product", "ListProducts", c.available() && c.Product != nil, func(ctx context.Context) (*productpb.ListProductsResp, error) {
		return c.Product.ListProducts(ctx, req)
	})
}

// GetProduct 调 srv-product GetProduct。
func (c *Clients) GetProduct(ctx context.Context, req *productpb.GetProductReq) (*productpb.GetProductResp, error) {
	return call(ctx, "srv-product", "GetProduct", c.available() && c.Product != nil, func(ctx context.Context) (*productpb.GetProductResp, error) {
		return c.Product.GetProduct(ctx, req)
	})
}

// ListOrders 调 srv-order ListOrders。
func (c *Clients) ListOrders(ctx context.Context, req *orderpb.ListOrdersReq) (*orderpb.ListOrdersResp, error) {
	return call(ctx, "srv-order", "ListOrders", c.available() && c.Order != nil, func(ctx context.Context) (*orderpb.ListOrdersResp, error) {
		return c.Order.ListOrders(ctx, req)
	})
}

// GetOrder 调 srv-order GetOrder。
func (c *Clients) GetOrder(ctx context.Context, req *orderpb.GetOrderReq) (*orderpb.GetOrderResp, error) {
	return call(ctx, "srv-order", "GetOrder", c.available() && c.Order != nil, func(ctx context.Context) (*orderpb.GetOrderResp, error) {
		return c.Order.GetOrder(ctx, req)
	})
}

// CreateOrder 调 srv-order CreateOrder。
func (c *Clients) CreateOrder(ctx context.Context, req *orderpb.CreateOrderReq) (*orderpb.CreateOrderResp, error) {
	return call(ctx, "srv-order", "CreateOrder", c.available() && c.Order != nil, func(ctx context.Context) (*orderpb.CreateOrderResp, error) {
		return c.Order.CreateOrder(ctx, req)
	})
}

// Report 调 srv-order Report。
func (c *Clients) Report(ctx context.Context, req *orderpb.ReportReq) (*orderpb.ReportResp, error) {
	return call(ctx, "srv-order", "Report", c.available() && c.Order != nil, func(ctx context.Context) (*orderpb.ReportResp, error) {
		return c.Order.Report(ctx, req)
	})
}

// ListCoupons 调 srv-coupon ListCoupons。
func (c *Clients) ListCoupons(ctx context.Context, req *couponpb.ListCouponsReq) (*couponpb.ListCouponsResp, error) {
	return call(ctx, "srv-coupon", "ListCoupons", c.available() && c.Coupon != nil, func(ctx context.Context) (*couponpb.ListCouponsResp, error) {
		return c.Coupon.ListCoupons(ctx, req)
	})
}
