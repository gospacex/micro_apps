// Package client bff-mini 的 kitex 下游客户端装配（srv-user / srv-product /
// srv-coupon；小程序端首页不展示订单流水，故不接 srv-order 的 ListOrders，
// 下单链路仍共用 srv-order 客户端）。
//
// 目标地址经 infra/consul.Target 读注册中心 KV（hubx/svc/<svc>）；续链由
// rpcx.BuildClientOptions 挂的 obs-opentelemetry ClientSuite 完成（traceparent
// 经 kitex metainfo 自动注入，无需手动）。
//
// 降级语义：任一目标未注册 → 对应客户端为 nil，service 层对 nil 客户端走
// 降级兜底，不阻断其余分支。
package client

import (
	"github.com/gospacex/logx"

	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/pkg/rpcx"

	couponcli "hubx/micro_apps/pkg/kitex_gen/coupon/coupon"
	ordercli "hubx/micro_apps/pkg/kitex_gen/order/order"
	productcli "hubx/micro_apps/pkg/kitex_gen/product/product"
	usercli "hubx/micro_apps/pkg/kitex_gen/user/user"
)

// Clients 下游 srv 客户端集合（nil 字段表示该下游未注册，调用方须降级）。
type Clients struct {
	User    usercli.Client
	Product productcli.Client
	Order   ordercli.Client
	Coupon  couponcli.Client
}

// New 装配客户端；单个失败仅告警，整体不返回 error。
func New(self, consulAgent string, traceOn bool, logger logx.Logger) *Clients {
	c := &Clients{}
	if cli, err := dial(consulAgent, "srv-user", func(target string) (any, error) {
		return usercli.NewClient("User", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-mini: srv-user unavailable (degraded)", logx.Err(err))
	} else {
		c.User = cli.(usercli.Client)
	}
	if cli, err := dial(consulAgent, "srv-product", func(target string) (any, error) {
		return productcli.NewClient("Product", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-mini: srv-product unavailable (degraded)", logx.Err(err))
	} else {
		c.Product = cli.(productcli.Client)
	}
	if cli, err := dial(consulAgent, "srv-order", func(target string) (any, error) {
		return ordercli.NewClient("Order", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-mini: srv-order unavailable (degraded)", logx.Err(err))
	} else {
		c.Order = cli.(ordercli.Client)
	}
	if cli, err := dial(consulAgent, "srv-coupon", func(target string) (any, error) {
		return couponcli.NewClient("Coupon", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-mini: srv-coupon unavailable (degraded)", logx.Err(err))
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
