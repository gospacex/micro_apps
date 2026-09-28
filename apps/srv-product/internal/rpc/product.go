// Package rpc srv-product 的 rpcx 服务端桩：实现 pkg/kitex_gen 生成的
// product.Product 接口（GetProduct / ReserveStock / ListProducts）。
//
// 职责边界（brief）：
//   - 每方法入口 telemetry.Start(ctx, "service", "product", "<Method>") 分层埋点
//   - 身份校验在 handler 内读 req.GetUserId()（kitex 无 metadata 透传约定，
//     批 2 已核实）；业务错误以 Response.Code/Message 表达（pkg/codes），
//     网络/内部错误走 Go error（rpcx 错误体），两套不混用
//
// 装配（main.go）：
//
//	product.NewServer(&rpc.Server{Svc: svc},
//	    rpcx.BuildServerOptions(rpcx.ServerOptions{ServiceName: "srv-product", Addr: ":9012"})...)
package rpc

import (
	"context"

	productpb "hubx/micro_apps/pkg/kitex_gen/product"

	"hubx/micro_apps/apps/srv-product/internal/service"
	"hubx/micro_apps/infra/telemetry"
)

// Server 实现生成的 product.Product 接口
type Server struct {
	Svc *service.ProductService
}

// guard 方法入口分层 span（telemetry.Start）
func (s *Server) guard(ctx context.Context, method string) context.Context {
	ctx, _ = telemetry.Start(ctx, "service", "product", method)
	return ctx
}

// GetProduct 查商品（3 表 JOIN + 热点缓存）
func (s *Server) GetProduct(ctx context.Context, req *productpb.GetProductReq) (*productpb.GetProductResp, error) {
	return s.Svc.GetProduct(s.guard(ctx, "GetProduct"), req)
}

// ReserveStock 库存预占（FOR UPDATE 行锁事务）
func (s *Server) ReserveStock(ctx context.Context, req *productpb.ReserveStockReq) (*productpb.ReserveStockResp, error) {
	return s.Svc.ReserveStock(s.guard(ctx, "ReserveStock"), req)
}

// ListProducts 商品列表
func (s *Server) ListProducts(ctx context.Context, req *productpb.ListProductsReq) (*productpb.ListProductsResp, error) {
	return s.Svc.ListProducts(s.guard(ctx, "ListProducts"), req)
}

// 编译期断言：Server 必须满足生成的 product.Product 接口
var _ productpb.Product = (*Server)(nil)
