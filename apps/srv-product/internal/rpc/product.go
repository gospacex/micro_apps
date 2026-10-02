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

	"go.opentelemetry.io/otel/trace"

	"hubx/micro_apps/apps/srv-product/internal/service"
	"hubx/micro_apps/infra/telemetry"
)

// Server 实现生成的 product.Product 接口
type Server struct {
	Svc *service.ProductService
}

// guard 方法入口分层 span（telemetry.Start）；返回的 span 必须由调用方
// defer End —— 丢弃句柄会让这一层永不导出，下游子 span 也就失去 parent。
func (s *Server) guard(ctx context.Context, method string) (context.Context, trace.Span) {
	return telemetry.Start(ctx, "service", "product", method)
}

// GetProduct 查商品（3 表 JOIN + 热点缓存）
func (s *Server) GetProduct(ctx context.Context, req *productpb.GetProductReq) (*productpb.GetProductResp, error) {
	ctx, span := s.guard(ctx, "GetProduct")
	defer span.End()
	return s.Svc.GetProduct(ctx, req)
}

// ReserveStock 库存预占（FOR UPDATE 行锁事务）
func (s *Server) ReserveStock(ctx context.Context, req *productpb.ReserveStockReq) (*productpb.ReserveStockResp, error) {
	ctx, span := s.guard(ctx, "ReserveStock")
	defer span.End()
	return s.Svc.ReserveStock(ctx, req)
}

// ListProducts 商品列表
func (s *Server) ListProducts(ctx context.Context, req *productpb.ListProductsReq) (*productpb.ListProductsResp, error) {
	ctx, span := s.guard(ctx, "ListProducts")
	defer span.End()
	return s.Svc.ListProducts(ctx, req)
}

// 编译期断言：Server 必须满足生成的 product.Product 接口
var _ productpb.Product = (*Server)(nil)
