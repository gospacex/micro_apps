// Package rpc srv-coupon 的 rpcx 服务端桩：实现 pkg/kitex_gen 生成的
// coupon.Coupon 接口（Consume / GrantPoints / ListCoupons）。
//
// 职责边界（brief）：
//   - 每方法入口 telemetry.Start(ctx, "service", "coupon", "<Method>") 分层埋点
//   - 身份校验在 handler 内读 req.GetUserId()（kitex 无 metadata 透传约定，
//     批 2 已核实）；业务错误以 Response.Code/Message 表达（pkg/codes），
//     网络/内部错误走 Go error（rpcx 错误体），两套不混用
//
// 装配（main.go）：
//
//	coupon.NewServer(&rpc.Server{Svc: svc},
//	    rpcx.BuildServerOptions(rpcx.ServerOptions{ServiceName: "srv-coupon", Addr: ":9015"})...)
package rpc

import (
	"context"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"

	"go.opentelemetry.io/otel/trace"

	"hubx/micro_apps/apps/srv-coupon/internal/service"
	"hubx/micro_apps/infra/telemetry"
)

// Server 实现生成的 coupon.Coupon 接口
type Server struct {
	Svc *service.CouponService
}

// guard 方法入口分层 span（telemetry.Start）；返回的 span 必须由调用方
// defer End —— 丢弃句柄会让这一层永不导出，下游子 span 也就失去 parent。
func (s *Server) guard(ctx context.Context, method string) (context.Context, trace.Span) {
	return telemetry.Start(ctx, "service", "coupon", method)
}

// Consume 用券核销（幂等：used 重放 → 6001；outbox 补发 CouponExchanged）
func (s *Server) Consume(ctx context.Context, req *couponpb.ConsumeReq) (*couponpb.ConsumeResp, error) {
	ctx, span := s.guard(ctx, "Consume")
	defer span.End()
	return s.Svc.Consume(ctx, req)
}

// GrantPoints 积分发放（points_log + outbox PointsGranted 同事务）
func (s *Server) GrantPoints(ctx context.Context, req *couponpb.GrantPointsReq) (*couponpb.GrantPointsResp, error) {
	ctx, span := s.guard(ctx, "GrantPoints")
	defer span.End()
	return s.Svc.GrantPoints(ctx, req)
}

// ListCoupons 用户持有的券
func (s *Server) ListCoupons(ctx context.Context, req *couponpb.ListCouponsReq) (*couponpb.ListCouponsResp, error) {
	ctx, span := s.guard(ctx, "ListCoupons")
	defer span.End()
	return s.Svc.ListCoupons(ctx, req)
}

// 编译期断言：Server 必须满足生成的 coupon.Coupon 接口
var _ couponpb.Coupon = (*Server)(nil)
