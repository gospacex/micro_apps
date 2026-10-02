// Package rpc srv-pay 的 rpcx 服务端桩：实现 pkg/kitex_gen 生成的
// pay.Pay 接口（Pay / Callback / ListPayFlows）。
//
// 职责边界（brief）：
//   - 每方法入口 telemetry.Start(ctx, "service", "pay", "<Method>") 分层埋点
//   - 身份校验在 handler 内读 req.GetUserId()（kitex 无 metadata 透传约定，
//     批 2 已核实）；业务错误以 Response.Code/Message 表达（pkg/codes），
//     网络/内部错误走 Go error（rpcx 错误体），两套不混用
//
// 装配（main.go）：
//
//	pay.NewServer(&rpc.Server{Svc: svc},
//	    rpcx.BuildServerOptions(rpcx.ServerOptions{ServiceName: "srv-pay", Addr: ":9014"})...)
package rpc

import (
	"context"

	paypb "hubx/micro_apps/pkg/kitex_gen/pay"

	"go.opentelemetry.io/otel/trace"

	"hubx/micro_apps/apps/srv-pay/internal/service"
	"hubx/micro_apps/infra/telemetry"
)

// Server 实现生成的 pay.Pay 接口
type Server struct {
	Svc *service.PayService
}

// guard 方法入口分层 span（telemetry.Start）；返回的 span 必须由调用方
// defer End —— 丢弃句柄会让这一层永不导出，下游子 span 也就失去 parent。
func (s *Server) guard(ctx context.Context, method string) (context.Context, trace.Span) {
	return telemetry.Start(ctx, "service", "pay", method)
}

// Pay 开支付单
func (s *Server) Pay(ctx context.Context, req *paypb.PayReq) (*paypb.PayResp, error) {
	ctx, span := s.guard(ctx, "Pay")
	defer span.End()
	return s.Svc.Pay(ctx, req)
}

// Callback 渠道异步回调
func (s *Server) Callback(ctx context.Context, req *paypb.PayCallbackReq) (*paypb.PayCallbackResp, error) {
	ctx, span := s.guard(ctx, "Callback")
	defer span.End()
	return s.Svc.Callback(ctx, req)
}

// ListPayFlows 支付流水
func (s *Server) ListPayFlows(ctx context.Context, req *paypb.ListPayFlowsReq) (*paypb.ListPayFlowsResp, error) {
	ctx, span := s.guard(ctx, "ListPayFlows")
	defer span.End()
	return s.Svc.ListPayFlows(ctx, req)
}

// 编译期断言：Server 必须满足生成的 pay.Pay 接口
var _ paypb.Pay = (*Server)(nil)
