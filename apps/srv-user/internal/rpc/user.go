// Package rpc srv-user 的 rpcx 服务端桩：实现 pkg/kitex_gen 生成的
// user.User 接口（Register / Login / GetProfile / CheckUser）。
//
// 职责边界（brief）：
//   - 每方法入口 telemetry.Start(ctx, "service", "user", "<Method>") 分层埋点
//   - 身份校验在 handler 内读 req.GetUserId()（kitex 无 metadata 透传约定，
//     批 2 已核实）；业务错误以 Response.Code/Message 表达（pkg/codes），
//     网络/内部错误走 Go error（rpcx 错误体），两套不混用
//
// 装配（main.go）：
//
//	user.NewServer(&rpc.Server{Svc: svc},
//	    rpcx.BuildServerOptions(rpcx.ServerOptions{ServiceName: "srv-user", Addr: ":9011"})...)
package rpc

import (
	"context"

	userpb "hubx/micro_apps/pkg/kitex_gen/user"

	"hubx/micro_apps/apps/srv-user/internal/service"
	"hubx/micro_apps/infra/telemetry"
)

// Server 实现生成的 user.User 接口
type Server struct {
	Svc *service.UserService
}

// guard 方法入口分层 span（telemetry.Start）
func (s *Server) guard(ctx context.Context, method string) context.Context {
	ctx, _ = telemetry.Start(ctx, "service", "user", method)
	return ctx
}

// Register 注册（username/phone 查重 → 2001；密码 sha256-hex）
func (s *Server) Register(ctx context.Context, req *userpb.RegisterReq) (*userpb.RegisterResp, error) {
	return s.Svc.Register(s.guard(ctx, "Register"), req)
}

// Login 登录（JWT HS256 + auth_sessions 落库）
func (s *Server) Login(ctx context.Context, req *userpb.LoginReq) (*userpb.LoginResp, error) {
	return s.Svc.Login(s.guard(ctx, "Login"), req)
}

// GetProfile 查资料（users ⋈ user_profiles JOIN）
func (s *Server) GetProfile(ctx context.Context, req *userpb.GetProfileReq) (*userpb.GetProfileResp, error) {
	return s.Svc.GetProfile(s.guard(ctx, "GetProfile"), req)
}

// CheckUser 跨域身份校验
func (s *Server) CheckUser(ctx context.Context, req *userpb.CheckUserReq) (*userpb.CheckUserResp, error) {
	return s.Svc.CheckUser(s.guard(ctx, "CheckUser"), req)
}

// 编译期断言：Server 必须满足生成的 user.User 接口
var _ userpb.User = (*Server)(nil)
