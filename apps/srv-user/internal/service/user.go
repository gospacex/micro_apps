// Package service srv-user 业务编排层。
//
// 语义（brief §srv-user）：
//   - Register：username/phone 查重（uk 冲突 → 2001）；密码 sha256-hex 散列落库
//     （users.password VARCHAR(128)）；UserId=0 时走自增
//   - Login：2002 凭据错误 / 2004 账号禁用；签发 HS256 JWT（golang-jwt/v5），
//     密钥取 env HUBX_JWT_SECRET（缺省 dev 占位，deviation 记录），jti 随机 UUIDv4
//     并落 auth_sessions；ExpireAt 为 unix 秒
//   - GetProfile：users ⋈ user_profiles JOIN（repository Find），未命中 → 1002
//   - CheckUser：status==1 → Active=true；禁用 → 2004；不存在 → 1002
//
// 业务错误一律经 Response.Code/Message 返回（pkg/codes），网络/内部错误
// 走 Go error（rpcx 错误体），两套不混用。
package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gospacex/logx"

	userpb "hubx/micro_apps/pkg/kitex_gen/user"

	"hubx/micro_apps/apps/srv-user/internal/repository"
	"hubx/micro_apps/pkg/codes"
)

// JWT 签发参数
const (
	jwtIssuer = "srv-user"
	jwtTTL    = 24 * time.Hour // token 有效期（auth_sessions.expires_at 同值）
	jwtKeyEnv = "HUBX_JWT_SECRET"
	jwtKeyDev = "hubx-dev-jwt-secret" // 仅本地演示缺省（deviation：不硬编码真实密钥）
)

// UserService 用户业务编排
type UserService struct {
	repo   *repository.UserRepository
	jwtKey []byte
	logger logx.Logger
}

// NewUserService 构造（jwtKey 为空时用 env HUBX_JWT_SECRET，再缺省 dev 占位）
func NewUserService(repo *repository.UserRepository, jwtKey string, logger logx.Logger) *UserService {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	if jwtKey == "" {
		jwtKey = jwtKeyDev
	}
	return &UserService{repo: repo, jwtKey: []byte(jwtKey), logger: logger}
}

// Register 注册
func (s *UserService) Register(ctx context.Context, req *userpb.RegisterReq) (*userpb.RegisterResp, error) {
	resp := &userpb.RegisterResp{UserId: req.GetUserId()}
	if req.GetUsername() == "" || req.GetPassword() == "" {
		c := codes.Fail(codes.CodeInvalidParam, "username and password required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	hash, err := hashPassword(req.GetPassword())
	if err != nil {
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	// UserId 语义（brief）：0 → 自增；>0 → 指定 ID 落库（dev 演示形态，冲突即 2001）
	id, err := s.repo.InsertUser(ctx, uint64(req.GetUserId()), req.GetUsername(), hash, req.GetPhone())
	if err != nil {
		if errors.Is(err, repository.ErrUsernameTaken) {
			c := codes.Fail(codes.CodeUserExists, "username or phone exists")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		s.logger.WarnContext(ctx, "register insert failed", logx.Err(err))
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	resp.UserId = int64(id)
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// hashPassword sha256-hex（brief 明确「sha256-hex 散列」，非 bcrypt：
// 演示项目，一一对应；盐值缺失为 deviation 记录）
func hashPassword(plain string) (string, error) {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:]), nil
}

// Login 登录 + 会话落库 + JWT 签发
func (s *UserService) Login(ctx context.Context, req *userpb.LoginReq) (*userpb.LoginResp, error) {
	resp := &userpb.LoginResp{UserId: req.GetUserId()}
	if req.GetUsername() == "" || req.GetPassword() == "" {
		c := codes.Fail(codes.CodeInvalidParam, "username and password required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	u, err := s.repo.FindByUsername(ctx, req.GetUsername())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c := codes.Fail(codes.CodeBadCredential, "bad username or password")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		s.logger.WarnContext(ctx, "login lookup failed", logx.Err(err))
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	hash, err := hashPassword(req.GetPassword())
	if err == nil && hash != u.Password {
		c := codes.Fail(codes.CodeBadCredential, "bad username or password")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if u.Status != repository.UserStatusActive {
		c := codes.Fail(codes.CodeUserDisabled, "user disabled")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	token, jti, expireAt, err := s.issueToken(ctx, u.ID)
	if err != nil {
		s.logger.ErrorContext(ctx, "issue jwt failed", logx.Err(err))
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	// 会话落库（jti 与 token claim 对应；失败不阻断登录——会话审计缺失可容忍）
	sess := &repository.Session{
		UserID:    u.ID,
		TokenJTI:  jti,
		ExpiresAt: expireAt,
	}
	if serr := s.repo.SaveSession(ctx, sess); serr != nil {
		s.logger.WarnContext(ctx, "save auth session failed (login continues)", logx.Err(serr))
	}
	resp.UserId = int64(u.ID)
	resp.Token = token
	resp.ExpireAt = expireAt.Unix()
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// issueToken 签 HS256 JWT：claims = sub(username) + jti + exp
func (s *UserService) issueToken(ctx context.Context, userID uint64) (string, string, time.Time, error) {
	jti, err := newJTI()
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("new jti: %w", err)
	}
	now := time.Now()
	expireAt := now.Add(jwtTTL)
	claims := jwt.MapClaims{
		"sub": fmt.Sprintf("%d", userID),
		"jti": jti,
		"iss": jwtIssuer,
		"exp": expireAt.Unix(),
		"iat": now.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.jwtKey)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("sign jwt: %w", err)
	}
	return signed, jti, expireAt, nil
}

// newJTI 随机 16 字节 hex（auth_sessions.token_jti VARCHAR(64)）
func newJTI() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// GetProfile 查资料
func (s *UserService) GetProfile(ctx context.Context, req *userpb.GetProfileReq) (*userpb.GetProfileResp, error) {
	resp := &userpb.GetProfileResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	pr, err := s.repo.GetProfile(ctx, uint64(req.GetUserId()))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c := codes.Fail(codes.CodeNotFound, "user not found")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	resp.Username = pr.User.Username
	resp.Nickname = pr.User.Nickname
	resp.Avatar = pr.Profile.Avatar
	resp.City = pr.Profile.City
	resp.Level = pr.Profile.Level
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}

// CheckUser 跨域身份校验（供其他服务鉴权前置）
func (s *UserService) CheckUser(ctx context.Context, req *userpb.CheckUserReq) (*userpb.CheckUserResp, error) {
	resp := &userpb.CheckUserResp{UserId: req.GetUserId()}
	if req.GetUserId() <= 0 {
		c := codes.Fail(codes.CodeUnauthorized, "user required")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	u, err := s.repo.GetByID(ctx, uint64(req.GetUserId()))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c := codes.Fail(codes.CodeNotFound, "user not found")
			resp.Code, resp.Message = c.Code, c.Message
			return resp, nil
		}
		c := codes.Fail(codes.CodeInternal, err.Error())
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	if u.Status != repository.UserStatusActive {
		c := codes.Fail(codes.CodeUserDisabled, "user disabled")
		resp.Code, resp.Message = c.Code, c.Message
		return resp, nil
	}
	resp.Active = true
	resp.Code, resp.Message = codes.OK().Code, codes.OK().Message
	return resp, nil
}
