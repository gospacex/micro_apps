// Package auth 网关 JWT 验签（验一次、透传身份，下游 BFF/SRV 只读不再验）。
//
// HS256 密钥经 routing.JWT.ResolvedSecret 取值（config.yaml gateway.jwt 段，
// env 优先）；校验 alg 固定 HS256（WithValidMethods 防 alg 混淆攻击），
// issuer 非空时强校验。解出 user_id（sub）+ roles（可选 claim），由
// 调用方写入 pkg/meta.HeaderUserID 等 Header 透传下游。
package auth

import (
	"context"
	"fmt"
	"strconv"

	"github.com/golang-jwt/jwt/v5"

	"hubx/micro_apps/infra/telemetry"
)

// HeaderRoles 网关透传的用户角色 Header（逗号分隔；pkg/meta 未上提此常量，
// 与 proxy.RolesHeader 同值，任务 11/12 需要时再进 pkg 共享层——deviation 记录）
const HeaderRoles = "X-User-Roles"

// Claims 网关侧解析的 JWT 载荷（srv-user 签 sub/exp/iss/jti/iat）
type Claims struct {
	jwt.RegisteredClaims
	// Roles 可选 claim（srv-user 当前不签；预留多角色透传）
	Roles []string `json:"roles,omitempty"`
}

// Verifier HS256 验签器（secret 为空时 Verify 恒失败——fail-closed）
type Verifier struct {
	secret []byte
	issuer string
}

// NewVerifier 构造（secret 来自 gateway.jwt 段；issuer 为空则不校验 iss）
func NewVerifier(secret []byte, issuer string) *Verifier {
	return &Verifier{secret: secret, issuer: issuer}
}

// Verify 验签 + 过期 + issuer + 格式，解出 user_id / roles。
// 任何失败返回 error（调用方统一 401，不区分细节防枚举）。
func (v *Verifier) Verify(ctx context.Context, token string) (int64, []string, error) {
	ctx, span := telemetry.Start(ctx, "middleware", "gateway", "auth")
	defer span.End()

	if token == "" {
		return 0, nil, fmt.Errorf("auth: missing bearer token")
	}
	if len(v.secret) == 0 {
		return 0, nil, fmt.Errorf("auth: jwt secret not configured")
	}
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("auth: unexpected signing method %v", t.Method.Alg())
		}
		return v.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return 0, nil, fmt.Errorf("auth: parse token: %w", err)
	}
	if v.issuer != "" && claims.Issuer != v.issuer {
		return 0, nil, fmt.Errorf("auth: issuer %q != %q", claims.Issuer, v.issuer)
	}
	if claims.Subject == "" {
		return 0, nil, fmt.Errorf("auth: empty sub claim")
	}
	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("auth: sub %q not int64: %w", claims.Subject, err)
	}
	return userID, claims.Roles, nil
}
