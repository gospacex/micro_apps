package auth

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// signToken 测试辅助：按 srv-user 同形态签 HS256 token（sub + iss + exp）
func signToken(t *testing.T, key, sub, iss string, ttl time.Duration) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": sub,
		"iss": iss,
		"exp": time.Now().Add(ttl).Unix(),
		"iat": time.Now().Unix(),
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return signed
}

func TestVerifyAcceptsValidToken(t *testing.T) {
	v := NewVerifier([]byte("s3cret"), "srv-user")
	tok := signToken(t, "s3cret", "42", "srv-user", time.Hour)
	userID, roles, err := v.Verify(context.Background(), tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if userID != 42 {
		t.Fatalf("userID = %d, want 42", userID)
	}
	if len(roles) != 0 {
		t.Fatalf("roles = %v, want empty", roles)
	}
}

func TestVerifyRejectsBadSignature(t *testing.T) {
	v := NewVerifier([]byte("s3cret"), "srv-user")
	tok := signToken(t, "other", "42", "srv-user", time.Hour)
	if _, _, err := v.Verify(context.Background(), tok); err == nil {
		t.Fatal("expected signature mismatch error")
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	v := NewVerifier([]byte("s3cret"), "srv-user")
	tok := signToken(t, "s3cret", "42", "srv-user", -time.Hour)
	if _, _, err := v.Verify(context.Background(), tok); err == nil {
		t.Fatal("expected expired error")
	}
}

func TestVerifyRejectsWrongIssuer(t *testing.T) {
	v := NewVerifier([]byte("s3cret"), "srv-user")
	tok := signToken(t, "s3cret", "42", "evil", time.Hour)
	if _, _, err := v.Verify(context.Background(), tok); err == nil {
		t.Fatal("expected issuer mismatch error")
	}
}

func TestVerifyRejectsMissingToken(t *testing.T) {
	v := NewVerifier([]byte("s3cret"), "srv-user")
	if _, _, err := v.Verify(context.Background(), ""); err == nil {
		t.Fatal("expected missing token error")
	}
}

func TestVerifyRejectsNumericSub(t *testing.T) {
	v := NewVerifier([]byte("s3cret"), "")
	tok := signToken(t, "s3cret", "abc", "srv-user", time.Hour)
	if _, _, err := v.Verify(context.Background(), tok); err == nil {
		t.Fatal("expected non-numeric sub error")
	}
}

// TestVerifyFailClosedNoSecret 密钥未配置时恒拒绝（fail-closed，非 fail-open）
func TestVerifyFailClosedNoSecret(t *testing.T) {
	v := NewVerifier(nil, "srv-user")
	tok := signToken(t, "s3cret", "42", "srv-user", time.Hour)
	if _, _, err := v.Verify(context.Background(), tok); err == nil {
		t.Fatal("expected secret-not-configured error")
	}
}
