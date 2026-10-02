package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gospacex/webx"

	"hubx/micro_apps/apps/bff-web/internal/client"
	"hubx/micro_apps/apps/bff-web/internal/service"
	"hubx/micro_apps/pkg/meta"
)

// newTestApp 起一个独立 webx 实例（随机端口）并挂载 bff-web 路由。
// 下游客户端全为 nil：Home 走全降级路径，足以验证统一响应与 SEO 兜底。
func newTestApp(t *testing.T) *webx.App {
	t.Helper()
	api := NewAPI(service.NewAggregator(&client.Clients{}, nil, nil), nil)
	app, err := webx.TestApp(
		// Addr 仅为校验占位（TestApp 走随机端口 listener，URL() 返回实际地址）
		webx.WithConfig(webx.Config{Addr: "127.0.0.1:18092", ReadyCheck: func(context.Context) error { return nil }}),
		webx.WithPreRun(func(a *webx.App) { Register(a.Engine, api) }),
	)
	if err != nil {
		t.Fatalf("webx.TestApp: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = app.Shutdown(ctx)
	})
	return app
}

func get(t *testing.T, url string, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

// TestReadyProbe 验证 /healthz/ready 就绪探针（webx 自动挂载）。
func TestReadyProbe(t *testing.T) {
	app := newTestApp(t)
	code, body := get(t, app.URL()+"/healthz/ready", nil)
	if code != http.StatusOK || !strings.Contains(body, "ready") {
		t.Fatalf("ready probe = %d %q, want 200 ready", code, body)
	}
}

// TestHomeEnvelopeAndIdentityHeader 验证统一响应封装：
// 缺身份头 → 401 + code 4010；带身份头 → 200 + 含 SEO 段的 data。
func TestHomeEnvelopeAndIdentityHeader(t *testing.T) {
	app := newTestApp(t)

	code, body := get(t, app.URL()+"/home", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("missing identity header: code = %d, want 401", code)
	}
	var envelope struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("body %q is not envelope json: %v", body, err)
	}
	if envelope.Code != 4010 {
		t.Fatalf("envelope code = %d, want 4010", envelope.Code)
	}

	code, body = get(t, app.URL()+"/home", map[string]string{meta.HeaderUserID: "42"})
	if code != http.StatusOK {
		t.Fatalf("home: code = %d body = %q", code, body)
	}
	var out struct {
		Code int `json:"code"`
		Data struct {
			Degraded []string `json:"degraded"`
			SEO      struct {
				Title     string `json:"title"`
				Canonical string `json:"canonical"`
			} `json:"seo"`
			SSR struct {
				Rendered bool   `json:"rendered"`
				Locale   string `json:"locale"`
			} `json:"ssr"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("home body %q not envelope json: %v", body, err)
	}
	if out.Code != 0 {
		t.Fatalf("envelope code = %d, want 0", out.Code)
	}
	if len(out.Data.Degraded) != 3 {
		t.Fatalf("degraded = %v, want 3 branches", out.Data.Degraded)
	}
	if out.Data.SEO.Title == "" || out.Data.SEO.Canonical == "" {
		t.Fatalf("seo segment missing: %+v", out.Data.SEO)
	}
	if !out.Data.SSR.Rendered || out.Data.SSR.Locale == "" {
		t.Fatalf("ssr segment missing: %+v", out.Data.SSR)
	}
}
