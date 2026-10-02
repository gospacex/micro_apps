package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// buildRC 构造最小 hertz RequestContext（方法/路径/Header/Body）供 Forward 测试。
// 就地取 &rc.Request 避免值拷贝（protocol.Request 含 NoCopy，copylocks 会拦）
func buildRC(method, target string, headers map[string]string, body string) *app.RequestContext {
	rc := new(app.RequestContext)
	req := &rc.Request
	req.SetMethod(method)
	// SetRequestURI 解析 path + query；Host 由 URL 携带
	req.SetRequestURI(target)
	req.SetBody([]byte(body))
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	return rc
}

// 说明：协议层 Request 的 Path() 经 URI() 取 path；测试 target 形如
// "/api/app/orders/1?x=2"。

// TestForwardInjections 转发注入语义：身份/角色/灰度 Header 透出 +
// 前缀剥离 + query 保留 + 上游状态/体回传（traceparent 仅总闸开时注入，
// 本测试 traceOn=false 不验证传播器，验注入面结构）
func TestForwardInjections(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen := map[string]string{
			"X-User-Id":    r.Header.Get("X-User-Id"),
			"X-User-Roles": r.Header.Get("X-User-Roles"),
			"X-Gray":       r.Header.Get("X-Gray"),
			"X-Tenant":     r.Header.Get("X-Tenant"),
			"Host":         r.Host,
			"Path":         r.URL.Path,
			"Query":        r.URL.RawQuery,
		}
		b, _ := json.Marshal(seen)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	}))
	defer upstream.Close()

	f := NewForwarder("127.0.0.1:8500", false)
	rc := buildRC("POST", "/api/app/orders/1?x=2", map[string]string{
		"X-Gray":   "b",
		"X-Tenant": "t1",
		"Host":     "ignored",
	}, `{"a":1}`)

	code, body, ct, err := f.Forward(context.Background(), rc, "/api/app", upstream.Listener.Addr().String(), "42", "admin,user")
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	s := string(body)
	// 上游回传的是 JSON map（{"K":"v"}）；按 JSON 键值形式断言
	check := func(k, v string) {
		if !strings.Contains(s, `"`+k+`":"`+v+`"`) {
			t.Errorf("upstream did not see %q=%q; body=%s", k, v, s)
		}
	}
	check("X-User-Id", "42")
	check("X-User-Roles", "admin,user")
	check("X-Gray", "b")
	check("X-Tenant", "t1")
	check("Path", "/orders/1")
	check("Query", "x=2")
	// 网管 Header 不透传：Host 是 httptest 自填的，改验「入站 host 未被带出」——
	// http 客户端按出站 Host 重写，断言上游收到的是 upstream 主机名
	// （入站 "ignored" 不应出现）
	if strings.Contains(s, "ignored") {
		t.Errorf("inbound Host leaked: %s", s)
	}
}

// TestForwardUpstreamError 上游连不通 → error 带 %w（handler 层 502）
func TestForwardUpstreamError(t *testing.T) {
	f := NewForwarder("127.0.0.1:8500", false)
	rc := buildRC("GET", "/api/app", nil, "")
	// 无监听端口的地址（宿主侧 127.0.0.1:1 惯例不通）
	_, _, _, err := f.Forward(context.Background(), rc, "/api/app", "127.0.0.1:1", "42", "")
	if err == nil {
		t.Fatal("expected error for unreachable upstream")
	}
	if !strings.Contains(err.Error(), "forward to") {
		t.Fatalf("error message = %q", err)
	}
}

// TestForwardInjectsFullW3CHeaders：转发时 traceparent/tracestate/baggage 三个头
// 必须同时注入下游，且入站旧的 tracestate/baggage 不得漏到下游。
func TestForwardInjectsFullW3CHeaders(t *testing.T) {
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { otel.SetTextMapPropagator(previousPropagator) })

	traceState, err := trace.ParseTraceState("vendor=new")
	if err != nil {
		t.Fatalf("parse tracestate: %v", err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
		TraceState: traceState,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatalf("new baggage member: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("new baggage: %v", err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)

	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := NewForwarder("127.0.0.1:8500", true)
	rc := buildRC("POST", "/api/app/orders", map[string]string{
		"tracestate": "old=vendor",
		"baggage":    "old=bag",
	}, `{}`)

	code, _, _, err := f.Forward(ctx, rc, "/api/app", srv.Listener.Addr().String(), "7", "admin")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if got.Get("traceparent") == "" {
		t.Fatal("traceparent not injected")
	}
	if got.Get("tracestate") != "vendor=new" {
		t.Fatalf("tracestate = %q, want %q", got.Get("tracestate"), "vendor=new")
	}
	if got.Get("baggage") != "tenant=acme" {
		t.Fatalf("baggage = %q, want %q", got.Get("baggage"), "tenant=acme")
	}
}
