// Package proxy 网关转发：hertz 入站请求 → 目标 BFF（HTTP 端口）HTTP 转发。
//
// 目标地址经 infra/consul.Resolve(agent, serviceID) 读 KV（各服务注册时写入
// 自身监听地址；BFF 冒烟期未上线时，路由表可指向已上线的 srv :90xx）。
// 多实例 round-robin（无 LB 依赖，deviation 记录在任务 10 报告）。
//
// 链路根 span 在 gateway 诞生：telemetry.Start(ctx, "handler", "gateway",
// "Proxy:"+prefix) 产出整条 trace 的根；转发前经 otel 全局传播器
// （Composite TraceContext+Baggage，telemetry.Init 注册）把 traceparent 注入
// 出站 Header，下游 BFF 入站 OTel 中间件 Extract 续链——跨 HTTP 边界靠
// traceparent 头（rpcx obs-opentelemetry 套件只覆盖 rpcx 端，HTTP 须手动续）。
//
// 透传：入站 Header（跳过 hop-by-hop/网关自管项）+ 身份 Header
// （pkg/meta.HeaderUserID/HeaderTenant + 角色，验签一次、下游不再验）
// + 灰度头 pkg/meta.HeaderGray 原样透传。
package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/telemetry"
	"hubx/micro_apps/pkg/meta"
)

// RolesHeader 角色透传 Header（逗号分隔；与 internal/auth.HeaderRoles 同值。
// pkg/meta 未上提此常量，gateway 内两处局部定义，任务 11 需要时再进 pkg）
const RolesHeader = "X-User-Roles"

// hopByHop 网管/网关自管的 Header（小写 key）：转发时跳过（traceparent 由
// 转发器经 otel 传播器重新注入，防双头）
var hopByHop = map[string]bool{
	"host":            true,
	"content-length":  true,
	"connection":      true,
	"accept-encoding": true,
	"traceparent":     true,
	"traceresponse":   true,
	"tracestate":      true, // 由转发器经传播器重新注入，防旧值与新 traceparent 拼出不一致三元组
	"baggage":         true,
}

// Forwarder consul 服务发现 + HTTP 转发
type Forwarder struct {
	consulAgent string
	traceOn     bool
	httpClient  *http.Client
	mu          sync.Mutex
	rr          map[string]int
}

// NewForwarder 构造（traceOn = pkg/config Trace.Enabled；总闸关时不注入
// traceparent，退化为裸转发）
func NewForwarder(consulAgent string, traceOn bool) *Forwarder {
	return &Forwarder{
		consulAgent: consulAgent,
		traceOn:     traceOn,
		rr:          map[string]int{},
		httpClient: &http.Client{
			Timeout: 15 * time.Second, // 覆盖下游 rpcx client 默认 10s + 转发开销
			Transport: &http.Transport{
				// 本地 docker 演示形态：目标可能自签 TLS；不用于生产
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
				MaxIdleConns:    64,
			},
		},
	}
}

// Forward 根 span + 身份/灰度 Header 透传 + traceparent 注入 + HTTP 转发。
//
// 参数：rc（hertz 入站，读路径/Header/Body）、routePrefix（路由表前缀，根
// span 名 Proxy:<prefix>，出站路径去前缀——前缀是 gateway 侧命名空间）、
// addr（目标 host:port，Resolve 解出）、userID/userRoles（auth 验签解出，
// 空串不写）。
//
// 返回 (状态码, 响应体, 上游 Content-Type, error)；error 非 nil 时调用方（handler）写 502。
// 响应体须整体读回（deviation：转发不做流式直通，demo 网关形态优先简单；
// 大文件场景任务 15 冒烟若有需要再升级 io.Copy 直通）。
func (f *Forwarder) Forward(ctx context.Context, rc *app.RequestContext, routePrefix, addr, userID, userRoles string) (int, []byte, string, error) {
	// 根 span：gateway 是整条 trace 的根，下游 BFF/SRV 的 span 挂其下
	ctx, span := telemetry.Start(ctx, "handler", "gateway", "Proxy:"+routePrefix)
	defer span.End()

	// 出站路径：去掉网关前缀（/api/app/orders/1 → /orders/1 由 BFF 路由）。
	// 用 PathOriginal（原始编码串）而非 URI.Path（解码+归一化）——保持
	// 上游收到的 path 与入站一致，避免 % 编码漂移
	rpath := strings.TrimPrefix(string(rc.Request.URI().PathOriginal()), routePrefix)
	if rpath == "" {
		rpath = "/"
	}
	u := &url.URL{
		Scheme:   "http",
		Host:     addr,
		Path:     rpath,
		RawQuery: string(rc.Request.QueryString()),
	}

	var body io.Reader
	if len(rc.Request.Body()) > 0 {
		body = strings.NewReader(string(rc.Request.Body()))
	}
	outReq, err := http.NewRequestWithContext(ctx, string(rc.Method()), u.String(), body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "build upstream request")
		return 0, nil, "", fmt.Errorf("proxy: build request: %w", err)
	}
	outReq.Header = copyInbound(rc)
	if userID != "" {
		outReq.Header.Set(meta.HeaderUserID, userID)
	}
	if userRoles != "" {
		outReq.Header.Set(RolesHeader, userRoles)
	}
	// 灰度头：入站已有则 copyInbound 已带；无则不下发（由上游按自身策略定）
	if f.traceOn {
		// 整体注入 traceparent/tracestate/baggage（Composite 传播器的完整字段集），
		// 替代原先只手写 traceparent 的单头注入。
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(outReq.Header))
	}

	resp, err := f.httpClient.Do(outReq)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "upstream error")
		return 0, nil, "", fmt.Errorf("proxy: forward to %s: %w", addr, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "read upstream body")
		return 0, nil, "", fmt.Errorf("proxy: read upstream body: %w", err)
	}
	return resp.StatusCode, respBody, resp.Header.Get("Content-Type"), nil
}

// Resolve 目标服务 → consul 实例列表，round-robin 取一台（单实例直返）。
// 未注册/空 KV 返回 error（handler 层 502「目标未注册」，不静默兜底）。
func (f *Forwarder) Resolve(ctx context.Context, service string) (string, error) {
	addrs, err := consul.Resolve(f.consulAgent, service)
	if err != nil {
		return "", fmt.Errorf("proxy: consul resolve %s: %w", service, err)
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("proxy: consul KV empty for %s (service not registered)", service)
	}
	f.mu.Lock()
	i := f.rr[service]
	f.rr[service] = i + 1
	f.mu.Unlock()
	return addrs[i%len(addrs)], nil
}

// copyInbound 入站 Header → http.Header（跳过 hopByHop；X-User-Id/X-Tenant/
// X-Gray 若入站自带则原样带——身份 Header 由 auth 层随后 Set 覆盖，
// 防伪造：auth 失败直接 401 不会到这一步）。
// hertz 的 VisitAll 回调给的是原始 key（规范形 Host/Content-Length），
// 比对前须 ToLower 归一。
func copyInbound(rc *app.RequestContext) http.Header {
	out := http.Header{}
	rc.Request.Header.VisitAll(func(k, v []byte) {
		if hopByHop[strings.ToLower(string(k))] {
			return
		}
		out.Add(string(k), string(v))
	})
	return out
}
