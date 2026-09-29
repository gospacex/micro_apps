// 网关 HTTP 入口：路由表匹配 → 全局限流 → JWT 验签 → 转发。
//
// 注册模型：webx 健康探针（/healthz/*，静态路由）之外的**全部路径**经
// catch-all（/*p）进 ServeHTTP；路由裁决在 handler 内按 routing.Table
// 最长前缀做（表外路径 404，探针不受影响）。
package proxy

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/gospacex/logx"

	"hubx/micro_apps/apps/gateway/internal/auth"
	"hubx/micro_apps/apps/gateway/internal/ratelimit"
	"hubx/micro_apps/apps/gateway/internal/routing"
	"hubx/micro_apps/pkg/codes"
	"hubx/micro_apps/pkg/meta"
)

// Deps 网关入口依赖（main 装配后注入；Limiter/Verifier 为 nil 时对应环节
// fail-open / 直接拒绝，语义见各 internal 包注释）
type Deps struct {
	Table     *routing.Table
	Limiter   *ratelimit.Limiter // nil = 缓存门控关闭，全放行
	Verifier  *auth.Verifier
	Forwarder *Forwarder
	Log       logx.Logger
}

// Register catch-all 路由（webx 先注册 /healthz，静态路由优先于 catch-all）
func Register(e *route.Engine, d *Deps) {
	e.Any("/*p", d.ServeHTTP)
}

// ServeHTTP 网关主 handler。
// 404（无路由）/ 429（限流）/ 401（验签）/ 502（目标不可用）。
// 身份 Header（X-User-Id/X-User-Roles）与 traceparent 由 Forwarder.Forward
// 注入；灰度头 X-Gray 入站原样透传（copyInbound 已带，meta.HeaderGray 见下）。
func (d *Deps) ServeHTTP(ctx context.Context, c *app.RequestContext) {
	path := string(c.Path())

	rt := d.Table.Match(path)
	if rt == nil {
		c.AbortWithStatusJSON(http.StatusNotFound, codes.Fail(codes.CodeNotFound, "no gateway route for "+path))
		return
	}

	// 全局限流：按 (ip, 路由前缀) 固定窗口计数；nil/缓存故障 → fail-open
	ip := c.ClientIP()
	if !d.Limiter.Allow(ctx, ip, rt.Prefix) {
		c.AbortWithStatusJSON(http.StatusTooManyRequests, codes.Fail(codes.CodeRateLimited, "rate limited"))
		return
	}

	// JWT 验一次（auth）：解出 user_id/roles 写 Header 透传，下游不再验签
	userID, roles, err := d.Verifier.Verify(ctx, bearerToken(c))
	if err != nil {
		d.Log.WarnContext(ctx, "gateway: jwt rejected", logx.Err(err))
		c.AbortWithStatusJSON(http.StatusUnauthorized, codes.Fail(codes.CodeUnauthorized, "unauthorized"))
		return
	}

	// 目标解析（consul KV）→ HTTP 转发；X-Gray 灰度头随入站 Header 原样出站
	addr, err := d.Forwarder.Resolve(ctx, rt.Service)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadGateway, codes.Fail(codes.CodeInternal, err.Error()))
		return
	}
	code, body, ct, err := d.Forwarder.Forward(ctx, c, rt.Prefix, addr,
		strconv.FormatInt(userID, 10), strings.Join(roles, ","))
	if err != nil {
		d.Log.WarnContext(ctx, "gateway: forward failed", logx.Err(err))
		c.AbortWithStatusJSON(http.StatusBadGateway, codes.Fail(codes.CodeInternal, "upstream error"))
		return
	}
	c.Response.SetStatusCode(code)
	c.Response.SetBody(body)
	if ct != "" {
		c.Response.Header.Set("Content-Type", ct)
	}
}

// bearerToken 取 Authorization: Bearer <token>（缺省空串 → Verify 拒绝 401）
func bearerToken(c *app.RequestContext) string {
	authz := c.Request.Header.Get("Authorization")
	if v, ok := strings.CutPrefix(string(authz), "Bearer "); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// 灰度头透传语义锚点：meta.HeaderGray（X-Gray）入站 → copyInbound 原样出站。
// 路由表本身不感知灰度（寻址走 consul KV，灰度分组由部署侧注册不同实例体现）。
var _ = meta.HeaderGray
