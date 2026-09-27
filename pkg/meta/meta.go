// Package meta 定义跨服务身份透传的 HTTP Header 常量（纯数据，无外部依赖）。
//
// gateway 验签一次后写入这些 Header，下游 BFF/SRV 只读不验（信任网关）。
package meta

// HeaderUserID 网关验签后注入的用户 ID（下游 srv 跨域透传身份）
const HeaderUserID = "X-User-Id"

// HeaderTenant 多租户标识
const HeaderTenant = "X-Tenant"

// HeaderGray 灰度分组标识（网关按路由表透传）
const HeaderGray = "X-Gray"
