# BFF×4 + job 补全与全链路 trace 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 修复现有链路缺口并实现 4 个 BFF 与 job，使 gateway → BFF 各层 → SRV（含 DB/缓存/MQ 命令）在开启统一 trace 开关后于 Jaeger 全链路同 trace 可见（≥6 服务），并交付 bring-up.sh / e2e-flow.py / trace-matrix.py / README / PROMPT / trace.stdout.yaml / matrix-report.md。

**架构：** 以已评审设计 `docs/superpowers/specs/2026-09-30-bff-job-full-tracing-design.md` 为准。装配 7 步公式逐字同构；HTTP 续链经 webx 最小补丁 Extract + gateway HeaderCarrier 注入；RPC 续链经 `pkg/rpcx` 的 kitex obs-opentelemetry suite；Kafka 续链经 message header traceparent；统一 trace 段经 `infra/nacos.LoadUnified` 双 dataId（`hubx.<svc>` + `hubx.trace`）+ `pkg/config.Config.ApplyTrace` 裁决。

**技术栈：** Go 1.27 / kitex v0.16.3 + obs-opentelemetry v0.3.0 / webx（hertz）/ dbx(GORM) / cachex(redis) / mqx(kafka) / cobra v1.10.2 + gocron v2 / OpenTelemetry v1.46 / Jaeger all-in-one（docker compose）。

**规格依据：** `openspec/changes/add-micro-apps-reference/` 全部 Requirement；端口事实源 `docs/superpowers/plans/local-docker-env.md`（gateway :8080，bff-app/web/mini/admin :8091-8094，srv :9011-9015，job 无端口）。

---

## 文件结构（将创建/修改的文件与职责）

**修改（修复包）：**
- `apps/srv-order/internal/rpc/order.go`、`apps/srv-user/internal/rpc/user.go`、`apps/srv-product/internal/rpc/product.go`、`apps/srv-pay/internal/rpc/pay.go`、`apps/srv-coupon/internal/rpc/coupon.go` — guard 返回 span 并 End（修 span 泄漏）
- `../webx/middleware/otel.go`（SDK 仓，唯一特许修改）— 入站 Extract traceparent/tracestate/baggage
- `apps/gateway/internal/proxy/proxy.go` — HeaderCarrier 整体注入 + hopByHop 补 tracestate/baggage
- `apps/gateway/internal/auth/auth.go`、`apps/gateway/internal/ratelimit/limiter.go` — middleware 层 span
- `apps/srv-user/internal/repository/user.go` — tx.Exec → tx.Create（SQL span 盲区）
- `apps/srv-order/internal/service/outbox.go`、`apps/srv-coupon/internal/service/coupon.go` — messaging 续链 span（traceOn 门控）
- `apps/srv-order/cmd/srv-order/main.go` 等 6 个 main + `infra/nacos/loader.go` + `pkg/config/trace.go` + `infra/telemetry/provider.go` — 双 dataId 装配、Endpoint/SampleRatio/Exporter 接线
- `openspec/changes/add-micro-apps-reference/` 下 spec 文件 — delta 补充

**创建：**
- `pkg/config/trace.go` 追加 `TraceSource`/`ApplyTrace`（本文件即上面的修改目标，职责：统一 trace 段裁决）
- `apps/bff-app/{config.yaml,cmd/bff-app/main.go,internal/{client/clients.go,cache/display.go,service/aggregate.go,handler/handler.go}}` + 各 `_test.go`
- `apps/bff-web/`、`apps/bff-mini/`、`apps/bff-admin/` 同构文件（按端独立，内容差异见任务 14）
- `apps/job/{config.yaml,cmd/job/main.go,internal/{setup/setup.go,consume/consume.go,retry/retry.go,schedule/schedule.go}}` + 测试
- `scripts/{bring-up.sh,e2e-flow.py,trace-matrix.py}`、`configs/trace.stdout.yaml`、`docs/verify/matrix-report.md`（脚本产出）、`README.md`、`PROMPT.md`

---

## 任务 0：基线 commit（工作树既有迁移改动入库）

工作树中有一批未提交的「2026-09-29 依赖迁移」改动（otelx/traceflag 替换 kitex-framework、On(kind) 语义修正、退出顺序修正、4318 端口切换，见 `openspec/changes/add-micro-apps-reference/tasks.md` 变更记录）。先验证再入库，后续任务全部基于该基线。

- [ ] **步骤 1：验证工作树全绿**

运行：`go build ./... && go vet ./... && gofmt -l . && go test -race ./... 2>&1 | tail -20`
预期：build/vet 退出码 0；gofmt 无输出；测试全 PASS（14 个包）。

- [ ] **步骤 2：入库**

```bash
git add -A
git commit -m "chore: 依赖迁移收尾入库（otelx/traceflag 替换 kitex-framework + On(kind) 语义修正 + 退出顺序修正）"
```

预期：`git status --short` 为空（`docs/case-study.md`、`pkg/rpcx/` 一并入库）。

## 任务 1：修复 srv rpc 桩 span 泄漏（5 服务）

**文件：**
- 修改：`apps/srv-order/internal/rpc/order.go:36-42`（guard）及各方法调用点
- 修改：`apps/srv-user/internal/rpc/user.go`、`apps/srv-product/internal/rpc/product.go`、`apps/srv-pay/internal/rpc/pay.go`、`apps/srv-coupon/internal/rpc/coupon.go` 同构位置
- 测试：上述 5 个包各新增 `rpc_span_test.go`

现状：`guard` 内 `ctx, _ = telemetry.Start(...)`，span 永不 End → 不导出，且子 span parent 悬空（Jaeger missing-parent）。

- [ ] **步骤 1：srv-order 写失败测试**

创建 `apps/srv-order/internal/rpc/rpc_span_test.go`：

```go
package rpc

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/infra/telemetry"
)

// TestGuardSpanEnds：guard 产出的分层 span 必须真实导出（End 后可见），
// 回归「ctx, _ = telemetry.Start」span 泄漏缺陷。
func TestGuardSpanEnds(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	telemetry.SetTracerForTest(tp.Tracer("test")) // 见步骤 3 注入点

	s := &Server{} // Limiter 为 nil 时 guard 直接放行
	ctx, span, ok := s.guard(context.Background(), "CreateOrder", 1)
	if !ok {
		t.Fatal("guard should allow when limiter is nil")
	}
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 exported span, got %d", len(spans))
	}
	if name := spans[0].Name; name != "service.order.CreateOrder" {
		t.Fatalf("unexpected span name %q", name)
	}
	_ = ctx
}
```

- [ ] **步骤 2：运行确认失败**

运行：`go test ./apps/srv-order/internal/rpc/ -run TestGuardSpanEnds -v`
预期：编译失败——`telemetry.SetTracerForTest` 未定义、`s.guard` 只返回 2 个值。这就是红。

- [ ] **步骤 3：实现（telemetry 注入点 + guard 修复）**

`infra/telemetry/layers.go` 追加测试注入点：

```go
// SetTracerForTest 仅供单测替换全局 tracer（infra 层不做对外承诺的私有测试钩子）。
func SetTracerForTest(tr trace.Tracer) { current = tr }
```

`apps/srv-order/internal/rpc/order.go` 改 guard 签名：

```go
// guard 方法入口公共逻辑：分层 span（调用方必须 defer span.End()）+ 限流。
func (s *Server) guard(ctx context.Context, method string, userID int64) (context.Context, trace.Span, bool) {
	ctx, span := telemetry.Start(ctx, "service", "order", method)
	if s.Limiter != nil && !s.Limiter.Allow(ctx, userID) {
		return ctx, span, false
	}
	return ctx, span, true
}
```

文件头 import 增加 `"go.opentelemetry.io/otel/trace"`。每个方法改为：

```go
func (s *Server) CreateOrder(ctx context.Context, req *orderpb.CreateOrderReq) (*orderpb.CreateOrderResp, error) {
	resp := &orderpb.CreateOrderResp{UserId: req.GetUserId()}
	ctx, span, ok := s.guard(ctx, "CreateOrder", req.GetUserId())
	defer span.End()
	if !ok {
		resp.Code, resp.Message = rateLimited()
		return resp, nil
	}
	return s.Svc.CreateOrder(ctx, req)
}
```

`GetOrder`/`Report`/`ListOrders` 同改（方法名与 span op 一一对应）。

- [ ] **步骤 4：srv-order 测试转绿**

运行：`go test ./apps/srv-order/internal/rpc/ -run TestGuardSpanEnds -v`
预期：PASS。

- [ ] **步骤 5：其余 4 个 srv 同构修复 + 各写同名测试**

- `apps/srv-user/internal/rpc/user.go` guard entity 为 `"user"`，span 名 `service.user.<Method>`；测试断言对应名字。
- `apps/srv-product/internal/rpc/product.go` entity `"product"`；`apps/srv-pay/internal/rpc/pay.go` entity `"pay"`；`apps/srv-coupon/internal/rpc/coupon.go` entity `"coupon"`。
- 每个包创建 `rpc_span_test.go`，结构与 srv-order 版相同（改 import 的包内类型 `Server` 与 span 名）。

运行：`go test ./apps/srv-user/internal/rpc/ ./apps/srv-product/internal/rpc/ ./apps/srv-pay/internal/rpc/ ./apps/srv-coupon/internal/rpc/ -v`
预期：各包 span 测试 PASS。

- [ ] **步骤 6：全仓回归**

运行：`go build ./... && go vet ./... && go test -race ./...`
预期：全绿。

- [ ] **步骤 7：Commit**

```bash
git add apps/srv-order/internal/rpc apps/srv-user/internal/rpc apps/srv-product/internal/rpc apps/srv-pay/internal/rpc apps/srv-coupon/internal/rpc infra/telemetry/layers.go
git commit -m "fix: srv rpc 桩分层 span 泄漏（guard 返回 span 并 defer End，Jaeger missing-parent 消除）"
```

## 任务 2：webx 入站 Extract 补丁（SDK 特许最小修改）

**文件：**
- 修改：`../webx/middleware/otel.go`（Otel 函数开头注入 Extract；hopByHop 语义不涉及）
- 创建：`../webx/middleware/otel_extract_test.go`

依据 `implementation-guardrails`：确属 SDK 缺陷（入站无视 W3C traceparent），走"说明 + README 记录 + 最小修改"。

- [ ] **步骤 1：写失败测试**

创建 `../webx/middleware/otel_extract_test.go`：

```go
package middleware

import (
	"context"
	"net/http"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"go.opentelemetry.io/otel/propagation"
)

// TestOtelExtractsTraceparent：入站带合法 traceparent 时，Otel 产出的 span
// 必须是该上游 trace 的子级（TraceID 一致）——gateway→BFF 续链的落点。
func TestOtelExtractsTraceparent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	// 上游 trace：00-<tid>-<sid>-01
	const tid = "0af7651916cd43dd8448eb211c80319c"
	const sid = "b7ad6b7169203331"
	req := protocol.Request{}
	req.Header.Set("traceparent", "00-"+tid+"-"+sid+"-01")

	c := app.RequestContext{}
	c.Request = req

	h := Otel(tp.Tracer("test"))
	h(context.Background(), &c)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if got := spans[0].SpanContext.TraceID().String(); got != tid {
		t.Fatalf("span trace id %q not continued from inbound traceparent %q", got, tid)
	}
	if spans[0].Parent.SpanID().String() != sid {
		t.Fatalf("parent span id not extracted from traceparent")
	}
}

var _ = http.MethodGet
var _ = propagation.TraceContext{}
```

- [ ] **步骤 2：运行确认失败**

运行：`cd ../webx && go test ./middleware/ -run TestOtelExtractsTraceparent -v`
预期：FAIL（TraceID 是随机新值，不等于 tid）。

- [ ] **步骤 3：最小补丁**

`../webx/middleware/otel.go` 的 `Otel` 函数体第一行（`seedInboundRequestID` 之前）插入：

```go
	// 入站 W3C 续链：gateway/上游经传播器注入的 traceparent/tracestate/baggage
	// 先 Extract 挂到上游 trace 下；无这些头时行为不变（走下方 X-Request-Id
	// 种子或 SDK 新生成）。Extract 只读不写，幂等无副作用。
	ctx = otel.GetTextMapPropagator().Extract(ctx, extractCarrier(c))
```

并在同文件追加：

```go
// extractCarrier 收集入站请求中与分布式追踪相关的头（大小写不敏感归一为小写键，
// 与 Composite(TraceContext,Baggage) 传播器的 Get 行为对齐）。
func extractCarrier(c *app.RequestContext) propagation.MapCarrier {
	carrier := propagation.MapCarrier{}
	c.Request.Header.VisitAll(func(k, v []byte) {
		switch strings.ToLower(string(k)) {
		case "traceparent", "tracestate", "baggage":
			carrier[strings.ToLower(string(k))] = string(v)
		}
	})
	return carrier
}
```

import 增加 `"strings"`、`"go.opentelemetry.io/otel"`、`"go.opentelemetry.io/otel/propagation"`（`otel` 若未导入）。

- [ ] **步骤 4：测试转绿 + webx 全量回归**

运行：`cd ../webx && go test ./... 2>&1 | tail -20`
预期：middleware 新测试 PASS，webx 原有测试无回归。

- [ ] **步骤 5：gateway 回归（webx 消费方）**

运行：`go build ./apps/gateway/... && go test ./apps/gateway/...`
预期：全绿（gateway 入站来自客户端、通常无 traceparent，Extract 不改变其行为）。

- [ ] **步骤 6：SDK 仓 commit + 记录缺陷**

```bash
cd ../webx && git add middleware/otel.go middleware/otel_extract_test.go && \
git commit -m "fix: Otel 中间件入站 Extract W3C traceparent 续链（原只认 X-Request-Id，HTTP 入站必断链）"
```

在仓库根 `README.md` 排查清单预留条目（任务 25 成文，此处先记进任务 25 的素材）：现象=HTTP 入站开新 trace、gateway→BFF 断链；根因=webx Otel 未 Extract；对策=最小补丁 + 测试。

## 任务 3：gateway 出站注入完整化

**文件：**
- 修改：`apps/gateway/internal/proxy/proxy.go:45-52`（hopByHop）、`:128-134`（注入）
- 测试：`apps/gateway/internal/proxy/proxy_test.go`（追加）

- [ ] **步骤 1：追加失败测试**

在 `apps/gateway/internal/proxy/proxy_test.go` 追加：

```go
// TestForwardInjectsFullW3CHeaders：转发时 traceparent/tracestate/baggage 三个头
// 必须同时注入下游，且入站旧的 tracestate/baggage 不得漏到下游。
func TestForwardInjectsFullW3CHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := NewForwarder("127.0.0.1:8500", true)
	rc := newRequestCtx("POST", "/api/app/orders", []byte(`{}`))
	// 入站残留旧值：不得透传给下游
	rc.Request.Header.Set("tracestate", "old=vendor")
	rc.Request.Header.Set("baggage", "old=bag")

	code, _, _, err := f.Forward(context.Background(), rc, "/api/app", hostOf(srv), "7", "admin")
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if got.Get("traceparent") == "" {
		t.Fatal("traceparent not injected")
	}
	if got.Get("tracestate") == "old=vendor" || got.Get("baggage") == "old=bag" {
		t.Fatal("stale inbound tracestate/baggage leaked downstream")
	}
}
```

（`newRequestCtx`/`hostOf` 若测试文件已有同名 helper 直接复用；否则在文件内补：

```go
func newRequestCtx(method, path string, body []byte) *app.RequestContext {
	rc := &app.RequestContext{}
	rc.Request.SetMethod(method)
	rc.Request.SetRequestURI(path)
	rc.Request.SetBody(body)
	return rc
}

func hostOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}
```

若已有定义则跳过补充。）

- [ ] **步骤 2：运行确认失败**

运行：`go test ./apps/gateway/internal/proxy/ -run TestForwardInjectsFullW3CHeaders -v`
预期：FAIL——当前实现丢 tracestate/baggage，且旧值漏到下游（断言命中）。

- [ ] **步骤 3：实现**

`proxy.go` hopByHop 表追加两个键：

```go
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
```

注入段替换为：

```go
	if f.traceOn {
		// 整体注入 traceparent/tracestate/baggage（Composite 传播器的完整字段集），
		// 替代原先只手写 traceparent 的单头注入。
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(outReq.Header))
	}
```

`propagation.MapCarrier` 相关旧代码删除；`otel` import 保留（HeaderCarrier 同属 propagation 包，已导入）。

- [ ] **步骤 4：测试转绿 + 回归**

运行：`go test ./apps/gateway/... -v`
预期：新测试 PASS，proxy/auth/ratelimit/routing 原测试无回归。

- [ ] **步骤 5：gateway 中间件层 span**

`apps/gateway/internal/auth/auth.go` 的 `Verify` 方法体开头与结尾加：

```go
	ctx, span := telemetry.Start(ctx, "middleware", "gateway", "auth")
	defer span.End()
```

`apps/gateway/internal/ratelimit/limiter.go` 的 `Allow` 同理（op `"ratelimit"`）。两文件 import `hubx/micro_apps/infra/telemetry`。

运行：`go build ./apps/gateway/... && go test ./apps/gateway/...`
预期：全绿。

- [ ] **步骤 6：Commit**

```bash
git add apps/gateway/internal/proxy apps/gateway/internal/auth apps/gateway/internal/ratelimit
git commit -m "feat(gateway): 出站 W3C 头完整注入 + hopByHop 剥离 tracestate/baggage + 中间件层 span"
```

## 任务 4：srv-user 注册事务 SQL span 盲区

**文件：**
- 修改：`apps/srv-user/internal/repository/user.go:93-97`（tx.Exec → tx.Create）
- 测试：`apps/srv-user/internal/repository/user_test.go`（追加）

- [ ] **步骤 1：追加失败测试**

在 `apps/srv-user/internal/repository/user_test.go` 追加（该文件已有 sqlite 内存库测试基建，复用其 openTestDB/helper 名称以现有文件为准）：

```go
// TestInsertUserProfileEmitsSQLSpan：user_profiles 写入必须走 GORM 回调
// （tx.Create）产出 db.create span——回归 tx.Exec 绕过回调的盲区。
func TestInsertUserProfileEmitsSQLSpan(t *testing.T) {
	repo, cleanup := newTestRepo(t) // 复用现有测试 helper（若名称不同以文件内为准）
	defer cleanup()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	ctx := ContextWithTracer(context.Background(), tp) // 复用现有 ctx 注入 helper

	if _, err := repo.InsertUser(ctx, 0, "spancheck", "hash", "13800000001"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var sawProfile bool
	for _, sp := range exporter.GetSpans() {
		for _, attr := range sp.Attributes {
			if attr.Key == "db.sql.table" && attr.Value.AsString() == "user_profiles" {
				sawProfile = true
			}
		}
	}
	if !sawProfile {
		t.Fatal("no db span for user_profiles insert (tx.Exec bypassed GORM callbacks)")
	}
}
```

import 增加 `sdktrace "go.opentelemetry.io/otel/sdk/trace"` 与 `"go.opentelemetry.io/otel/sdk/trace/tracetest"`。若现有测试文件没有 tracer 注入 helper，则在该文件补：

```go
type tracerKey struct{}

func ContextWithTracer(ctx context.Context, tp *sdktrace.TracerProvider) context.Context {
	return context.WithValue(ctx, tracerKey{}, tp)
}
```

并实现 dbx 回调读取该 tracer 的桥接——**若现有 dbx 回调只认全局 provider，则改用全局替换**：测试内 `otel.SetTracerProvider(tp)` 后再 `dbx` 重新取 tracer 的方式不可行时，退化为：用全局 provider 断言（在 TestMain 或测试开头 `otel.SetTracerProvider(tp)`，openTestDB 的 GORM 回调从全局取 tracer 即生效）。以现有文件基建为准选择其一，测试意图不变。

- [ ] **步骤 2：运行确认失败**

运行：`go test ./apps/srv-user/internal/repository/ -run TestInsertUserProfileEmitsSQLSpan -v`
预期：FAIL——tx.Exec 不产生任何 user_profiles 的 db span。

- [ ] **步骤 3：实现**

`user.go` 的 `InsertUser` 内替换：

```go
		if err := tx.Exec(
			"INSERT INTO user_profiles (user_id, avatar, bio, city, level) VALUES (?, '', '', '', 1)",
			id,
		).Error; err != nil {
			return fmt.Errorf("insert profile: %w", err)
		}
```

为：

```go
		// 走 GORM create 回调（而非 tx.Exec 裸 SQL）：dbx 的 OTel 埋点挂在
		// create 回调组上，裸 Exec 会绕过埋点让这条 INSERT 在 Jaeger 不可见。
		prof := UserProfile{UserID: id, Level: 1}
		if err := tx.Create(&prof).Error; err != nil {
			return fmt.Errorf("insert profile: %w", err)
		}
```

（`UserProfile` 模型 struct 以 `apps/srv-user/internal/repository` 现有模型为准——grep 该目录确认模型名与字段；若模型文件缺 `Level` 字段则按现有字段补零值创建，目标只有一条：走 create 回调。）

- [ ] **步骤 4：测试转绿 + 回归**

运行：`go test ./apps/srv-user/... -v && go build ./...`
预期：全绿。

- [ ] **步骤 5：Commit**

```bash
git add apps/srv-user/internal/repository
git commit -m "fix(srv-user): 注册事务 profile 写入改走 GORM create 回调，补齐 SQL span 盲区"
```

## 任务 5：outbox / coupon messaging 续链 span

**文件：**
- 修改：`apps/srv-order/internal/service/outbox.go`（OutboxRunner 加 traceOn、sendOne 续链）
- 修改：`apps/srv-order/cmd/srv-order/main.go`（NewOutboxRunner 调用点传 traceOn）
- 修改：`apps/srv-coupon/internal/service/coupon.go`（同构：构造器加 traceOn、flushOnce 发送处续链）
- 修改：`apps/srv-coupon/cmd/srv-coupon/main.go`（调用点）
- 测试：`apps/srv-order/internal/service/outbox_test.go`、`apps/srv-coupon/internal/service/` 对应测试（追加）

语义：`Trace.On("mq")` 门控（与 messaging span 同闸，规格 `trace-propagation`：mq 关时无 span、无 header——traceparent 为空已由写入侧保证，此处只门控 span 与 Extract）。

- [ ] **步骤 1：srv-order 失败测试**

`outbox_test.go` 追加：

```go
// TestSendOneContinuesTrace：outbox 行内持久化的 traceparent 必须让 send span
// 挂回下单链路（回归 context.Background() 丢链）。
func TestSendOneContinuesTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	telemetry.SetTracerForTest(tp.Tracer("test"))

	repo := &repository.OrderRepository{} // sendOne 只用 ev，不触 repo
	r := NewOutboxRunner(repo, &fakeProducer{}, logx.GetGlobal(), true)

	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	ev := &repository.OutboxEvent{
		Topic:       "order.completed.v1",
		Payload:     `{"order_id":1,"user_id":1,"schema_ver":1}`,
		Traceparent: "00-" + tid + "-00f067aa0ba902b7-01",
	}
	if err := r.sendOne(context.Background(), ev); err != nil {
		t.Fatalf("sendOne: %v", err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "messaging.outbox.send" {
		t.Fatalf("expected messaging.outbox.send span, got %+v", spans)
	}
	if got := spans[0].SpanContext.TraceID().String(); got != tid {
		t.Fatalf("send span not continued from outbox traceparent: %s != %s", got, tid)
	}
}

type fakeProducer struct{ mqx.Producer }

func (f *fakeProducer) Send(ctx context.Context, msg *mqx.Message) error { return nil }
```

（若包内已有 fakeProducer 同名类型则复用。）

- [ ] **步骤 2：运行确认失败**

运行：`go test ./apps/srv-order/internal/service/ -run TestSendOneContinuesTrace -v`
预期：编译失败（NewOutboxRunner 第 4 参、span 名均不存在）= 红。

- [ ] **步骤 3：实现 srv-order**

`outbox.go`：

1. `OutboxRunner` struct 加字段 `traceOn bool`；`NewOutboxRunner(repo, producer, logger, traceOn bool)`（logger 为 nil 时回退全局的现有逻辑保留）。
2. `sendOne` 改为：

```go
func (r *OutboxRunner) sendOne(ctx context.Context, ev *repository.OutboxEvent) error {
	// key 取事件内嵌的订单 ID（消费端按订单分区去重）；解析失败回退 outbox.id
	key := fmt.Sprintf("%d", ev.ID)
	if parsed, err := events.UnmarshalOrderCompleted([]byte(ev.Payload)); err == nil && parsed.OrderID > 0 {
		key = fmt.Sprintf("%d", parsed.OrderID)
	}
	msg := &mqx.Message{
		Topic:   ev.Topic,
		Key:     key,
		Value:   []byte(ev.Payload),
		Headers: map[string]string{},
	}
	if ev.Traceparent != "" {
		msg.Headers[events.TraceHeaderKey] = ev.Traceparent
	}
	// messaging 续链：mq 开关开时，从 outbox 行内持久化的 traceparent 重建
	// 下单链路 ctx，send span 挂回原 trace（send/receive 同 trace 的断言锚点）；
	// mq 关时（traceparent 必为空串）退化为无 span 的裸发送。
	if r.traceOn && ev.Traceparent != "" {
		ctx = otel.GetTextMapPropagator().Extract(ctx,
			propagation.MapCarrier{events.TraceHeaderKey: ev.Traceparent})
		ctx, span := telemetry.Start(ctx, "messaging", "outbox", "send")
		defer span.End()
		span.SetAttributes(attribute.String("messaging.topic", ev.Topic))
	}
	return r.producer.Send(ctx, msg)
}
```

import 增加 `otel`、`propagation`、`attribute`、`hubx/micro_apps/infra/telemetry`。

3. `apps/srv-order/cmd/srv-order/main.go` 调用点：`NewOutboxRunner(repo, producer, logger, pcfg.Trace.On("mq"))`。

- [ ] **步骤 4：测试转绿**

运行：`go test ./apps/srv-order/internal/service/ -v`
预期：PASS（含既有 abandonCandidates 回归用例）。

- [ ] **步骤 5：srv-coupon 同构实现**

`coupon.go`：构造器加 `traceOn bool` 字段；`flushOnce` 内 `producer.Send(context.Background(), msg)` 处改为（发送循环内的局部 ctx）：

```go
		sendCtx := context.Background()
		if r.traceOn && ev.Traceparent != "" {
			sendCtx = otel.GetTextMapPropagator().Extract(sendCtx,
				propagation.MapCarrier{events.TraceHeaderKey: ev.Traceparent})
			sendCtx, span := telemetry.Start(sendCtx, "messaging", "outbox", "send")
			span.SetAttributes(attribute.String("messaging.topic", ev.Topic))
			defer span.End() // 循环内逐条：End 随当轮迭代结束（单条粒度足够演示）
		}
		if serr := r.producer.Send(sendCtx, msg); serr != nil {
```

`apps/srv-coupon/cmd/srv-coupon/main.go` 调用点传 `pcfg.Trace.On("mq")`。测试：coupon service 包内追加与 srv-order 同构的「send 挂回原 trace」用例（fakeProducer 复用该包现有测试基建）。

运行：`go test ./apps/srv-coupon/... -v && go build ./...`
预期：全绿。

- [ ] **步骤 6：Commit**

```bash
git add apps/srv-order apps/srv-coupon
git commit -m "feat: outbox 补发 messaging 续链（Extract 行内 traceparent 挂回原链路，mq 开关同闸门控）"
```

## 任务 6：统一 trace 配置接线（双 dataId + Endpoint/SampleRatio/Exporter）

**文件：**
- 修改：`infra/nacos/loader.go`（追加 `LoadUnified`）
- 修改：`pkg/config/trace.go`（追加 `TraceSource` 接口 + `ApplyTrace`；更新 Endpoint 字段「未接线」注释）
- 修改：`infra/telemetry/provider.go`（Options 加 `SampleRatio`/`Exporter`，采样器与导出器接线）
- 修改：6 个现有 main（gateway、srv-user/product/order/pay/coupon）：`LoadUnified` + `ApplyTrace` + `Endpoint/SampleRatio/Exporter` 透传、删 `jaegerEndpoint` 常量
- 测试：`pkg/config/trace_test.go`（追加表驱动）、`infra/nacos/loader_test.go`（追加，如该文件不存在则创建）

- [ ] **步骤 1：pkg/config 失败测试**

`pkg/config/trace_test.go` 追加：

```go
// fakeTraceSource 内存实现 TraceSource。
type fakeTraceSource struct {
	set   map[string]bool
	bools map[string]bool
	raw   map[string]Trace
}

func (f *fakeTraceSource) IsSet(key string) bool        { return f.set[key] }
func (f *fakeTraceSource) GetBool(key string) bool      { return f.bools[key] }
func (f *fakeTraceSource) Unmarshal(key string, v any) error {
	*(v.(*Trace)) = f.raw[key]
	return nil
}

func TestApplyTracePrimaryWins(t *testing.T) {
	c := Default()
	primary := &fakeTraceSource{
		set: map[string]bool{"trace": true, "trace.db": true},
		raw: map[string]Trace{{Enabled: true, Exporter: "otlp", Endpoint: "127.0.0.1:4318", DB: false, Cache: true, MQ: true}},
	}
	fallback := &fakeTraceSource{set: map[string]bool{"trace": true},
		raw: map[string]Trace{{Enabled: true, Exporter: "stdout", Endpoint: "", DB: true, Cache: true, MQ: true}}}
	c.ApplyTrace(primary, fallback, "bff-app")
	if c.Trace.DB {
		t.Fatal("hubx.trace trace.db=false must win over fallback")
	}
	if c.Trace.Exporter != "otlp" {
		t.Fatal("trace section must come from primary (hubx.trace)")
	}
}

func TestApplyTraceFallbackWhenPrimaryAbsent(t *testing.T) {
	c := Default()
	primary := &fakeTraceSource{set: map[string]bool{}} // 无 trace 段
	fallback := &fakeTraceSource{set: map[string]bool{"trace": true},
		raw: map[string]Trace{{Enabled: true, Exporter: "stdout", Endpoint: "", DB: true, Cache: true, MQ: true}}}
	c.ApplyTrace(primary, fallback, "bff-app")
	if c.Trace.Exporter != "stdout" {
		t.Fatal("fallback local trace section must apply when hubx.trace absent")
	}
}

func TestApplyTraceOfficialSegmentFallback(t *testing.T) {
	c := Default()
	primary := &fakeTraceSource{set: map[string]bool{}} // 无 trace 段
	fallback := &fakeTraceSource{
		set:   map[string]bool{"trace": true}, // trace 段无 db 键 → 回退官方段
		bools: map[string]bool{SQLXTraceKey("bff-app"): false},
		raw:   map[string]Trace{{Enabled: true, Exporter: "otlp", Endpoint: "127.0.0.1:4318", DB: true, Cache: true, MQ: true}},
	}
	c.ApplyTrace(primary, fallback, "bff-app")
	if c.Trace.DB {
		t.Fatal("official segment enable_trace=false must fall through when trace.db unset")
	}
}
```

- [ ] **步骤 2：运行确认失败**

运行：`go test ./pkg/config/ -run TestApplyTrace -v`
预期：编译失败（ApplyTrace 未定义）= 红。

- [ ] **步骤 3：实现 pkg/config**

`trace.go` 追加：

```go
// TraceSource 统一 trace 段的来源句柄（configx.Config 天然满足）。
// 双 dataId 形态下 primary = hubx.trace 句柄，fallback = 服务私有句柄。
type TraceSource interface {
	Switches
	// Unmarshal 解出 key 段的原始内容（与 configx.Config.Unmarshal 同签名）
	Unmarshal(key string, rawVal any) error
}

// ApplyTrace 统一 trace 段裁决（openspec unified-trace-switch 的落点）：
//   - primary（Nacos hubx.trace 句柄，可为 nil）显式含 trace 段 → 单一事实源生效；
//   - 否则 fallback（私有句柄）含 trace 段 → 本地段生效；
//   - 都没有 → 保持 Default() 值。
//
// 三段正交回退（Resolve）始终对「实际提供 trace 段的句柄」做，保证
// 「官方段仅在 trace.<kind> 未显式写出时生效」在两条来源下语义一致。
func (c *Config) ApplyTrace(primary TraceSource, fallback Switches, instance string) {
	var src Switches = fallback
	if primary != nil && primary.IsSet("trace") {
		src = primary
	}
	if src != nil {
		if ts, ok := src.(TraceSource); ok && ts.IsSet("trace") {
			_ = ts.Unmarshal("trace", &c.Trace)
		}
		c.Trace.Resolve(src, instance)
	}
}
```

同时把 `Trace.Endpoint` 字段注释中「⚠ 已知未接线……」整段替换为：

```go
	// Endpoint 导出端点（OTLP/HTTP，对应 Jaeger/collector 的 4318 端口）。
	// 由统一 trace 段（Nacos hubx.trace / 本地 trace 段）供给，
	// 装配方（telemetry.Init）必须读本字段而非硬编码常量。
```

- [ ] **步骤 4：pkg/config 转绿**

运行：`go test ./pkg/config/ -v`
预期：3 个新用例 PASS，原 7 组合表驱动用例无回归。

- [ ] **步骤 5：telemetry.Init 接线**

`infra/telemetry/provider.go`：
1. `Options` 追加两字段：

```go
	// SampleRatio 采样率 0~1（0 = 不采样，1 = 全采；trace 段 sample_ratio 供给）。
	// 默认 0 视为 1（缺省全采，与既有 always_on 行为兼容）。
	SampleRatio float64
	// Exporter 导出类型：otlp / stdout / none（trace 段 exporter 供给；空则按
	// Endpoint 是否为空裁决 otlp/none，保持旧行为）。
	Exporter string
```

2. Init 内导出器与采样器段替换为：

```go
	exported := strings.ToLower(strings.TrimSpace(opts.Exporter))
	if exported == "" {
		exported = "none"
		if opts.Endpoint != "" {
			exported = "otlp"
		}
	}
	ratio := opts.SampleRatio
	if ratio <= 0 {
		ratio = 1
	}
	sampler := otelx.SamplerConfig{Type: "always_on"}
	if ratio < 1 {
		sampler = otelx.SamplerConfig{Type: "traceidratio", Ratio: ratio}
	}
	p, err := otelx.New(&otelx.Config{
		ServiceName: opts.ServiceName,
		Tracer: otelx.TracerConfig{
			EnableTrace: true,
			Exported:    exported,
			Endpoint:    endpointWithScheme(opts.Endpoint, opts.Insecure),
		},
		Sampler: sampler,
		Meter:   otelx.MeterConfig{Enabled: false},
	})
```

（`exported` 变量原定义处删除；`strings` 加入 import。）

- [ ] **步骤 6：loader 双 dataId**

`infra/nacos/loader.go` 追加：

```go
// TraceDataID 统一 trace 配置 dataId（单一事实源；configs/trace.yaml 经
// configs/seed-nacos.sh 灌入）。服务私有 config.yaml 不写 trace 段，避免漂移。
const TraceDataID = "hubx.trace"

// LoadUnified 在私有 dataId 之外叠加统一 trace dataId。
//
// 返回 (私有句柄, trace 句柄, error)：trace 句柄为 nil 表示统一段当前不可用
// （Nacos 未灌入 hubx.trace 或不可达）——此时调用方回退本地/默认，不阻断启动；
// 私有句柄加载失败仍返回 error（与 Load 语义一致：远程开启但不可用必须报错）。
func LoadUnified(ctx context.Context, localPath, dataID, group string) (configx.Config, configx.Config, error) {
	h, err := Load(ctx, localPath, dataID, group)
	if err != nil {
		return nil, nil, err
	}
	ht, terr := Load(ctx, localPath, TraceDataID, group)
	if terr != nil {
		return h, nil, nil
	}
	return h, ht, nil
}
```

loader 包注释首条「单实例单 dataId」 bullet 更新为：「私有 dataId + 统一 trace dataId（hubx.trace，LoadUnified）；服务私有 config.yaml 不写 trace 段」。

- [ ] **步骤 7：6 个现有 main 接线**

每个 main（gateway、srv-user、srv-product、srv-order、srv-pay、srv-coupon）统一三处替换：

(a) 加载段：

```go
	h, err := nacos.Load(ctx, configPath, "hubx.<svc>", "HUBX")
```

改为：

```go
	h, ht, err := nacos.LoadUnified(ctx, configPath, "hubx.<svc>", "HUBX")
```

(b) trace 段裁决段（`pcfg := pbconfig.Default()` 起的 6 行块）替换为：

```go
	// 统一 trace 段：hubx.trace（单一事实源）优先，缺省回退本地/默认；
	// 细分开关三段正交由 ApplyTrace 内 Resolve 完成（唯一开关入口 Trace.On）。
	pcfg := pbconfig.Default()
	pcfg.ApplyTrace(ht, h, serviceName)
```

(c) telemetry.Init 调用替换为：

```go
	traceSetup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    pcfg.Trace.Endpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
		SampleRatio: pcfg.Trace.SampleRatio,
		Exporter:    pcfg.Trace.Exporter,
	})
```

并删除各 main 的 `jaegerEndpoint` 常量及其注释块。

- [ ] **步骤 8：回归 + 双 dataId 行为冒烟**

运行：`go build ./... && go vet ./... && go test -race ./...`
预期：全绿。

运行（docker 底座已就绪前提下）：

```bash
curl -s "http://127.0.0.1:8848/nacos/v1/cs/configs?dataId=hubx.trace&group=HUBX" | head -5
```

预期：返回 configs/trace.yaml 的 trace 段内容（未灌入则先执行 `bash configs/seed-nacos.sh`）。

改 Nacos 验证（重启任一服务后查启动日志的 trace 生效值）：

```bash
curl -X POST "http://127.0.0.1:8848/nacos/v1/cs/configs" \
  -d "dataId=hubx.trace&group=HUBX&content=$(python3 -c "print('trace:\n  enabled: false\n  exporter: otlp\n  endpoint: 127.0.0.1:4318')")"
cd apps/gateway && timeout 5 go run ./cmd/gateway 2>&1 | grep -i trace | head -3
```

预期：日志中 `trace=false`。验后恢复：`bash configs/seed-nacos.sh`。

- [ ] **步骤 9：Commit**

```bash
git add infra/nacos infra/telemetry pkg/config apps/gateway/cmd apps/srv-user/cmd apps/srv-product/cmd apps/srv-order/cmd apps/srv-pay/cmd apps/srv-coupon/cmd
git commit -m "feat: 统一 trace 配置接线（hubx.trace 双 dataId 单一事实源 + endpoint/sample_ratio/exporter 入 telemetry.Init）"
```


---

## 任务 7：bff-app 骨架（config + main）

**文件：**
- 创建：`apps/bff-app/config.yaml`（复制既有预制配置）
- 创建：`apps/bff-app/cmd/bff-app/main.go`
- 创建：`apps/bff-app/internal/`（后续任务填充分层文件）

- [ ] **步骤 1：config.yaml 就位**

```bash
cp configs/services/bff-app.yaml apps/bff-app/config.yaml
cmp configs/services/bff-app.yaml apps/bff-app/config.yaml && echo "identical"
```

预期：输出 `identical`（缓存实例名 `bff-app`，无 sqlx 段——openspec BFF 无 DB 红线）。

- [ ] **步骤 2：main.go**

创建 `apps/bff-app/cmd/bff-app/main.go`：

```go
// bff-app：移动端 BFF（webx 监听 :8091，聚合 srv-user / srv-product /
// srv-order / srv-coupon；无 DB、无 MQ，只挂展示层缓存）。
//
// 装配总公式（7 步，与 11 服务逐字同构，顺序本身有约束，telemetry 必须先于 infra）：
//  1. infra/nacos.LoadUnified(ctx, "config.yaml", "hubx.bff-app", "HUBX")：
//     私有 dataId + 统一 trace dataId（hubx.trace 单一事实源，缺省回退本地/默认）。
//  2. pkg/config Default() 打底 → ApplyTrace 裁决统一 trace 段
//     （细分开关三段正交，唯一开关入口 Trace.On）。
//  3. infra/telemetry.Init（读 Trace.Endpoint/SampleRatio/Exporter/Enabled）。
//     【红线】必须先于第 5 步：cachex 的 OTel 回调从全局 TracerProvider 取 tracer。
//  4. logx。
//  5. infra/drivers：cachex 展示层缓存（Trace.On("cache") 只门控 OTel 回调，
//     不门控构造——缓存不可用降级为无缓存运行）。
//  6. 业务装配：kitex client 桩 ×4（rpcx.BuildClientOptions，TraceEnabled 透传，
//     obs-opentelemetry ClientSuite 负责向 srv 注入 traceparent）+ 分层
//     handler/service/cache。
//  7. infra/consul.Register("127.0.0.1:8500","bff-app","127.0.0.1:8091") →
//     webx.New（路由 + 就绪探针）→ 优雅退出反序 + OTel flush（≥10s 窗口）。
//
// 入站续链：webx Otel 中间件 Extract 入站 traceparent（gateway 注入），
// 本服务 handler/service/cache/rpc client 各层 span 全部挂同一条 trace。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 logx zap provider（必须空导入）
	"github.com/gospacex/webx"

	"hubx/micro_apps/apps/bff-app/internal/cache"
	"hubx/micro_apps/apps/bff-app/internal/client"
	"hubx/micro_apps/apps/bff-app/internal/handler"
	"hubx/micro_apps/apps/bff-app/internal/service"
	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/infra/drivers"
	"hubx/micro_apps/infra/nacos"
	"hubx/micro_apps/infra/telemetry"
	pbconfig "hubx/micro_apps/pkg/config"
)

// 服务常量（端口唯一事实源：docs/superpowers/plans/local-docker-env.md）
const (
	serviceName = "bff-app"
	listenAddr  = "127.0.0.1:8091"
	consulAgent = "127.0.0.1:8500"
	configPath  = "config.yaml" // 与 configs/services/bff-app.yaml 逐字同构（服务自包含）
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("bff-app: %v", err)
	}
}

func run() error {
	// ---------- 1) 配置：私有 dataId + 统一 trace dataId ----------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, ht, err := nacos.LoadUnified(ctx, configPath, "hubx."+serviceName, "HUBX")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	defer func() { _ = h.Close() }()
	if ht != nil {
		defer func() { _ = ht.Close() }()
	}

	// ---------- 2) 统一 trace 段裁决 ----------
	pcfg := pbconfig.Default()
	pcfg.ApplyTrace(ht, h, serviceName)

	// ---------- 3) 调用链（必须先于 5） ----------
	traceSetup, err := telemetry.Init(ctx, telemetry.Options{
		ServiceName: serviceName,
		Endpoint:    pcfg.Trace.Endpoint,
		Insecure:    true,
		Enabled:     pcfg.Trace.Enabled,
		SampleRatio: pcfg.Trace.SampleRatio,
		Exporter:    pcfg.Trace.Exporter,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}

	// ---------- 4) 日志 ----------
	logger, err := logx.New(logx.Config{Level: "info", ServiceName: serviceName})
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	logx.SetGlobal(logger)
	defer func() { _ = logger.Close() }()

	// ---------- 5) 展示层缓存（On("cache") 只门控 span，不门控构造） ----------
	cacheH, err := drivers.NewCache(ctx, h, serviceName, "", pcfg.Trace.On("cache"))
	if err != nil {
		// 缓存不可用降级：无展示层缓存运行（回源直取 srv）
		logger.Warn("cache open failed (display cache disabled)", logx.Err(err))
		cacheH = nil
	}
	var displayCache *cache.DisplayCache
	if cacheH != nil {
		displayCache = cache.New(cacheH.Instance(serviceName), 30*time.Second)
	}

	// ---------- 6) 业务装配：下游客户端 + 分层 ----------
	clients := client.New(serviceName, consulAgent, pcfg.Trace.Enabled, logger)
	agg := service.NewAggregator(clients, displayCache, logger)
	api := handler.NewAPI(agg, logger)

	// 就绪探针：缓存可用时 PING 缓存；缓存故障不摘流量（降级运行形态）
	var readyCheck func(context.Context) error
	if cacheH != nil {
		inst := cacheH.Instance(serviceName)
		readyCheck = func(rctx context.Context) error { return inst.HealthCheck(rctx) }
	}

	// ---------- 7a) 服务注册 ----------
	registered := false
	if rerr := consul.Register(consulAgent, serviceName, listenAddr); rerr != nil {
		logger.Warn("consul register failed (service still running unregistered)", logx.Err(rerr))
	} else {
		registered = true
	}

	// ---------- 7b) webx HTTP（路由 + 探针） ----------
	app, err := webx.New(
		webx.WithConfig(webx.Config{
			Addr:           listenAddr,
			Logger:         logger,
			Tracer:         traceSetup.Tracer,
			MetricsEnabled: true,
		}),
		webx.WithReadyCheck(readyCheck),
		webx.WithPreRun(func(a *webx.App) { handler.Register(a.Engine, api) }),
	)
	if err != nil {
		return fmt.Errorf("webx.New: %w", err)
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "bff-app listening",
			logx.String("addr", listenAddr),
			logx.Any("trace", traceSetup.Enabled))
		srvErr <- app.Run()
	}()

	// ---------- 7c) 优雅退出（反序）：web → consul → cache → OTel flush → logger ----------
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-quit:
		logger.Info("signal received, shutting down", logx.String("signal", s.String()))
	case e := <-srvErr:
		if e != nil {
			logger.Error("server stopped with error", logx.Err(e))
		}
	}
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if serr := app.Shutdown(shutCtx); serr != nil {
		logger.Error("webx shutdown failed", logx.Err(serr))
	}
	shutCancel()
	if registered {
		if derr := consul.Deregister(consulAgent, serviceName, listenAddr); derr != nil {
			logger.Warn("consul deregister failed", logx.Err(derr))
		}
	}
	if cacheH != nil {
		if cerr := cacheH.Close(); cerr != nil {
			logger.Error("cache close failed", logx.Err(cerr))
		}
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if serr := traceSetup.Shutdown(flushCtx); serr != nil {
		logger.Error("tracing shutdown failed", logx.Err(serr))
	}
	flushCancel()
	return nil
}
```

- [ ] **步骤 3：编译检查（预期失败——引用的三个 internal 包还不存在）**

运行：`go build ./apps/bff-app/...`
预期：FAIL，unknown package 错误。继续后续任务补齐。

## 任务 8：bff-app client 层

**文件：**
- 创建：`apps/bff-app/internal/client/clients.go`

- [ ] **步骤 1：实现**

```go
// Package client bff-app 的 kitex 下游客户端装配（srv-user / srv-product /
// srv-order / srv-coupon）。
//
// 目标地址经 infra/consul.Target 读注册中心 KV（hubx/svc/<svc>）；续链由
// rpcx.BuildClientOptions 挂的 obs-opentelemetry ClientSuite 完成（traceparent
// 经 kitex metainfo 自动注入，无需手动）。
//
// 降级语义：任一目标未注册 → 对应客户端为 nil，service 层对 nil 客户端走
// 降级兜底（openspec 2.4），不阻断其余分支。
package client

import (
	"github.com/gospacex/logx"

	"hubx/micro_apps/infra/consul"
	"hubx/micro_apps/pkg/rpcx"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	couponcli "hubx/micro_apps/pkg/kitex_gen/coupon/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	ordercli "hubx/micro_apps/pkg/kitex_gen/order/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	productcli "hubx/micro_apps/pkg/kitex_gen/product/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
	usercli "hubx/micro_apps/pkg/kitex_gen/user/user"
)

// 编译期断言：生成代码接口与本包使用面一致。
var (
	_ usercli.Client    = (usercli.Client)(nil)
	_ productcli.Client = (productcli.Client)(nil)
	_ ordercli.Client   = (ordercli.Client)(nil)
	_ couponcli.Client  = (couponcli.Client)(nil)
)

// Clients 四个下游 srv 客户端集合（nil 字段 = 该下游未注册，调用方须降级）
type Clients struct {
	User    usercli.Client
	Product productcli.Client
	Order   ordercli.Client
	Coupon  couponcli.Client
}

// New 装配四个客户端；单个失败仅告警（降级兜底的前提），整体不返回 error。
func New(self, consulAgent string, traceOn bool, logger logx.Logger) *Clients {
	c := &Clients{}
	if cli, err := dial(consulAgent, "srv-user", func(target string) (any, error) {
		return usercli.NewClient("User", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-app: srv-user unavailable (degraded)", logx.Err(err))
	} else {
		c.User = cli.(usercli.Client)
	}
	if cli, err := dial(consulAgent, "srv-product", func(target string) (any, error) {
		return productcli.NewClient("Product", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-app: srv-product unavailable (degraded)", logx.Err(err))
	} else {
		c.Product = cli.(productcli.Client)
	}
	if cli, err := dial(consulAgent, "srv-order", func(target string) (any, error) {
		return ordercli.NewClient("Order", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-app: srv-order unavailable (degraded)", logx.Err(err))
	} else {
		c.Order = cli.(ordercli.Client)
	}
	if cli, err := dial(consulAgent, "srv-coupon", func(target string) (any, error) {
		return couponcli.NewClient("Coupon", rpcx.BuildClientOptions(rpcx.ClientOptions{
			ServiceName: self, Target: target, ConnType: "short", TraceEnabled: traceOn,
		})...)
	}); err != nil {
		logger.Warn("bff-app: srv-coupon unavailable (degraded)", logx.Err(err))
	} else {
		c.Coupon = cli.(couponcli.Client)
	}
	return c
}

// dial 解析目标并建连；错误向上返回（调用方决定降级策略）。
func dial(consulAgent, service string, factory func(target string) (any, error)) (any, error) {
	target, err := consul.Target(consulAgent, service)
	if err != nil {
		return nil, err
	}
	return factory(target)
}
```

（`_ usercli.Client = (usercli.Client)(nil)` 这类断言恒真、无意义——若书写时意识到这一点，删除该断言块，保留结构注释即可。）

- [ ] **步骤 2：编译检查**

运行：`go build ./apps/bff-app/internal/client/`
预期：PASS（唯一可独立编译的分层；其余包待补）。

## 任务 9：bff-app cache 层（命中/未命中两种 span）

**文件：**
- 创建：`apps/bff-app/internal/cache/display.go`
- 测试：`apps/bff-app/internal/cache/display_test.go`

- [ ] **步骤 1：实现**

```go
// Package cache 展示层缓存：cachex 实例上的 JSON 读写 + 命中/未命中分层 span。
//
// 命中/未命中两种 span（openspec tasks 2.4 冒烟锚点）：两种路径都出
// service.display.get span，以 cache.hit 属性区分；底层 redis 命令另由
// cachex OTel 回调出 span（Trace.On("cache") 门控）。
package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gospacex/cachex"
	"go.opentelemetry.io/otel/attribute"

	"hubx/micro_apps/infra/telemetry"
)

// DisplayCache 展示层 JSON 缓存
type DisplayCache struct {
	inst cachex.Cache
	ttl  time.Duration
}

// New 构造（inst 允许 nil——缓存不可用时 Get 恒未命中、Set no-op）
func New(inst cachex.Cache, ttl time.Duration) *DisplayCache {
	return &DisplayCache{inst: inst, ttl: ttl}
}

// Get 读缓存：命中返回 true 并反序列化进 dest；未命中/缓存不可用/数据损坏
// 返回 false（三种未命中形态都以 cache.hit=false 区分记录）。
func (c *DisplayCache) Get(ctx context.Context, key string, dest any) bool {
	ctx, span := telemetry.Start(ctx, "service", "display", "get")
	defer span.End()
	span.SetAttributes(attribute.String("cache.key", key))
	if c.inst == nil {
		span.SetAttributes(attribute.Bool("cache.hit", false), attribute.Bool("cache.disabled", true))
		return false
	}
	raw, err := c.inst.Get(ctx, key)
	if err != nil || raw == "" {
		span.SetAttributes(attribute.Bool("cache.hit", false))
		return false
	}
	if err := json.Unmarshal([]byte(raw), dest); err != nil {
		span.SetAttributes(attribute.Bool("cache.hit", false), attribute.Bool("cache.corrupt", true))
		return false
	}
	span.SetAttributes(attribute.Bool("cache.hit", true))
	return true
}

// Set 写缓存（JSON 序列化；任何失败静默——展示层缓存不阻断主流程）
func (c *DisplayCache) Set(ctx context.Context, key string, v any) {
	if c.inst == nil {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	_ = c.inst.Set(ctx, key, string(b), c.ttl)
}
```

- [ ] **步骤 2：失败测试**

创建 `apps/bff-app/internal/cache/display_test.go`：

```go
package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/infra/telemetry"
)

type fakeCache struct {
	get func(ctx context.Context, key string) (string, error)
	set func(ctx context.Context, key, value string, ttl time.Duration) error
}

func (f *fakeCache) Get(ctx context.Context, key string) (string, error) {
	return f.get(ctx, key)
}
func (f *fakeCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return f.set(ctx, key, value, ttl)
}
func (f *fakeCache) Delete(ctx context.Context, key string) error { return nil }
func (f *fakeCache) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}
func (f *fakeCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	return nil, nil
}
func (f *fakeCache) Close() error { return nil }

// TestGetHitAndMissSpans：命中/未命中两种 span 形态（cache.hit 属性区分）。
func TestGetHitAndMissSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	telemetry.SetTracerForTest(tp.Tracer("test"))

	c := New(&fakeCache{
		get: func(ctx context.Context, key string) (string, error) {
			if key == "hit" {
				return `{"v":1}`, nil
			}
			return "", errors.New("miss")
		},
		set: func(ctx context.Context, key, value string, ttl time.Duration) error { return nil },
	}, time.Minute)

	var dst struct {
		V int `json:"v"`
	}
	if !c.Get(context.Background(), "hit", &dst) || dst.V != 1 {
		t.Fatal("hit path should unmarshal cached value")
	}
	if c.Get(context.Background(), "miss", &dst) {
		t.Fatal("miss path should return false")
	}

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans (hit+miss), got %d", len(spans))
	}
	hitAttr, missAttr := spans[0].Attributes, spans[1].Attributes
	assertAttrBool(t, hitAttr, "cache.hit", true)
	assertAttrBool(t, missAttr, "cache.hit", false)
}

func assertAttrBool(t *testing.T, attrs []sdktrace.ReadOnlySpan, key string, want bool) {
	t.Helper()
	for _, a := range attrs {
		if a.Key == key {
			if a.Value.AsBool() != want {
				t.Fatalf("attr %s = %v, want %v", key, a.Value.AsBool(), want)
			}
			return
		}
	}
	t.Fatalf("attr %s missing", key)
}
```

（`assertAttrBool` 的参数类型修正为 `[]attribute.KeyValue`——`spans[0].Attributes` 的实际类型；import `"go.opentelemetry.io/otel/attribute"`。）

- [ ] **步骤 3：运行测试**

运行：`go test ./apps/bff-app/internal/cache/ -v`
预期：PASS（红→绿已完成：测试随实现一起写出时，先只写测试跑一次确认红，再写实现）。

## 任务 10：bff-app service 层（errgroup 聚合 + 降级兜底）

**文件：**
- 创建：`apps/bff-app/internal/service/aggregate.go`
- 测试：`apps/bff-app/internal/service/aggregate_test.go`

- [ ] **步骤 1：实现**

```go
// Package service bff-app 业务编排层：errgroup 并发聚合 + 降级兜底 + 展示层缓存。
//
// 分层 span 约定：每个出口方法开 service.bff-app.<Op> span；Home 的并发分支
// 各开 service.bff-app.Home.<branch> 子 span，降级时打 degraded=true 属性
// （降级不是错误——不标 span 状态，只留属性便于 Jaeger 筛选）。
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"hubx/micro_apps/apps/bff-app/internal/cache"
	"hubx/micro_apps/apps/bff-app/internal/client"
	"hubx/micro_apps/infra/telemetry"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// Aggregator 聚合编排（依赖注入：下游客户端 + 展示层缓存 + 日志）
type Aggregator struct {
	Cli   *client.Clients
	Cache *cache.DisplayCache
	Log   logx.Logger
}

// NewAggregator 构造
func NewAggregator(cli *client.Clients, c *cache.DisplayCache, logger logx.Logger) *Aggregator {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &Aggregator{Cli: cli, Cache: c, Log: logger}
}

// HomeView 首页聚合视图（部分字段可为 nil——对应降级分支）
type HomeView struct {
	Profile     *userpb.GetProfileResp    `json:"profile,omitempty"`
	Coupons     *couponpb.ListCouponsResp `json:"coupons,omitempty"`
	Recent      *orderpb.ListOrdersResp   `json:"recent_orders,omitempty"`
	Degraded    []string                  `json:"degraded,omitempty"` // 降级分支清单
	CachedAt    int64                     `json:"cached_at"`          // unix 秒（回填缓存时的时刻）
	CacheHit    bool                      `json:"cache_hit"`
}

// Home 首页聚合：缓存命中直接返回；未命中 errgroup 并发拉三域，单分支失败降级。
func (a *Aggregator) Home(ctx context.Context, userID int64) (*HomeView, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home")
	defer span.End()

	key := fmt.Sprintf("home:%d", userID)
	var view HomeView
	if a.Cache != nil && a.Cache.Get(ctx, key, &view) {
		span.SetAttributes(attribute.Bool("cache.hit", true))
		view.CacheHit = true
		return &view, nil
	}
	span.SetAttributes(attribute.Bool("cache.hit", false))

	view = HomeView{CachedAt: time.Now().Unix()}
	var mu sync.Mutex // view 与 Degraded 的并发写保护
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.fetchProfile(gctx, userID, &view, &mu) })
	g.Go(func() error { return a.fetchCoupons(gctx, userID, &view, &mu) })
	g.Go(func() error { return a.fetchRecent(gctx, userID, &view, &mu) })
	_ = g.Wait() // 分支内已各自降级，恒 nil

	if a.Cache != nil {
		a.Cache.Set(ctx, key, &view)
	}
	return &view, nil
}

// fetchProfile 并发分支：srv-user GetProfile；失败 → 记 degraded，主流程继续。
func (a *Aggregator) fetchProfile(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home.profile")
	defer span.End()
	if a.Cli.User == nil {
		return a.degrade(span, view, mu, "profile")
	}
	resp, err := a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "profile")
	}
	mu.Lock()
	view.Profile = resp
	mu.Unlock()
	return nil
}

// fetchCoupons 并发分支：srv-coupon ListCoupons（含已用券）。
func (a *Aggregator) fetchCoupons(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home.coupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return a.degrade(span, view, mu, "coupons")
	}
	resp, err := a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: true})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "coupons")
	}
	mu.Lock()
	view.Coupons = resp
	mu.Unlock()
	return nil
}

// fetchRecent 并发分支：srv-order ListOrders 最近流水（limit=5）。
func (a *Aggregator) fetchRecent(ctx context.Context, userID int64, view *HomeView, mu *sync.Mutex) error {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Home.recent")
	defer span.End()
	if a.Cli.Order == nil {
		return a.degrade(span, view, mu, "recent_orders")
	}
	resp, err := a.Cli.Order.ListOrders(ctx, &orderpb.ListOrdersReq{UserId: userID, Limit: 5})
	if err != nil || resp.GetCode() != 0 {
		if err != nil {
			span.RecordError(err)
		}
		return a.degrade(span, view, mu, "recent_orders")
	}
	mu.Lock()
	view.Recent = resp
	mu.Unlock()
	return nil
}

// degrade 降级记录：span 打属性 + view 记分支名（降级 DTO = 部分字段为 nil）
func (a *Aggregator) degrade(span trace.Span, view *HomeView, mu *sync.Mutex, branch string) error {
	span.SetAttributes(attribute.Bool("degraded", true))
	mu.Lock()
	view.Degraded = append(view.Degraded, branch)
	mu.Unlock()
	return nil
}

// ---- 透传型出口（薄编排：nil 客户端检查 + 分层 span + 原样透传 pb 响应）----

func (a *Aggregator) Register(ctx context.Context, username, password, phone string) (*userpb.RegisterResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Register")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.Register(ctx, &userpb.RegisterReq{Username: username, Password: password, Phone: phone})
}

func (a *Aggregator) Login(ctx context.Context, username, password string) (*userpb.LoginResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Login")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.Login(ctx, &userpb.LoginReq{Username: username, Password: password})
}

func (a *Aggregator) Profile(ctx context.Context, userID int64) (*userpb.GetProfileResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Profile")
	defer span.End()
	if a.Cli.User == nil {
		return nil, fmt.Errorf("srv-user unavailable")
	}
	return a.Cli.User.GetProfile(ctx, &userpb.GetProfileReq{UserId: userID})
}

func (a *Aggregator) ListProducts(ctx context.Context, userID int64, limit, offset int32) (*productpb.ListProductsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "ListProducts")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	return a.Cli.Product.ListProducts(ctx, &productpb.ListProductsReq{UserId: userID, Limit: limit, Offset: offset})
}

func (a *Aggregator) GetProduct(ctx context.Context, userID, productID int64) (*productpb.GetProductResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "GetProduct")
	defer span.End()
	if a.Cli.Product == nil {
		return nil, fmt.Errorf("srv-product unavailable")
	}
	return a.Cli.Product.GetProduct(ctx, &productpb.GetProductReq{UserId: userID, ProductId: productID})
}

func (a *Aggregator) CreateOrder(ctx context.Context, userID int64, items []*orderpb.OrderItem) (*orderpb.CreateOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "CreateOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.CreateOrder(ctx, &orderpb.CreateOrderReq{UserId: userID, Items: items})
}

func (a *Aggregator) GetOrder(ctx context.Context, userID, orderID int64) (*orderpb.GetOrderResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "GetOrder")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.GetOrder(ctx, &orderpb.GetOrderReq{UserId: userID, OrderId: orderID})
}

func (a *Aggregator) Report(ctx context.Context, userID int64, fromDate, toDate string) (*orderpb.ReportResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Report")
	defer span.End()
	if a.Cli.Order == nil {
		return nil, fmt.Errorf("srv-order unavailable")
	}
	return a.Cli.Order.Report(ctx, &orderpb.ReportReq{UserId: userID, FromDate: fromDate, ToDate: toDate})
}

func (a *Aggregator) Coupons(ctx context.Context, userID int64, includeUsed bool) (*couponpb.ListCouponsResp, error) {
	ctx, span := telemetry.Start(ctx, "service", "bff-app", "Coupons")
	defer span.End()
	if a.Cli.Coupon == nil {
		return nil, fmt.Errorf("srv-coupon unavailable")
	}
	return a.Cli.Coupon.ListCoupons(ctx, &couponpb.ListCouponsReq{UserId: userID, IncludeUsed: includeUsed})
}
```

（`golang.org/x/sync` 已在 go.mod indirect 组，引用后 `go mod tidy` 将其提升为直接依赖。）

- [ ] **步骤 2：降级测试**

创建 `apps/bff-app/internal/service/aggregate_test.go`：

```go
package service

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudwego/kitex/client/callopt"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/attribute"

	"hubx/micro_apps/apps/bff-app/internal/client"
	"hubx/micro_apps/infra/telemetry"

	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// 下游接口的故障桩：全部方法返回错误（触发降级分支）。
type failingUser struct{ userpb.UnimplementedUserService? } // kitex 生成代码若无 Unimplemented 桩，改为显式实现 4 个方法
```

（kitex 生成的 client interface 需完整实现。生成代码不带 Unimplemented helper 时按下面显式写法实现四个接口：）

```go
type failingUserClient struct{}

func (f failingUserClient) Register(ctx context.Context, req *userpb.RegisterReq, opts ...callopt.Option) (*userpb.RegisterResp, error) {
	return nil, errors.New("user down")
}
func (f failingUserClient) Login(ctx context.Context, req *userpb.LoginReq, opts ...callopt.Option) (*userpb.LoginResp, error) {
	return nil, errors.New("user down")
}
func (f failingUserClient) GetProfile(ctx context.Context, req *userpb.GetProfileReq, opts ...callopt.Option) (*userpb.GetProfileResp, error) {
	return nil, errors.New("user down")
}
func (f failingUserClient) CheckUser(ctx context.Context, req *userpb.CheckUserReq, opts ...callopt.Option) (*userpb.CheckUserResp, error) {
	return nil, errors.New("user down")
}

// TestHomeDegradesOnBranchFailure：单分支（user）故障时 Home 必须整体成功、
// 记 degraded 分支，且分支 span 带 degraded=true。
func TestHomeDegradesOnBranchFailure(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	telemetry.SetTracerForTest(tp.Tracer("test"))

	agg := NewAggregator(&client.Clients{
		User: failingUserClient{},
		// Order/Coupon 为 nil → 也会走降级；断言 degraded 含三分支
	}, nil, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home must not fail on branch failure: %v", err)
	}
	want := map[string]bool{"profile": false, "coupons": false, "recent_orders": false}
	for _, b := range view.Degraded {
		want[b] = true
	}
	for b, seen := range want {
		if !seen {
			t.Fatalf("branch %s not recorded degraded; got %v", b, view.Degraded)
		}
	}
	// 分支 span 必须有 degraded=true 属性
	var degradedSpans int
	for _, sp := range exporter.GetSpans() {
		for _, attr := range sp.Attributes {
			if attr.Key == "degraded" && attr.Value.AsBool() {
				degradedSpans++
			}
		}
	}
	if degradedSpans != 3 {
		t.Fatalf("expected 3 degraded branch spans, got %d", degradedSpans)
	}
}

var _ = attribute.Bool // import 锚定（若上面未直接使用 attribute 包则删除）
var _ = productpb.ProductInfo{}
var _ = couponpb.CouponInfo{}
var _ = orderpb.OrderFlow{}
```

运行：`go test ./apps/bff-app/internal/service/ -run TestHomeDegradesOnBranchFailure -v`
预期：PASS（先写测试确认红——OrderCli 为 nil 时旧逻辑没有降级概念，断言必然失败；本任务为新代码，红=编译失败/断言失败均可接受）。

## 任务 11：bff-app handler 层 + 路由

**文件：**
- 创建：`apps/bff-app/internal/handler/handler.go`

- [ ] **步骤 1：实现**

```go
// Package handler bff-app HTTP 层：hertz 路由 → service 编排 → envelope 统一响应。
//
// 身份：只读 gateway 验签后透传的 meta.HeaderUserID（openspec：一次验签，
// BFF 不二次验签）；缺失/非法 → 401 envelope。
package handler

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/gospacex/logx"
	"github.com/gospacex/webx/envelope"

	"hubx/micro_apps/apps/bff-app/internal/service"
	"hubx/micro_apps/pkg/meta"

	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
)

// API HTTP 处理器集合
type API struct {
	Svc *service.Aggregator
	Log logx.Logger
}

// NewAPI 构造
func NewAPI(svc *service.Aggregator, logger logx.Logger) *API {
	if logger == nil {
		logger = logx.GetGlobal()
	}
	return &API{Svc: svc, Log: logger}
}

// Register 路由表（webx.WithPreRun 挂到 engine）。
// 注意 /orders/report 与 /orders/:id：hertz 静态路由优先于参数路由，无冲突。
func Register(e *route.Engine, api *API) {
	e.POST("/users/register", api.Register)
	e.POST("/users/login", api.Login)
	e.GET("/users/profile", api.Profile)
	e.GET("/products", api.ListProducts)
	e.GET("/products/:id", api.GetProduct)
	e.POST("/orders", api.CreateOrder)
	e.GET("/orders/report", api.Report)
	e.GET("/orders/:id", api.GetOrder)
	e.GET("/coupons", api.Coupons)
	e.GET("/home", api.Home)
}

// userIDOf 读网关透传身份头；缺失/非法 → envelope 401 错误。
func userIDOf(c *app.RequestContext) (int64, error) {
	raw := string(c.Request.Header.Get(meta.HeaderUserID))
	if raw == "" {
		return 0, envelope.Errorf(envelope.CodeUnauthorized, "missing identity header")
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, envelope.Errorf(envelope.CodeUnauthorized, "invalid identity header")
	}
	return id, nil
}

func (a *API) Home(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	view, err := a.Svc.Home(ctx, uid)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "home: %v", err))
		return
	}
	envelope.Render(c, view, nil)
}

func (a *API) Register(ctx context.Context, c *app.RequestContext) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Phone    string `json:"phone"`
	}
	if err := c.BindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "username/password required"))
		return
	}
	resp, err := a.Svc.Register(ctx, req.Username, req.Password, req.Phone)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "register: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

func (a *API) Login(ctx context.Context, c *app.RequestContext) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.BindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "username/password required"))
		return
	}
	resp, err := a.Svc.Login(ctx, req.Username, req.Password)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "login: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil) // data 内含 token，端上后续请求经 gateway 验签
}

func (a *API) Profile(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.Profile(ctx, uid)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "profile: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

func (a *API) ListProducts(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 32)
	offset, _ := strconv.ParseInt(c.Query("offset"), 10, 32)
	if limit <= 0 {
		limit = 20
	}
	resp, err := a.Svc.ListProducts(ctx, uid, int32(limit), int32(offset))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "products: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

func (a *API) GetProduct(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	pid, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || pid <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid product id"))
		return
	}
	resp, err := a.Svc.GetProduct(ctx, uid, pid)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "product: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

// createOrderReq 下单入参 DTO（端上只传 product_id/quantity/unit_price）
type createOrderReq struct {
	Items []struct {
		ProductId int64 `json:"product_id"`
		Quantity  int32 `json:"quantity"`
		UnitPrice int64 `json:"unit_price"`
	} `json:"items"`
}

func (a *API) CreateOrder(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	var req createOrderReq
	if err := c.BindJSON(&req); err != nil || len(req.Items) == 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "items required"))
		return
	}
	items := make([]*orderpb.OrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		if it.ProductId <= 0 || it.Quantity <= 0 {
			envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid item"))
			return
		}
		items = append(items, &orderpb.OrderItem{
			ProductId: it.ProductId,
			Quantity:  it.Quantity,
			UnitPrice: it.UnitPrice,
		})
	}
	resp, err := a.Svc.CreateOrder(ctx, uid, items)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "create order: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

func (a *API) GetOrder(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	oid, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || oid <= 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBadRequest, "invalid order id"))
		return
	}
	resp, err := a.Svc.GetOrder(ctx, uid, oid)
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "order: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

func (a *API) Report(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.Report(ctx, uid, c.Query("from_date"), c.Query("to_date"))
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "report: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}

func (a *API) Coupons(ctx context.Context, c *app.RequestContext) {
	uid, err := userIDOf(c)
	if err != nil {
		envelope.Render(c, nil, err)
		return
	}
	resp, err := a.Svc.Coupons(ctx, uid, c.Query("include_used") == "true")
	if err != nil {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeInternal, "coupons: %v", err))
		return
	}
	if resp.GetCode() != 0 {
		envelope.Render(c, nil, envelope.Errorf(envelope.CodeBizRule, "%s", resp.GetMessage()))
		return
	}
	envelope.Render(c, resp, nil)
}
```

- [ ] **步骤 2：编译 + tidy**

运行：`go mod tidy && go build ./apps/bff-app/... && go vet ./apps/bff-app/...`
预期：全绿；go.mod 中 `golang.org/x/sync` 变为直接依赖。

## 任务 12：bff-app 全量测试 + 入库

- [ ] **步骤 1：全量回归**

运行：`go build ./... && go vet ./... && gofmt -l . && go test -race ./...`
预期：全绿，gofmt 无输出。

- [ ] **步骤 2：Commit**

```bash
git add apps/bff-app go.mod go.sum
git commit -m "feat(bff-app): 移动端 BFF 模板（errgroup 聚合 + 降级兜底 + 展示层缓存命中/未命中双 span + 统一 envelope 响应）"
```

## 任务 13：bff-app 冒烟（gateway→bff-app→srv 同 trace 实测）

依赖：docker compose 六件套已启动（用户已确认）、`configs/seed-nacos.sh` 已灌配置。

- [ ] **步骤 1：启动依赖服务（每个后台运行，日志落 /tmp）**

```bash
bash configs/seed-nacos.sh   # 幂等；灌入 hubx.* 11 份 + hubx.trace
cd apps/srv-user && nohup go run ./cmd/srv-user > /tmp/srv-user.log 2>&1 & 
cd apps/srv-product && nohup go run ./cmd/srv-product > /tmp/srv-product.log 2>&1 &
cd apps/srv-order && nohup go run ./cmd/srv-order > /tmp/srv-order.log 2>&1 &
cd apps/srv-coupon && nohup go run ./cmd/srv-coupon > /tmp/srv-coupon.log 2>&1 &
cd apps/srv-pay && nohup go run ./cmd/srv-pay > /tmp/srv-pay.log 2>&1 &
cd apps/gateway && nohup go run ./cmd/gateway > /tmp/gateway.log 2>&1 &
cd apps/bff-app && nohup go run ./cmd/bff-app > /tmp/bff-app.log 2>&1 &
sleep 25
```

预期：`grep -L "listening" /tmp/srv-user.log /tmp/srv-product.log /tmp/srv-order.log /tmp/srv-coupon.log /tmp/gateway.log /tmp/bff-app.log` 无输出（全部就绪）；`curl -s 127.0.0.1:8500/v1/kv/hubx/svc/bff-app?raw` 返回 `127.0.0.1:8091`。

（bring-up.sh 在任务 21 固化为脚本；此处先手工验证链路。）

- [ ] **步骤 2：造数据 + 打主链路**

```bash
TOKEN=$(python3 - <<'EOF'
import jwt, time  # 若环境无 PyJWT：pip3 install PyJWT
print(jwt.encode({"sub":"1","iss":"srv-user","exp":int(time.time())+3600}, "hubx-dev-jwt-secret", algorithm="HS256"))
EOF
)
curl -s -X POST 127.0.0.1:8080/api/app/users/register -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"username":"smoke_'$RANDOM'","password":"pw123456","phone":"13800000000"}'
```

预期：`{"code":0,...}`。随后：

```bash
LOGIN=$(curl -s -X POST 127.0.0.1:8080/api/app/users/login -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"username":"<上一步用户名>","password":"pw123456"}')
UTOKEN=$(echo "$LOGIN" | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['token'])")
curl -s "127.0.0.1:8080/api/app/products?limit=1" -H "Authorization: Bearer $UTOKEN"
```

预期：商品列表 `{"code":0,"data":{...}}`，记下首个 product_id。

```bash
curl -s -X POST 127.0.0.1:8080/api/app/orders -H "Authorization: Bearer $UTOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"product_id":<pid>,"quantity":1,"unit_price":100}]}'
curl -s "127.0.0.1:8080/api/app/orders/report" -H "Authorization: Bearer $UTOKEN"
curl -s "127.0.0.1:8080/api/app/home" -H "Authorization: Bearer $UTOKEN"
curl -s "127.0.0.1:8080/api/app/home" -H "Authorization: Bearer $UTOKEN"   # 第二次：缓存命中
sleep 12   # outbox 3s 补发 + BatchSpanProcessor flush
```

预期：全部 `code:0`；第二次 home 响应中 `cache_hit=true`。

- [ ] **步骤 3：Jaeger 验证同 trace（本阶段 ≥5 服务）**

```bash
curl -s "http://127.0.0.1:16686/api/traces?service=srv-order&limit=5" | python3 - <<'EOF'
import sys, json
data = json.load(sys.stdin)
best = None
for tr in data["data"]:
    svcs = {p["serviceName"] for p in tr["processes"].values()}
    if len(svcs) > (len(best["svcs"]) if best else 0):
        best = {"traceID": tr["traceID"], "svcs": svcs}
print(best["traceID"], sorted(best["svcs"]))
assert len(best["svcs"]) >= 5, f"only {len(best['svcs'])} services on one trace"
EOF
```

预期：同一 trace 覆盖 ≥5 个服务（gateway、bff-app、srv-order、srv-product、srv-coupon），job 尚未实现不计。任一断言失败即停在任务 13 排查，不推进。

- [ ] **步骤 4：span 分层清点**

在 Jaeger UI（http://127.0.0.1:16686）打开该 trace 人工核对，或：

```bash
curl -s "http://127.0.0.1:16686/api/traces/<traceID>" | python3 -c "
import sys,json
tr=json.load(sys.stdin)['data'][0]
ops=sorted({s['operationName'] for s in tr['spans']})
print('\n'.join(ops))"
```

预期：能同时看到 `POST /orders`（或 webx 入站 span 名）、`handler.gateway.Proxy:/api/app`、`middleware.gateway.auth`、`service.bff-app.CreateOrder`、kitex client/server span 对、`service.order.CreateOrder`、`db.*`（含 4 表 JOIN 在 report trace 里）、`service.display.get`（cache.hit=false）、`redis.GET`。把这组 operationName 列表粘贴到任务 24 的报表素材。
