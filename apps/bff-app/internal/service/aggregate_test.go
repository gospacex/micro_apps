package service

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudwego/kitex/client/callopt"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"hubx/micro_apps/apps/bff-app/internal/client"

	userpb "hubx/micro_apps/pkg/kitex_gen/user"
)

// failingUserClient 的全部方法都返回错误，用于触发 profile 降级分支。
type failingUserClient struct{}

func (f failingUserClient) Register(context.Context, *userpb.RegisterReq, ...callopt.Option) (*userpb.RegisterResp, error) {
	return nil, errors.New("user down")
}

func (f failingUserClient) Login(context.Context, *userpb.LoginReq, ...callopt.Option) (*userpb.LoginResp, error) {
	return nil, errors.New("user down")
}

func (f failingUserClient) GetProfile(context.Context, *userpb.GetProfileReq, ...callopt.Option) (*userpb.GetProfileResp, error) {
	return nil, errors.New("user down")
}

func (f failingUserClient) CheckUser(context.Context, *userpb.CheckUserReq, ...callopt.Option) (*userpb.CheckUserResp, error) {
	return nil, errors.New("user down")
}

// TestHomeDegradesOnBranchFailure 验证任一分支故障不影响 Home 整体成功，
// 三个失败分支均进入降级清单，且对应 span 标记 degraded=true。
func TestHomeDegradesOnBranchFailure(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	agg := NewAggregator(&client.Clients{
		User: failingUserClient{},
		// Order/Coupon 为 nil，同样应进入降级分支。
	}, nil, nil)

	view, err := agg.Home(context.Background(), 42)
	if err != nil {
		t.Fatalf("home must not fail on branch failure: %v", err)
	}
	want := map[string]bool{"profile": false, "coupons": false, "recent_orders": false}
	for _, branch := range view.Degraded {
		want[branch] = true
	}
	for branch, seen := range want {
		if !seen {
			t.Fatalf("branch %s not recorded degraded; got %v", branch, view.Degraded)
		}
	}

	var degradedSpans int
	for _, span := range exporter.GetSpans() {
		for _, attr := range span.Attributes {
			if string(attr.Key) == "degraded" && attr.Value.AsBool() {
				degradedSpans++
			}
		}
	}
	if degradedSpans != 3 {
		t.Fatalf("expected 3 degraded branch spans, got %d", degradedSpans)
	}
}
