// Package schedule 测试：定时任务的自建根 span 与降级行为。
//
// 守两件事：
//  1. 每个任务自建根 span（task.<name>.Run）——定时任务没有入站请求可续，
//     若不自建，Jaeger 上根本看不到对账跑过没有。
//  2. 下游 srv 不可用（未注册/超时）时任务降级不崩（对账是旁路，不能把 job 打挂）。
package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	orderpb "hubx/micro_apps/pkg/kitex_gen/order"
	paypb "hubx/micro_apps/pkg/kitex_gen/pay"
	productpb "hubx/micro_apps/pkg/kitex_gen/product"
)

type fakeOrders struct {
	flows []*orderpb.OrderFlow
	err   error
}

func (f *fakeOrders) ListOrders(context.Context, *orderpb.ListOrdersReq) (*orderpb.ListOrdersResp, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &orderpb.ListOrdersResp{Flows: f.flows}, nil
}
func (f *fakeOrders) Report(context.Context, *orderpb.ReportReq) (*orderpb.ReportResp, error) {
	return &orderpb.ReportResp{}, f.err
}

type fakePays struct {
	flows []*paypb.PayFlow
	err   error
}

func (f *fakePays) ListPayFlows(context.Context, *paypb.ListPayFlowsReq) (*paypb.ListPayFlowsResp, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &paypb.ListPayFlowsResp{Flows: f.flows}, nil
}

type fakeProducts struct {
	items []*productpb.ProductInfo
	err   error
}

func (f *fakeProducts) ListProducts(context.Context, *productpb.ListProductsReq) (*productpb.ListProductsResp, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &productpb.ListProductsResp{Products: f.items}, nil
}

func TestNamesRegistered(t *testing.T) {
	s := New(Clients{}, nil, true)
	want := map[string]bool{TaskReconcile: true, TaskStockDaily: true, TaskReport: true}
	got := s.Names()
	if len(got) != len(want) {
		t.Fatalf("names = %v, want %d tasks", got, len(want))
	}
	for _, n := range got {
		if !want[n] {
			t.Fatalf("unexpected task %q", n)
		}
	}
}

// TestRunOnceCreatesRootSpan：任务必须自建根 span（有 trace 才谈得上排查）
func TestRunOnceCreatesRootSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	s := New(Clients{Orders: &fakeOrders{}, Pays: &fakePays{}, Products: &fakeProducts{}}, nil, true)
	if err := s.RunOnce(context.Background(), TaskReconcile); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("want root span, got none")
	}
	found := false
	for _, sp := range spans {
		if sp.Name == "task.reconcile.Run" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want task.reconcile.Run, got %+v", spans)
	}
}

func TestRunOnceNoSpanWhenTraceOff(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	s := New(Clients{Orders: &fakeOrders{}, Pays: &fakePays{}, Products: &fakeProducts{}}, nil, false)
	if err := s.RunOnce(context.Background(), TaskReconcile); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("trace off: want no span, got %+v", spans)
	}
}

// TestRunOnceUnknownTask
func TestRunOnceUnknownTask(t *testing.T) {
	s := New(Clients{}, nil, true)
	if err := s.RunOnce(context.Background(), "nope"); err == nil {
		t.Fatal("unknown task must error")
	}
}

// TestReconcileDegradesOnDownstreamFailure：下游挂了只告警不崩（旁路任务）
func TestReconcileDegradesOnDownstreamFailure(t *testing.T) {
	s := New(Clients{Orders: &fakeOrders{err: errors.New("srv-order down")}, Pays: &fakePays{}}, nil, true)
	if err := s.RunOnce(context.Background(), TaskReconcile); err != nil {
		t.Fatalf("reconcile must degrade, got %v", err)
	}
}

// TestReconcileDetectsMismatch：已支付订单缺支付流水 → 判定不一致
func TestReconcileDetectsMismatch(t *testing.T) {
	orders := &fakeOrders{flows: []*orderpb.OrderFlow{
		{OrderId: 1, TotalAmount: 1000, Status: "paid"},
		{OrderId: 2, TotalAmount: 2000, Status: "paid"},
	}}
	pays := &fakePays{flows: []*paypb.PayFlow{
		{OrderId: 2, Amount: 2000, Status: "success"},
	}}
	s := New(Clients{Orders: orders, Pays: pays}, nil, true)
	res, err := s.Reconcile(context.Background(), time.Now().Format("2006-01-02"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Mismatch != 1 {
		t.Fatalf("mismatch = %d, want 1（order 1 无支付流水）", res.Mismatch)
	}
	if res.Orders != 2 || res.Pays != 1 {
		t.Fatalf("orders=%d pays=%d, want 2/1", res.Orders, res.Pays)
	}
}
