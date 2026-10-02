// Package handler 测试：topic → 业务动作的派发正确性。
//
// 这里守的是「消费侧拿到消息后做什么」：订单完成事件必须换算积分并经
// gRPC 发给 srv-coupon；未知 topic 与坏载荷必须显式报错（而不是静默吞掉，
// 否则消息被 ack 却什么都没做，是最难查的一类丢数据）。
package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gospacex/mqx"

	"hubx/micro_apps/pkg/events"
	couponpb "hubx/micro_apps/pkg/kitex_gen/coupon"
)

// fakeCoupon 记录每次 GrantPoints 的入参（不落真实 RPC）
type fakeCoupon struct {
	calls []*couponpb.GrantPointsReq
	err   error
}

func (f *fakeCoupon) GrantPoints(_ context.Context, req *couponpb.GrantPointsReq) (*couponpb.GrantPointsResp, error) {
	f.calls = append(f.calls, req)
	if f.err != nil {
		return nil, f.err
	}
	return &couponpb.GrantPointsResp{Code: 0}, nil
}

func TestDispatchOrderCompletedGrantsPoints(t *testing.T) {
	ev := events.OrderCompleted{OrderID: 7, UserID: 3, Amount: 12800, SchemaVer: events.SchemaVer1}
	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	fc := &fakeCoupon{}
	d := New(fc, nil)

	msg := &mqx.Message{Topic: TopicOrderCompleted, Key: "7", Value: body, Headers: map[string]string{}}
	if err := d.Dispatch(context.Background(), msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(fc.calls) != 1 {
		t.Fatalf("GrantPoints calls = %d, want 1", len(fc.calls))
	}
	got := fc.calls[0]
	// 12800 分 = 128 元 → 128 分积分（PointsOfAmount 契约）
	if got.GetUserId() != 3 || got.GetPoints() != 128 || got.GetOrderId() != 7 {
		t.Fatalf("GrantPoints req = %+v, want user=3 points=128 order=7", got)
	}
	if got.GetSource() != "order.completed" {
		t.Fatalf("source = %q, want order.completed（对账/排障靠它定位来源）", got.GetSource())
	}
}

func TestDispatchCouponExchangedGrantsPoints(t *testing.T) {
	ev := events.CouponExchanged{UserID: 9, CouponID: 5, OrderID: 11, Discount: 500}
	body, _ := json.Marshal(ev)
	fc := &fakeCoupon{}
	d := New(fc, nil)
	msg := &mqx.Message{Topic: TopicCouponExchanged, Value: body, Headers: map[string]string{}}
	if err := d.Dispatch(context.Background(), msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(fc.calls) != 1 || fc.calls[0].GetSource() != "coupon.exchanged" {
		t.Fatalf("unexpected calls: %+v", fc.calls)
	}
	if fc.calls[0].GetUserId() != 9 || fc.calls[0].GetOrderId() != 11 {
		t.Fatalf("req = %+v, want user=9 order=11", fc.calls[0])
	}
}

// TestDispatchZeroAmountSkipsGrant：金额不足 1 元不发放（避免给下游刷 0 积分调用）
func TestDispatchZeroAmountSkipsGrant(t *testing.T) {
	ev := events.OrderCompleted{OrderID: 1, UserID: 1, Amount: 99}
	body, _ := json.Marshal(ev)
	fc := &fakeCoupon{}
	d := New(fc, nil)
	if err := d.Dispatch(context.Background(), &mqx.Message{Topic: TopicOrderCompleted, Value: body}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(fc.calls) != 0 {
		t.Fatalf("want no GrantPoints for amount<1元, got %+v", fc.calls)
	}
}

func TestDispatchUnknownTopic(t *testing.T) {
	d := New(&fakeCoupon{}, nil)
	err := d.Dispatch(context.Background(), &mqx.Message{Topic: "whatever.v1"})
	if err == nil {
		t.Fatal("unknown topic must error（静默 ack 会丢消息）")
	}
}

func TestDispatchBadPayload(t *testing.T) {
	d := New(&fakeCoupon{}, nil)
	err := d.Dispatch(context.Background(), &mqx.Message{Topic: TopicOrderCompleted, Value: []byte("{not json")})
	if err == nil {
		t.Fatal("bad payload must error")
	}
}

func TestPointsOfAmount(t *testing.T) {
	cases := []struct {
		cents int64
		want  int32
	}{
		{0, 0}, {99, 0}, {100, 1}, {12800, 128}, {199, 1},
	}
	for _, c := range cases {
		if got := PointsOfAmount(c.cents); got != c.want {
			t.Errorf("PointsOfAmount(%d) = %d, want %d", c.cents, got, c.want)
		}
	}
}
