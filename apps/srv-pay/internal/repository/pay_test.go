// repository 层单测（sqlite cgo 驱动，验证 GORM 查询语义：
// 支付单开单事务/幂等查重、状态迁移守卫、回调流水、流水分页。
// uk_pay_no 唯一约束 + dateRange 解析跨方言行为不在本层覆盖）。
package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&PayOrder{}, &PayCallback{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	return db
}

func TestCreatePayOrderTx(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewPayRepository(db)

	po, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 1, OrderID: 100, Channel: "alipay"}, "P-1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if po.PayOrderNo != "P-1" || po.Status != PayStatusPending {
		t.Fatalf("pay order = %+v", po)
	}
	// 同 user+order 已有 pending 单 → 幂等重放 ErrPayDuplicate
	if _, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 1, OrderID: 100, Channel: "alipay"}, "P-2"); !errors.Is(err, ErrPayDuplicate) {
		t.Fatalf("want ErrPayDuplicate, got %v", err)
	}
	// 不同 order → 正常开单
	if _, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 1, OrderID: 101, Channel: "wechat"}, "P-3"); err != nil {
		t.Fatalf("create other order: %v", err)
	}
}

func TestMarkSuccessAndFailedGuards(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewPayRepository(db)

	if _, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 1, OrderID: 1, Channel: "alipay"}, "P-S1"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 1, OrderID: 2, Channel: "alipay"}, "P-S2"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// pending → success（守卫 status IN pending/processing）
	changed, err := repo.MarkSuccess(ctx, "P-S1")
	if err != nil {
		t.Fatalf("MarkSuccess: %v", err)
	}
	if !changed {
		t.Fatalf("want status transition")
	}
	// 重复 success 迁移 → 守卫不命中（幂等）
	if changed, _ = repo.MarkSuccess(ctx, "P-S1"); changed {
		t.Fatalf("re-mark success should be no-op")
	}
	// pending → failed
	if changed, err := repo.MarkFailed(ctx, "P-S2"); err != nil || !changed {
		t.Fatalf("MarkFailed = (%v, %v)", changed, err)
	}
	// failed 后不可再 success（status 已终态）
	if changed, _ = repo.MarkSuccess(ctx, "P-S2"); changed {
		t.Fatalf("failed → success should be blocked by guard")
	}
}

func TestRecordCallbackAndFind(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewPayRepository(db)

	if _, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 1, OrderID: 1, Channel: "alipay"}, "P-C1"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cb := &PayCallback{PayOrderNo: "P-C1", Channel: "alipay", RawBody: `{"ok":true}`, Success: 1}
	if err := repo.RecordCallback(ctx, cb); err != nil {
		t.Fatalf("RecordCallback: %v", err)
	}
	po, err := repo.FindByPayOrderNo(ctx, "P-C1")
	if err != nil {
		t.Fatalf("FindByPayOrderNo: %v", err)
	}
	if po.Channel != "alipay" {
		t.Fatalf("pay order = %+v", po)
	}
	if _, err := repo.FindByPayOrderNo(ctx, "P-NOPE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// 回调流水落库（2 行）
	var n int64
	if err := db.Model(&PayCallback{}).Count(&n).Error; err != nil {
		t.Fatalf("count callbacks: %v", err)
	}
	if n != 1 {
		t.Fatalf("callbacks = %d, want 1", n)
	}
}

func TestListFlows(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewPayRepository(db)

	for i, no := range []string{"P-L1", "P-L2", "P-L3"} {
		_, err := repo.CreatePayOrderTx(ctx, PayOrder{UserID: 9, OrderID: uint64(i + 1), Channel: "alipay"}, no)
		if err != nil {
			t.Fatalf("seed %s: %v", no, err)
		}
	}
	flows, err := repo.ListFlows(ctx, 9, "", "", 2)
	if err != nil {
		t.Fatalf("ListFlows: %v", err)
	}
	if len(flows) != 2 || flows[0].PayOrderNo != "P-L3" {
		t.Fatalf("flows = %+v (want newest P-L3 first, 2 rows)", flows)
	}
	// 他人查不到
	if others, _ := repo.ListFlows(ctx, 10, "", "", 100); len(others) != 0 {
		t.Fatalf("cross-user rows: %d", len(others))
	}
}

func TestDateRangePay(t *testing.T) {
	from, to, err := dateRange("2026-01-01", "2026-01-31")
	if err != nil {
		t.Fatalf("dateRange: %v", err)
	}
	wantFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wantTo := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if !from.Equal(wantFrom) || !to.Equal(wantTo) {
		t.Fatalf("range = (%v, %v), want (%v, %v)", from, to, wantFrom, wantTo)
	}
	if _, _, err := dateRange("bad", ""); err == nil {
		t.Fatalf("want error for bad from_date")
	}
}
