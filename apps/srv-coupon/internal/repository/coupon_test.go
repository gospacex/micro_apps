// repository 层单测（sqlite cgo 驱动，验证 GORM 查询语义：
// 核销事务/幂等/过期守卫、outbox 事件同事务落库、积分发放事务、
// ListCoupons JOIN Find。MySQL 方言差异（FOR UPDATE 等）不在本层覆盖）。
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
	if err := db.AutoMigrate(&Coupon{}, &UserCoupon{}, &PointsLog{}, &OutboxEvent{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	return db
}

// seedHeld 造 模板 + 用户持有券（held 态）
func seedHeld(t *testing.T, db *gorm.DB, userID, couponID uint64, status int, validTo time.Time) {
	t.Helper()
	c := Coupon{ID: couponID, Name: "券" + string(rune('0'+couponID%10)), Discount: 100, Status: status,
		ValidFrom: time.Now().Add(-time.Hour), ValidTo: validTo}
	if err := db.Create(&c).Error; err != nil {
		t.Fatalf("seed coupon: %v", err)
	}
	uc := UserCoupon{UserID: userID, CouponID: couponID, Status: UserCouponHeld}
	if err := db.Create(&uc).Error; err != nil {
		t.Fatalf("seed user coupon: %v", err)
	}
}

func TestConsumeTxHappyPath(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)
	seedHeld(t, db, 1, 11, 1, time.Now().Add(24*time.Hour))

	wasUsed, err := repo.ConsumeTx(ctx, ConsumeTxInput{
		UserID: 1, CouponID: 11, OrderID: 555,
		OutboxFill: func() (string, string) { return `{"coupon_id":11}`, "tp-test" },
	})
	if err != nil || wasUsed {
		t.Fatalf("consume = (%v, %v), want success", wasUsed, err)
	}
	// 状态迁移 + used_order_id 回填
	var uc UserCoupon
	if err := db.Where("user_id = 1 AND coupon_id = 11").Find(&uc).Error; err != nil {
		t.Fatalf("read user coupon: %v", err)
	}
	if uc.Status != UserCouponUsed || uc.UsedOrderID != 555 {
		t.Fatalf("user coupon = %+v (want used/555)", uc)
	}
	// outbox 事件同事务落库（payload/traceparent 经 OutboxFill）
	var ev OutboxEvent
	if err := db.Find(&ev).Error; err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if ev.Topic != TopicCouponExchanged || ev.Payload == "" || ev.Traceparent != "tp-test" {
		t.Fatalf("outbox = %+v", ev)
	}
}

func TestConsumeTxIdempotentReplay(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)
	seedHeld(t, db, 1, 12, 1, time.Now().Add(24*time.Hour))
	// 预先核销（status=used）
	if err := db.Model(&UserCoupon{}).Where("coupon_id = 12").
		Updates(map[string]interface{}{"status": UserCouponUsed, "used_order_id": 77}).Error; err != nil {
		t.Fatalf("pre-use: %v", err)
	}
	// 重放 → wasUsed=true + ErrCouponUsed（幂等 6001）
	wasUsed, err := repo.ConsumeTx(ctx, ConsumeTxInput{UserID: 1, CouponID: 12, OrderID: 999})
	if !errors.Is(err, ErrCouponUsed) || !wasUsed {
		t.Fatalf("replay = (%v, %v), want wasUsed + ErrCouponUsed", wasUsed, err)
	}
}

func TestConsumeTxExpired(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)
	// 模板已过期（valid_to 早于 now）
	seedHeld(t, db, 1, 13, 1, time.Now().Add(-time.Hour))
	_, err := repo.ConsumeTx(ctx, ConsumeTxInput{UserID: 1, CouponID: 13, OrderID: 1})
	if !errors.Is(err, ErrCouponExpired) {
		t.Fatalf("want ErrCouponExpired, got %v", err)
	}
	// 模板停用（status=0）→ 同样按过期拒
	seedHeld(t, db, 1, 14, 0, time.Now().Add(24*time.Hour))
	if _, err := repo.ConsumeTx(ctx, ConsumeTxInput{UserID: 1, CouponID: 14, OrderID: 2}); !errors.Is(err, ErrCouponExpired) {
		t.Fatalf("disabled template: want ErrCouponExpired, got %v", err)
	}
}

func TestConsumeTxNotHeld(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)
	seedHeld(t, db, 1, 15, 1, time.Now().Add(24*time.Hour))
	// 他人未持有 → ErrCouponHeld（服务层映射 6003）
	_, err := repo.ConsumeTx(ctx, ConsumeTxInput{UserID: 2, CouponID: 15, OrderID: 3})
	if !errors.Is(err, ErrCouponHeld) {
		t.Fatalf("want ErrCouponHeld, got %v", err)
	}
}

func TestGrantPointsTx(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)

	evID, err := repo.GrantPointsTx(ctx, 7, 50, "coupon.exchanged", 42,
		TopicPointsGranted, OutboxEventTypePointsGranted,
		func(uint64) (string, string) { return `{"points":50}`, "" })
	if err != nil {
		t.Fatalf("GrantPointsTx: %v", err)
	}
	if evID == 0 {
		t.Fatalf("outbox event id = 0")
	}
	// points_log 同事务落库
	var pl PointsLog
	if err := db.Find(&pl).Error; err != nil {
		t.Fatalf("read points log: %v", err)
	}
	if pl.UserID != 7 || pl.Points != 50 || pl.Source != "coupon.exchanged" || pl.RefOrder != 42 {
		t.Fatalf("points log = %+v", pl)
	}
}

func TestListCoupons(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)
	seedHeld(t, db, 1, 21, 1, time.Now().Add(24*time.Hour))
	// 第二张 held + 一张 used
	seedHeld(t, db, 1, 22, 1, time.Now().Add(24*time.Hour))
	seedHeld(t, db, 1, 23, 1, time.Now().Add(24*time.Hour))
	if err := db.Model(&UserCoupon{}).Where("coupon_id = 23").
		Updates(map[string]interface{}{"status": UserCouponUsed, "used_order_id": 777}).Error; err != nil {
		t.Fatalf("mark used: %v", err)
	}

	// includeUsed=false → 仅 held（2 条）
	held, err := repo.ListCoupons(ctx, 1, false)
	if err != nil {
		t.Fatalf("ListCoupons: %v", err)
	}
	if len(held) != 2 {
		t.Fatalf("held = %d, want 2", len(held))
	}
	// includeUsed=true → 3 条；used 态回填 used_order_id（JOIN Find 投影）
	all, err := repo.ListCoupons(ctx, 1, true)
	if err != nil {
		t.Fatalf("ListCoupons all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all = %d, want 3", len(all))
	}
	var usedFound bool
	for i := range all {
		if all[i].UserCoupon.CouponID == 23 && all[i].UserCoupon.Status == UserCouponUsed &&
			all[i].UserCoupon.UsedOrderID == 777 {
			usedFound = true
		}
	}
	if !usedFound {
		t.Fatalf("used row not found: %+v", all)
	}
	// 他人查不到
	if others, _ := repo.ListCoupons(ctx, 2, true); len(others) != 0 {
		t.Fatalf("cross-user rows: %d", len(others))
	}
}

func TestCouponOutboxTransitions(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewCouponRepository(db)

	for i := 0; i < 3; i++ {
		seedHeld(t, db, 1, uint64(31+i), 1, time.Now().Add(24*time.Hour))
		_, err := repo.ConsumeTx(ctx, ConsumeTxInput{UserID: 1, CouponID: uint64(31 + i), OrderID: uint64(i + 1),
			OutboxFill: func() (string, string) { return `{"coupon_id":31}`, "" }})
		if err != nil {
			t.Fatalf("consume %d: %v", i, err)
		}
	}
	pending, err := repo.PendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("PendingOutbox: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending = %d, want 3", len(pending))
	}
	// 1→已发（守卫）
	if err := repo.MarkOutboxSent(ctx, []uint64{pending[0].ID}); err != nil {
		t.Fatalf("MarkOutboxSent: %v", err)
	}
	if err := repo.MarkOutboxRetry(ctx, []uint64{pending[1].ID}); err != nil {
		t.Fatalf("MarkOutboxRetry: %v", err)
	}
	// 超限放弃
	if err := repo.AbandonOutbox(ctx, []uint64{pending[1].ID}); err != nil {
		t.Fatalf("AbandonOutbox: %v", err)
	}
	rest, _ := repo.PendingOutbox(ctx, 10)
	if len(rest) != 1 {
		t.Fatalf("pending rest = %d, want 1", len(rest))
	}
}
