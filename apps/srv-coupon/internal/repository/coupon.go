// Package repository srv-coupon 持久层（私有库 coupon_db，账号 coupon_rw）。
//
// 纪律（brief）：
//   - GORM 连表查询一律 Find（多行投影到 slice），禁用 Scan（会静默丢列）
//   - 错误一律 %w 包装
//
// outbox（coupon_db.outbox，批 2 修订：DDL 已同步进
// pkg/seedsql/coupon_db.sql 与 docker/mysql/init.sql）：
// 券核销 / 积分发放事件经事务发件箱 3s 轮询补发到 Kafka。
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// 哨兵错误（服务层映射 6001/6002）
var (
	// ErrNotFound 无记录
	ErrNotFound = errors.New("repository: record not found")
	// ErrCouponHeld 用户未持有该券
	ErrCouponHeld = errors.New("repository: user coupon not held")
	// ErrCouponUsed 券已使用（uk_user_coupon 幂等重放 → 6001）
	ErrCouponUsed = errors.New("repository: coupon already used")
	// ErrCouponExpired 券有效期外 → 6002
	ErrCouponExpired = errors.New("repository: coupon expired")
)

// user_coupons.status 枚举
const (
	UserCouponHeld    = "held"
	UserCouponUsed    = "used"
	UserCouponExpired = "expired"
)

// outbox.status 常量（与 srv-order 同语义）
const (
	OutboxStatusPending = 0
	OutboxStatusSent    = 1
	OutboxStatusAbandon = 2
)

// outbox 事件 topic / 类型（coupon_db）
const (
	TopicCouponExchanged = "coupon.exchanged.v1"
	TopicPointsGranted   = "points.granted.v1"

	OutboxEventTypeCouponExchanged = "coupon.exchanged"
	OutboxEventTypePointsGranted   = "points.granted"
)

// Coupon 优惠券模板行
type Coupon struct {
	ID         uint64    `gorm:"primaryKey;column:id"`
	Name       string    `gorm:"column:name"`
	Discount   int64     `gorm:"column:discount"`
	Type       string    `gorm:"column:type"`
	TotalQuota int       `gorm:"column:total_quota"`
	UsedQuota  int       `gorm:"column:used_quota"`
	ValidFrom  time.Time `gorm:"column:valid_from"`
	ValidTo    time.Time `gorm:"column:valid_to"`
	Status     int       `gorm:"column:status"`
}

// TableName 显式表名
func (Coupon) TableName() string { return "coupons" }

// UserCoupon 用户持有的券行（uk_user_coupon 幂等）
type UserCoupon struct {
	ID          uint64    `gorm:"primaryKey;column:id"`
	UserID      uint64    `gorm:"column:user_id"`
	CouponID    uint64    `gorm:"column:coupon_id"`
	Status      string    `gorm:"column:status"`
	UsedOrderID uint64    `gorm:"column:used_order_id"`
	GrantedAt   time.Time `gorm:"column:granted_at"`
	UsedAt      time.Time `gorm:"column:used_at"`
}

// TableName 显式表名
func (UserCoupon) TableName() string { return "user_coupons" }

// PointsLog 积分流水行
type PointsLog struct {
	ID        uint64    `gorm:"primaryKey;column:id"`
	UserID    uint64    `gorm:"column:user_id"`
	Points    int       `gorm:"column:points"`
	Source    string    `gorm:"column:source"`
	RefOrder  uint64    `gorm:"column:ref_order_id"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName 显式表名
func (PointsLog) TableName() string { return "points_log" }

// OutboxEvent 事务发件箱行（coupon_db.outbox）
type OutboxEvent struct {
	ID          uint64    `gorm:"primaryKey;column:id"`
	Topic       string    `gorm:"column:topic"`
	EventType   string    `gorm:"column:event_type"`
	Payload     string    `gorm:"column:payload"`
	Traceparent string    `gorm:"column:traceparent"`
	Status      int       `gorm:"column:status"`
	RetryCount  int       `gorm:"column:retry_count"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

// TableName 显式表名
func (OutboxEvent) TableName() string { return "outbox" }

// CouponInfoRow user_coupons ⋈ coupons JOIN 结果
type CouponInfoRow struct {
	UserCoupon UserCoupon
	Coupon     Coupon
}

// CouponRepository coupon_db 仓储
type CouponRepository struct {
	db *gorm.DB
}

// NewCouponRepository 构造
func NewCouponRepository(db *gorm.DB) *CouponRepository {
	return &CouponRepository{db: db}
}

// DB 暴露句柄（组装期健康检查用）
func (r *CouponRepository) DB() *gorm.DB {
	return r.db
}

// GetHeldUserCoupon 查用户持有的券（status 不限，幂等判定需要 used 态）
func (r *CouponRepository) GetHeldUserCoupon(ctx context.Context, userID, couponID uint64) (*UserCoupon, error) {
	var rows []UserCoupon
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND coupon_id = ?", userID, couponID).
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("get user coupon (%d,%d): %w", userID, couponID, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("user coupon (%d,%d): %w", userID, couponID, ErrCouponHeld)
	}
	return &rows[0], nil
}

// FindCoupon 查券模板
func (r *CouponRepository) FindCoupon(ctx context.Context, id uint64) (*Coupon, error) {
	var c Coupon
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&c).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("coupon %d: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("find coupon %d: %w", id, err)
	}
	return &c, nil
}

// ConsumeTxInput 核销事务输入
type ConsumeTxInput struct {
	UserID   uint64
	CouponID uint64
	OrderID  uint64
	// OutboxFill 在事务内、状态迁移成功后回调：构造 CouponExchanged 事件
	// JSON 与发送时随 header 透传的 traceparent（nil 时不写 outbox 行）
	OutboxFill func() (payload, traceparent string)
}

// ConsumeTx 用券核销事务（brief：user_coupons held → used + used_order_id 回填）：
//  1. 事务内读 user_coupons 行：
//     - 无行 → ErrCouponHeld
//     - status=used → ErrCouponUsed（幂等重放，服务层映射 6001）
//  2. 模板有效期/状态校验（valid_to 早于 now 或停用 → ErrCouponExpired → 6002）
//  3. 条件更新 user_coupons SET status='used', used_order_id=?, used_at=now
//     （WHERE status='held' 守卫：并发双核销只有一条命中）
//  4. outbox CouponExchanged 事件（同事务，OutboxFill 定稿 payload/traceparent）
//
// 返回 (wasUsed, err)：wasUsed=true 表示券原本就是 used 态（幂等重放）。
func (r *CouponRepository) ConsumeTx(ctx context.Context, in ConsumeTxInput) (bool, error) {
	wasUsed := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		uc, uerr := r.getHeldTx(tx, ctx, in.UserID, in.CouponID)
		if uerr != nil {
			return uerr
		}
		if uc.Status == UserCouponUsed {
			wasUsed = true
			return fmt.Errorf("consume coupon: %w", ErrCouponUsed)
		}
		// 模板有效性（valid_to 早于 now → 6002；status=0 停用 → 同样按过期拒）
		var c Coupon
		if ferr := tx.WithContext(ctx).
			Where("id = ?", in.CouponID).First(&c).Error; ferr != nil {
			return fmt.Errorf("find coupon template %d: %w", in.CouponID, ferr)
		}
		if c.Status != 1 || time.Now().After(c.ValidTo) {
			return fmt.Errorf("coupon %d: %w", in.CouponID, ErrCouponExpired)
		}
		res := tx.WithContext(ctx).
			Model(&UserCoupon{}).
			Where("id = ? AND status = ?", uc.ID, UserCouponHeld).
			Updates(map[string]interface{}{
				"status":        UserCouponUsed,
				"used_order_id": in.OrderID,
				"used_at":       time.Now(),
			})
		if res.Error != nil {
			return fmt.Errorf("consume user coupon %d: %w", uc.ID, res.Error)
		}
		if res.RowsAffected == 0 {
			// 并发竞争：另一事务已核销（本事务读的是旧快照）
			wasUsed = true
			return fmt.Errorf("consume coupon: %w", ErrCouponUsed)
		}
		// outbox 事件（状态迁移成功后同事务写入）
		if in.OutboxFill != nil {
			payload, tp := in.OutboxFill()
			ev := OutboxEvent{
				Topic:       TopicCouponExchanged,
				EventType:   OutboxEventTypeCouponExchanged,
				Payload:     payload,
				Traceparent: tp,
				Status:      OutboxStatusPending,
			}
			if err := tx.Create(&ev).Error; err != nil {
				return fmt.Errorf("insert outbox event: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return wasUsed, err
	}
	return false, nil
}

// getHeldTx 事务内读 user_coupons 行（Find：uk 约束下至多 1 行）
func (r *CouponRepository) getHeldTx(tx *gorm.DB, ctx context.Context, userID, couponID uint64) (*UserCoupon, error) {
	var rows []UserCoupon
	err := tx.WithContext(ctx).
		Where("user_id = ? AND coupon_id = ?", userID, couponID).
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("get user coupon (%d,%d): %w", userID, couponID, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("user coupon (%d,%d): %w", userID, couponID, ErrCouponHeld)
	}
	return &rows[0], nil
}

// GrantPointsTx 积分发放事务：points_log 插入 + outbox PointsGranted 事件
// （同事务提交；OutboxFill 在事件行落库前回调构造 payload/traceparent）。
// 返回 outbox 事件行 ID。
func (r *CouponRepository) GrantPointsTx(ctx context.Context, userID uint64, points int, source string, refOrderID uint64, outboxTopic, outboxEventType string, outboxFill func(eventID uint64) (payload, traceparent string)) (uint64, error) {
	var evID uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1) 积分流水
		pl := PointsLog{UserID: userID, Points: points, Source: source, RefOrder: refOrderID}
		if err := tx.Create(&pl).Error; err != nil {
			return fmt.Errorf("insert points log: %w", err)
		}
		// 2) outbox 事件（fill 回调在事件行落库前定稿 payload/traceparent；
		// 与 srv-order 不同：coupon 事件无需内嵌自增 ID，按 0 占位调用）
		ev := OutboxEvent{
			Topic:     outboxTopic,
			EventType: outboxEventType,
			Status:    OutboxStatusPending,
		}
		if outboxFill != nil {
			ev.Payload, ev.Traceparent = outboxFill(0)
		}
		if err := tx.Create(&ev).Error; err != nil {
			return fmt.Errorf("insert outbox event: %w", err)
		}
		evID = ev.ID
		return nil
	})
	if err != nil {
		return 0, err
	}
	return evID, nil
}

// ListCoupons 用户持有的券列表（user_coupons ⋈ coupons 2 表 LEFT JOIN Find；
// includeUsed=false 时仅 held 态）
func (r *CouponRepository) ListCoupons(ctx context.Context, userID uint64, includeUsed bool) ([]CouponInfoRow, error) {
	q := r.db.WithContext(ctx).
		Table("user_coupons").
		Select("user_coupons.id AS uc_id, user_coupons.coupon_id, user_coupons.status AS uc_status, "+
			"user_coupons.used_order_id, user_coupons.granted_at, "+
			"coupons.name, coupons.discount, coupons.type").
		Joins("LEFT JOIN coupons ON coupons.id = user_coupons.coupon_id").
		Where("user_coupons.user_id = ?", userID)
	if !includeUsed {
		q = q.Where("user_coupons.status = ?", UserCouponHeld)
	} else {
		q = q.Where("user_coupons.status IN ?", []string{UserCouponHeld, UserCouponUsed})
	}
	var rows []struct {
		UCID        uint64    `gorm:"column:uc_id"`
		CouponID    uint64    `gorm:"column:coupon_id"`
		UCStatus    string    `gorm:"column:uc_status"`
		UsedOrderID uint64    `gorm:"column:used_order_id"`
		GrantedAt   time.Time `gorm:"column:granted_at"`
		Name        string    `gorm:"column:name"`
		Discount    int64     `gorm:"column:discount"`
		Type        string    `gorm:"column:type"`
	}
	if err := q.Order("user_coupons.id ASC").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list coupons %d: %w", userID, err)
	}
	out := make([]CouponInfoRow, 0, len(rows))
	for i := range rows {
		out = append(out, CouponInfoRow{
			UserCoupon: UserCoupon{
				ID: rows[i].UCID, CouponID: rows[i].CouponID, Status: rows[i].UCStatus,
				UsedOrderID: rows[i].UsedOrderID, GrantedAt: rows[i].GrantedAt,
			},
			Coupon: Coupon{ID: rows[i].CouponID, Name: rows[i].Name, Discount: rows[i].Discount, Type: rows[i].Type},
		})
	}
	return out, nil
}

// PendingOutbox 取待发事件（3s 轮询补发用，status=0）
func (r *CouponRepository) PendingOutbox(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows []OutboxEvent
	err := r.db.WithContext(ctx).
		Where("status = ?", OutboxStatusPending).
		Order("id ASC").
		Limit(limit).
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("pending outbox: %w", err)
	}
	return rows, nil
}

// MarkOutboxSent 标记已发（幂等：仅 status=0 的行可迁移到 1）
func (r *CouponRepository) MarkOutboxSent(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	err := r.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Where("id IN ? AND status = ?", ids, OutboxStatusPending).
		Update("status", OutboxStatusSent).
		Error
	if err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	return nil
}

// MarkOutboxRetry 补发失败：retry_count+1
func (r *CouponRepository) MarkOutboxRetry(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	err := r.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Where("id IN ? AND status = ?", ids, OutboxStatusPending).
		UpdateColumn("retry_count", gorm.Expr("retry_count + 1")).
		Error
	if err != nil {
		return fmt.Errorf("mark outbox retry: %w", err)
	}
	return nil
}

// AbandonOutbox 重试超限：放弃（status=2，人工对账兜底）
func (r *CouponRepository) AbandonOutbox(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	err := r.db.WithContext(ctx).
		Model(&OutboxEvent{}).
		Where("id IN ? AND status = ?", ids, OutboxStatusPending).
		Update("status", OutboxStatusAbandon).
		Error
	if err != nil {
		return fmt.Errorf("abandon outbox: %w", err)
	}
	return nil
}
