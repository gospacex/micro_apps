// Package repository srv-pay 持久层（私有库 pay_db，账号 pay_rw）。
//
// 纪律（brief）：
//   - GORM 连表查询一律 Find（多行投影到 slice），禁用 Scan（会静默丢列）
//   - 错误一律 %w 包装
//
// 支付单状态机（pay_orders.status）：
//
//	pending → success（回调校验通过）/ failed（回调 success=false）
//	refunded 由离线对账（job）改写，本服务不产
package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// 支付单状态
const (
	PayStatusPending    = "pending"
	PayStatusProcessing = "processing"
	PayStatusSuccess    = "success"
	PayStatusFailed     = "failed"
	PayStatusRefunded   = "refunded"
)

// 哨兵错误（服务层映射业务码）
var (
	// ErrNotFound 无记录
	ErrNotFound = errors.New("repository: record not found")
	// ErrPayDuplicate 同 user+order 已存在未终态/成功支付单（映射 5002）
	ErrPayDuplicate = errors.New("repository: duplicate pay order")
	// ErrPayOrderMissing 回调引用的 pay_order_no 不存在（映射 5003）
	ErrPayOrderMissing = errors.New("repository: pay order missing")
)

// PayOrder 支付单行
type PayOrder struct {
	ID         uint64     `gorm:"primaryKey;column:id"`
	UserID     uint64     `gorm:"column:user_id"`
	OrderID    uint64     `gorm:"column:order_id"`
	OrderNo    string     `gorm:"column:order_no"`
	PayOrderNo string     `gorm:"column:pay_order_no"`
	Amount     int64      `gorm:"column:amount"`
	Channel    string     `gorm:"column:channel"`
	Status     string     `gorm:"column:status"`
	PaidAt     *time.Time `gorm:"column:paid_at"`
	CreatedAt  time.Time  `gorm:"column:created_at"`
	UpdatedAt  time.Time  `gorm:"column:updated_at"`
}

// TableName 显式表名
func (PayOrder) TableName() string { return "pay_orders" }

// PayCallback 回调流水行
type PayCallback struct {
	ID         uint64    `gorm:"primaryKey;column:id"`
	PayOrderNo string    `gorm:"column:pay_order_no"`
	Channel    string    `gorm:"column:channel"`
	RawBody    string    `gorm:"column:raw_body"`
	Success    int       `gorm:"column:success"`
	ReceivedAt time.Time `gorm:"column:received_at"`
}

// TableName 显式表名
func (PayCallback) TableName() string { return "pay_callbacks" }

// FlowRow 支付流水投影（ListPayFlows）
type FlowRow struct {
	ID         uint64
	PayOrderNo string
	OrderID    uint64
	Amount     int64
	Status     string
	CreatedAt  time.Time
}

// PayRepository pay_db 仓储
type PayRepository struct {
	db *gorm.DB
}

// NewPayRepository 构造
func NewPayRepository(db *gorm.DB) *PayRepository {
	return &PayRepository{db: db}
}

// DB 暴露句柄（组装期健康检查用）
func (r *PayRepository) DB() *gorm.DB { return r.db }

// FindByPayOrderNo 按支付单号查（回调校验入口）
func (r *PayRepository) FindByPayOrderNo(ctx context.Context, payOrderNo string) (*PayOrder, error) {
	var po PayOrder
	err := r.db.WithContext(ctx).Where("pay_order_no = ?", payOrderNo).First(&po).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("pay order %q: %w", payOrderNo, ErrNotFound)
		}
		return nil, fmt.Errorf("find pay order %q: %w", payOrderNo, err)
	}
	return &po, nil
}

// FindByUserOrder 按 user+order 查既有支付单（幂等重放返回既有单号；
// 无记录时返回 (nil, nil)，由调用方区分「不存在」与查询失败）
func (r *PayRepository) FindByUserOrder(ctx context.Context, userID, orderID uint64) (*PayOrder, error) {
	var rows []PayOrder
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND order_id = ?", userID, orderID).
		Order("id ASC").
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("find pay orders (%d,%d): %w", userID, orderID, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[len(rows)-1], nil
}

// CreatePayOrderTx 开支付单事务：
//  1. 幂等查重：同 user+order 已存在 pending/processing/success 支付单 → ErrPayDuplicate
//  2. 插入新支付单（status=pending，uk_pay_no 冲突即并发重复，同样返回 ErrPayDuplicate）
func (r *PayRepository) CreatePayOrderTx(ctx context.Context, in PayOrder, payOrderNo string) (*PayOrder, error) {
	out := &PayOrder{}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var n int64
		if err := tx.WithContext(ctx).
			Model(&PayOrder{}).
			Where("user_id = ? AND order_id = ? AND status IN ?", in.UserID, in.OrderID,
				[]string{PayStatusPending, PayStatusProcessing, PayStatusSuccess}).
			Count(&n).
			Error; err != nil {
			return fmt.Errorf("dup check: %w", err)
		}
		if n > 0 {
			return fmt.Errorf("create pay order: %w", ErrPayDuplicate)
		}
		po := PayOrder{
			UserID:     in.UserID,
			OrderID:    in.OrderID,
			OrderNo:    in.OrderNo,
			PayOrderNo: payOrderNo,
			Amount:     in.Amount,
			Channel:    in.Channel,
			Status:     PayStatusPending,
		}
		if err := tx.Create(&po).Error; err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("create pay order: %w", ErrPayDuplicate)
			}
			return fmt.Errorf("insert pay order: %w", err)
		}
		*out = po
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RecordCallback 回调流水落库（pay_callbacks 追加写，幂等由服务层去重判定先行）
func (r *PayRepository) RecordCallback(ctx context.Context, cb *PayCallback) error {
	cb.ReceivedAt = time.Now()
	if err := r.db.WithContext(ctx).Create(cb).Error; err != nil {
		return fmt.Errorf("record callback %s: %w", cb.PayOrderNo, err)
	}
	return nil
}

// MarkSuccess 支付单终态迁移 pending/processing → success + paid_at
// （WHERE status 守卫：已 success 的行不受影响，回调幂等）
func (r *PayRepository) MarkSuccess(ctx context.Context, payOrderNo string) (bool, error) {
	res := r.db.WithContext(ctx).
		Model(&PayOrder{}).
		Where("pay_order_no = ? AND status IN ?", payOrderNo,
			[]string{PayStatusPending, PayStatusProcessing}).
		Updates(map[string]interface{}{
			"status":  PayStatusSuccess,
			"paid_at": time.Now(),
		})
	if res.Error != nil {
		return false, fmt.Errorf("mark success %s: %w", payOrderNo, res.Error)
	}
	return res.RowsAffected > 0, nil
}

// MarkFailed 回调 success=false：pending → failed
func (r *PayRepository) MarkFailed(ctx context.Context, payOrderNo string) (bool, error) {
	res := r.db.WithContext(ctx).
		Model(&PayOrder{}).
		Where("pay_order_no = ? AND status = ?", payOrderNo, PayStatusPending).
		Update("status", PayStatusFailed)
	if res.Error != nil {
		return false, fmt.Errorf("mark failed %s: %w", payOrderNo, res.Error)
	}
	return res.RowsAffected > 0, nil
}

// ListFlows 支付流水（user_id + 日期闭区间 + limit，id DESC）
func (r *PayRepository) ListFlows(ctx context.Context, userID uint64, fromDate, toDate string, limit int) ([]FlowRow, error) {
	from, to, err := dateRange(fromDate, toDate)
	if err != nil {
		return nil, fmt.Errorf("list flows: parse date range: %w", err)
	}
	if limit <= 0 {
		limit = 100
	}
	q := r.db.WithContext(ctx).
		Model(&PayOrder{}).
		Select("id, pay_order_no, order_id, amount, status, created_at").
		Where("user_id = ?", userID)
	if !from.IsZero() {
		q = q.Where("created_at >= ?", from)
	}
	if !to.IsZero() {
		q = q.Where("created_at < ?", to)
	}
	var rows []FlowRow
	if err := q.Order("id DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list flows %d: %w", userID, err)
	}
	return rows, nil
}

// 唯一约束冲突判定（MySQL 1062 / SQLite UNIQUE constraint failed）
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "1062") || strings.Contains(msg, "duplicate entry") ||
		strings.Contains(msg, "unique constraint failed")
}

// dateRange 解析 yyyy-MM-dd 闭区间为 [from, to)；空串为零值（与 srv-order 同语义）
func dateRange(fromDate, toDate string) (time.Time, time.Time, error) {
	var from, to time.Time
	if fromDate != "" {
		t, err := time.Parse("2006-01-02", fromDate)
		if err != nil {
			return from, to, fmt.Errorf("from_date %q: %w", fromDate, err)
		}
		from = t
	}
	if toDate != "" {
		t, err := time.Parse("2006-01-02", toDate)
		if err != nil {
			return from, to, fmt.Errorf("to_date %q: %w", toDate, err)
		}
		// 闭区间转半开区间（含当日全天）
		to = t.Add(24 * time.Hour)
	}
	return from, to, nil
}
