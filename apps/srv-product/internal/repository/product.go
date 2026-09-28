// Package repository srv-product 持久层（私有库 product_db，账号 product_rw）。
//
// 纪律（brief）：
//   - GORM 连表查询一律 Find（多行投影到 slice），禁用 Scan（会静默丢列）
//   - 错误一律 %w 包装
//
// 库存预占（ReserveStock）用 tx 「SELECT ... FOR UPDATE 行锁 + 条件更新」
// 两步语义（brief 修订：inventories 行锁扣减，防超卖）：
// 行锁未命中（商品无库存行）→ ErrInventoryMissing；
// available < qty → ErrStockNotEnough；更新影响 0 行 → ErrReserveFailed。
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 库存预占哨兵错误（服务层映射 3001/3002/3003）
var (
	// ErrNotFound 无记录
	ErrNotFound = errors.New("repository: record not found")
	// ErrInventoryMissing 商品无库存行（inventories 未初始化）
	ErrInventoryMissing = errors.New("repository: inventory row missing")
	// ErrStockNotEnough available < 请求量
	ErrStockNotEnough = errors.New("repository: stock not enough")
	// ErrReserveFailed 行锁更新未生效（并发竞态/约束失败）
	ErrReserveFailed = errors.New("repository: reserve stock failed")
)

// Product 商品主表行
type Product struct {
	ID          uint64    `gorm:"primaryKey;column:id"`
	Title       string    `gorm:"column:title"`
	Price       int64     `gorm:"column:price"`
	CategoryID  uint64    `gorm:"column:category_id"`
	OnShelf     int       `gorm:"column:on_shelf"`
	Description string    `gorm:"column:description"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

// TableName 显式表名
func (Product) TableName() string { return "products" }

// Category 分类行
type Category struct {
	ID   uint64 `gorm:"primaryKey;column:id"`
	Name string `gorm:"column:name"`
}

// TableName 显式表名
func (Category) TableName() string { return "categories" }

// Inventory 库存行（products 1:1）
type Inventory struct {
	ProductID uint64 `gorm:"column:product_id"`
	Total     int    `gorm:"column:total"`
	Reserved  int    `gorm:"column:reserved"`
	Available int    `gorm:"column:available"`
}

// ProductRow products ⋈ categories ⋈ inventories 3 表 JOIN 结果
type ProductRow struct {
	Product   Product
	Category  Category
	Inventory Inventory
}

// ProductRepository product_db 仓储
type ProductRepository struct {
	db *gorm.DB
}

// NewProductRepository 构造
func NewProductRepository(db *gorm.DB) *ProductRepository {
	return &ProductRepository{db: db}
}

// DB 暴露句柄（组装期健康检查用）
func (r *ProductRepository) DB() *gorm.DB { return r.db }

// GetProduct 查商品（products ⋈ categories ⋈ inventories 3 表 LEFT JOIN Find）。
// 库存行/分类未命中时对应列零值（LEFT JOIN 语义）。
func (r *ProductRepository) GetProduct(ctx context.Context, productID uint64) (*ProductRow, error) {
	var rows []struct {
		ProductID   uint64    `gorm:"column:product_id"`
		Title       string    `gorm:"column:title"`
		Price       int64     `gorm:"column:price"`
		CategoryID  uint64    `gorm:"column:category_id"`
		OnShelf     int       `gorm:"column:on_shelf"`
		Description string    `gorm:"column:description"`
		CreatedAt   time.Time `gorm:"column:created_at"`
		UpdatedAt   time.Time `gorm:"column:updated_at"`
		CatID       uint64    `gorm:"column:cat_id"`
		CatName     string    `gorm:"column:cat_name"`
		Total       int       `gorm:"column:total"`
		Reserved    int       `gorm:"column:reserved"`
		Available   int       `gorm:"column:available"`
	}
	err := r.db.WithContext(ctx).
		Table("products").
		Select("products.id AS product_id, products.title, products.price, products.category_id, "+
			"products.on_shelf, products.description, products.created_at, products.updated_at, "+
			"categories.id AS cat_id, categories.name AS cat_name, "+
			"inventories.total, inventories.reserved, inventories.available").
		Joins("LEFT JOIN categories ON categories.id = products.category_id").
		Joins("LEFT JOIN inventories ON inventories.product_id = products.id").
		Where("products.id = ?", productID).
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("get product %d: %w", productID, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("product %d: %w", productID, ErrNotFound)
	}
	head := rows[0]
	return &ProductRow{
		Product: Product{
			ID: head.ProductID, Title: head.Title, Price: head.Price, CategoryID: head.CategoryID,
			OnShelf: head.OnShelf, Description: head.Description,
			CreatedAt: head.CreatedAt, UpdatedAt: head.UpdatedAt,
		},
		Category:  Category{ID: head.CatID, Name: head.CatName},
		Inventory: Inventory{ProductID: head.ProductID, Total: head.Total, Reserved: head.Reserved, Available: head.Available},
	}, nil
}

// ListProducts 商品列表（limit/offset 分页，id ASC 稳定序）
func (r *ProductRepository) ListProducts(ctx context.Context, limit, offset int) ([]Product, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	var rows []Product
	err := r.db.WithContext(ctx).
		Order("id ASC").
		Limit(limit).
		Offset(offset).
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}
	return rows, nil
}

// ReserveStockTx 库存预占本地事务（brief：FOR UPDATE 行锁 + available 判定 + 条件更新）：
//  1. SELECT total,reserved,available FROM inventories WHERE product_id=? [FOR UPDATE]
//     （行锁仅 mysql 方言追加——sqlite 测试库不认 FOR UPDATE 语法，经
//     Dialector.Name() 门控；sqlite 单写者模型下事务内串行等价）
//  2. 无行 → ErrInventoryMissing；available < qty → ErrStockNotEnough
//  3. 条件更新 reserved+qty / available-qty（WHERE available >= qty）；
//     影响 0 行 → ErrReserveFailed
//
// 同事务提交，返回预占后剩余 available。
func (r *ProductRepository) ReserveStockTx(ctx context.Context, productID uint64, qty int) (int, error) {
	var remaining int
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := tx.WithContext(ctx).
			Table("inventories").
			Select("total, reserved, available").
			Where("product_id = ?", productID)
		if r.db.Dialector.Name() == "mysql" {
			q = q.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var invs []Inventory
		if err := q.Find(&invs).Error; err != nil {
			return fmt.Errorf("lock inventory %d: %w", productID, err)
		}
		if len(invs) == 0 {
			return fmt.Errorf("inventory %d: %w", productID, ErrInventoryMissing)
		}
		inv := invs[0]
		// 库存行未初始化（total/reserved/available 全 0，seedsql 种子形态）
		// 视同无库存：不可预占
		if inv.Total == 0 && inv.Reserved == 0 && inv.Available == 0 {
			return fmt.Errorf("inventory %d: %w", productID, ErrInventoryMissing)
		}
		if inv.Available < qty {
			return fmt.Errorf("inventory %d (available %d): %w", productID, inv.Available, ErrStockNotEnough)
		}
		res := tx.WithContext(ctx).
			Table("inventories").
			Where("product_id = ? AND available >= ?", productID, qty).
			Updates(map[string]interface{}{
				"reserved":  gorm.Expr("reserved + ?", qty),
				"available": gorm.Expr("available - ?", qty),
			})
		if res.Error != nil {
			return fmt.Errorf("update inventory %d: %w", productID, res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("inventory %d: %w", productID, ErrReserveFailed)
		}
		remaining = inv.Available - qty
		return nil
	})
	if err != nil {
		return 0, err
	}
	return remaining, nil
}

// GetStock 查库存（列表页 Stock 列投影用；无库存行 → 0）
func (r *ProductRepository) GetStock(ctx context.Context, productIDs []uint64) (map[uint64]int, error) {
	out := make(map[uint64]int, len(productIDs))
	if len(productIDs) == 0 {
		return out, nil
	}
	var rows []struct {
		ProductID uint64 `gorm:"column:product_id"`
		Available int    `gorm:"column:available"`
	}
	err := r.db.WithContext(ctx).
		Table("inventories").
		Select("product_id, available").
		Where("product_id IN ?", productIDs).
		Find(&rows).
		Error
	if err != nil {
		return nil, fmt.Errorf("get stocks: %w", err)
	}
	for i := range rows {
		out[rows[i].ProductID] = rows[i].Available
	}
	return out, nil
}
