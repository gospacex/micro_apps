// repository 层单测（sqlite cgo 驱动，验证 GORM 查询语义：
// 3 表 JOIN Find 投影、条件更新扣减、库存哨兵错误分支。
// FOR UPDATE 行锁为 mysql 方言行为（经 Dialector 门控），
// sqlite 单写者模型下退化为普通读，此处不覆盖并发行锁语义）。
package repository

import (
	"context"
	"errors"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newTestDB 建 sqlite 内存库：三张表 DDL（与 pkg/seedsql/product_db.sql 同构）
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, ddl := range []string{
		`CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			price INTEGER NOT NULL DEFAULT 0,
			category_id INTEGER NOT NULL DEFAULT 0,
			on_shelf INTEGER NOT NULL DEFAULT 1,
			description TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE categories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			parent_id INTEGER NOT NULL DEFAULT 0,
			name TEXT NOT NULL)`,
		`CREATE TABLE inventories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			product_id INTEGER NOT NULL UNIQUE,
			total INTEGER NOT NULL DEFAULT 0,
			reserved INTEGER NOT NULL DEFAULT 0,
			available INTEGER NOT NULL DEFAULT 0)`,
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	// 种子：分类 1 + 商品 1/2 + 库存行
	if err := db.Exec(`INSERT INTO categories (id, name) VALUES (1, '数码')`).Error; err != nil {
		t.Fatalf("seed category: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO products (id, title, price, category_id, on_shelf) VALUES
			(1, '手机', 10000, 1, 1),
			(2, '下架品', 100, 1, 0)`,
	).Error; err != nil {
		t.Fatalf("seed products: %v", err)
	}
	if err := db.Exec(
		`INSERT INTO inventories (product_id, total, reserved, available) VALUES
			(1, 10, 0, 10),
			(3, 0, 0, 0)`,
	).Error; err != nil {
		t.Fatalf("seed inventories: %v", err)
	}
	return db
}

func TestGetProduct(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProductRepository(db)

	pr, err := repo.GetProduct(ctx, 1)
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if pr.Product.Title != "手机" || pr.Product.Price != 10000 {
		t.Fatalf("product = %+v", pr.Product)
	}
	// 3 表 JOIN：分类名 + 库存列均投影到
	if pr.Category.Name != "数码" || pr.Inventory.Available != 10 {
		t.Fatalf("join cols = cat %q inv %+v (want 数码/10)", pr.Category.Name, pr.Inventory)
	}
	// 库存行未命中（商品 2 无 inventories 行）→ 库存列零值（LEFT JOIN 语义）
	pr2, err := repo.GetProduct(ctx, 2)
	if err != nil {
		t.Fatalf("GetProduct 2: %v", err)
	}
	if pr2.Inventory.Available != 0 {
		t.Fatalf("no-inventory available = %d, want 0", pr2.Inventory.Available)
	}
	if _, err := repo.GetProduct(ctx, 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestReserveStockTx(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProductRepository(db)

	// 正常扣减
	remaining, err := repo.ReserveStockTx(ctx, 1, 3)
	if err != nil {
		t.Fatalf("reserve 3: %v", err)
	}
	if remaining != 7 {
		t.Fatalf("remaining = %d, want 7", remaining)
	}
	// 超额 → ErrStockNotEnough
	if _, err := repo.ReserveStockTx(ctx, 1, 99); !errors.Is(err, ErrStockNotEnough) {
		t.Fatalf("want ErrStockNotEnough, got %v", err)
	}
	// 无库存行（商品 3）→ ErrInventoryMissing
	if _, err := repo.ReserveStockTx(ctx, 3, 1); !errors.Is(err, ErrInventoryMissing) {
		t.Fatalf("want ErrInventoryMissing, got %v", err)
	}
	// 扣减后状态落库
	var inv Inventory
	if err := db.Table("inventories").Where("product_id = 1").Find(&inv).Error; err != nil {
		t.Fatalf("read inventory: %v", err)
	}
	if inv.Reserved != 3 || inv.Available != 7 {
		t.Fatalf("inventory = %+v (want reserved 3 / available 7)", inv)
	}
}

func TestListProductsAndGetStock(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProductRepository(db)

	products, err := repo.ListProducts(ctx, 20, 0)
	if err != nil {
		t.Fatalf("ListProducts: %v", err)
	}
	if len(products) != 2 {
		t.Fatalf("products = %d, want 2", len(products))
	}
	// 库存批量投影：商品 1 有库存行（10），商品 2 无（缺省 0）
	stocks, err := repo.GetStock(ctx, []uint64{1, 2, 999})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if stocks[1] != 10 || stocks[2] != 0 || stocks[999] != 0 {
		t.Fatalf("stocks = %+v", stocks)
	}
}
