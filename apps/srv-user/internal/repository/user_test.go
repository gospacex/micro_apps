// repository 层单测（sqlite cgo 驱动，仅验证 GORM 查询语义：
// 注册事务/唯一约束判定、JOIN Find 投影、会话落库。
// DSN 密码占位 / MySQL 方言差异不在本层覆盖）。
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

// newTestDB 建 sqlite 内存库：三张表手工 DDL（与 pkg/seedsql/user_db.sql 同构，
// 保留 uk_username / uk_phone / uk_jti 唯一约束——GORM AutoMigrate 不翻译
// MySQL DDL 的 UNIQUE KEY，须手工建）
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			password TEXT NOT NULL DEFAULT '',
			nickname TEXT NOT NULL DEFAULT '',
			phone TEXT NOT NULL DEFAULT '',
			status INTEGER NOT NULL DEFAULT 1,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (username),
			UNIQUE (phone))`,
		`CREATE TABLE IF NOT EXISTS user_profiles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL UNIQUE,
			avatar TEXT NOT NULL DEFAULT '',
			bio TEXT NOT NULL DEFAULT '',
			city TEXT NOT NULL DEFAULT '',
			level INTEGER NOT NULL DEFAULT 1)`,
		`CREATE TABLE IF NOT EXISTS auth_sessions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			token_jti TEXT NOT NULL DEFAULT '',
			login_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at DATETIME NOT NULL,
			revoked_at DATETIME NULL DEFAULT NULL,
			UNIQUE (token_jti))`,
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

func TestInsertUser(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewUserRepository(db)

	id, err := repo.InsertUser(ctx, 0, "alice", "hash-1", "13800000000")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if id == 0 {
		t.Fatalf("auto-increment id = 0")
	}
	// profile 行同事务落库
	var n int64
	if err := db.Table("user_profiles").Count(&n).Error; err != nil {
		t.Fatalf("count profiles: %v", err)
	}
	if n != 1 {
		t.Fatalf("profiles = %d, want 1", n)
	}

	// 指定 ID 落库（dev 演示形态）
	id2, err := repo.InsertUser(ctx, 88, "bob", "hash-2", "13800000001")
	if err != nil {
		t.Fatalf("insert id=88: %v", err)
	}
	if id2 != 88 {
		t.Fatalf("id = %d, want 88", id2)
	}
	// 同 ID 重复 → 唯一约束冲突 → ErrUsernameTaken（sqlite 走 pk 冲突）
	if _, err := repo.InsertUser(ctx, 88, "bob2", "hash-3", "13800000002"); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("dup insert: want ErrUsernameTaken, got %v", err)
	}
	// 同 username 重复 → ErrUsernameTaken（sqlite 错误信息含 "UNIQUE constraint failed"）
	if _, err := repo.InsertUser(ctx, 0, "alice", "hash-4", "13800000003"); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("dup username: want ErrUsernameTaken, got %v", err)
	}
}

func TestFindByUsernameAndGetByID(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewUserRepository(db)

	id, _ := repo.InsertUser(ctx, 0, "carol", "hash-c", "13800000004")

	u, err := repo.FindByUsername(ctx, "carol")
	if err != nil {
		t.Fatalf("FindByUsername: %v", err)
	}
	if u.Password != "hash-c" || u.Status != UserStatusActive {
		t.Fatalf("user = %+v", u)
	}
	if _, err := repo.FindByUsername(ctx, "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Username != "carol" {
		t.Fatalf("GetByID = %+v", got)
	}
}

func TestGetProfile(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewUserRepository(db)

	id, _ := repo.InsertUser(ctx, 0, "dave", "hash-d", "13800000005")

	// 资料未手工更新 → JOIN 行零值（level 默认 1 由 DDL DEFAULT 提供）
	pr, err := repo.GetProfile(ctx, id)
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if pr.User.Username != "dave" || pr.Profile.Level != 1 {
		t.Fatalf("profile = %+v", pr)
	}
	// 无该用户 → ErrNotFound
	if _, err := repo.GetProfile(ctx, 99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestSaveSessionAndCount(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewUserRepository(db)

	id, _ := repo.InsertUser(ctx, 0, "erin", "hash-e", "13800000006")
	sess := &Session{UserID: id, TokenJTI: "jti-1", ExpiresAt: time.Now().Add(time.Hour)}
	if err := repo.SaveSession(ctx, sess); err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	n, err := repo.CountActiveSessions(ctx, id)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Fatalf("active sessions = %d, want 1", n)
	}
	// 过期会话不计入
	expired := &Session{UserID: id, TokenJTI: "jti-2", ExpiresAt: time.Now().Add(-time.Hour)}
	if err := repo.SaveSession(ctx, expired); err != nil {
		t.Fatalf("SaveSession expired: %v", err)
	}
	if n, _ = repo.CountActiveSessions(ctx, id); n != 1 {
		t.Fatalf("active sessions = %d, want 1 (expired excluded)", n)
	}
}
