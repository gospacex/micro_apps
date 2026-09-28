// Package repository srv-user 持久层（私有库 user_db，账号 user_rw）。
//
// 纪律（brief）：
//   - GORM 连表查询一律 Find（多行投影到 slice），禁用 Scan（会静默丢列）
//   - 错误一律 %w 包装
package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ErrNotFound 无记录
var ErrNotFound = errors.New("repository: record not found")

// ErrUsernameTaken 唯一约束冲突（users.uk_username / uk_phone）
var ErrUsernameTaken = errors.New("repository: username or phone taken")

// UserStatusActive 用户主表 status：1 正常 0 禁用
const UserStatusActive = 1

// User 用户主表行
type User struct {
	ID        uint64    `gorm:"primaryKey;column:id"`
	Username  string    `gorm:"column:username"`
	Password  string    `gorm:"column:password"`
	Nickname  string    `gorm:"column:nickname"`
	Phone     string    `gorm:"column:phone"`
	Status    int       `gorm:"column:status"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName 显式表名
func (User) TableName() string { return "users" }

// Profile 用户资料（users 1:1，LEFT JOIN 未命中时零值）
type Profile struct {
	Avatar string `gorm:"column:avatar"`
	Bio    string `gorm:"column:bio"`
	City   string `gorm:"column:city"`
	Level  int32  `gorm:"column:level"`
}

// Session 登录态会话行（auth_sessions，jti 唯一）
type Session struct {
	ID        uint64    `gorm:"primaryKey;column:id"`
	UserID    uint64    `gorm:"column:user_id"`
	TokenJTI  string    `gorm:"column:token_jti"`
	LoginAt   time.Time `gorm:"column:login_at"`
	ExpiresAt time.Time `gorm:"column:expires_at"`
}

// TableName 显式表名
func (Session) TableName() string { return "auth_sessions" }

// ProfileRow users ⋈ user_profiles JOIN 结果
type ProfileRow struct {
	User    User
	Profile Profile
}

// UserRepository user_db 仓储
type UserRepository struct {
	db *gorm.DB
}

// NewUserRepository 构造
func NewUserRepository(db *gorm.DB) *UserRepository {
	return &UserRepository{db: db}
}

// DB 暴露句柄（组装期健康检查用）
func (r *UserRepository) DB() *gorm.DB { return r.db }

// InsertUser 注册：users + user_profiles 同事务。指定 ID>0 时按 ID 落库
// （dev 演示形态），否则自增。唯一约束冲突（uk_username/uk_phone）
// 返回 ErrUsernameTaken（服务层映射 2001）。
func (r *UserRepository) InsertUser(ctx context.Context, id uint64, username, passwordHash, phone string) (uint64, error) {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		u := User{ID: id, Username: username, Password: passwordHash, Phone: phone, Status: UserStatusActive}
		if err := tx.Create(&u).Error; err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("insert user: %w", ErrUsernameTaken)
			}
			return fmt.Errorf("insert user: %w", err)
		}
		id = u.ID
		if err := tx.Exec(
			"INSERT INTO user_profiles (user_id, avatar, bio, city, level) VALUES (?, '', '', '', 1)",
			id,
		).Error; err != nil {
			return fmt.Errorf("insert profile: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// isUniqueViolation 跨方言唯一约束判定（MySQL err 1062 / SQLite UNIQUE constraint failed）
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "1062") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "UNIQUE constraint failed")
}

// FindByUsername 按 username 查主表（含密码散列与状态）
func (r *UserRepository) FindByUsername(ctx context.Context, username string) (*User, error) {
	var u User
	err := r.db.WithContext(ctx).Where("username = ?", username).First(&u).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("user %q: %w", username, ErrNotFound)
		}
		return nil, fmt.Errorf("find user %q: %w", username, err)
	}
	return &u, nil
}

// GetByID 按主键查用户（CheckUser 用；含 status）
func (r *UserRepository) GetByID(ctx context.Context, id uint64) (*User, error) {
	var u User
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&u).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("user %d: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("get user %d: %w", id, err)
	}
	return &u, nil
}

// GetProfile 查资料（users ⋈ user_profiles 2 表 LEFT JOIN Find；
// 资料未落库时明细列零值，主行仍命中）。
func (r *UserRepository) GetProfile(ctx context.Context, id uint64) (*ProfileRow, error) {
	type row struct {
		UserID   uint64    `gorm:"column:user_id"`
		Username string    `gorm:"column:username"`
		Nickname string    `gorm:"column:nickname"`
		Status   int       `gorm:"column:status"`
		Avatar   string    `gorm:"column:avatar"`
		Bio      string    `gorm:"column:bio"`
		City     string    `gorm:"column:city"`
		Level    int32     `gorm:"column:level"`
		Created  time.Time `gorm:"column:created_at"`
	}
	var rw row
	err := r.db.WithContext(ctx).
		Table("users").
		Select("users.id AS user_id, users.username, users.nickname, users.status, "+
			"user_profiles.avatar, user_profiles.bio, user_profiles.city, user_profiles.level, users.created_at").
		Joins("LEFT JOIN user_profiles ON user_profiles.user_id = users.id").
		Where("users.id = ?", id).
		Find(&rw).
		Error
	if err != nil {
		return nil, fmt.Errorf("get profile %d: %w", id, err)
	}
	if rw.UserID == 0 {
		return nil, fmt.Errorf("user %d: %w", id, ErrNotFound)
	}
	return &ProfileRow{
		User:    User{ID: rw.UserID, Username: rw.Username, Nickname: rw.Nickname, Status: rw.Status, CreatedAt: rw.Created},
		Profile: Profile{Avatar: rw.Avatar, Bio: rw.Bio, City: rw.City, Level: rw.Level},
	}, nil
}

// SaveSession 登录态落库（auth_sessions；jti 唯一，重复登录追加新行）
func (r *UserRepository) SaveSession(ctx context.Context, s *Session) error {
	s.LoginAt = time.Now()
	if err := r.db.WithContext(ctx).Create(s).Error; err != nil {
		return fmt.Errorf("save session user %d: %w", s.UserID, err)
	}
	return nil
}

// CountActiveSessions 统计用户当前有效会话数（演示/对账用）
func (r *UserRepository) CountActiveSessions(ctx context.Context, userID uint64) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).
		Model(&Session{}).
		Where("user_id = ? AND revoked_at IS NULL AND expires_at > ?", userID, time.Now()).
		Count(&n).
		Error
	if err != nil {
		return 0, fmt.Errorf("count sessions %d: %w", userID, err)
	}
	return n, nil
}
