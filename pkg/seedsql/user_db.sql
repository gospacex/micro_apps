-- user_db：srv-user 私有库（一库一 srv 一账号，账号 user_rw）
CREATE DATABASE IF NOT EXISTS user_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE user_db;

-- 用户主表
CREATE TABLE IF NOT EXISTS users (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(64)  NOT NULL,
    password    VARCHAR(128) NOT NULL DEFAULT '',
    nickname    VARCHAR(64)  NOT NULL DEFAULT '',
    phone       VARCHAR(32)  NOT NULL DEFAULT '',
    status      TINYINT      NOT NULL DEFAULT 1 COMMENT '1 正常 0 禁用',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_username (username),
    UNIQUE KEY uk_phone (phone)
) ENGINE=InnoDB COMMENT='用户主表';

-- 用户资料（users 1:1）
CREATE TABLE IF NOT EXISTS user_profiles (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id     BIGINT UNSIGNED NOT NULL,
    avatar      VARCHAR(256) NOT NULL DEFAULT '',
    bio         VARCHAR(512) NOT NULL DEFAULT '',
    city        VARCHAR(64)  NOT NULL DEFAULT '',
    level       INT          NOT NULL DEFAULT 1,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user (user_id)
) ENGINE=InnoDB COMMENT='用户资料（与 users 1:1）';

-- 登录态会话
CREATE TABLE IF NOT EXISTS auth_sessions (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id     BIGINT UNSIGNED NOT NULL,
    token_jti   VARCHAR(64)  NOT NULL DEFAULT '',
    login_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at  DATETIME     NOT NULL,
    revoked_at  DATETIME     NULL DEFAULT NULL,
    KEY idx_user (user_id),
    UNIQUE KEY uk_jti (token_jti)
) ENGINE=InnoDB COMMENT='登录态会话';
