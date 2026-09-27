-- coupon_db：srv-coupon 私有库（一库一 srv 一账号，账号 coupon_rw）
CREATE DATABASE IF NOT EXISTS coupon_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE coupon_db;

-- 优惠券模板
CREATE TABLE IF NOT EXISTS coupons (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(64)  NOT NULL,
    discount     BIGINT       NOT NULL DEFAULT 0 COMMENT '面额/折扣（分或百分比）',
    type         VARCHAR(16)  NOT NULL DEFAULT 'amount' COMMENT 'amount 满减 / percent 折扣',
    total_quota  INT          NOT NULL DEFAULT 0,
    used_quota   INT          NOT NULL DEFAULT 0,
    valid_from   DATETIME     NOT NULL,
    valid_to     DATETIME     NOT NULL,
    status       TINYINT      NOT NULL DEFAULT 1 COMMENT '1 有效 0 停用',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_status (status)
) ENGINE=InnoDB COMMENT='优惠券模板';

-- 用户持有的券（唯一约束承担核销幂等：同一 user 同一 coupon 只能一条）
CREATE TABLE IF NOT EXISTS user_coupons (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id      BIGINT UNSIGNED NOT NULL,
    coupon_id    BIGINT UNSIGNED NOT NULL,
    status       VARCHAR(16)  NOT NULL DEFAULT 'held' COMMENT 'held/used/expired',
    used_order_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    granted_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_at      DATETIME     NULL DEFAULT NULL,
    UNIQUE KEY uk_user_coupon (user_id, coupon_id),
    KEY idx_status (status)
) ENGINE=InnoDB COMMENT='用户持有的券（唯一约束防重/幂等）';

-- 积分流水
CREATE TABLE IF NOT EXISTS points_log (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id      BIGINT UNSIGNED NOT NULL,
    points       INT         NOT NULL DEFAULT 0,
    source       VARCHAR(32) NOT NULL DEFAULT 'order.completed' COMMENT 'order.completed/coupon.exchanged/job.retry',
    ref_order_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_user (user_id, created_at)
) ENGINE=InnoDB COMMENT='积分流水';
