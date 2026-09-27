-- order_db：srv-order 私有库（一库一 srv 一账号，账号 order_rw）
CREATE DATABASE IF NOT EXISTS order_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE order_db;

-- 订单主表
CREATE TABLE IF NOT EXISTS orders (
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id        BIGINT UNSIGNED NOT NULL,
    order_no       VARCHAR(40)  NOT NULL,
    status         VARCHAR(16)  NOT NULL DEFAULT 'created' COMMENT 'created/paid/shipped/done/cancelled/refunded',
    total_amount   BIGINT       NOT NULL DEFAULT 0 COMMENT '分',
    paid_amount    BIGINT       NOT NULL DEFAULT 0 COMMENT '分',
    pay_order_no   VARCHAR(64)  NOT NULL DEFAULT '',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_order_no (order_no),
    KEY idx_user (user_id),
    KEY idx_status (status),
    KEY idx_user_created (user_id, created_at)
) ENGINE=InnoDB COMMENT='订单主表';

-- 订单明细
CREATE TABLE IF NOT EXISTS order_items (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id    BIGINT UNSIGNED NOT NULL,
    product_id  BIGINT UNSIGNED NOT NULL,
    product_name VARCHAR(128) NOT NULL DEFAULT '',
    quantity    INT  NOT NULL DEFAULT 0,
    unit_price  BIGINT NOT NULL DEFAULT 0 COMMENT '分',
    KEY idx_order (order_id),
    KEY idx_product (product_id)
) ENGINE=InnoDB COMMENT='订单明细';

-- 订单状态流转日志
CREATE TABLE IF NOT EXISTS order_status_log (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id    BIGINT UNSIGNED NOT NULL,
    from_status VARCHAR(16) NOT NULL DEFAULT '',
    to_status   VARCHAR(16) NOT NULL,
    remark      VARCHAR(128) NOT NULL DEFAULT '',
    created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_order (order_id),
    KEY idx_to_status (to_status)
) ENGINE=InnoDB COMMENT='订单状态流转日志';

-- 退款记录
CREATE TABLE IF NOT EXISTS refund_records (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id    BIGINT UNSIGNED NOT NULL,
    user_id     BIGINT UNSIGNED NOT NULL,
    amount      BIGINT       NOT NULL DEFAULT 0 COMMENT '分',
    status      VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending/success/failed',
    reason      VARCHAR(128) NOT NULL DEFAULT '',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_order (order_id),
    KEY idx_status (status)
) ENGINE=InnoDB COMMENT='退款记录';

-- 事务发件箱（outbox 模式：本地事务写事件，后台轮询补发）
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    topic       VARCHAR(64)  NOT NULL,
    event_type  VARCHAR(64)  NOT NULL,
    payload     TEXT         NOT NULL,
    traceparent VARCHAR(128) NOT NULL DEFAULT '' COMMENT '发送时随 header 透传，mq 关时为空',
    status      TINYINT      NOT NULL DEFAULT 0 COMMENT '0 待发 1 已发 2 失败放弃',
    retry_count INT          NOT NULL DEFAULT 0,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_status (status, created_at)
) ENGINE=InnoDB COMMENT='事务发件箱（3s 轮询补发）';
