-- pay_db：srv-pay 私有库（一库一 srv 一账号，账号 pay_rw）
CREATE DATABASE IF NOT EXISTS pay_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE pay_db;

-- 支付单
CREATE TABLE IF NOT EXISTS pay_orders (
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id        BIGINT UNSIGNED NOT NULL,
    order_id       BIGINT UNSIGNED NOT NULL,
    order_no       VARCHAR(64)  NOT NULL DEFAULT '',
    pay_order_no   VARCHAR(64)  NOT NULL,
    amount         BIGINT       NOT NULL DEFAULT 0 COMMENT '分',
    channel        VARCHAR(16)  NOT NULL DEFAULT 'alipay' COMMENT 'alipay/wechat/union',
    status         VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending/processing/success/failed/refunded',
    paid_at        DATETIME     NULL DEFAULT NULL,
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_pay_no (pay_order_no),
    KEY idx_user (user_id),
    KEY idx_order (order_id),
    KEY idx_status (status, created_at)
) ENGINE=InnoDB COMMENT='支付单';

-- 支付回调（渠道异步通知流水）
CREATE TABLE IF NOT EXISTS pay_callbacks (
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    pay_order_no   VARCHAR(64)  NOT NULL,
    channel        VARCHAR(16)  NOT NULL DEFAULT '',
    raw_body       TEXT         NULL,
    success        TINYINT      NOT NULL DEFAULT 0 COMMENT '1 校验通过 0 校验失败',
    received_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_pay_no (pay_order_no),
    KEY idx_channel (channel, received_at)
) ENGINE=InnoDB COMMENT='支付回调流水';
