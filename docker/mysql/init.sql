-- ============================================================
-- hubx/micro_apps docker 底座 MySQL 初始化脚本
-- 挂载点：/docker-entrypoint-initdb.d/01-init.sql
--
-- 与 pkg/seedsql 同步维护：user_db.sql / product_db.sql / order_db.sql /
-- pay_db.sql / coupon_db.sql 五份 DDL 直接内嵌于此（文件头部注释 + 逐库
-- CREATE DATABASE / USE 与 seedsql 逐字一致），改表结构时两处必须同改。
--
-- 五库五账号：一库一 srv 一账号，各账号只 GRANT 自己库。
-- 密码统一 hubx123（本地开发环境，与 configs/services/*.yaml 的 DSN 对齐）。
-- ============================================================

CREATE DATABASE IF NOT EXISTS user_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS product_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS order_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS pay_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS coupon_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------- 账号（一库一账号，各只授权自己库）----------
CREATE USER IF NOT EXISTS 'user_rw'@'%' IDENTIFIED BY 'hubx123';
CREATE USER IF NOT EXISTS 'product_rw'@'%' IDENTIFIED BY 'hubx123';
CREATE USER IF NOT EXISTS 'order_rw'@'%' IDENTIFIED BY 'hubx123';
CREATE USER IF NOT EXISTS 'pay_rw'@'%' IDENTIFIED BY 'hubx123';
CREATE USER IF NOT EXISTS 'coupon_rw'@'%' IDENTIFIED BY 'hubx123';

GRANT ALL PRIVILEGES ON user_db.*    TO 'user_rw'@'%';
GRANT ALL PRIVILEGES ON product_db.*  TO 'product_rw'@'%';
GRANT ALL PRIVILEGES ON order_db.*    TO 'order_rw'@'%';
GRANT ALL PRIVILEGES ON pay_db.*      TO 'pay_rw'@'%';
GRANT ALL PRIVILEGES ON coupon_db.*   TO 'coupon_rw'@'%';
FLUSH PRIVILEGES;

-- ============================================================
-- user_db：srv-user 私有库（一库一 srv 一账号，账号 user_rw）
-- ============================================================
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

-- ============================================================
-- product_db：srv-product 私有库（一库一 srv 一账号，账号 product_rw）
-- ============================================================
USE product_db;

-- 商品主表
CREATE TABLE IF NOT EXISTS products (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title       VARCHAR(128) NOT NULL,
    price       BIGINT       NOT NULL DEFAULT 0 COMMENT '分',
    category_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    on_shelf    TINYINT      NOT NULL DEFAULT 1 COMMENT '1 上架 0 下架',
    description VARCHAR(512) NOT NULL DEFAULT '',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_category (category_id),
    KEY idx_shelf (on_shelf)
) ENGINE=InnoDB COMMENT='商品主表';

-- 分类
CREATE TABLE IF NOT EXISTS categories (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    parent_id  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    name       VARCHAR(64)   NOT NULL,
    sort_order INT           NOT NULL DEFAULT 0,
    created_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_parent (parent_id)
) ENGINE=InnoDB COMMENT='商品分类';

-- 库存（products 1:1）
CREATE TABLE IF NOT EXISTS inventories (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    product_id   BIGINT UNSIGNED NOT NULL,
    total        INT NOT NULL DEFAULT 0,
    reserved     INT NOT NULL DEFAULT 0 COMMENT '已预占',
    available    INT NOT NULL DEFAULT 0 COMMENT '可售 = total - reserved',
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_product (product_id)
) ENGINE=InnoDB COMMENT='库存（与 products 1:1）';

-- 商品标签（products M:N tags）
CREATE TABLE IF NOT EXISTS product_tags (
    id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name  VARCHAR(32) NOT NULL,
    UNIQUE KEY uk_name (name)
) ENGINE=InnoDB COMMENT='商品标签';

-- 标签关联（M:N）
CREATE TABLE IF NOT EXISTS tag_relations (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    product_id BIGINT UNSIGNED NOT NULL,
    tag_id     BIGINT UNSIGNED NOT NULL,
    created_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_product_tag (product_id, tag_id),
    KEY idx_tag (tag_id)
) ENGINE=InnoDB COMMENT='商品-标签关联';

-- ============================================================
-- order_db：srv-order 私有库（一库一 srv 一账号，账号 order_rw）
-- ============================================================
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

-- ============================================================
-- pay_db：srv-pay 私有库（一库一 srv 一账号，账号 pay_rw）
-- ============================================================
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

-- ============================================================
-- coupon_db：srv-coupon 私有库（一库一 srv 一账号，账号 coupon_rw）
-- ============================================================
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
