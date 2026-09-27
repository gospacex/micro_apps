-- product_db：srv-product 私有库（一库一 srv 一账号，账号 product_rw）
CREATE DATABASE IF NOT EXISTS product_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
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
