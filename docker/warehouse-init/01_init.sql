-- 数据仓库初始化：分层 schema（ods 源层 / dwh 明细层 / dm 集市层）+ 示例源数据
-- 该脚本由 postgres 容器首次启动时自动执行（docker-entrypoint-initdb.d）

CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS dm;

-- ---------- ODS 源层：模拟业务库数据（updated_at 供增量抽取水位线使用） ----------
CREATE TABLE ods.users (
    user_id    INT PRIMARY KEY,
    user_name  TEXT NOT NULL,
    city       TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ods.orders (
    order_id   INT PRIMARY KEY,
    user_id    INT NOT NULL REFERENCES ods.users (user_id),
    amount     NUMERIC(10, 2) NOT NULL,
    status     TEXT NOT NULL,
    order_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_updated_at ON ods.orders (updated_at);
CREATE INDEX idx_users_updated_at ON ods.users (updated_at);

INSERT INTO ods.users (user_id, user_name, city) VALUES
    (1, '张三', '上海'),
    (2, '李四', '北京'),
    (3, '王五', '上海'),
    (4, '赵六', '深圳'),
    (5, '钱七', '北京');

INSERT INTO ods.orders (order_id, user_id, amount, status, order_date) VALUES
    (101, 1, 199.00, 'paid',    CURRENT_DATE - 1),
    (102, 2, 59.50,  'paid',    CURRENT_DATE - 1),
    (103, 1, 899.00, 'paid',    CURRENT_DATE),
    (104, 3, 1299.00,'paid',    CURRENT_DATE),
    (105, 4, 45.00,  'cancel',  CURRENT_DATE),
    (106, 2, 320.00, 'paid',    CURRENT_DATE),
    (107, 5, 76.80,  'paid',    CURRENT_DATE),
    (108, 3, 210.00, 'refund',  CURRENT_DATE),
    (109, 1, 66.00,  'paid',    CURRENT_DATE),
    (110, 4, 500.00, 'paid',    CURRENT_DATE);

-- ---------- DWH / DM 层目标表 ----------
CREATE TABLE dwh.fact_orders (
    order_id   INT PRIMARY KEY,
    user_id    INT NOT NULL,
    user_name  TEXT NOT NULL,
    city       TEXT NOT NULL,
    amount     NUMERIC(10, 2) NOT NULL,
    status     TEXT NOT NULL,
    order_date DATE NOT NULL,
    etl_date   DATE NOT NULL
);

-- 用户维表（增量 upsert 维护，供事实表关联）
CREATE TABLE dwh.dim_users (
    user_id    INT PRIMARY KEY,
    user_name  TEXT NOT NULL,
    city       TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- 增量落地暂存区（源表结构 + loaded_at）
CREATE TABLE dwh.stg_orders (
    order_id   INT,
    user_id    INT,
    amount     NUMERIC(10, 2),
    status     TEXT,
    order_date DATE,
    updated_at TIMESTAMPTZ,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dwh.stg_users (
    user_id    INT,
    user_name  TEXT,
    city       TEXT,
    updated_at TIMESTAMPTZ,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 增量水位线：每张源表记录上次抽取到的 updated_at
CREATE TABLE dwh.etl_watermark (
    table_name     TEXT PRIMARY KEY,
    last_watermark TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dm.daily_city_sales (
    stat_date    DATE NOT NULL,
    city         TEXT NOT NULL,
    order_cnt    INT NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (stat_date, city)
);

CREATE TABLE dm.daily_summary_report (
    stat_date    DATE PRIMARY KEY,
    city_cnt     INT NOT NULL,
    order_cnt    INT NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
