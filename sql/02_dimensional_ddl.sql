CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key    INT PRIMARY KEY, -- YYYYMMDD
    full_date   DATE,
    year        SMALLINT,
    month       SMALLINT,
    day         SMALLINT,
    day_of_week SMALLINT,
    is_weekend  BOOLEAN
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key   BIGINT IDENTITY(1, 1),
    customer_id    VARCHAR(32),
    customer_city  VARCHAR(64),
    customer_state CHAR(2)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_seller (
    seller_key   BIGINT IDENTITY(1, 1),
    seller_id    VARCHAR(32),
    seller_city  VARCHAR(64),
    seller_state CHAR(2)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key       BIGINT IDENTITY(1, 1),
    product_id        VARCHAR(32),
    category_name_en  VARCHAR(64),
    product_weight_g  INT
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_key              BIGINT IDENTITY(1, 1),
    order_id               VARCHAR(32),
    customer_key           BIGINT,
    order_date_key         INT,
    order_status           VARCHAR(20),
    delivery_delay_days    INT,
    total_order_value      DECIMAL(12, 2),
    total_freight_value    DECIMAL(12, 2),
    payment_installments   SMALLINT
)
DISTSTYLE KEY DISTKEY (customer_key)
SORTKEY COMPOUND (order_date_key, customer_key);

CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_item_key BIGINT IDENTITY(1, 1),
    order_key      BIGINT,
    product_key    BIGINT,
    seller_key     BIGINT,
    price          DECIMAL(10, 2),
    freight_value  DECIMAL(10, 2)
)
DISTSTYLE KEY DISTKEY (product_key)
SORTKEY COMPOUND (product_key, seller_key);

-- Populate dim_date once for the dataset's date range (Olist covers 2016-2018;
-- padded a little on each side). Re-running is safe: INSERT only fires when empty.
INSERT INTO dw.dim_date (date_key, full_date, year, month, day, day_of_week, is_weekend)
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INT),
    d,
    EXTRACT(year FROM d),
    EXTRACT(month FROM d),
    EXTRACT(day FROM d),
    EXTRACT(dow FROM d),
    EXTRACT(dow FROM d) IN (0, 6)
FROM (
    SELECT DATEADD(day, seq, '2015-12-01'::date) AS d
    FROM (SELECT ROW_NUMBER() OVER () - 1 AS seq FROM staging.orders LIMIT 1100)
) dates
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date LIMIT 1);
