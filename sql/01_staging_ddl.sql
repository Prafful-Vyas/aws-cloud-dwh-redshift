CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id           VARCHAR(32),
    customer_id        VARCHAR(32),
    order_status       VARCHAR(20),
    order_purchase_ts  TIMESTAMP,
    order_delivered_ts TIMESTAMP,
    order_estimated_ts TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (order_purchase_ts);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id      VARCHAR(32),
    order_item_id SMALLINT,
    product_id    VARCHAR(32),
    seller_id     VARCHAR(32),
    price         DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id    VARCHAR(32),
    customer_city  VARCHAR(64),
    customer_state CHAR(2)
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id    VARCHAR(32),
    seller_city  VARCHAR(64),
    seller_state CHAR(2)
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS staging.products (
    product_id        VARCHAR(32),
    category_name_en  VARCHAR(64),
    product_weight_g  INT
)
DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS staging.payments (
    order_id             VARCHAR(32),
    payment_type         VARCHAR(20),
    payment_installments SMALLINT,
    payment_value        DECIMAL(10, 2)
)
DISTSTYLE EVEN;
