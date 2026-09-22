-- Wrapped in a stored procedure so the Step Functions state machine can invoke
-- one short "CALL dw.sp_transform_orders();" statement instead of embedding this
-- whole multi-statement script as an inline string inside the state machine's
-- ASL JSON (which is unwieldy to quote/escape correctly).
CREATE OR REPLACE PROCEDURE dw.sp_transform_orders()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO dw.dim_customer AS tgt
    USING (SELECT DISTINCT customer_id, customer_city, customer_state FROM staging.customers) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET customer_city = src.customer_city, customer_state = src.customer_state
    WHEN NOT MATCHED THEN INSERT (customer_id, customer_city, customer_state)
        VALUES (src.customer_id, src.customer_city, src.customer_state);

    MERGE INTO dw.dim_seller AS tgt
    USING (SELECT DISTINCT seller_id, seller_city, seller_state FROM staging.sellers) AS src
    ON tgt.seller_id = src.seller_id
    WHEN MATCHED THEN UPDATE SET seller_city = src.seller_city, seller_state = src.seller_state
    WHEN NOT MATCHED THEN INSERT (seller_id, seller_city, seller_state)
        VALUES (src.seller_id, src.seller_city, src.seller_state);

    MERGE INTO dw.dim_product AS tgt
    USING (SELECT DISTINCT product_id, category_name_en, product_weight_g FROM staging.products) AS src
    ON tgt.product_id = src.product_id
    WHEN MATCHED THEN UPDATE SET category_name_en = src.category_name_en, product_weight_g = src.product_weight_g
    WHEN NOT MATCHED THEN INSERT (product_id, category_name_en, product_weight_g)
        VALUES (src.product_id, src.category_name_en, src.product_weight_g);

    DELETE FROM dw.fact_orders WHERE order_id IN (SELECT order_id FROM staging.orders);

    INSERT INTO dw.fact_orders (order_id, customer_key, order_date_key, order_status,
                                 delivery_delay_days, total_order_value, total_freight_value,
                                 payment_installments)
    SELECT o.order_id, c.customer_key,
           CAST(TO_CHAR(o.order_purchase_ts, 'YYYYMMDD') AS INT),
           o.order_status,
           DATEDIFF(day, o.order_estimated_ts, o.order_delivered_ts),
           SUM(oi.price + oi.freight_value),
           SUM(oi.freight_value),
           MAX(p.payment_installments)
    FROM staging.orders o
    JOIN dw.dim_customer c ON c.customer_id = o.customer_id
    JOIN staging.order_items oi ON oi.order_id = o.order_id
    JOIN staging.payments p ON p.order_id = o.order_id
    GROUP BY o.order_id, c.customer_key, o.order_purchase_ts, o.order_status,
             o.order_estimated_ts, o.order_delivered_ts;

    DELETE FROM dw.fact_order_items
    WHERE order_key IN (SELECT order_key FROM dw.fact_orders WHERE order_id IN (SELECT order_id FROM staging.orders));

    INSERT INTO dw.fact_order_items (order_key, product_key, seller_key, price, freight_value)
    SELECT fo.order_key, dp.product_key, ds.seller_key, oi.price, oi.freight_value
    FROM staging.order_items oi
    JOIN dw.fact_orders fo ON fo.order_id = oi.order_id
    JOIN dw.dim_product dp ON dp.product_id = oi.product_id
    JOIN dw.dim_seller ds ON ds.seller_id = oi.seller_id;
END;
$$;
