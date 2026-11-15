-- Grain: one row per order line.
-- The customer key is resolved point-in-time: the dimension version that was
-- valid when the order was placed, not the customer's current version.
SELECT item.order_item_id,
       orders.order_id,
       CAST(date_format(orders.order_ts, 'yyyyMMdd') AS INT) AS order_date_key,
       coalesce(customer.customer_sk, -1) AS customer_sk,
       coalesce(product.product_sk, -1) AS product_sk,
       coalesce(seller.seller_sk, -1) AS seller_sk,
       coalesce(method.payment_method_sk, -1) AS payment_method_sk,
       orders.order_ts,
       item.quantity,
       item.unit_price,
       item.discount_amount AS discount,
       item.item_total,
       -- The fee is charged once per order; putting it on the first line keeps it additive.
       CASE
           WHEN item.order_item_id = min(item.order_item_id) OVER (PARTITION BY item.order_id)
           THEN orders.shipping_fee
           ELSE CAST(0 AS DECIMAL(12, 2))
       END AS shipping_fee,
       orders.order_status,
       CAST(
           round((unix_seconds(orders.delivered_at) - unix_seconds(orders.order_ts)) / 86400.0, 2)
           AS DECIMAL(6, 2)
       ) AS delivery_days
FROM silver.order_items AS item
JOIN silver.orders AS orders ON orders.order_id = item.order_id
LEFT JOIN gold.dim_customer AS customer
       ON customer.customer_id = orders.customer_id
      AND orders.order_ts >= customer.valid_from
      AND orders.order_ts < customer.valid_to
LEFT JOIN gold.dim_product AS product ON product.product_id = item.product_id
LEFT JOIN gold.dim_seller AS seller ON seller.seller_id = item.seller_id
LEFT JOIN (
    SELECT order_id, max_by(payment_method, attempt_no) AS payment_method
    FROM silver.payments
    GROUP BY order_id
) AS last_payment ON last_payment.order_id = orders.order_id
LEFT JOIN gold.dim_payment_method AS method
       ON method.payment_method = last_payment.payment_method
WHERE CAST(date_trunc('month', orders.order_ts) AS DATE) IN ({order_months})
