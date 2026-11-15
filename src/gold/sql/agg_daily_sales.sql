-- Daily sales by department and customer city (the city at order time).
-- GMV and units exclude cancelled orders. `orders` counts orders with at least
-- one line in the department, so it is not additive across departments.
SELECT fact.order_date_key AS date_key,
       product.department AS category,
       customer.city,
       CAST(
           sum(CASE WHEN fact.order_status <> 'cancelled' THEN fact.item_total ELSE 0 END)
           AS DECIMAL(18, 2)
       ) AS gmv,
       count(DISTINCT fact.order_id) AS orders,
       sum(CASE WHEN fact.order_status <> 'cancelled' THEN fact.quantity ELSE 0 END) AS units,
       CAST(
           sum(CASE WHEN fact.order_status <> 'cancelled' THEN fact.item_total ELSE 0 END)
           / nullif(
               count(DISTINCT CASE WHEN fact.order_status <> 'cancelled' THEN fact.order_id END),
               0
           )
           AS DECIMAL(18, 2)
       ) AS avg_order_value,
       CAST(
           count(DISTINCT CASE WHEN fact.order_status = 'cancelled' THEN fact.order_id END)
           / count(DISTINCT fact.order_id)
           AS DECIMAL(6, 4)
       ) AS cancellation_rate
FROM gold.fact_order_items AS fact
JOIN gold.dim_product AS product ON product.product_sk = fact.product_sk
JOIN gold.dim_customer AS customer ON customer.customer_sk = fact.customer_sk
GROUP BY fact.order_date_key, product.department, customer.city
