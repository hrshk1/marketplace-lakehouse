-- Business question: Do customers spend differently on weekends? Compare
-- order count, GMV, average order value and share of GMV.
WITH order_values AS (
    SELECT f.order_id,
           d.is_weekend,
           sum(f.item_total) AS order_value
    FROM dw.fact_order_items AS f
    JOIN dw.dim_date AS d ON d.date_key = f.order_date_key
    WHERE f.order_status <> 'cancelled'
    GROUP BY f.order_id, d.is_weekend
)
SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       count(*) AS orders,
       sum(order_value) AS gmv,
       round(avg(order_value), 2) AS avg_order_value,
       round(100 * sum(order_value) / sum(sum(order_value)) OVER (), 1) AS gmv_share_pct
FROM order_values
GROUP BY is_weekend
ORDER BY day_type;
