-- Business question: How is GMV trending month by month, and how fast is it
-- growing compared with the previous month?
-- GMV counts every order line except those of cancelled orders.
WITH monthly AS (
    SELECT date_trunc('month', d.full_date)::date AS month_start,
           sum(f.item_total) AS gmv,
           count(DISTINCT f.order_id) AS orders
    FROM dw.fact_order_items AS f
    JOIN dw.dim_date AS d ON d.date_key = f.order_date_key
    WHERE f.order_status <> 'cancelled'
    GROUP BY 1
)
SELECT to_char(month_start, 'YYYY-MM') AS month,
       gmv,
       orders,
       lag(gmv) OVER (ORDER BY month_start) AS previous_gmv,
       round(
           100.0 * (gmv - lag(gmv) OVER (ORDER BY month_start))
           / lag(gmv) OVER (ORDER BY month_start),
           1
       ) AS growth_pct
FROM monthly
ORDER BY month_start;
