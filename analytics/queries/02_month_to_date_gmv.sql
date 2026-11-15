-- Business question: On any given day, how much GMV has the month accumulated
-- so far? (A running total that restarts every month.)
WITH daily AS (
    SELECT d.full_date,
           sum(f.item_total) AS daily_gmv
    FROM dw.fact_order_items AS f
    JOIN dw.dim_date AS d ON d.date_key = f.order_date_key
    WHERE f.order_status <> 'cancelled'
    GROUP BY d.full_date
)
SELECT full_date,
       daily_gmv,
       sum(daily_gmv) OVER (
           PARTITION BY date_trunc('month', full_date)
           ORDER BY full_date
       ) AS month_to_date_gmv
FROM daily
ORDER BY full_date;
