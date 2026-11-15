-- Business question: Which departments lose the most orders to cancellations
-- and returns?
WITH order_departments AS (
    SELECT DISTINCT f.order_id, p.department, f.order_status
    FROM dw.fact_order_items AS f
    JOIN dw.dim_product AS p ON p.product_sk = f.product_sk
)
SELECT department,
       count(*) AS orders,
       count(*) FILTER (WHERE order_status = 'cancelled') AS cancelled_orders,
       round(
           100.0 * count(*) FILTER (WHERE order_status = 'cancelled') / count(*), 1
       ) AS cancellation_rate_pct,
       count(*) FILTER (WHERE order_status = 'returned') AS returned_orders,
       round(
           100.0 * count(*) FILTER (WHERE order_status = 'returned') / count(*), 1
       ) AS return_rate_pct
FROM order_departments
GROUP BY department
ORDER BY department;
