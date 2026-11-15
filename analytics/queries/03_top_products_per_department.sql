-- Business question: Which three products earn the most revenue in each
-- department?
WITH product_revenue AS (
    SELECT p.department,
           p.product_name,
           sum(f.item_total) AS revenue,
           sum(f.quantity) AS units
    FROM dw.fact_order_items AS f
    JOIN dw.dim_product AS p ON p.product_sk = f.product_sk
    WHERE f.order_status <> 'cancelled'
    GROUP BY p.department, p.product_name
),
ranked AS (
    SELECT product_revenue.*,
           row_number() OVER (
               PARTITION BY department
               ORDER BY revenue DESC, product_name
           ) AS rank_in_department
    FROM product_revenue
)
SELECT department, rank_in_department, product_name, revenue, units
FROM ranked
WHERE rank_in_department <= 3
ORDER BY department, rank_in_department;
