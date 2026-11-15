-- Business question: Each month, how many buying customers are new and how
-- many are returning?
WITH purchases AS (
    SELECT DISTINCT c.customer_id,
           date_trunc('month', d.full_date)::date AS order_month
    FROM dw.fact_order_items AS f
    JOIN dw.dim_customer AS c ON c.customer_sk = f.customer_sk
    JOIN dw.dim_date AS d ON d.date_key = f.order_date_key
    WHERE f.order_status <> 'cancelled'
      AND c.customer_id > 0
),
labelled AS (
    SELECT order_month,
           order_month = min(order_month) OVER (PARTITION BY customer_id) AS is_first_month
    FROM purchases
)
SELECT to_char(order_month, 'YYYY-MM') AS month,
       count(*) FILTER (WHERE is_first_month) AS new_customers,
       count(*) FILTER (WHERE NOT is_first_month) AS returning_customers
FROM labelled
GROUP BY order_month
ORDER BY order_month;
