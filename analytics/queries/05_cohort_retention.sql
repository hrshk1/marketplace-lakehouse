-- Business question: Of the customers who first bought in a given month, what
-- share came back and bought again one month later, two months later, and so on?
WITH purchases AS (
    SELECT DISTINCT c.customer_id,
           date_trunc('month', d.full_date)::date AS order_month
    FROM dw.fact_order_items AS f
    JOIN dw.dim_customer AS c ON c.customer_sk = f.customer_sk
    JOIN dw.dim_date AS d ON d.date_key = f.order_date_key
    WHERE f.order_status <> 'cancelled'
      AND c.customer_id > 0
),
cohorts AS (
    SELECT customer_id, min(order_month) AS cohort_month
    FROM purchases
    GROUP BY customer_id
),
cohort_sizes AS (
    SELECT cohort_month, count(*) AS cohort_size
    FROM cohorts
    GROUP BY cohort_month
),
activity AS (
    SELECT cohorts.cohort_month,
           (
               12 * extract(YEAR FROM age(purchases.order_month, cohorts.cohort_month))
               + extract(MONTH FROM age(purchases.order_month, cohorts.cohort_month))
           )::int AS months_since_first_order,
           count(*) AS active_customers
    FROM purchases
    JOIN cohorts ON cohorts.customer_id = purchases.customer_id
    GROUP BY 1, 2
)
SELECT to_char(activity.cohort_month, 'YYYY-MM') AS cohort,
       cohort_sizes.cohort_size,
       activity.months_since_first_order,
       activity.active_customers,
       round(100.0 * activity.active_customers / cohort_sizes.cohort_size, 1) AS retention_pct
FROM activity
JOIN cohort_sizes ON cohort_sizes.cohort_month = activity.cohort_month
ORDER BY activity.cohort_month, activity.months_since_first_order;
