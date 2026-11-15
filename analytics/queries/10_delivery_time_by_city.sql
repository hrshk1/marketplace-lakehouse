-- Business question: How long do deliveries take in each city (average,
-- median and worst case), so slow cities can be investigated?
WITH delivered_orders AS (
    SELECT DISTINCT f.order_id, c.city, f.delivery_days
    FROM dw.fact_order_items AS f
    JOIN dw.dim_customer AS c ON c.customer_sk = f.customer_sk
    WHERE f.delivery_days IS NOT NULL
)
SELECT city,
       count(*) AS delivered_orders,
       round(avg(delivery_days), 2) AS avg_delivery_days,
       round(
           (percentile_cont(0.5) WITHIN GROUP (ORDER BY delivery_days))::numeric, 2
       ) AS median_delivery_days,
       max(delivery_days) AS slowest_delivery_days
FROM delivered_orders
GROUP BY city
ORDER BY avg_delivery_days DESC, city;
