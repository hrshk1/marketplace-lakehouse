-- Business question: Do higher loyalty tiers place bigger orders? Each order
-- is counted under the tier the customer held when they placed it.
WITH order_values AS (
    SELECT f.order_id,
           c.loyalty_tier,
           sum(f.item_total) AS order_value
    FROM dw.fact_order_items AS f
    JOIN dw.dim_customer AS c ON c.customer_sk = f.customer_sk
    WHERE f.order_status <> 'cancelled'
    GROUP BY f.order_id, c.loyalty_tier
)
SELECT loyalty_tier,
       count(*) AS orders,
       sum(order_value) AS revenue,
       round(avg(order_value), 2) AS avg_order_value,
       rank() OVER (ORDER BY avg(order_value) DESC) AS rank_by_order_value
FROM order_values
GROUP BY loyalty_tier
ORDER BY rank_by_order_value, loyalty_tier;
