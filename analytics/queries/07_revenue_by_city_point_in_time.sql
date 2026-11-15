-- Business question: How much revenue came from each city, counting every
-- order under the city the customer lived in when they placed it? And how
-- different is the answer if we (wrongly) use each customer's current city?
--
-- fact_order_items.customer_sk points at the SCD2 version valid at order
-- time, so joining on it gives the point-in-time view. Going through
-- customer_id to the current version re-attributes history to today's city.
WITH at_order_time AS (
    SELECT c.city, sum(f.item_total) AS revenue
    FROM dw.fact_order_items AS f
    JOIN dw.dim_customer AS c ON c.customer_sk = f.customer_sk
    WHERE f.order_status <> 'cancelled'
    GROUP BY c.city
),
by_current_city AS (
    SELECT today.city, sum(f.item_total) AS revenue
    FROM dw.fact_order_items AS f
    JOIN dw.dim_customer AS then_version ON then_version.customer_sk = f.customer_sk
    JOIN dw.dim_customer AS today
      ON today.customer_id = then_version.customer_id AND today.is_current
    WHERE f.order_status <> 'cancelled'
    GROUP BY today.city
)
SELECT coalesce(at_order_time.city, by_current_city.city) AS city,
       coalesce(at_order_time.revenue, 0) AS revenue_at_order_time,
       coalesce(by_current_city.revenue, 0) AS revenue_by_current_city
FROM at_order_time
FULL JOIN by_current_city ON by_current_city.city = at_order_time.city
ORDER BY city;
