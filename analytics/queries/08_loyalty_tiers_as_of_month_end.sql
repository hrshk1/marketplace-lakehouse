-- Business question: How many customers were in each loyalty tier at the end
-- of each month? (An "as of" question, answered from SCD2 history.)
WITH month_ends AS (
    SELECT d.full_date AS as_of_date,
           -- The instant the day ends, in UTC, to compare with version validity.
           ((d.full_date + 1)::timestamp AT TIME ZONE 'UTC') AS end_of_day
    FROM dw.dim_date AS d
    WHERE d.is_month_end
      AND d.full_date >= (SELECT date_trunc('month', min(order_ts))::date FROM dw.fact_order_items)
      AND d.full_date < (
          SELECT (date_trunc('month', max(order_ts)) + INTERVAL '1 month')::date
          FROM dw.fact_order_items
      )
)
SELECT month_ends.as_of_date,
       c.loyalty_tier,
       count(*) AS customers
FROM month_ends
JOIN dw.dim_customer AS c
  ON c.valid_from <= month_ends.end_of_day
 AND c.valid_to > month_ends.end_of_day
 AND c.customer_since <= month_ends.as_of_date
WHERE c.customer_id > 0
GROUP BY month_ends.as_of_date, c.loyalty_tier
ORDER BY month_ends.as_of_date, c.loyalty_tier;
