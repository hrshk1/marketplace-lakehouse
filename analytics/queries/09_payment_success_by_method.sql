-- Business question: How reliable is each payment method? What share of
-- attempts succeed, how often does the first attempt succeed, and how much
-- money did each method collect?
SELECT m.payment_method,
       count(*) AS attempts,
       count(*) FILTER (WHERE p.is_successful) AS successful_attempts,
       round(100.0 * count(*) FILTER (WHERE p.is_successful) / count(*), 1) AS success_rate_pct,
       round(
           100.0 * count(*) FILTER (WHERE p.attempt_no = 1 AND p.is_successful)
           / nullif(count(*) FILTER (WHERE p.attempt_no = 1), 0),
           1
       ) AS first_attempt_success_pct,
       coalesce(sum(p.amount) FILTER (WHERE p.is_successful), 0) AS collected_amount
FROM dw.fact_payments AS p
JOIN dw.dim_payment_method AS m ON m.payment_method_sk = p.payment_method_sk
GROUP BY m.payment_method
ORDER BY attempts DESC, m.payment_method;
