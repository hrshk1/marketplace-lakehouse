-- Grain: one row per payment attempt.
SELECT payment.payment_id,
       payment.order_id,
       CAST(date_format(payment.created_at, 'yyyyMMdd') AS INT) AS payment_date_key,
       coalesce(customer.customer_sk, -1) AS customer_sk,
       coalesce(method.payment_method_sk, -1) AS payment_method_sk,
       orders.order_ts,
       payment.attempt_no,
       payment.amount,
       payment.payment_status,
       payment.payment_status = 'success' AS is_successful,
       payment.paid_at
FROM silver.payments AS payment
JOIN silver.orders AS orders ON orders.order_id = payment.order_id
LEFT JOIN gold.dim_customer AS customer
       ON customer.customer_id = orders.customer_id
      AND orders.order_ts >= customer.valid_from
      AND orders.order_ts < customer.valid_to
LEFT JOIN gold.dim_payment_method AS method ON method.payment_method = payment.payment_method
WHERE CAST(date_trunc('month', orders.order_ts) AS DATE) IN ({order_months})
