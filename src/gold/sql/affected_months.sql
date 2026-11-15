-- Order months touched by the batch: an order, one of its items or one of its
-- payments changed. Only these fact partitions are rebuilt.
SELECT DISTINCT CAST(date_trunc('month', orders.order_ts) AS DATE) AS order_month
FROM silver.orders AS orders
WHERE orders._batch_id = '{batch_id}'
   OR orders.order_id IN (
       SELECT order_id FROM silver.order_items WHERE _batch_id = '{batch_id}'
   )
   OR orders.order_id IN (
       SELECT order_id FROM silver.payments WHERE _batch_id = '{batch_id}'
   )
