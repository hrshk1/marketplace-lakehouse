-- Payment method dimension. The set is small and fixed by the checkout application.
CREATE TABLE gold.dim_payment_method USING iceberg AS
SELECT payment_method_sk, payment_method, method_name, is_prepaid
FROM VALUES
    (-1, 'UNKNOWN', 'Unknown', CAST(NULL AS BOOLEAN)),
    (1, 'UPI', 'UPI', true),
    (2, 'CARD', 'Credit or debit card', true),
    (3, 'NET_BANKING', 'Net banking', true),
    (4, 'WALLET', 'Wallet', true),
    (5, 'COD', 'Cash on delivery', false)
    AS methods (payment_method_sk, payment_method, method_name, is_prepaid)
