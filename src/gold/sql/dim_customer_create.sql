-- Customer dimension, SCD Type 2 on city, state, pincode and loyalty_tier.
-- Personal data (name, email, phone) is deliberately not published to Gold.
CREATE TABLE gold.dim_customer (
    customer_sk    BIGINT,
    customer_id    BIGINT,
    city           STRING,
    state          STRING,
    pincode        STRING,
    loyalty_tier   STRING,
    customer_since DATE,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN,
    row_hash       STRING
) USING iceberg;

INSERT INTO gold.dim_customer VALUES (
    -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', CAST(NULL AS DATE),
    TIMESTAMP '1900-01-01 00:00:00', TIMESTAMP '9999-12-31 00:00:00', true, ''
)
