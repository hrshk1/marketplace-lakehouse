-- Seller dimension, SCD Type 1: attributes are overwritten in place.
CREATE TABLE gold.dim_seller (
    seller_sk   BIGINT,
    seller_id   BIGINT,
    seller_name STRING,
    gstin       STRING,
    city        STRING,
    state       STRING,
    rating      DECIMAL(2, 1),
    is_active   BOOLEAN
) USING iceberg;

INSERT INTO gold.dim_seller VALUES (
    -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', CAST(NULL AS DECIMAL(2, 1)),
    CAST(NULL AS BOOLEAN)
)
