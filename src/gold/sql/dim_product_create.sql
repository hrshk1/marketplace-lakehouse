-- Product dimension, SCD Type 1, with the two-level category tree and the
-- promoted document attributes flattened in.
CREATE TABLE gold.dim_product (
    product_sk    BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    seller_id     BIGINT,
    category_id   INT,
    category_name STRING,
    department    STRING,
    brand         STRING,
    color         STRING,
    material      STRING,
    mrp           DECIMAL(12, 2),
    price         DECIMAL(12, 2),
    is_active     BOOLEAN,
    is_deleted    BOOLEAN
) USING iceberg;

INSERT INTO gold.dim_product VALUES (
    -1, -1, 'Unknown', -1, -1, 'Unknown', 'Unknown', CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS DECIMAL(12, 2)), CAST(NULL AS DECIMAL(12, 2)),
    CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN)
)
