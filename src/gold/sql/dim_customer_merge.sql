-- SCD Type 2 merge. `{changes}` holds one row per customer with the latest
-- tracked attributes and the time they took effect (updated_at).
--
-- The source carries two kinds of rows:
--   merge_key IS NULL      a version to insert (new customer or changed attributes);
--   merge_key = customer   the current version of a changed customer, to be closed.
-- A change only counts if it is newer than the current version, so replaying
-- an old batch or receiving versions out of order never rewrites history.
MERGE INTO gold.dim_customer AS target
USING (
    WITH staged AS (
        SELECT customer_id, city, state, pincode, loyalty_tier, customer_since, updated_at,
               sha2(concat_ws('|', city, state, pincode, loyalty_tier), 256) AS row_hash
        FROM {changes}
    ),
    new_versions AS (
        SELECT staged.*, existing.customer_id IS NOT NULL AS is_change
        FROM staged
        LEFT JOIN gold.dim_customer AS existing
               ON existing.customer_id = staged.customer_id AND existing.is_current
        WHERE existing.customer_id IS NULL
           OR (existing.row_hash <> staged.row_hash AND staged.updated_at > existing.valid_from)
    ),
    numbered AS (
        SELECT new_versions.*,
               (SELECT greatest(coalesce(max(customer_sk), 0), 0) FROM gold.dim_customer)
                   + row_number() OVER (ORDER BY customer_id) AS customer_sk
        FROM new_versions
    )
    SELECT CAST(NULL AS BIGINT) AS merge_key, * FROM numbered
    UNION ALL
    SELECT customer_id AS merge_key, * FROM numbered WHERE is_change
) AS source
ON target.customer_id = source.merge_key AND target.is_current
WHEN MATCHED THEN UPDATE SET
    valid_to = source.updated_at,
    is_current = false
WHEN NOT MATCHED THEN INSERT (
    customer_sk, customer_id, city, state, pincode, loyalty_tier, customer_since,
    valid_from, valid_to, is_current, row_hash
) VALUES (
    source.customer_sk, source.customer_id, source.city, source.state, source.pincode,
    source.loyalty_tier, source.customer_since,
    CASE WHEN source.is_change THEN source.updated_at ELSE TIMESTAMP '1900-01-01 00:00:00' END,
    TIMESTAMP '9999-12-31 00:00:00', true, source.row_hash
)
