# Data quality

Quality is enforced in two places:

- **Silver hard rules** decide row by row what is allowed into Silver. A row that breaks a rule is
  moved to `silver_quarantine.<table>` with the rule id, the reason, the batch and the original
  row. Nothing is dropped silently.
- **Checks** run after each layer and decide whether the pipeline may continue. They are declared
  in `config/dq/bronze.yml`, `silver.yml` and `gold.yml`.

The YAML files are the source of truth. The tables below list every check and rule in them.

## How checks work

Each check has an id, a type, a severity and a threshold.

| Type | Passes when | Threshold means |
| --- | --- | --- |
| `not_null` | the column has no nulls | largest tolerated share of failing rows |
| `unique` | no value (or combination) appears twice | same |
| `accepted_values` | every non-null value is in the list | same |
| `relationships` | every non-null foreign key exists in the parent table | same |
| `range` | every value lies within `min` and `max` | same |
| `regex` | every non-null value matches the pattern | same |
| `custom_sql` | the query returns no failing rows | same |
| `freshness` | the newest timestamp is recent enough, measured against the end of the batch | largest lag in hours |
| `row_count_reconciliation` | two counts (or sums) agree | largest relative difference |

Freshness is measured against the batch's logical end, not the wall clock, so backfilling an old
date is not reported as stale.

Severity decides what a failure does:

- `error`: the layer's gate fails and every later layer is blocked.
- `warn`: the result is recorded and shown, and the pipeline continues.

Results are written to `dq.check_results` (run id, batch id, layer, table, check id, type,
severity, status, observed value, threshold, failing and checked row counts, timestamp). The keys
of failing rows are written to `dq.failed_rows`, capped at 100,000 per check and run.

The layers use severity differently on purpose:

- **Bronze is raw.** Checks that describe a problem in the source are warnings; Silver decides
  what happens to those rows. Errors are reserved for a broken load.
- **Silver is the contract.** After quarantine the data must be clean, so almost every check is
  an error.
- **Gold is what people query.** Keys, referential integrity, the SCD2 invariants and
  reconciliation with Silver are all errors.

Checks declared under `common_checks` are applied to every table of the layer. In the tables
below they appear once per table.

## Checks

### Bronze (27 checks)

| Check | Table | Type | Severity | Threshold | Why it exists |
| --- | --- | --- | --- | ---: | --- |
| `brz_orders_pk_not_null` | `orders` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_orders_source_reconciliation` | `orders` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_orders_customer_id_not_null` | `orders` | not_null | warn | 0 | Guest-checkout bug in the source; such orders map to the unknown customer in Gold. |
| `brz_orders_status_accepted` | `orders` | accepted_values | warn | 0 | A new status means the order lifecycle changed and Gold logic needs a review. |
| `brz_orders_late_arriving` | `orders` | custom_sql | warn | 0 | Rows whose updated_at is older than the previous watermark were committed late in the source. The lookback window saved them; a growing count means it is too short. |
| `brz_orders_freshness` | `orders` | freshness | warn | 24 | No order activity for a full day almost always means extraction is broken. |
| `brz_order_items_pk_not_null` | `order_items` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_order_items_source_reconciliation` | `order_items` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_order_items_order_exists` | `order_items` | relationships | warn | 0 | The source has no foreign key here, so items can point at orders that never existed. |
| `brz_order_items_quantity_positive` | `order_items` | range | warn | 0 | Zero or negative quantities would distort units and revenue. |
| `brz_payments_pk_not_null` | `payments` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_payments_source_reconciliation` | `payments` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_payments_amount_positive` | `payments` | range | warn | 0 | The source has no CHECK constraint on amount; non-positive amounts are gateway errors. |
| `brz_payments_method_accepted` | `payments` | accepted_values | warn | 0 | A new payment method must be added to dim_payment_method before it is reported on. |
| `brz_customers_pk_not_null` | `customers` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_customers_source_reconciliation` | `customers` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_customers_pincode_format` | `customers` | regex | warn | 0 | Pincode feeds the SCD2 customer dimension; malformed values are rejected in Silver. |
| `brz_products_pk_not_null` | `products` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_products_source_reconciliation` | `products` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_sellers_pk_not_null` | `sellers` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_sellers_source_reconciliation` | `sellers` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_categories_pk_not_null` | `categories` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_categories_source_reconciliation` | `categories` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_reviews_pk_not_null` | `reviews` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_reviews_source_reconciliation` | `reviews` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |
| `brz_product_attributes_pk_not_null` | `product_attributes` | not_null | error | 0 | A row without its key cannot be deduplicated or merged downstream. |
| `brz_product_attributes_source_reconciliation` | `product_attributes` | row_count_reconciliation | error | 0 | Every row the source returned for the extraction window (counted with a separate query) must have landed in the batch, exactly once. |

### Silver (47 checks)

| Check | Table | Type | Severity | Threshold | Why it exists |
| --- | --- | --- | --- | ---: | --- |
| `slv_categories_pk_unique` | `categories` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_categories_pk_not_null` | `categories` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_categories_bronze_reconciliation` | `categories` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_sellers_pk_unique` | `sellers` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_sellers_pk_not_null` | `sellers` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_sellers_bronze_reconciliation` | `sellers` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_sellers_gstin_unique` | `sellers` | unique | error | 0 | GSTIN identifies a registered business; two sellers sharing one is a data error. |
| `slv_customers_pk_unique` | `customers` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_customers_pk_not_null` | `customers` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_customers_bronze_reconciliation` | `customers` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_customers_email_format` | `customers` | regex | error | 0 | Email is the customer contact key for CRM exports. |
| `slv_customers_tier_accepted` | `customers` | accepted_values | error | 0 | loyalty_tier is a tracked SCD2 attribute; an unknown value would create bad history. |
| `slv_customers_pincode_format` | `customers` | regex | error | 0 | pincode is a tracked SCD2 attribute and must be a valid 6-digit code. |
| `slv_products_pk_unique` | `products` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_products_pk_not_null` | `products` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_products_bronze_reconciliation` | `products` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_products_seller_exists` | `products` | relationships | error | 0 | dim_product and seller revenue reports need every product to resolve to a seller. |
| `slv_products_category_exists` | `products` | relationships | error | 0 | dim_product flattens the category tree; a missing category breaks the rollup. |
| `slv_products_price_positive` | `products` | range | error | 0 | Prices must be positive for revenue measures to be meaningful. |
| `slv_orders_pk_unique` | `orders` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_orders_pk_not_null` | `orders` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_orders_bronze_reconciliation` | `orders` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_orders_order_number_unique` | `orders` | unique | error | 0 | Checkout retries are quarantined, so each order_number must appear once. |
| `slv_orders_status_accepted` | `orders` | accepted_values | error | 0 | Cancellation rate and delivery measures depend on the exact set of statuses. |
| `slv_orders_customer_exists` | `orders` | relationships | error | 0 | A non-null customer_id must resolve; null ones are handled by the unknown member. |
| `slv_orders_total_not_negative` | `orders` | range | error | 0 | Negative order totals would understate GMV. |
| `slv_orders_freshness` | `orders` | freshness | warn | 48 | Stale Silver orders mean the Bronze-to-Silver step is not keeping up. |
| `slv_order_items_pk_unique` | `order_items` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_order_items_pk_not_null` | `order_items` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_order_items_bronze_reconciliation` | `order_items` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_order_items_order_exists` | `order_items` | relationships | error | 0 | The fact table joins every line to its order; orphans are quarantined before this. |
| `slv_order_items_product_exists` | `order_items` | relationships | error | 0 | Every line must resolve to a product for category and seller reporting. |
| `slv_order_items_quantity_positive` | `order_items` | range | error | 0 | Units sold must be positive. |
| `slv_payments_pk_unique` | `payments` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_payments_pk_not_null` | `payments` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_payments_bronze_reconciliation` | `payments` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_payments_order_exists` | `payments` | relationships | error | 0 | fact_payments takes its customer and order date from the order. |
| `slv_payments_amount_positive` | `payments` | range | error | 0 | Non-positive amounts are quarantined; any left would corrupt collected revenue. |
| `slv_payments_method_accepted` | `payments` | accepted_values | error | 0 | Each method must exist in dim_payment_method. |
| `slv_reviews_pk_unique` | `reviews` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_reviews_pk_not_null` | `reviews` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_reviews_bronze_reconciliation` | `reviews` | row_count_reconciliation | error | 0 | Every key seen in Bronze must be either in Silver or in quarantine. A difference means rows were lost between the layers. |
| `slv_reviews_rating_range` | `reviews` | range | error | 0 | Ratings outside 1 to 5 would skew average ratings. |
| `slv_reviews_product_exists` | `reviews` | relationships | warn | 0 | Reviews come from a separate database, so a reference can lag behind the product. |
| `slv_product_attributes_pk_unique` | `product_attributes` | unique | error | 0 | Silver holds exactly one current row per business key; duplicates would fan out joins. |
| `slv_product_attributes_pk_not_null` | `product_attributes` | not_null | error | 0 | A null key cannot be joined to and would be dropped silently by inner joins. |
| `slv_product_attributes_product_exists` | `product_attributes` | relationships | warn | 0 | Attributes come from a separate database, so a reference can lag behind the product. |

### Gold (30 checks)

| Check | Table | Type | Severity | Threshold | Why it exists |
| --- | --- | --- | --- | ---: | --- |
| `gld_dim_date_key_unique` | `dim_date` | unique | error | 0 | A duplicated date would double every fact row that joins to it. |
| `gld_dim_payment_method_sk_unique` | `dim_payment_method` | unique | error | 0 | Surrogate keys must identify exactly one dimension row. |
| `gld_dim_seller_sk_unique` | `dim_seller` | unique | error | 0 | Surrogate keys must identify exactly one dimension row. |
| `gld_dim_seller_id_unique` | `dim_seller` | unique | error | 0 | A Type 1 dimension holds one row per seller. |
| `gld_dim_product_sk_unique` | `dim_product` | unique | error | 0 | Surrogate keys must identify exactly one dimension row. |
| `gld_dim_product_id_unique` | `dim_product` | unique | error | 0 | A Type 1 dimension holds one row per product. |
| `gld_dim_customer_sk_unique` | `dim_customer` | unique | error | 0 | Each version of a customer needs its own surrogate key. |
| `gld_dim_customer_one_current_version` | `dim_customer` | custom_sql | error | 0 | SCD2 invariant; exactly one version per customer may be current. |
| `gld_dim_customer_no_overlapping_versions` | `dim_customer` | custom_sql | error | 0 | SCD2 invariant; overlapping validity ranges would make the point-in-time join return two versions and duplicate fact rows. |
| `gld_dim_customer_valid_range` | `dim_customer` | custom_sql | error | 0 | A version must start before it ends. |
| `gld_dim_customer_covers_silver` | `dim_customer` | row_count_reconciliation | error | 0 | Every Silver customer must have a current version in the dimension. |
| `gld_fact_order_items_pk_unique` | `fact_order_items` | unique | error | 0 | The grain is one row per order line. |
| `gld_fact_order_items_customer_fk` | `fact_order_items` | relationships | error | 0 | Every fact row must join to a dimension row, the unknown member at worst. |
| `gld_fact_order_items_product_fk` | `fact_order_items` | relationships | error | 0 | Every fact row must join to a dimension row, the unknown member at worst. |
| `gld_fact_order_items_seller_fk` | `fact_order_items` | relationships | error | 0 | Every fact row must join to a dimension row, the unknown member at worst. |
| `gld_fact_order_items_date_fk` | `fact_order_items` | relationships | error | 0 | An order date outside the calendar would vanish from time-based reports. |
| `gld_fact_order_items_payment_method_fk` | `fact_order_items` | relationships | error | 0 | Every fact row must join to a dimension row, the unknown member at worst. |
| `gld_fact_order_items_point_in_time` | `fact_order_items` | custom_sql | error | 0 | The customer key must be the version valid when the order was placed. A row whose order time falls outside its version's validity means the join used the wrong one. |
| `gld_fact_order_items_total_not_negative` | `fact_order_items` | range | error | 0 | Negative line totals would understate GMV. |
| `gld_fact_order_items_known_customer` | `fact_order_items` | range | warn | 0.02 | A few orders without a customer are expected; more than 2 percent is a source problem. |
| `gld_fact_order_items_silver_reconciliation` | `fact_order_items` | row_count_reconciliation | error | 0 | Every accepted order line in Silver must appear in the fact table exactly once. |
| `gld_fact_payments_pk_unique` | `fact_payments` | unique | error | 0 | The grain is one row per payment attempt. |
| `gld_fact_payments_customer_fk` | `fact_payments` | relationships | error | 0 | Every fact row must join to a dimension row, the unknown member at worst. |
| `gld_fact_payments_payment_method_fk` | `fact_payments` | relationships | error | 0 | Every fact row must join to a dimension row, the unknown member at worst. |
| `gld_fact_payments_date_fk` | `fact_payments` | relationships | error | 0 | A payment date outside the calendar would vanish from time-based reports. |
| `gld_fact_payments_amount_positive` | `fact_payments` | range | error | 0 | Non-positive amounts are quarantined in Silver and must never reach the fact. |
| `gld_fact_payments_silver_reconciliation` | `fact_payments` | row_count_reconciliation | error | 0 | Every accepted payment attempt in Silver must appear in the fact table exactly once. |
| `gld_agg_daily_sales_grain_unique` | `agg_daily_sales` | unique | error | 0 | The aggregate has one row per date, department and city. |
| `gld_agg_daily_sales_cancellation_rate_range` | `agg_daily_sales` | range | error | 0 | A rate outside 0 to 1 means the aggregate logic is wrong. |
| `gld_agg_daily_sales_gmv_matches_fact` | `agg_daily_sales` | row_count_reconciliation | error | 0 | Total GMV in the aggregate must equal the fact it was computed from. |

## Silver hard rules

A row breaking one of these is moved to `silver_quarantine.<table>` instead of Silver. When a
row breaks several rules, the first one listed for its table is recorded.

| Rule | Table | A row is rejected when |
| --- | --- | --- |
| `CAT_MISSING_NAME` | `categories` | category_name is empty |
| `SEL_INVALID_GSTIN` | `sellers` | gstin does not match the 15-character GSTIN format |
| `SEL_RATING_OUT_OF_RANGE` | `sellers` | rating is outside 1 to 5 |
| `CUS_INVALID_PINCODE` | `customers` | pincode is not a 6-digit Indian pincode |
| `CUS_INVALID_TIER` | `customers` | loyalty_tier is not a known tier |
| `PRD_NON_POSITIVE_PRICE` | `products` | price is zero or negative |
| `PRD_PRICE_ABOVE_MRP` | `products` | price is higher than mrp |
| `ORD_FUTURE_DATE` | `orders` | order_ts is more than a day after the row was created |
| `ORD_DUPLICATE_NUMBER` | `orders` | order_number already belongs to an earlier order_id |
| `ORD_INVALID_STATUS` | `orders` | order_status is not a known status |
| `ORD_NEGATIVE_TOTAL` | `orders` | total_amount is negative |
| `OI_ORPHAN` | `order_items` | order_id does not exist in the orders source |
| `OI_PARENT_REJECTED` | `order_items` | the parent order was rejected by a Silver rule |
| `OI_NON_POSITIVE_QUANTITY` | `order_items` | quantity is zero or negative |
| `OI_NON_POSITIVE_PRICE` | `order_items` | unit_price is zero or negative |
| `PAY_NON_POSITIVE_AMOUNT` | `payments` | amount is zero or negative |
| `PAY_PARENT_REJECTED` | `payments` | the order is missing or was rejected by a Silver rule |
| `PAY_INVALID_METHOD` | `payments` | payment_method is not a known method |
| `PAY_INVALID_STATUS` | `payments` | payment_status is not a known status |
| `REV_INVALID_RATING` | `reviews` | rating is missing or outside 1 to 5 |
| `REV_MISSING_PRODUCT` | `reviews` | product_id is missing |
| `PA_MISSING_PRODUCT_ID` | `product_attributes` | product_id is missing |

## Measuring detection against the defect ledger

The data generator injects seven kinds of defects and records each one in a ledger
(`data/generator_ledger.sqlite`: day, type, table, record key). The ledger is the answer key.
`config/dq/defect_detectors.yml` names the rule or check responsible for each type, and
`python scripts/dev.py dq` compares what was detected with what was injected, key by key.

| Defect type | What the generator does | Detected by |
| --- | --- | --- |
| `duplicate_order` | Inserts a second order row with the same `order_number` and a new `order_id`, as a checkout retry would | Silver rule `ORD_DUPLICATE_NUMBER` |
| `future_order_date` | Inserts an order dated 2 to 30 days after it was created | Silver rule `ORD_FUTURE_DATE` |
| `orphan_order_item` | Inserts an order line whose `order_id` does not exist | Silver rule `OI_ORPHAN` |
| `non_positive_amount` | Sets a payment amount to zero or to a negative value | Silver rule `PAY_NON_POSITIVE_AMOUNT` |
| `null_customer_id` | Sets `customer_id` to null on an order | Bronze check `brz_orders_customer_id_not_null` |
| `late_arriving_update` | Applies a status update stamped 5 to 50 minutes before the previous watermark | Bronze check `brz_orders_late_arriving` |
| `schema_drift` | Runs `ALTER TABLE oltp.order_items ADD COLUMN gift_wrap boolean` on a configured day | Entry in `dq.schema_changes` |

- **Precision**: of the records the pipeline flagged, the share that are in the ledger.
- **Recall**: of the records in the ledger, the share the pipeline flagged.

### Result

Default profile, day 0 plus three simulated days, pipeline run after each day
(`python scripts/dev.py demo`):

| Defect type | Injected | Detected | True positives | Precision | Recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplicate_order | 1016 | 1016 | 1016 | 100.0% | 100.0% |
| future_order_date | 1016 | 1016 | 1016 | 100.0% | 100.0% |
| orphan_order_item | 1016 | 1016 | 1016 | 100.0% | 100.0% |
| non_positive_amount | 1098 | 1098 | 1098 | 100.0% | 100.0% |
| null_customer_id | 1016 | 1016 | 1016 | 100.0% | 100.0% |
| late_arriving_update | 38 | 38 | 38 | 100.0% | 100.0% |
| schema_drift | 1 | 1 | 1 | 100.0% | 100.0% |
| all types | 5201 | 5201 | 5201 | 100.0% | 100.0% |

What this result does and does not show:

- It shows that every injected defect travels from the source to the right quarantine table or
  check result, and that healthy rows are not flagged along the way. The integration test asserts
  the same on the small profile on every run.
- The defects are ones the generator was written to inject, and each has a rule aimed at it. The
  result is evidence that the mechanism works, not that the rules would catch problems nobody
  anticipated.
- Detection depends on the pipeline running after each simulated day. If several days pass
  between runs, the source overwrites intermediate row states, and a late update that was
  overwritten again before extraction can no longer be seen.

Orders rejected for a future date take their order lines with them: those lines are quarantined
under `OI_PARENT_REJECTED`, which is a consequence of the defect rather than a separate one, so it
is not scored.
