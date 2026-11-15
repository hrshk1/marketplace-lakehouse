# Data dictionary: Gold layer

Every column of every Gold table. The same tables are published to the `dw` schema in the
serving warehouse with the same names and types.

Money is in Indian rupees with two decimals. Timestamps are UTC.

## `dim_date`

One row per calendar day from 2024-01-01 to 2029-12-31, plus the unknown member.

| Column | Type | Description |
| --- | --- | --- |
| `date_key` | int | Primary key, the date as `yyyymmdd`. `-1` is the unknown member. |
| `full_date` | date | The calendar date. |
| `day_of_week` | int | ISO day number: 1 is Monday, 7 is Sunday. |
| `day_name` | string | English day name. |
| `week_of_year` | int | ISO week number. |
| `month` | int | Month number, 1 to 12. |
| `month_name` | string | English month name. |
| `quarter` | int | Calendar quarter, 1 to 4. |
| `year` | int | Calendar year. |
| `is_weekend` | boolean | True on Saturday and Sunday. |
| `is_month_end` | boolean | True on the last day of a month. |

## `dim_customer`

One row per version of a customer (SCD Type 2).

| Column | Type | Description |
| --- | --- | --- |
| `customer_sk` | bigint | Primary key of the version. `-1` is the unknown member. |
| `customer_id` | bigint | Business key from the source. Repeats across versions. |
| `city` | string | City, conformed to its official name. Tracked. |
| `state` | string | State name. Tracked. |
| `pincode` | string | Six-digit postal code. Tracked. |
| `loyalty_tier` | string | `bronze`, `silver`, `gold` or `platinum`. Tracked. |
| `customer_since` | date | Date the customer account was created. |
| `valid_from` | timestamp | Start of the version, inclusive. 1900-01-01 for a customer's first version. |
| `valid_to` | timestamp | End of the version, exclusive. 9999-12-31 for the current version. |
| `is_current` | boolean | True for the version in effect now. Exactly one per customer. |
| `row_hash` | string | SHA-256 of the tracked attributes, used to detect changes. |

## `dim_seller`

One row per seller (SCD Type 1).

| Column | Type | Description |
| --- | --- | --- |
| `seller_sk` | bigint | Primary key. `-1` is the unknown member. |
| `seller_id` | bigint | Business key from the source. |
| `seller_name` | string | Registered business name. |
| `gstin` | string | 15-character GST identification number. |
| `city` | string | City the seller operates from. |
| `state` | string | State the seller operates from. |
| `rating` | decimal(2,1) | Current marketplace rating, 1.0 to 5.0. |
| `is_active` | boolean | False when the seller has been deactivated. |

## `dim_product`

One row per product (SCD Type 1), with the category tree flattened in.

| Column | Type | Description |
| --- | --- | --- |
| `product_sk` | bigint | Primary key. `-1` is the unknown member. |
| `product_id` | bigint | Business key from the source. |
| `product_name` | string | Listing title. |
| `seller_id` | bigint | Business key of the seller listing the product. |
| `category_id` | int | Business key of the leaf category. |
| `category_name` | string | Leaf category, for example `Smartphones`. |
| `department` | string | Top-level category, for example `Electronics`. |
| `brand` | string | Brand, promoted from the attribute document. Null when not provided. |
| `color` | string | Colour, promoted from the attribute document. Null when not provided. |
| `material` | string | Material, promoted from the attribute document. Null when not provided. |
| `mrp` | decimal(12,2) | Current maximum retail price. |
| `price` | decimal(12,2) | Current selling price. Past sales keep their own price in the fact. |
| `is_active` | boolean | False when the product is not on sale. |
| `is_deleted` | boolean | True when the product was soft-deleted in the source. |

## `dim_payment_method`

One row per payment method.

| Column | Type | Description |
| --- | --- | --- |
| `payment_method_sk` | int | Primary key. `-1` is the unknown member. |
| `payment_method` | string | Code used by the source: `UPI`, `CARD`, `NET_BANKING`, `WALLET`, `COD`. |
| `method_name` | string | Display name. |
| `is_prepaid` | boolean | True when money is collected before shipping; false for cash on delivery. |

## `fact_order_items`

Grain: one row per order line. Partitioned by `months(order_ts)`.

| Column | Type | Description |
| --- | --- | --- |
| `order_item_id` | bigint | Degenerate key of the order line. Unique. |
| `order_id` | bigint | Degenerate key of the order. |
| `order_date_key` | int | Date the order was placed. References `dim_date`. |
| `customer_sk` | bigint | Customer version valid when the order was placed. `-1` when the order has no customer. |
| `product_sk` | bigint | Product sold. References `dim_product`. |
| `seller_sk` | bigint | Seller fulfilling the line. References `dim_seller`. |
| `payment_method_sk` | int | Method of the order's latest payment attempt. `-1` when none was accepted. |
| `order_ts` | timestamp | When the order was placed. Source of the partition. |
| `quantity` | int | Units ordered on this line. Additive. |
| `unit_price` | decimal(12,2) | Price per unit charged on this line. |
| `discount` | decimal(12,2) | Discount applied to the line. Additive. |
| `item_total` | decimal(14,2) | `quantity * unit_price - discount`. Additive. |
| `shipping_fee` | decimal(12,2) | Order's shipping fee, stored on its first line only so that it sums correctly. |
| `order_status` | string | Latest order status: `placed`, `paid`, `packed`, `shipped`, `delivered`, `cancelled` or `returned`. |
| `delivery_days` | decimal(6,2) | Days from order to delivery. Null until delivered. Average over distinct orders. |

## `fact_payments`

Grain: one row per payment attempt. Partitioned by `months(order_ts)`.

| Column | Type | Description |
| --- | --- | --- |
| `payment_id` | bigint | Degenerate key of the attempt. Unique. |
| `order_id` | bigint | Degenerate key of the order being paid. |
| `payment_date_key` | int | Date the attempt was made. References `dim_date`. |
| `customer_sk` | bigint | Customer version valid when the order was placed. |
| `payment_method_sk` | int | Method used for the attempt. References `dim_payment_method`. |
| `order_ts` | timestamp | When the order was placed. Source of the partition. |
| `attempt_no` | int | 1 for the first attempt, 2 for a retry. |
| `amount` | decimal(12,2) | Amount of the attempt. Always positive. |
| `payment_status` | string | Latest status: `pending`, `success`, `failed` or `refunded`. |
| `is_successful` | boolean | True when the status is `success`. A refunded payment is not counted as successful. |
| `paid_at` | timestamp | When the money was collected. Null when it never was. |

## `agg_daily_sales`

Grain: one row per date, department and customer city. Rebuilt from `fact_order_items` every run.

| Column | Type | Description |
| --- | --- | --- |
| `date_key` | int | Order date. References `dim_date`. |
| `category` | string | Department (top-level category) of the products sold. |
| `city` | string | Customer's city when the order was placed. `Unknown` for orders without a customer. |
| `gmv` | decimal(18,2) | Sum of `item_total`, excluding cancelled orders. |
| `orders` | bigint | Orders with at least one line in the department, cancelled ones included. Not additive across departments. |
| `units` | bigint | Units sold, excluding cancelled orders. |
| `avg_order_value` | decimal(18,2) | `gmv` divided by the number of non-cancelled orders. Null when all were cancelled. |
| `cancellation_rate` | decimal(6,4) | Cancelled orders divided by `orders`, between 0 and 1. |
