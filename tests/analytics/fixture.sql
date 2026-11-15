-- A star schema small enough to work every analytics answer out by hand.
--
-- Customers (SCD2):
--   101  Pune/bronze until 2026-06-15, then Mumbai/silver   (versions 1 and 5)
--   102  Delhi/gold                                          (version 2)
--   103  Pune/bronze                                         (version 3)
--   104  Bengaluru/silver until 2026-07-10, then gold        (versions 4 and 6)
--
-- Orders (one line each, except order 6 which has two):
--    1  2026-05-05 Tue  cust 101  Phone X  x1  10000  delivered in 2 days   UPI
--    2  2026-05-10 Sun  cust 102  Kurta    x2   2000  delivered in 3 days   CARD (2nd attempt)
--    3  2026-05-20 Wed  cust 103  Novel    x1    500  cancelled             COD
--    4  2026-06-03 Wed  cust 101  Earbuds  x1   3000  delivered in 1 day    UPI
--    5  2026-06-18 Thu  cust 101  Sneakers x1   4000  delivered in 4 days   UPI   (after the move)
--    6  2026-06-20 Sat  cust 102  Phone X + Earbuds  13000  delivered in 2 days   CARD
--    7  2026-06-27 Sat  cust 104  Kurta    x1   1000  returned, 5 days      WALLET
--    8  2026-07-04 Sat  cust 102  Novel    x2   1000  delivered in 1 day    CARD
--    9  2026-07-12 Sun  cust 104  Sneakers x1   4000  shipped               UPI   (after the upgrade)
--   10  2026-07-15 Wed  cust 103  Phone X  x1  10000  delivered in 3 days   NET_BANKING (2nd attempt)
--   11  2026-07-20 Mon  unknown   Earbuds  x1   3000  placed                COD

DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

CREATE TABLE dw.dim_date AS
SELECT to_char(day, 'YYYYMMDD')::int AS date_key,
       day::date AS full_date,
       extract(ISODOW FROM day)::int AS day_of_week,
       trim(to_char(day, 'Day')) AS day_name,
       extract(WEEK FROM day)::int AS week_of_year,
       extract(MONTH FROM day)::int AS month,
       trim(to_char(day, 'Month')) AS month_name,
       extract(QUARTER FROM day)::int AS quarter,
       extract(YEAR FROM day)::int AS year,
       extract(ISODOW FROM day) >= 6 AS is_weekend,
       (day + INTERVAL '1 day')::date = date_trunc('month', day + INTERVAL '1 month')::date
           AS is_month_end
FROM generate_series(TIMESTAMP '2026-05-01', TIMESTAMP '2026-08-31', INTERVAL '1 day') AS day;

CREATE TABLE dw.dim_customer (
    customer_sk bigint PRIMARY KEY,
    customer_id bigint,
    city text,
    state text,
    pincode text,
    loyalty_tier text,
    customer_since date,
    valid_from timestamptz,
    valid_to timestamptz,
    is_current boolean,
    row_hash text
);
INSERT INTO dw.dim_customer VALUES
    (-1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', NULL, '1900-01-01 00:00+00', '9999-12-31 00:00+00', true, ''),
    (1, 101, 'Pune', 'Maharashtra', '411001', 'bronze', '2026-01-01', '1900-01-01 00:00+00', '2026-06-15 00:00+00', false, 'h1'),
    (2, 102, 'Delhi', 'Delhi', '110001', 'gold', '2026-01-01', '1900-01-01 00:00+00', '9999-12-31 00:00+00', true, 'h2'),
    (3, 103, 'Pune', 'Maharashtra', '411002', 'bronze', '2026-01-01', '1900-01-01 00:00+00', '9999-12-31 00:00+00', true, 'h3'),
    (4, 104, 'Bengaluru', 'Karnataka', '560001', 'silver', '2026-01-01', '1900-01-01 00:00+00', '2026-07-10 00:00+00', false, 'h4'),
    (5, 101, 'Mumbai', 'Maharashtra', '400001', 'silver', '2026-01-01', '2026-06-15 00:00+00', '9999-12-31 00:00+00', true, 'h5'),
    (6, 104, 'Bengaluru', 'Karnataka', '560001', 'gold', '2026-01-01', '2026-07-10 00:00+00', '9999-12-31 00:00+00', true, 'h6');

CREATE TABLE dw.dim_seller (
    seller_sk bigint PRIMARY KEY,
    seller_id bigint,
    seller_name text,
    gstin text,
    city text,
    state text,
    rating numeric(2, 1),
    is_active boolean
);
INSERT INTO dw.dim_seller VALUES
    (1, 1, 'Alpha Traders', '27ABCDE0001F1Z5', 'Mumbai', 'Maharashtra', 4.5, true),
    (2, 2, 'Beta Stores', '29ABCDE0002F1Z5', 'Bengaluru', 'Karnataka', 4.1, true),
    (3, 3, 'Gamma Mart', '07ABCDE0003F1Z5', 'Delhi', 'Delhi', 3.9, true);

CREATE TABLE dw.dim_product (
    product_sk bigint PRIMARY KEY,
    product_id bigint,
    product_name text,
    seller_id bigint,
    category_id int,
    category_name text,
    department text,
    brand text,
    color text,
    material text,
    mrp numeric(12, 2),
    price numeric(12, 2),
    is_active boolean,
    is_deleted boolean
);
INSERT INTO dw.dim_product VALUES
    (1, 1, 'Phone X', 1, 2, 'Smartphones', 'Electronics', 'Voltix', 'Black', NULL, 12000, 10000, true, false),
    (2, 2, 'Earbuds', 2, 3, 'Headphones', 'Electronics', 'Auralite', 'White', NULL, 3500, 3000, true, false),
    (3, 3, 'Kurta', 2, 5, 'Men''s Clothing', 'Fashion', 'Rangrez', 'Navy', 'Cotton', 1200, 1000, true, false),
    (4, 4, 'Sneakers', 3, 6, 'Footwear', 'Fashion', 'Mirai', 'White', 'Leather', 5000, 4000, true, false),
    (5, 5, 'Novel', 3, 8, 'Fiction', 'Books', 'Pothi Press', NULL, NULL, 600, 500, true, false);

CREATE TABLE dw.dim_payment_method (
    payment_method_sk int PRIMARY KEY,
    payment_method text,
    method_name text,
    is_prepaid boolean
);
INSERT INTO dw.dim_payment_method VALUES
    (-1, 'UNKNOWN', 'Unknown', NULL),
    (1, 'UPI', 'UPI', true),
    (2, 'CARD', 'Credit or debit card', true),
    (3, 'NET_BANKING', 'Net banking', true),
    (4, 'WALLET', 'Wallet', true),
    (5, 'COD', 'Cash on delivery', false);

CREATE TABLE dw.fact_order_items (
    order_item_id bigint PRIMARY KEY,
    order_id bigint,
    order_date_key int,
    customer_sk bigint,
    product_sk bigint,
    seller_sk bigint,
    payment_method_sk int,
    order_ts timestamptz,
    quantity int,
    unit_price numeric(12, 2),
    discount numeric(12, 2),
    item_total numeric(14, 2),
    shipping_fee numeric(12, 2),
    order_status text,
    delivery_days numeric(6, 2)
);
INSERT INTO dw.fact_order_items VALUES
    (101, 1, 20260505, 1, 1, 1, 1, '2026-05-05 10:00+00', 1, 10000, 0, 10000, 0, 'delivered', 2.00),
    (201, 2, 20260510, 2, 3, 2, 2, '2026-05-10 11:00+00', 2, 1000, 0, 2000, 0, 'delivered', 3.00),
    (301, 3, 20260520, 3, 5, 3, 5, '2026-05-20 12:00+00', 1, 500, 0, 500, 49, 'cancelled', NULL),
    (401, 4, 20260603, 1, 2, 2, 1, '2026-06-03 09:00+00', 1, 3000, 0, 3000, 0, 'delivered', 1.00),
    (501, 5, 20260618, 5, 4, 3, 1, '2026-06-18 18:00+00', 1, 4000, 0, 4000, 0, 'delivered', 4.00),
    (601, 6, 20260620, 2, 1, 1, 2, '2026-06-20 14:00+00', 1, 10000, 0, 10000, 0, 'delivered', 2.00),
    (602, 6, 20260620, 2, 2, 2, 2, '2026-06-20 14:00+00', 1, 3000, 0, 3000, 0, 'delivered', 2.00),
    (701, 7, 20260627, 4, 3, 2, 4, '2026-06-27 16:00+00', 1, 1000, 0, 1000, 0, 'returned', 5.00),
    (801, 8, 20260704, 2, 5, 3, 2, '2026-07-04 10:00+00', 2, 500, 0, 1000, 0, 'delivered', 1.00),
    (901, 9, 20260712, 6, 4, 3, 1, '2026-07-12 20:00+00', 1, 4000, 0, 4000, 0, 'shipped', NULL),
    (1001, 10, 20260715, 3, 1, 1, 3, '2026-07-15 08:00+00', 1, 10000, 0, 10000, 0, 'delivered', 3.00),
    (1101, 11, 20260720, -1, 2, 2, 5, '2026-07-20 13:00+00', 1, 3000, 0, 3000, 0, 'placed', NULL);

CREATE TABLE dw.fact_payments (
    payment_id bigint PRIMARY KEY,
    order_id bigint,
    payment_date_key int,
    customer_sk bigint,
    payment_method_sk int,
    order_ts timestamptz,
    attempt_no int,
    amount numeric(12, 2),
    payment_status text,
    is_successful boolean,
    paid_at timestamptz
);
INSERT INTO dw.fact_payments VALUES
    (11, 1, 20260505, 1, 1, '2026-05-05 10:00+00', 1, 10000, 'success', true, '2026-05-05 10:01+00'),
    (21, 2, 20260510, 2, 2, '2026-05-10 11:00+00', 1, 2000, 'failed', false, NULL),
    (22, 2, 20260510, 2, 2, '2026-05-10 11:00+00', 2, 2000, 'success', true, '2026-05-10 11:05+00'),
    (31, 3, 20260520, 3, 5, '2026-05-20 12:00+00', 1, 549, 'failed', false, NULL),
    (41, 4, 20260603, 1, 1, '2026-06-03 09:00+00', 1, 3000, 'success', true, '2026-06-03 09:01+00'),
    (51, 5, 20260618, 5, 1, '2026-06-18 18:00+00', 1, 4000, 'success', true, '2026-06-18 18:01+00'),
    (61, 6, 20260620, 2, 2, '2026-06-20 14:00+00', 1, 13000, 'success', true, '2026-06-20 14:01+00'),
    (71, 7, 20260627, 4, 4, '2026-06-27 16:00+00', 1, 1000, 'refunded', false, '2026-06-27 16:01+00'),
    (81, 8, 20260704, 2, 2, '2026-07-04 10:00+00', 1, 1000, 'success', true, '2026-07-04 10:01+00'),
    (91, 9, 20260712, 6, 1, '2026-07-12 20:00+00', 1, 4000, 'success', true, '2026-07-12 20:01+00'),
    (101, 10, 20260715, 3, 3, '2026-07-15 08:00+00', 1, 10000, 'failed', false, NULL),
    (102, 10, 20260715, 3, 3, '2026-07-15 08:00+00', 2, 10000, 'success', true, '2026-07-15 08:06+00'),
    (111, 11, 20260720, -1, 5, '2026-07-20 13:00+00', 1, 3000, 'pending', false, NULL);

CREATE TABLE dw.agg_daily_sales (
    date_key int,
    category text,
    city text,
    gmv numeric(18, 2),
    orders bigint,
    units bigint,
    avg_order_value numeric(18, 2),
    cancellation_rate numeric(6, 4),
    PRIMARY KEY (date_key, category, city)
);
INSERT INTO dw.agg_daily_sales VALUES
    (20260505, 'Electronics', 'Pune', 10000, 1, 1, 10000, 0),
    (20260510, 'Fashion', 'Delhi', 2000, 1, 2, 2000, 0),
    (20260520, 'Books', 'Pune', 0, 1, 0, NULL, 1),
    (20260603, 'Electronics', 'Pune', 3000, 1, 1, 3000, 0),
    (20260618, 'Fashion', 'Mumbai', 4000, 1, 1, 4000, 0),
    (20260620, 'Electronics', 'Delhi', 13000, 1, 2, 13000, 0),
    (20260627, 'Fashion', 'Bengaluru', 1000, 1, 1, 1000, 0),
    (20260704, 'Books', 'Delhi', 1000, 1, 2, 1000, 0),
    (20260712, 'Fashion', 'Bengaluru', 4000, 1, 1, 4000, 0),
    (20260715, 'Electronics', 'Pune', 10000, 1, 1, 10000, 0),
    (20260720, 'Electronics', 'Unknown', 3000, 1, 1, 3000, 0);
