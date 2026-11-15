-- Operational schema of the marketplace application.
--
-- Three gaps are deliberate, because they are how bad data reaches a pipeline
-- in practice and the generator needs them to inject its controlled defects:
--   * orders.customer_id is nullable (guest checkout writes orders without it);
--   * orders.order_number has no unique constraint (checkout retries can insert
--     the same order twice);
--   * order_items.order_id has no foreign key (items are written by a separate
--     service) and payments.amount has no CHECK constraint.

DROP SCHEMA IF EXISTS oltp CASCADE;
CREATE SCHEMA oltp;

CREATE TABLE oltp.categories (
    category_id        integer PRIMARY KEY,
    category_name      text NOT NULL,
    parent_category_id integer REFERENCES oltp.categories (category_id),
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL,
    UNIQUE (parent_category_id, category_name)
);

CREATE TABLE oltp.sellers (
    seller_id   bigint PRIMARY KEY,
    seller_name text NOT NULL,
    gstin       text NOT NULL UNIQUE,
    city        text NOT NULL,
    state       text NOT NULL,
    rating      numeric(2, 1) NOT NULL CHECK (rating BETWEEN 1 AND 5),
    is_active   boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE TABLE oltp.customers (
    customer_id  bigint PRIMARY KEY,
    full_name    text NOT NULL,
    email        text NOT NULL UNIQUE,
    phone        text NOT NULL,
    city         text NOT NULL,
    state        text NOT NULL,
    pincode      text NOT NULL,
    loyalty_tier text NOT NULL CHECK (loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum')),
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);

CREATE TABLE oltp.products (
    product_id   bigint PRIMARY KEY,
    seller_id    bigint NOT NULL REFERENCES oltp.sellers (seller_id),
    category_id  integer NOT NULL REFERENCES oltp.categories (category_id),
    product_name text NOT NULL,
    mrp          numeric(12, 2) NOT NULL CHECK (mrp > 0),
    price        numeric(12, 2) NOT NULL CHECK (price > 0),
    is_active    boolean NOT NULL DEFAULT true,
    deleted_at   timestamptz,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    CHECK (price <= mrp)
);

CREATE TABLE oltp.orders (
    order_id         bigint PRIMARY KEY,
    order_number     text NOT NULL,
    customer_id      bigint REFERENCES oltp.customers (customer_id),
    order_status     text NOT NULL CHECK (
        order_status IN ('placed', 'paid', 'packed', 'shipped', 'delivered', 'cancelled', 'returned')
    ),
    order_ts         timestamptz NOT NULL,
    total_amount     numeric(12, 2) NOT NULL,
    shipping_fee     numeric(12, 2) NOT NULL CHECK (shipping_fee >= 0),
    shipping_city    text NOT NULL,
    shipping_state   text NOT NULL,
    shipping_pincode text NOT NULL,
    delivered_at     timestamptz,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);

CREATE TABLE oltp.order_items (
    order_item_id   bigint PRIMARY KEY,
    order_id        bigint NOT NULL,
    product_id      bigint NOT NULL REFERENCES oltp.products (product_id),
    seller_id       bigint NOT NULL REFERENCES oltp.sellers (seller_id),
    quantity        integer NOT NULL CHECK (quantity > 0),
    unit_price      numeric(12, 2) NOT NULL CHECK (unit_price > 0),
    discount_amount numeric(12, 2) NOT NULL CHECK (discount_amount >= 0),
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE oltp.payments (
    payment_id      bigint PRIMARY KEY,
    order_id        bigint NOT NULL REFERENCES oltp.orders (order_id),
    payment_method  text NOT NULL CHECK (
        payment_method IN ('UPI', 'CARD', 'NET_BANKING', 'COD', 'WALLET')
    ),
    payment_status  text NOT NULL CHECK (
        payment_status IN ('pending', 'success', 'failed', 'refunded')
    ),
    amount          numeric(12, 2) NOT NULL,
    attempt_no      integer NOT NULL CHECK (attempt_no >= 1),
    gateway_txn_ref text,
    paid_at         timestamptz,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

-- Incremental extraction filters every table on updated_at.
CREATE INDEX ix_categories_updated_at ON oltp.categories (updated_at);
CREATE INDEX ix_sellers_updated_at ON oltp.sellers (updated_at);
CREATE INDEX ix_customers_updated_at ON oltp.customers (updated_at);
CREATE INDEX ix_products_updated_at ON oltp.products (updated_at);
CREATE INDEX ix_orders_updated_at ON oltp.orders (updated_at);
CREATE INDEX ix_order_items_updated_at ON oltp.order_items (updated_at);
CREATE INDEX ix_payments_updated_at ON oltp.payments (updated_at);

CREATE INDEX ix_products_seller_id ON oltp.products (seller_id);
CREATE INDEX ix_products_category_id ON oltp.products (category_id);
CREATE INDEX ix_orders_customer_id ON oltp.orders (customer_id);
CREATE INDEX ix_orders_order_ts ON oltp.orders (order_ts);
CREATE INDEX ix_orders_order_number ON oltp.orders (order_number);
CREATE INDEX ix_order_items_order_id ON oltp.order_items (order_id);
CREATE INDEX ix_order_items_product_id ON oltp.order_items (product_id);
CREATE INDEX ix_payments_order_id ON oltp.payments (order_id);
