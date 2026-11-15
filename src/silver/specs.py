"""Declarative definition of every Silver table.

Each table has a staging query that cleans and conforms the latest Bronze
version of every row (exposed as the view `bronze_latest`), and a list of hard
rules. A row matching a rule is rejected into `silver_quarantine.<table>`.

Columns prefixed `_chk_` are helpers for the rules and are not stored. Every
staging query also passes `_record_key` and `_payload` through, so a rejected
row can be quarantined with its source key and its original content.
"""

from __future__ import annotations

from dataclasses import dataclass

from marketplace.silver.reference import CITY_ALIAS_VIEW, STATE_CODE_VIEW

LATEST_VIEW = "bronze_latest"
_LINEAGE = "b._record_key, b._payload"

ORDER_STATUSES = ("placed", "paid", "packed", "shipped", "delivered", "cancelled", "returned")
PAYMENT_METHODS = ("UPI", "CARD", "NET_BANKING", "COD", "WALLET")
PAYMENT_STATUSES = ("pending", "success", "failed", "refunded")
LOYALTY_TIERS = ("bronze", "silver", "gold", "platinum")


@dataclass(frozen=True)
class Rule:
    """A hard rule. `condition` is a SQL predicate that is true for a bad row."""

    rule_id: str
    reason: str
    condition: str


@dataclass(frozen=True)
class SilverTable:
    """How one Bronze table becomes a Silver table."""

    name: str
    key: str
    source_key: str
    staging_sql: str
    rules: tuple[Rule, ...]

    @property
    def target(self) -> str:
        return f"silver.{self.name}"

    @property
    def quarantine(self) -> str:
        return f"silver_quarantine.{self.name}"


def _in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _city(column: str) -> str:
    return f"coalesce(city_alias.city, initcap(trim(b.{column})))"


def _state(column: str) -> str:
    return f"coalesce(state_code.state, initcap(trim(b.{column})))"


def _address_joins(city_column: str, state_column: str) -> str:
    return f"""
        LEFT JOIN {CITY_ALIAS_VIEW} AS city_alias ON city_alias.alias = lower(trim(b.{city_column}))
        LEFT JOIN {STATE_CODE_VIEW} AS state_code ON state_code.alias = lower(trim(b.{state_column}))
    """


CATEGORIES = SilverTable(
    name="categories",
    key="category_id",
    source_key="category_id",
    staging_sql=f"""
        SELECT b.category_id,
               trim(b.category_name) AS category_name,
               b.parent_category_id,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE}
        FROM {LATEST_VIEW} AS b
    """,
    rules=(Rule("CAT_MISSING_NAME", "category_name is empty", "category_name = ''"),),
)

SELLERS = SilverTable(
    name="sellers",
    key="seller_id",
    source_key="seller_id",
    staging_sql=f"""
        SELECT b.seller_id,
               trim(b.seller_name) AS seller_name,
               upper(trim(b.gstin)) AS gstin,
               {_city("city")} AS city,
               {_state("state")} AS state,
               b.rating,
               b.is_active,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE}
        FROM {LATEST_VIEW} AS b
        {_address_joins("city", "state")}
    """,
    rules=(
        Rule(
            "SEL_INVALID_GSTIN",
            "gstin does not match the 15-character GSTIN format",
            "NOT gstin RLIKE '^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]Z[0-9A-Z]$'",
        ),
        Rule("SEL_RATING_OUT_OF_RANGE", "rating is outside 1 to 5", "rating NOT BETWEEN 1 AND 5"),
    ),
)

CUSTOMERS = SilverTable(
    name="customers",
    key="customer_id",
    source_key="customer_id",
    staging_sql=f"""
        SELECT b.customer_id,
               trim(b.full_name) AS full_name,
               lower(trim(b.email)) AS email,
               trim(b.phone) AS phone,
               {_city("city")} AS city,
               {_state("state")} AS state,
               trim(b.pincode) AS pincode,
               lower(trim(b.loyalty_tier)) AS loyalty_tier,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE}
        FROM {LATEST_VIEW} AS b
        {_address_joins("city", "state")}
    """,
    rules=(
        Rule(
            "CUS_INVALID_PINCODE",
            "pincode is not a 6-digit Indian pincode",
            "NOT pincode RLIKE '^[1-9][0-9]{5}$'",
        ),
        Rule(
            "CUS_INVALID_TIER",
            "loyalty_tier is not a known tier",
            f"loyalty_tier NOT IN ({_in_list(LOYALTY_TIERS)})",
        ),
    ),
)

PRODUCTS = SilverTable(
    name="products",
    key="product_id",
    source_key="product_id",
    staging_sql=f"""
        SELECT b.product_id,
               b.seller_id,
               b.category_id,
               trim(b.product_name) AS product_name,
               b.mrp,
               b.price,
               b.is_active,
               b.deleted_at IS NOT NULL AS is_deleted,
               b.deleted_at,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE}
        FROM {LATEST_VIEW} AS b
    """,
    rules=(
        Rule("PRD_NON_POSITIVE_PRICE", "price is zero or negative", "price <= 0"),
        Rule("PRD_PRICE_ABOVE_MRP", "price is higher than mrp", "price > mrp"),
    ),
)

ORDERS = SilverTable(
    name="orders",
    key="order_id",
    source_key="order_id",
    # A checkout retry inserts the same order_number under a new order_id. The
    # first order_id ever seen for a number is the real order; every Bronze
    # batch is consulted so a retry arriving in a later batch is still caught.
    staging_sql=f"""
        SELECT b.order_id,
               trim(b.order_number) AS order_number,
               b.customer_id,
               lower(trim(b.order_status)) AS order_status,
               b.order_ts,
               b.total_amount,
               b.shipping_fee,
               {_city("shipping_city")} AS shipping_city,
               {_state("shipping_state")} AS shipping_state,
               trim(b.shipping_pincode) AS shipping_pincode,
               b.delivered_at,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE},
               first_seen.first_order_id AS _chk_first_order_id
        FROM {LATEST_VIEW} AS b
        LEFT JOIN (
            SELECT order_number, min(order_id) AS first_order_id
            FROM bronze.orders
            GROUP BY order_number
        ) AS first_seen ON first_seen.order_number = b.order_number
        {_address_joins("shipping_city", "shipping_state")}
    """,
    rules=(
        Rule(
            "ORD_FUTURE_DATE",
            "order_ts is more than a day after the row was created",
            "order_ts > created_at + INTERVAL 1 DAY",
        ),
        Rule(
            "ORD_DUPLICATE_NUMBER",
            "order_number already belongs to an earlier order_id",
            "order_id <> _chk_first_order_id",
        ),
        Rule(
            "ORD_INVALID_STATUS",
            "order_status is not a known status",
            f"order_status NOT IN ({_in_list(ORDER_STATUSES)})",
        ),
        Rule("ORD_NEGATIVE_TOTAL", "total_amount is negative", "total_amount < 0"),
    ),
)

ORDER_ITEMS = SilverTable(
    name="order_items",
    key="order_item_id",
    source_key="order_item_id",
    staging_sql=f"""
        SELECT b.order_item_id,
               b.order_id,
               b.product_id,
               b.seller_id,
               b.quantity,
               b.unit_price,
               b.discount_amount,
               CAST(b.quantity * b.unit_price - b.discount_amount AS DECIMAL(14, 2)) AS item_total,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE},
               known.order_id IS NOT NULL AS _chk_order_known,
               accepted.order_id IS NOT NULL AS _chk_order_accepted
        FROM {LATEST_VIEW} AS b
        LEFT JOIN (SELECT DISTINCT order_id FROM bronze.orders) AS known
               ON known.order_id = b.order_id
        LEFT JOIN silver.orders AS accepted ON accepted.order_id = b.order_id
    """,
    rules=(
        Rule("OI_ORPHAN", "order_id does not exist in the orders source", "NOT _chk_order_known"),
        Rule(
            "OI_PARENT_REJECTED",
            "the parent order was rejected by a Silver rule",
            "NOT _chk_order_accepted",
        ),
        Rule("OI_NON_POSITIVE_QUANTITY", "quantity is zero or negative", "quantity <= 0"),
        Rule("OI_NON_POSITIVE_PRICE", "unit_price is zero or negative", "unit_price <= 0"),
    ),
)

PAYMENTS = SilverTable(
    name="payments",
    key="payment_id",
    source_key="payment_id",
    staging_sql=f"""
        SELECT b.payment_id,
               b.order_id,
               upper(trim(b.payment_method)) AS payment_method,
               lower(trim(b.payment_status)) AS payment_status,
               b.amount,
               b.attempt_no,
               b.gateway_txn_ref,
               b.paid_at,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE},
               accepted.order_id IS NOT NULL AS _chk_order_accepted
        FROM {LATEST_VIEW} AS b
        LEFT JOIN silver.orders AS accepted ON accepted.order_id = b.order_id
    """,
    rules=(
        Rule("PAY_NON_POSITIVE_AMOUNT", "amount is zero or negative", "amount <= 0"),
        Rule(
            "PAY_PARENT_REJECTED",
            "the order is missing or was rejected by a Silver rule",
            "NOT _chk_order_accepted",
        ),
        Rule(
            "PAY_INVALID_METHOD",
            "payment_method is not a known method",
            f"payment_method NOT IN ({_in_list(PAYMENT_METHODS)})",
        ),
        Rule(
            "PAY_INVALID_STATUS",
            "payment_status is not a known status",
            f"payment_status NOT IN ({_in_list(PAYMENT_STATUSES)})",
        ),
    ),
)

REVIEWS = SilverTable(
    name="reviews",
    key="review_id",
    source_key="doc_id",
    staging_sql=f"""
        SELECT b.doc_id AS review_id,
               CAST(get_json_object(b.payload, '$.order_id') AS BIGINT) AS order_id,
               CAST(get_json_object(b.payload, '$.product_id') AS BIGINT) AS product_id,
               CAST(get_json_object(b.payload, '$.customer_id') AS BIGINT) AS customer_id,
               CAST(get_json_object(b.payload, '$.rating') AS INT) AS rating,
               trim(get_json_object(b.payload, '$.title')) AS title,
               trim(get_json_object(b.payload, '$.text')) AS review_text,
               CAST(get_json_object(b.payload, '$.helpful_votes') AS INT) AS helpful_votes,
               get_json_object(b.payload, '$.seller_reply.text') IS NOT NULL AS has_seller_reply,
               get_json_object(b.payload, '$.seller_reply.text') AS seller_reply_text,
               to_timestamp(get_json_object(b.payload, '$.seller_reply.replied_at'))
                   AS seller_replied_at,
               b.created_at,
               b.updated_at,
               b._batch_id,
               {_LINEAGE}
        FROM {LATEST_VIEW} AS b
    """,
    rules=(
        Rule(
            "REV_INVALID_RATING",
            "rating is missing or outside 1 to 5",
            "rating IS NULL OR rating NOT BETWEEN 1 AND 5",
        ),
        Rule("REV_MISSING_PRODUCT", "product_id is missing", "product_id IS NULL"),
    ),
)

# The attribute set differs by category, so it stays a map. The three
# attributes analysts filter on most are also promoted to real columns.
PRODUCT_ATTRIBUTES = SilverTable(
    name="product_attributes",
    key="product_id",
    source_key="doc_id",
    staging_sql=f"""
        SELECT product_id,
               department,
               attributes,
               attributes['brand'] AS brand,
               attributes['color'] AS color,
               attributes['material'] AS material,
               created_at,
               updated_at,
               _batch_id,
               _record_key,
               _payload
        FROM (
            SELECT CAST(get_json_object(b.payload, '$.product_id') AS BIGINT) AS product_id,
                   get_json_object(b.payload, '$.department') AS department,
                   from_json(get_json_object(b.payload, '$.attributes'), 'map<string,string>')
                       AS attributes,
                   b.created_at,
                   b.updated_at,
                   b._batch_id,
                   {_LINEAGE}
            FROM {LATEST_VIEW} AS b
        )
    """,
    rules=(Rule("PA_MISSING_PRODUCT_ID", "product_id is missing", "product_id IS NULL"),),
)

# Order matters: order_items and payments check their parent in silver.orders.
SILVER_TABLES: tuple[SilverTable, ...] = (
    CATEGORIES,
    SELLERS,
    CUSTOMERS,
    PRODUCTS,
    ORDERS,
    ORDER_ITEMS,
    PAYMENTS,
    REVIEWS,
    PRODUCT_ATTRIBUTES,
)


def silver_table(name: str) -> SilverTable:
    """Look up a Silver table definition by name."""
    for table in SILVER_TABLES:
        if table.name == name:
            return table
    raise KeyError(f"Unknown Silver table: {name!r}")
