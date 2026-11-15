"""Shape of the operational source tables, as the generator writes them.

The Postgres DDL (constraints, indexes) lives in `sql/oltp_schema.sql`. This
module carries the column order used for bulk inserts and the column types the
in-memory store reports to the extraction layer in tests.
"""

from __future__ import annotations

from dataclasses import dataclass

MONEY = "decimal(12,2)"


@dataclass(frozen=True)
class TableDef:
    """Primary key and typed columns of one source table."""

    primary_key: str
    columns: dict[str, str]


_AUDIT = {"created_at": "timestamp", "updated_at": "timestamp"}

POSTGRES_TABLES: dict[str, TableDef] = {
    "categories": TableDef(
        "category_id",
        {
            "category_id": "int",
            "category_name": "string",
            "parent_category_id": "int",
            **_AUDIT,
        },
    ),
    "sellers": TableDef(
        "seller_id",
        {
            "seller_id": "bigint",
            "seller_name": "string",
            "gstin": "string",
            "city": "string",
            "state": "string",
            "rating": "decimal(2,1)",
            "is_active": "boolean",
            **_AUDIT,
        },
    ),
    "customers": TableDef(
        "customer_id",
        {
            "customer_id": "bigint",
            "full_name": "string",
            "email": "string",
            "phone": "string",
            "city": "string",
            "state": "string",
            "pincode": "string",
            "loyalty_tier": "string",
            **_AUDIT,
        },
    ),
    "products": TableDef(
        "product_id",
        {
            "product_id": "bigint",
            "seller_id": "bigint",
            "category_id": "int",
            "product_name": "string",
            "mrp": MONEY,
            "price": MONEY,
            "is_active": "boolean",
            "deleted_at": "timestamp",
            **_AUDIT,
        },
    ),
    "orders": TableDef(
        "order_id",
        {
            "order_id": "bigint",
            "order_number": "string",
            "customer_id": "bigint",
            "order_status": "string",
            "order_ts": "timestamp",
            "total_amount": MONEY,
            "shipping_fee": MONEY,
            "shipping_city": "string",
            "shipping_state": "string",
            "shipping_pincode": "string",
            "delivered_at": "timestamp",
            **_AUDIT,
        },
    ),
    "order_items": TableDef(
        "order_item_id",
        {
            "order_item_id": "bigint",
            "order_id": "bigint",
            "product_id": "bigint",
            "seller_id": "bigint",
            "quantity": "int",
            "unit_price": MONEY,
            "discount_amount": MONEY,
            **_AUDIT,
        },
    ),
    "payments": TableDef(
        "payment_id",
        {
            "payment_id": "bigint",
            "order_id": "bigint",
            "payment_method": "string",
            "payment_status": "string",
            "amount": MONEY,
            "attempt_no": "int",
            "gateway_txn_ref": "string",
            "paid_at": "timestamp",
            **_AUDIT,
        },
    ),
}

MONGO_COLLECTIONS: tuple[str, ...] = ("reviews", "product_attributes")
MONGO_KEY = "_id"

# The column added by the simulated schema-drift event.
DRIFT_TABLE = "order_items"
DRIFT_COLUMN = "gift_wrap"
DRIFT_SPARK_TYPE = "boolean"


def primary_key(table: str) -> str:
    """Return the key column of a Postgres table or Mongo collection."""
    if table in MONGO_COLLECTIONS:
        return MONGO_KEY
    return POSTGRES_TABLES[table].primary_key
