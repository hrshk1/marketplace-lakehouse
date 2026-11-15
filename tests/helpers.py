"""Builders for hand-written source rows used across the unit tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from pyspark.sql import SparkSession

from marketplace.batch import Batch
from marketplace.bronze.extract import extract_table
from marketplace.generator.stores import MemoryStore
from marketplace.silver.build import SilverResult, build_silver_table
from marketplace.silver.specs import silver_table
from marketplace.sources.readers import MemoryReader, source_table

Row = dict[str, Any]


def ts(text: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM[:SS]' as a UTC timestamp."""
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def batch(day: str, run_id: str = "test-run") -> Batch:
    return Batch(date.fromisoformat(day), run_id)


def customer_row(customer_id: int, updated_at: datetime, **overrides: Any) -> Row:
    row = {
        "customer_id": customer_id,
        "full_name": f"Customer {customer_id}",
        "email": f"customer{customer_id}@mail.test",
        "phone": "+919800000000",
        "city": "Pune",
        "state": "Maharashtra",
        "pincode": "411001",
        "loyalty_tier": "bronze",
        "created_at": ts("2026-01-01 00:00"),
        "updated_at": updated_at,
    }
    row.update(overrides)
    return row


def upsert(store: MemoryStore, table: str, key: str, rows: list[Row]) -> None:
    """Insert rows, or update them when the key already exists."""
    existing = store.tables[table]
    store.insert(table, [row for row in rows if row[key] not in existing])
    store.update(table, [row for row in rows if row[key] in existing])


def order_row(order_id: int, updated_at: datetime, **overrides: Any) -> Row:
    row = {
        "order_id": order_id,
        "order_number": f"MP-{order_id:06d}",
        "customer_id": 1,
        "order_status": "placed",
        "order_ts": updated_at,
        "total_amount": Decimal("500.00"),
        "shipping_fee": Decimal("0.00"),
        "shipping_city": "Pune",
        "shipping_state": "Maharashtra",
        "shipping_pincode": "411001",
        "delivered_at": None,
        "created_at": updated_at,
        "updated_at": updated_at,
    }
    row.update(overrides)
    return row


def order_item_row(
    order_item_id: int, order_id: int, updated_at: datetime, **overrides: Any
) -> Row:
    row = {
        "order_item_id": order_item_id,
        "order_id": order_id,
        "product_id": 1,
        "seller_id": 1,
        "quantity": 1,
        "unit_price": Decimal("500.00"),
        "discount_amount": Decimal("0.00"),
        "created_at": updated_at,
        "updated_at": updated_at,
    }
    row.update(overrides)
    return row


def payment_row(payment_id: int, order_id: int, updated_at: datetime, **overrides: Any) -> Row:
    row = {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_method": "UPI",
        "payment_status": "success",
        "amount": Decimal("500.00"),
        "attempt_no": 1,
        "gateway_txn_ref": f"TXN{payment_id}",
        "paid_at": updated_at,
        "created_at": updated_at,
        "updated_at": updated_at,
    }
    row.update(overrides)
    return row


def load_to_silver(
    spark: SparkSession, store: MemoryStore, tables: list[str], day: str
) -> dict[str, SilverResult]:
    """Extract the given tables for one batch and build their Silver tables, in order."""
    reader = MemoryReader(spark, store)
    current = batch(day)
    results = {}
    for name in tables:
        extract_table(spark, reader, source_table(name), current, timedelta(minutes=60))
        results[name] = build_silver_table(spark, silver_table(name), current)
    return results


def rows(spark: SparkSession, query: str) -> list[tuple]:
    """Run a query and return plain tuples, which compare cleanly in assertions."""
    return [tuple(row) for row in spark.sql(query).collect()]
