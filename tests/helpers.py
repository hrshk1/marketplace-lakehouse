"""Builders for hand-written source rows used across the unit tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from marketplace.batch import Batch
from marketplace.generator.stores import MemoryStore

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
