"""Schema drift on Bronze: additive changes are applied, breaking ones stop the batch."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from marketplace.bronze.extract import extract_table
from marketplace.bronze.schema_evolution import SchemaDriftError, diff_schemas
from marketplace.generator.stores import MemoryStore
from marketplace.sources.readers import MemoryReader, source_table
from tests.helpers import batch, customer_row, order_item_row, ts

pytestmark = pytest.mark.spark

LOOKBACK = timedelta(minutes=60)
CUSTOMERS = source_table("customers")


def schema(**columns) -> StructType:
    return StructType([StructField(name, dtype) for name, dtype in columns.items()])


def test_diff_reports_added_removed_and_retyped_columns():
    existing = schema(id=LongType(), name=StringType(), qty=IntegerType(), _batch_id=StringType())
    incoming = schema(id=LongType(), qty=LongType(), gift_wrap=StringType())

    diff = diff_schemas(existing, incoming)

    assert [f.name for f in diff.added] == ["gift_wrap"]
    assert diff.removed == ["name"]
    assert [(c.name, c.old_type, c.new_type) for c in diff.retyped] == [("qty", "int", "bigint")]
    assert diff.is_breaking


def test_identical_schemas_produce_an_empty_diff():
    existing = schema(id=LongType(), _ingested_at=StringType())

    assert diff_schemas(existing, schema(id=LongType())).is_empty


@pytest.fixture
def loaded(lake) -> tuple[MemoryStore, MemoryReader]:
    store = MemoryStore()
    store.insert("customers", [customer_row(1, ts("2026-06-30 10:00"))])
    reader = MemoryReader(lake, store)
    extract_table(lake, reader, CUSTOMERS, batch("2026-06-30"), LOOKBACK)
    return store, reader


def schema_changes(lake) -> list[tuple]:
    rows = lake.sql(
        "SELECT table_name, column_name, change_type, old_type, new_type, batch_id "
        "FROM dq.schema_changes ORDER BY column_name"
    ).collect()
    return [tuple(row) for row in rows]


def test_new_source_column_is_added_to_bronze_and_logged(lake, loaded):
    store, reader = loaded
    store.add_column("customers", "referral_code", "string")
    store.insert("customers", [customer_row(2, ts("2026-07-01 09:00"), referral_code="FRIEND50")])

    extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    values = {
        row.customer_id: row.referral_code
        for row in lake.sql("SELECT customer_id, referral_code FROM bronze.customers").collect()
    }
    assert values == {1: None, 2: "FRIEND50"}
    assert schema_changes(lake) == [
        ("customers", "referral_code", "column_added", None, "string", "2026-07-01")
    ]


def test_type_change_fails_loudly_and_quarantines_the_batch(lake, loaded):
    store, reader = loaded
    store.columns["customers"]["pincode"] = "int"
    store.insert("customers", [customer_row(2, ts("2026-07-01 09:00"), pincode=411001)])

    with pytest.raises(SchemaDriftError, match="type of pincode changed string -> int"):
        extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    assert lake.table("bronze.customers").count() == 1
    checkpoint = lake.sql(
        "SELECT status, message FROM ops.extract_checkpoints WHERE batch_id = '2026-07-01'"
    ).collect()[0]
    assert checkpoint.status == "quarantined"
    assert "pincode" in checkpoint.message
    assert schema_changes(lake) == [
        ("customers", "pincode", "type_changed", "string", "int", "2026-07-01")
    ]


def test_removed_column_fails_loudly_and_loads_nothing(lake, loaded):
    store, reader = loaded
    del store.columns["customers"]["phone"]
    store.insert("customers", [customer_row(2, ts("2026-07-01 09:00"))])

    with pytest.raises(SchemaDriftError, match="column removed: phone"):
        extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    assert lake.table("bronze.customers").count() == 1
    assert schema_changes(lake) == [
        ("customers", "phone", "column_removed", None, None, "2026-07-01")
    ]


def test_runbook_dropping_the_bronze_column_unblocks_a_removed_source_column(lake, loaded):
    store, reader = loaded
    del store.columns["customers"]["phone"]
    store.insert("customers", [customer_row(2, ts("2026-07-01 09:00"))])
    with pytest.raises(SchemaDriftError):
        extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    lake.sql("ALTER TABLE bronze.customers DROP COLUMN phone")
    result = extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    # The quarantined attempt did not move the watermark, so nothing was skipped.
    assert result.rows_extracted == 1
    assert lake.table("bronze.customers").count() == 2
    assert "phone" not in lake.table("bronze.customers").columns
    status = lake.sql(
        "SELECT status FROM ops.extract_checkpoints WHERE batch_id = '2026-07-01'"
    ).collect()[0][0]
    assert status == "committed"


def test_runbook_widening_the_bronze_column_unblocks_a_widened_source_type(lake):
    items = source_table("order_items")
    store = MemoryStore()
    store.insert("order_items", [order_item_row(1, 1, ts("2026-06-30 10:00"))])
    reader = MemoryReader(lake, store)
    extract_table(lake, reader, items, batch("2026-06-30"), LOOKBACK)
    store.columns["order_items"]["quantity"] = "bigint"
    store.insert(
        "order_items", [order_item_row(2, 1, ts("2026-07-01 09:00"), quantity=3_000_000_000)]
    )
    with pytest.raises(SchemaDriftError, match="type of quantity changed int -> bigint"):
        extract_table(lake, reader, items, batch("2026-07-01"), LOOKBACK)

    lake.sql("ALTER TABLE bronze.order_items ALTER COLUMN quantity TYPE bigint")
    extract_table(lake, reader, items, batch("2026-07-01"), LOOKBACK)

    quantities = [
        row.quantity
        for row in lake.sql("SELECT quantity FROM bronze.order_items ORDER BY 1").collect()
    ]
    assert quantities == [1, 3_000_000_000]
