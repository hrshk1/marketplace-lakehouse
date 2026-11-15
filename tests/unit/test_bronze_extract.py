"""Watermark logic and idempotent incremental extraction into Bronze."""

from __future__ import annotations

from datetime import timedelta

import pytest

from marketplace.bronze import checkpoints
from marketplace.bronze.extract import extract_table
from marketplace.generator.stores import MemoryStore
from marketplace.sources.readers import MemoryReader, source_table
from tests.helpers import batch, customer_row, ts, upsert

pytestmark = pytest.mark.spark

LOOKBACK = timedelta(minutes=60)
CUSTOMERS = source_table("customers")


def test_first_run_has_no_lower_bound():
    window = checkpoints.resolve_window(None, ts("2026-07-01 00:00"), LOOKBACK)

    assert window.is_full_snapshot
    assert window.read_from is None
    assert window.high_watermark == ts("2026-07-01 00:00")


def test_incremental_window_starts_one_lookback_before_previous_watermark():
    window = checkpoints.resolve_window(ts("2026-07-01 00:00"), ts("2026-07-02 00:00"), LOOKBACK)

    assert not window.is_full_snapshot
    assert window.low_watermark == ts("2026-07-01 00:00")
    assert window.read_from == ts("2026-06-30 23:00")


def bronze_ids(lake, batch_id: str) -> list[int]:
    rows = lake.sql(
        f"SELECT customer_id FROM bronze.customers WHERE _batch_id = '{batch_id}' ORDER BY 1"
    ).collect()
    return [row.customer_id for row in rows]


@pytest.fixture
def store() -> MemoryStore:
    store = MemoryStore()
    store.insert(
        "customers",
        [
            customer_row(1, ts("2026-06-29 10:00")),
            customer_row(2, ts("2026-06-30 23:30")),
            customer_row(3, ts("2026-06-30 12:00")),
        ],
    )
    return store


def test_first_batch_is_a_full_snapshot_up_to_the_batch_end(lake, store):
    store.insert("customers", [customer_row(4, ts("2026-07-01 08:00"))])

    result = extract_table(
        lake, MemoryReader(lake, store), CUSTOMERS, batch("2026-06-30"), LOOKBACK
    )

    assert result.full_snapshot
    assert result.rows_extracted == 3
    assert result.source_row_count == 3
    assert bronze_ids(lake, "2026-06-30") == [1, 2, 3]


def test_next_batch_extracts_changes_plus_the_lookback_window(lake, store):
    reader = MemoryReader(lake, store)
    extract_table(lake, reader, CUSTOMERS, batch("2026-06-30"), LOOKBACK)
    upsert(
        store,
        "customers",
        "customer_id",
        [
            customer_row(1, ts("2026-07-01 09:00"), city="Mumbai"),
            customer_row(4, ts("2026-07-01 08:00")),
        ],
    )

    result = extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    # Customer 2 changed at 23:30, inside the hour before the previous
    # watermark, so the lookback re-reads it. Customer 3 is older and is not.
    assert not result.full_snapshot
    assert bronze_ids(lake, "2026-07-01") == [1, 2, 4]


def test_metadata_columns_describe_the_batch(lake, store):
    extract_table(lake, MemoryReader(lake, store), CUSTOMERS, batch("2026-06-30"), LOOKBACK)

    row = lake.sql(
        """
        SELECT DISTINCT _batch_id, _source_system,
               unix_micros(_extract_watermark) AS watermark_micros,
               _ingested_at IS NOT NULL AS has_ingested_at
        FROM bronze.customers
        """
    ).collect()

    assert len(row) == 1
    assert row[0]._batch_id == "2026-06-30"
    assert row[0]._source_system == "postgres"
    assert row[0].watermark_micros == int(ts("2026-07-01 00:00").timestamp() * 1_000_000)
    assert row[0].has_ingested_at


def test_rerunning_a_batch_replaces_its_rows_instead_of_duplicating(lake, store):
    reader = MemoryReader(lake, store)
    first = extract_table(lake, reader, CUSTOMERS, batch("2026-06-30", "run-1"), LOOKBACK)
    second = extract_table(lake, reader, CUSTOMERS, batch("2026-06-30", "run-2"), LOOKBACK)

    assert first.rows_extracted == second.rows_extracted == 3
    assert lake.table("bronze.customers").count() == 3
    checkpoint_rows = lake.sql(
        "SELECT count(*) FROM ops.extract_checkpoints WHERE table_name = 'customers'"
    ).collect()[0][0]
    assert checkpoint_rows == 1


def test_rerun_of_an_earlier_batch_ignores_later_checkpoints(lake, store):
    reader = MemoryReader(lake, store)
    extract_table(lake, reader, CUSTOMERS, batch("2026-06-30"), LOOKBACK)
    extract_table(lake, reader, CUSTOMERS, batch("2026-07-01"), LOOKBACK)

    rerun = extract_table(lake, reader, CUSTOMERS, batch("2026-06-30"), LOOKBACK)

    assert rerun.full_snapshot
    assert bronze_ids(lake, "2026-06-30") == [1, 2, 3]


def test_quarantined_checkpoints_do_not_advance_the_watermark(lake):
    checkpoints.ensure_table(lake)
    window = checkpoints.resolve_window(None, ts("2026-07-01 00:00"), LOOKBACK)
    checkpoints.record(lake, "customers", batch("2026-06-30"), window, checkpoints.QUARANTINED)

    assert checkpoints.previous_high_watermark(lake, "customers", "2026-07-01") is None

    checkpoints.record(lake, "customers", batch("2026-06-30"), window, checkpoints.COMMITTED)

    assert checkpoints.previous_high_watermark(lake, "customers", "2026-07-01") == ts(
        "2026-07-01 00:00"
    )
