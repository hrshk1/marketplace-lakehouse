"""Pipeline control flow: gates that block, failures that are recorded, status and maintenance."""

from __future__ import annotations

import dataclasses
from datetime import date

import pytest

from marketplace import pipeline
from marketplace.dq.model import DataQualityError
from marketplace.generator.stores import MemoryStore
from marketplace.lake import table_exists
from marketplace.ops.maintenance import compact, list_tables, maintain_table
from marketplace.ops.status import PipelineStatus, pipeline_status
from tests.helpers import batch, customer_row, memory_context, order_row, rows, ts

pytestmark = pytest.mark.spark

DAY = "2026-06-30"
NOON = ts("2026-06-30 12:00")
# An order without a key breaks an error-severity Bronze check.
KEYLESS_ORDER = {**order_row(2, NOON), "order_id": None}


def test_error_severity_failure_blocks_the_next_layer_and_is_recorded(lake, settings):
    store = MemoryStore()
    store.insert("customers", [customer_row(1, NOON)])
    store.insert("orders", [order_row(1, NOON), KEYLESS_ORDER])
    context = memory_context(lake, settings, store)

    with pytest.raises(DataQualityError, match="brz_orders_pk_not_null"):
        pipeline.run_batch(context, batch(DAY, "blocked-run"))

    assert table_exists(lake, "bronze.orders")
    assert not table_exists(lake, "silver.orders")
    assert not table_exists(lake, "gold.fact_order_items")
    assert rows(
        lake,
        "SELECT check_id, severity, status, failed_rows FROM dq.check_results WHERE status = 'fail'",
    ) == [("brz_orders_pk_not_null", "error", "fail", 1)]
    failures = rows(
        lake,
        "SELECT run_id, batch_id, layer, step, status FROM ops.pipeline_runs WHERE status = 'failed'",
    )
    assert failures == [("blocked-run", DAY, "dq_bronze", "checks", "failed")]


def test_operational_tables_are_exported_even_when_the_batch_fails(lake, settings, tmp_path):
    export_root = tmp_path / "exports"
    failing_settings = dataclasses.replace(settings, export_root=str(export_root))
    store = MemoryStore()
    store.insert("orders", [KEYLESS_ORDER])

    with pytest.raises(DataQualityError):
        pipeline.run_batch(memory_context(lake, failing_settings, store), batch(DAY))

    exported = sorted(path.name for path in export_root.iterdir())
    assert "pipeline_runs" in exported
    assert "dq_check_results" in exported
    assert "fact_order_items" not in exported


def test_unexpected_step_failure_is_recorded_with_its_layer_and_step(lake, settings, monkeypatch):
    def broken_model(spark, batch):
        raise RuntimeError("disk full")

    monkeypatch.setitem(pipeline.GOLD_MODELS, "dim_seller", broken_model)
    store = MemoryStore()
    store.insert("customers", [customer_row(1, NOON)])
    store.insert("orders", [order_row(1, NOON)])

    with pytest.raises(RuntimeError, match="disk full"):
        pipeline.run_batch(memory_context(lake, settings, store), batch(DAY, "broken-run"))

    assert rows(
        lake, "SELECT layer, step, error_message FROM ops.pipeline_runs WHERE status = 'failed'"
    ) == [("gold", "dim_seller", "RuntimeError('disk full')")]
    # Dimensions built before the failure are kept; nothing after it was built.
    assert table_exists(lake, "gold.dim_customer")
    assert not table_exists(lake, "gold.fact_order_items")


def test_successful_steps_record_row_counts_and_sla(lake, settings):
    store = MemoryStore()
    store.insert("customers", [customer_row(1, NOON), customer_row(2, NOON, pincode="12")])
    context = memory_context(lake, settings, store)
    current = batch(DAY, "metrics-run")

    pipeline.extract_to_bronze(context, current, "customers")
    pipeline.build_silver(context, current, "customers")

    assert rows(
        lake,
        "SELECT layer, step, status, rows_in, rows_out, rows_quarantined, sla_seconds, sla_breached "
        "FROM ops.pipeline_runs ORDER BY started_at",
    ) == [
        ("bronze", "customers", "success", 2, 2, 0, 600, False),
        ("silver", "customers", "success", 2, 1, 1, 600, False),
    ]


def test_pending_dates_are_the_simulated_days_after_the_last_completed_batch():
    days = tuple(
        date(2026, 6, 30 + offset) if offset == 0 else date(2026, 7, offset) for offset in range(4)
    )

    assert PipelineStatus(days, None).pending_dates == days
    assert PipelineStatus(days, date(2026, 7, 1)).pending_dates == (
        date(2026, 7, 2),
        date(2026, 7, 3),
    )
    assert PipelineStatus(days, date(2026, 7, 3)).pending_dates == ()
    assert PipelineStatus((), None).latest_simulated_date is None


def test_a_batch_counts_as_completed_only_after_its_gold_gate(lake, settings):
    start = date(2026, 6, 30)
    assert pipeline_status(lake, start, None).simulated_dates == ()
    assert pipeline_status(lake, start, 1).latest_completed_batch is None

    store = MemoryStore()
    store.insert("customers", [customer_row(1, NOON)])
    store.insert("orders", [order_row(1, NOON)])
    pipeline.run_batch(memory_context(lake, settings, store), batch(DAY))

    status = pipeline_status(lake, start, 2)
    assert status.latest_completed_batch == start
    assert status.pending_dates == (date(2026, 7, 1), date(2026, 7, 2))


def test_compaction_merges_small_files_without_changing_the_data(lake):
    lake.sql("CREATE TABLE ops.events (id INT, note STRING) USING iceberg")
    for number in range(5):
        lake.sql(f"INSERT INTO ops.events VALUES ({number}, 'event {number}')")
    assert rows(lake, "SELECT count(*) FROM ops.events.files") == [(5,)]

    rewritten, added = compact(lake, "ops.events")

    assert (rewritten, added) == (5, 1)
    assert rows(lake, "SELECT count(*) FROM ops.events.files") == [(1,)]
    assert rows(lake, "SELECT id FROM ops.events ORDER BY id") == [(n,) for n in range(5)]


def test_maintenance_covers_every_table_and_keeps_recent_snapshots(lake, settings):
    lake.sql("CREATE TABLE bronze.things (id INT) USING iceberg")
    lake.sql("CREATE TABLE gold.widgets (id INT) USING iceberg")
    for number in range(3):
        lake.sql(f"INSERT INTO gold.widgets VALUES ({number})")
    snapshots_before = rows(lake, "SELECT count(*) FROM gold.widgets.snapshots")[0][0]

    assert list_tables(lake) == ["bronze.things", "gold.widgets"]
    result = maintain_table(lake, "gold.widgets")
    results = pipeline.maintain(memory_context(lake, settings, MemoryStore()), batch(DAY, "maint"))

    assert (result.data_files_rewritten, result.data_files_added) == (3, 1)
    # Snapshots younger than the retention window survive, so time travel still works.
    assert rows(lake, "SELECT count(*) FROM gold.widgets.snapshots")[0][0] == snapshots_before + 1
    assert result.orphan_files_deleted == 0
    assert {r.table for r in results} >= {"bronze.things", "gold.widgets"}
    assert rows(
        lake,
        "SELECT count(*) FROM ops.pipeline_runs WHERE layer = 'maintenance' AND run_id = 'maint'",
    ) == [(len(results),)]


def test_reset_lakehouse_drops_every_table(lake, settings):
    lake.sql("CREATE TABLE silver.leftover (id INT) USING iceberg")
    lake.sql("INSERT INTO silver.leftover VALUES (1)")

    pipeline.reset_lakehouse(memory_context(lake, settings, MemoryStore()))

    assert list_tables(lake) == []
