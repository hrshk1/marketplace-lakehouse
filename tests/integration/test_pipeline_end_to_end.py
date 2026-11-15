"""The whole pipeline on the small profile, with no containers.

The generator writes to an in-memory store, Spark runs in local mode and the
Iceberg catalog lives on the local file system. Day 0 is loaded as a full
snapshot, then three more days are simulated and loaded one by one. Day 2
carries the schema drift event.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta

import pyarrow.dataset as pads
import pytest

from marketplace.batch import Batch
from marketplace.config import Settings
from marketplace.dq.reconciliation import load_detectors, score_detection
from marketplace.generator.ledger import Ledger
from marketplace.generator.model import PROFILES, DaySummary, GeneratorConfig
from marketplace.generator.simulator import MarketplaceSimulator
from marketplace.generator.stores import MemoryStore
from marketplace.pipeline import batch_steps, run_batch
from marketplace.publish.export import export_path
from marketplace.publish.tables import load_publish_config
from tests.conftest import use_fresh_catalog
from tests.helpers import memory_context, rows

pytestmark = pytest.mark.spark

SIMULATED_DAYS = 3
VOLATILE_COLUMNS = {"_ingested_at", "quarantined_at"}
COMPARED_TABLES = (
    "silver.customers",
    "silver.orders",
    "silver.order_items",
    "silver.payments",
    "silver.reviews",
    "silver_quarantine.orders",
    "silver_quarantine.order_items",
    "silver_quarantine.payments",
    "gold.dim_customer",
    "gold.dim_product",
    "gold.dim_seller",
    "gold.fact_order_items",
    "gold.fact_payments",
    "gold.agg_daily_sales",
)


@dataclass
class Scenario:
    """Everything the tests need to know about the simulated run."""

    settings: Settings
    store: MemoryStore
    ledger: Ledger
    summaries: list[DaySummary]
    batches: list[Batch]

    def defect_keys(self, defect_type: str) -> set[str]:
        return self.ledger.keys_by_type().get(defect_type, set())

    def defect_count(self, defect_type: str) -> int:
        return len(self.defect_keys(defect_type))


@pytest.fixture(scope="module")
def scenario(spark, settings, tmp_path_factory) -> Scenario:
    use_fresh_catalog(spark, settings, tmp_path_factory.mktemp("end_to_end"))
    store = MemoryStore()
    ledger = Ledger(tmp_path_factory.mktemp("ledger") / "ledger.sqlite")
    config = GeneratorConfig.from_settings(settings, PROFILES["small"])
    simulator = MarketplaceSimulator(store, ledger, config)
    context = memory_context(spark, settings, store)

    summaries = [simulator.seed()]
    batches = [Batch(config.start_date, "run-day-0")]
    run_batch(context, batches[0])
    for day in range(1, SIMULATED_DAYS + 1):
        summaries.extend(simulator.advance(1))
        batches.append(Batch(config.start_date + timedelta(days=day), f"run-day-{day}"))
        run_batch(context, batches[-1])
    return Scenario(settings, store, ledger, summaries, batches)


def count(spark, table: str, where: str = "true") -> int:
    return spark.sql(f"SELECT count(*) FROM {table} WHERE {where}").collect()[0][0]


def test_every_step_of_every_batch_succeeded(spark, scenario):
    context = memory_context(spark, scenario.settings, scenario.store)
    gated_steps = len(batch_steps(context, scenario.batches[0])) - 1

    assert count(spark, "ops.pipeline_runs", "status = 'failed'") == 0
    for batch in scenario.batches:
        succeeded = count(
            spark,
            "ops.pipeline_runs",
            f"batch_id = '{batch.batch_id}' AND status = 'success' AND layer <> 'publish'",
        )
        assert succeeded == gated_steps


def test_no_error_severity_check_failed_and_warnings_were_recorded(spark, scenario):
    statuses = dict(rows(spark, "SELECT status, count(*) FROM dq.check_results GROUP BY status"))

    assert "fail" not in statuses
    assert statuses["warn"] > 0
    assert statuses["pass"] > statuses["warn"]


def test_silver_row_counts_are_source_rows_minus_quarantined_defects(spark, scenario):
    tables = scenario.store.tables
    duplicates = scenario.defect_count("duplicate_order")
    future = scenario.defect_count("future_order_date")
    orphans = scenario.defect_count("orphan_order_item")
    bad_payments = scenario.defect_count("non_positive_amount")

    assert count(spark, "silver.customers") == len(tables["customers"])
    assert count(spark, "silver.products") == len(tables["products"])
    assert count(spark, "silver.orders") == len(tables["orders"]) - duplicates - future
    # Each future-dated order carries exactly one item, rejected with its parent.
    assert count(spark, "silver.order_items") == len(tables["order_items"]) - orphans - future
    assert count(spark, "silver.payments") == len(tables["payments"]) - bad_payments
    assert count(spark, "silver.reviews") == len(tables["reviews"])


def test_gold_facts_have_one_row_per_accepted_line_and_payment(spark, scenario):
    assert count(spark, "gold.fact_order_items") == count(spark, "silver.order_items")
    assert count(spark, "gold.fact_payments") == count(spark, "silver.payments")
    assert count(spark, "gold.fact_order_items", "customer_sk = -1") == count(
        spark,
        "silver.order_items AS i JOIN silver.orders AS o ON o.order_id = i.order_id",
        "o.customer_id IS NULL",
    )


def test_silver_orders_hold_the_latest_source_state_including_late_updates(spark, scenario):
    accepted = {
        order_id: (status, updated_micros)
        for order_id, status, updated_micros in rows(
            spark, "SELECT order_id, order_status, unix_micros(updated_at) FROM silver.orders"
        )
    }
    source = scenario.store.tables["orders"]

    assert accepted == {
        order_id: (
            source[order_id]["order_status"],
            int(source[order_id]["updated_at"].timestamp() * 1e6),
        )
        for order_id in accepted
    }
    late = {int(key) for key in scenario.defect_keys("late_arriving_update")}
    assert late
    assert late <= accepted.keys()


def test_quarantine_holds_exactly_the_injected_defects(spark, scenario):
    quarantined = {
        (table, rule): {key for (key,) in rows(
            spark,
            f"SELECT DISTINCT record_key FROM silver_quarantine.{table} WHERE rule_id = '{rule}'",
        )}
        for table, rule in [
            ("orders", "ORD_DUPLICATE_NUMBER"),
            ("orders", "ORD_FUTURE_DATE"),
            ("order_items", "OI_ORPHAN"),
            ("order_items", "OI_PARENT_REJECTED"),
            ("payments", "PAY_NON_POSITIVE_AMOUNT"),
        ]
    }  # fmt: skip

    assert quarantined["orders", "ORD_DUPLICATE_NUMBER"] == scenario.defect_keys("duplicate_order")
    assert quarantined["orders", "ORD_FUTURE_DATE"] == scenario.defect_keys("future_order_date")
    assert quarantined["order_items", "OI_ORPHAN"] == scenario.defect_keys("orphan_order_item")
    assert quarantined["payments", "PAY_NON_POSITIVE_AMOUNT"] == scenario.defect_keys(
        "non_positive_amount"
    )
    assert len(quarantined["order_items", "OI_PARENT_REJECTED"]) == scenario.defect_count(
        "future_order_date"
    )
    other_rules = rows(
        spark,
        "SELECT DISTINCT rule_id FROM silver_quarantine.orders "
        "WHERE rule_id NOT IN ('ORD_DUPLICATE_NUMBER', 'ORD_FUTURE_DATE')",
    )
    assert other_rules == []


def test_quarantined_rows_keep_reason_batch_and_payload(spark, scenario):
    sample = spark.sql(
        "SELECT record_key, reject_reason, _batch_id, get_json_object(payload, '$.order_id') AS id "
        "FROM silver_quarantine.orders WHERE rule_id = 'ORD_FUTURE_DATE' LIMIT 1"
    ).collect()[0]

    assert sample.reject_reason == "order_ts is more than a day after the row was created"
    assert sample._batch_id in {batch.batch_id for batch in scenario.batches}
    assert sample.id == sample.record_key


def test_defect_detection_matches_the_ledger_exactly(spark, scenario):
    scores = score_detection(
        spark, scenario.ledger.keys_by_type(), load_detectors(scenario.settings.config_dir)
    )

    assert len(scores) == 7
    for score in scores:
        assert score.injected > 0, score.defect_type
        assert (score.detected, score.true_positives) == (score.injected, score.injected), score
        assert (score.precision, score.recall) == (1.0, 1.0)


def test_scd2_dimension_has_one_version_per_customer_change(spark, scenario):
    customers = len(scenario.store.tables["customers"])
    changes = sum(summary.updated["customers"] for summary in scenario.summaries)

    assert changes > 0
    assert count(spark, "gold.dim_customer", "customer_id > 0") == customers + changes
    assert count(spark, "gold.dim_customer", "customer_id > 0 AND is_current") == customers
    assert count(spark, "gold.dim_customer", "customer_id > 0 AND NOT is_current") == changes


def test_orders_placed_before_a_change_keep_the_historical_customer_version(spark, scenario):
    on_old_versions = count(
        spark,
        "gold.fact_order_items AS f JOIN gold.dim_customer AS c ON c.customer_sk = f.customer_sk",
        "NOT c.is_current",
    )
    outside_validity = count(
        spark,
        "gold.fact_order_items AS f JOIN gold.dim_customer AS c ON c.customer_sk = f.customer_sk",
        "f.customer_sk <> -1 AND NOT (f.order_ts >= c.valid_from AND f.order_ts < c.valid_to)",
    )

    assert on_old_versions > 0
    assert outside_validity == 0


def test_schema_drift_is_applied_to_bronze_and_logged_but_not_promoted(spark, scenario):
    drift_batch = scenario.batches[scenario.settings.schema_drift_day].batch_id

    assert "gift_wrap" in spark.table("bronze.order_items").columns
    assert "gift_wrap" not in spark.table("silver.order_items").columns
    assert rows(
        spark,
        "SELECT table_name, column_name, change_type, new_type, batch_id FROM dq.schema_changes",
    ) == [("order_items", "gift_wrap", "column_added", "boolean", drift_batch)]
    wrapped = dict(
        rows(
            spark,
            f"SELECT _batch_id < '{drift_batch}', count(gift_wrap) FROM bronze.order_items GROUP BY 1",
        )
    )
    assert wrapped[True] == 0
    assert wrapped[False] > 0


def test_bronze_is_append_only_with_one_partition_per_batch(spark, scenario):
    per_batch = Counter(
        dict(rows(spark, "SELECT _batch_id, count(*) FROM bronze.orders GROUP BY 1"))
    )

    assert sorted(per_batch) == [batch.batch_id for batch in scenario.batches]
    # Day 0 is the full snapshot; later batches only carry changed rows.
    assert per_batch[scenario.batches[0].batch_id] > 5 * per_batch[scenario.batches[1].batch_id]


def test_gold_is_exported_as_parquet_for_the_latest_batch(spark, scenario):
    config = load_publish_config(scenario.settings.config_dir)
    table = next(t for t in config.tables if t.target == "fact_order_items")

    export = pads.dataset(
        export_path(scenario.settings.export_root, table, scenario.batches[-1]), format="parquet"
    )

    assert export.count_rows() == count(spark, "gold.fact_order_items")
    assert "customer_sk" in export.schema.names


def fingerprints(spark) -> dict[str, tuple[int, int]]:
    """Row count and an order-independent content hash of each compared table."""
    result = {}
    for table in COMPARED_TABLES:
        df = spark.table(table)
        stable = df.drop(*(VOLATILE_COLUMNS & set(df.columns)))
        result[table] = tuple(
            stable.selectExpr(
                "count(*)", "coalesce(sum(CAST(xxhash64(*) AS DECIMAL(38, 0))), 0)"
            ).collect()[0]
        )
    return result


def test_rerunning_batches_changes_nothing(spark, scenario):
    """Runs last in this module on purpose: it replays batches on the shared lakehouse."""
    context = memory_context(spark, scenario.settings, scenario.store)
    latest = scenario.batches[-1]
    before = fingerprints(spark)
    latest_bronze_rows = count(spark, "bronze.orders", f"_batch_id = '{latest.batch_id}'")

    run_batch(context, Batch(latest.batch_date, "rerun-latest"))
    run_batch(context, Batch(scenario.batches[1].batch_date, "rerun-earlier"))

    assert fingerprints(spark) == before
    assert count(spark, "bronze.orders", f"_batch_id = '{latest.batch_id}'") == latest_bronze_rows
    duplicated_versions = rows(
        spark,
        "SELECT order_id FROM bronze.orders GROUP BY order_id, updated_at, _batch_id "
        "HAVING count(*) > 1",
    )
    assert duplicated_versions == []
