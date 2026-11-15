"""DAG structure and the Airflow-facing entry points."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from marketplace import orchestration, pipeline
from marketplace.generator.stores import MemoryStore
from tests.helpers import memory_context, rows

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


def test_daily_schedule_defaults_to_daily_and_can_be_made_manual(monkeypatch):
    monkeypatch.delenv("PIPELINE_SCHEDULE", raising=False)
    assert orchestration.daily_schedule() == "@daily"

    monkeypatch.setenv("PIPELINE_SCHEDULE", "manual")
    assert orchestration.daily_schedule() is None

    monkeypatch.setenv("PIPELINE_SCHEDULE", "0 2 * * *")
    assert orchestration.daily_schedule() == "0 2 * * *"


def test_watcher_body_always_fails():
    with pytest.raises(orchestration.UpstreamFailedError):
        orchestration.fail_run()


@pytest.mark.spark
def test_failure_callback_writes_a_structured_record(lake, settings, monkeypatch):
    monkeypatch.setattr(
        pipeline, "open_context", lambda: memory_context(lake, settings, MemoryStore())
    )
    context = {
        "ti": SimpleNamespace(task_id="build_silver.orders"),
        "dag_run": SimpleNamespace(
            run_id="scheduled__2026-07-01",
            logical_date=datetime(2026, 7, 1, tzinfo=UTC),
            run_after=None,
        ),
        "exception": ValueError("boom"),
    }

    orchestration.record_task_failure(context)

    assert rows(
        lake, "SELECT run_id, batch_id, layer, step, status, error_message FROM ops.pipeline_runs"
    ) == [
        (
            "scheduled__2026-07-01",
            "2026-07-01",
            "build_silver",
            "orders",
            "failed",
            "ValueError('boom')",
        )
    ]


@pytest.mark.spark
def test_failure_callback_uses_run_after_when_there_is_no_logical_date(lake, settings, monkeypatch):
    monkeypatch.setattr(
        pipeline, "open_context", lambda: memory_context(lake, settings, MemoryStore())
    )
    context = {
        "ti": SimpleNamespace(task_id="maintain_tables"),
        "dag_run": SimpleNamespace(
            run_id="manual__1", logical_date=None, run_after=datetime(2026, 7, 4, 3, tzinfo=UTC)
        ),
        "exception": None,
    }

    orchestration.record_task_failure(context)

    assert rows(lake, "SELECT batch_id, layer, step FROM ops.pipeline_runs") == [
        ("2026-07-04", "maintain_tables", "maintain_tables")
    ]


def load_dag(file_name: str):
    pytest.importorskip("airflow")
    spec = importlib.util.spec_from_file_location(
        file_name.removesuffix(".py"), DAGS_DIR / file_name
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


@pytest.fixture(scope="module")
def daily_dag():
    return load_dag("daily_marketplace_pipeline.py")


@pytest.mark.airflow
def test_daily_dag_has_one_task_per_pipeline_step(daily_dag):
    expected = {
        *(f"extract_to_bronze.{name}" for name in pipeline.EXTRACT_TABLES),
        *(f"build_silver.{name}" for name in pipeline.SILVER_TABLE_NAMES),
        *(f"build_gold.{name}" for name in pipeline.GOLD_MODEL_NAMES),
        "dq_bronze.checks",
        "dq_silver.checks",
        "dq_gold.checks",
        "publish_to_dw.analytics_tables",
        "publish_to_dw.operational_tables",
        "fail_run_if_any_task_failed",
    }

    assert set(daily_dag.task_ids) == expected


@pytest.mark.airflow
def test_layers_run_in_order_with_a_quality_gate_between_them(daily_dag):
    def downstream(task_id: str) -> set[str]:
        return daily_dag.get_task(task_id).get_flat_relative_ids(upstream=False)

    assert "dq_bronze.checks" in downstream("extract_to_bronze.orders")
    assert "build_silver.categories" in downstream("dq_bronze.checks")
    assert "dq_silver.checks" in downstream("build_silver.product_attributes")
    assert "build_gold.dim_date" in downstream("dq_silver.checks")
    assert "dq_gold.checks" in downstream("build_gold.agg_daily_sales")
    assert "publish_to_dw.analytics_tables" in downstream("dq_gold.checks")
    # Within Silver and Gold, parents and dimensions are built first.
    assert "build_silver.order_items" in downstream("build_silver.orders")
    assert "build_gold.fact_order_items" in downstream("build_gold.dim_customer")


@pytest.mark.airflow
def test_tasks_retry_with_backoff_and_report_failures(daily_dag):
    extract = daily_dag.get_task("extract_to_bronze.orders")

    assert extract.retries == 2
    assert extract.retry_exponential_backoff == 2.0
    assert extract.execution_timeout.total_seconds() == 1800
    assert extract.on_failure_callback == [orchestration.record_task_failure]
    assert extract.op_kwargs["table_name"] == "orders"
    assert "logical_date" in extract.op_kwargs["batch_date"]


@pytest.mark.airflow
def test_quality_gates_do_not_retry(daily_dag):
    assert {
        daily_dag.get_task(f"dq_{layer}.checks").retries for layer in ("bronze", "silver", "gold")
    } == {0}


@pytest.mark.airflow
def test_operational_publish_and_watcher_run_after_failures(daily_dag):
    assert daily_dag.get_task("publish_to_dw.operational_tables").trigger_rule == "all_done"
    assert daily_dag.get_task("fail_run_if_any_task_failed").trigger_rule == "one_failed"
    assert daily_dag.max_active_runs == 1
    assert daily_dag.catchup is False


@pytest.mark.airflow
def test_maintenance_dag_runs_weekly():
    dag = load_dag("iceberg_maintenance.py")

    assert dag.task_ids == ["maintain_tables"]
    assert dag.schedule == "@weekly"
