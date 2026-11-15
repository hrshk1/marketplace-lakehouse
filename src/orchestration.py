"""Entry points the Airflow DAGs call.

Each function is one task: it opens a pipeline context, runs a single step
from `marketplace.pipeline` and returns. Nothing here knows about Airflow
except the failure callback, which reads the task context it is handed.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any

from marketplace import pipeline
from marketplace.batch import Batch
from marketplace.logging_setup import configure_logging, get_logger
from marketplace.ops.runs import record_failure
from marketplace.publish.tables import ANALYTICS, OPERATIONS

log = get_logger("orchestration")

MANUAL_SCHEDULE = "manual"


class UpstreamFailedError(RuntimeError):
    """Raised by the watcher task to mark a run failed when any task failed."""


def daily_schedule() -> str | None:
    """Schedule of the daily DAG.

    `@daily` by default. The local stack sets PIPELINE_SCHEDULE=manual because
    its sources live in simulated time, so "today" has no data to process and
    runs are triggered for explicit logical dates instead.
    """
    schedule = os.environ.get("PIPELINE_SCHEDULE", "@daily").strip()
    return None if schedule == MANUAL_SCHEDULE else schedule


def _start(batch_date: str, run_id: str) -> tuple[pipeline.PipelineContext, Batch]:
    configure_logging()
    return pipeline.open_context(), Batch(date.fromisoformat(batch_date), run_id)


def extract_table(table_name: str, batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.extract_to_bronze(ctx, batch, table_name)


def build_silver_table(table_name: str, batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.build_silver(ctx, batch, table_name)


def build_gold_model(model_name: str, batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.build_gold(ctx, batch, model_name)


def quality_gate(layer: str, batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.quality_gate(ctx, batch, layer)


def publish_analytics_tables(batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.publish(ctx, batch, ANALYTICS)


def publish_operational_tables(batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.publish(ctx, batch, OPERATIONS)


def maintain_tables(batch_date: str, run_id: str) -> None:
    ctx, batch = _start(batch_date, run_id)
    pipeline.maintain(ctx, batch)


def fail_run() -> None:
    """Body of the watcher task, which only runs when another task has failed."""
    raise UpstreamFailedError("At least one task of this run failed; see ops.pipeline_runs.")


def record_task_failure(context: dict[str, Any]) -> None:
    """Airflow failure callback: write a structured record to `ops.pipeline_runs`.

    The task id is `<task group>.<step>`, which maps directly onto the layer
    and step columns of the run log.
    """
    configure_logging()
    task_instance = context["ti"]
    dag_run = context["dag_run"]
    layer, _, step = task_instance.task_id.rpartition(".")
    batch_date = dag_run.logical_date or dag_run.run_after
    try:
        record_failure(
            pipeline.open_context().spark,
            run_id=dag_run.run_id,
            batch_id=batch_date.date().isoformat(),
            layer=layer or task_instance.task_id,
            step=step,
            error=repr(context.get("exception")),
        )
    except Exception:
        # The callback must never hide the original failure behind its own.
        log.exception("failure_record_not_written", extra={"task_id": task_instance.task_id})
