"""Run summary log: one row per pipeline step in `ops.pipeline_runs`."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from marketplace.batch import Batch
from marketplace.lake import append_rows
from marketplace.logging_setup import get_logger

log = get_logger("ops")

RUNS_TABLE = "ops.pipeline_runs"
SUCCESS = "success"
FAILED = "failed"

_SCHEMA = (
    "run_id string, batch_id string, layer string, step string, status string, "
    "rows_in bigint, rows_out bigint, rows_quarantined bigint, duration_seconds double, "
    "sla_seconds int, sla_breached boolean, error_message string, "
    "started_at timestamp, finished_at timestamp"
)
_MAX_ERROR_LENGTH = 2000


@dataclass
class StepMetrics:
    """Row counts a step reports while it runs."""

    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0


@contextmanager
def track_step(
    spark: SparkSession, batch: Batch, layer: str, step: str, sla_seconds: int
) -> Iterator[StepMetrics]:
    """Time a pipeline step and record its outcome when it succeeds.

    Failures are recorded by whoever handles the exception (the Airflow
    failure callback or the command-line runner), so a failed step yields
    exactly one row whichever way the pipeline was started.
    """
    metrics = StepMetrics()
    started_at = datetime.now(UTC)
    started = time.monotonic()
    yield metrics
    duration = time.monotonic() - started
    append_rows(
        spark,
        RUNS_TABLE,
        _SCHEMA,
        [
            (
                batch.run_id,
                batch.batch_id,
                layer,
                step,
                SUCCESS,
                metrics.rows_in,
                metrics.rows_out,
                metrics.rows_quarantined,
                round(duration, 3),
                sla_seconds,
                duration > sla_seconds,
                None,
                started_at,
                datetime.now(UTC),
            )
        ],
    )
    log.info(
        "step_succeeded",
        extra={
            "run_id": batch.run_id,
            "batch_id": batch.batch_id,
            "layer": layer,
            "step": step,
            "rows_in": metrics.rows_in,
            "rows_out": metrics.rows_out,
            "rows_quarantined": metrics.rows_quarantined,
            "duration_seconds": round(duration, 3),
        },
    )


def record_failure(
    spark: SparkSession,
    run_id: str,
    batch_id: str,
    layer: str,
    step: str,
    error: str,
    duration_seconds: float | None = None,
) -> None:
    """Write a structured failure record for a step and log it."""
    now = datetime.now(UTC)
    append_rows(
        spark,
        RUNS_TABLE,
        _SCHEMA,
        [
            (
                run_id,
                batch_id,
                layer,
                step,
                FAILED,
                None,
                None,
                None,
                duration_seconds,
                None,
                None,
                error[:_MAX_ERROR_LENGTH],
                now,
                now,
            )
        ],
    )
    log.error(
        "step_failed",
        extra={
            "run_id": run_id,
            "batch_id": batch_id,
            "layer": layer,
            "step": step,
            "error": error,
        },
    )
