"""Run a layer's checks, persist the results and gate the pipeline on them."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from marketplace.batch import Batch
from marketplace.dq.checks import RECORD_KEY, Evaluation, RowCounts, evaluate
from marketplace.dq.model import (
    ERROR,
    FAILED,
    PASSED,
    WARNED,
    CheckResult,
    CheckSpec,
    DataQualityError,
)
from marketplace.lake import append_rows, create_table, table_exists
from marketplace.logging_setup import get_logger

log = get_logger("dq")

RESULTS_TABLE = "dq.check_results"
FAILED_ROWS_TABLE = "dq.failed_rows"

# Upper bound on failing keys stored per check and run, to keep one very
# broken table from flooding the failed-rows log.
MAX_FAILED_KEYS_PER_CHECK = 100_000

_RESULTS_SCHEMA = (
    "run_id string, batch_id string, layer string, table_name string, check_id string, "
    "check_type string, severity string, status string, observed_value double, "
    "threshold double, failed_rows bigint, rows_checked bigint, details string, "
    "checked_at timestamp"
)


def _status(spec: CheckSpec, evaluation: Evaluation) -> str:
    """A check passes when its observed value is within the threshold."""
    within = evaluation.observed_value is not None and evaluation.observed_value <= spec.threshold
    if within:
        return PASSED
    return FAILED if spec.severity == ERROR else WARNED


def _store_failed_rows(
    spark: SparkSession, spec: CheckSpec, evaluation: Evaluation, batch: Batch, checked_at: datetime
) -> None:
    failed = evaluation.failing.limit(MAX_FAILED_KEYS_PER_CHECK).select(
        F.lit(batch.run_id).alias("run_id"),
        F.lit(batch.batch_id).alias("batch_id"),
        F.lit(spec.layer).alias("layer"),
        F.lit(spec.table).alias("table_name"),
        F.lit(spec.check_id).alias("check_id"),
        F.col(RECORD_KEY),
        F.lit(checked_at).alias("checked_at"),
    )
    if not table_exists(spark, FAILED_ROWS_TABLE):
        create_table(failed, FAILED_ROWS_TABLE)
    failed.writeTo(FAILED_ROWS_TABLE).append()


def run_checks(spark: SparkSession, checks: list[CheckSpec], batch: Batch) -> list[CheckResult]:
    """Evaluate the checks for one batch and write results and failing keys."""
    checked_at = datetime.now(UTC)
    counts: RowCounts = {}
    results = []
    for spec in checks:
        evaluation = evaluate(spark, spec, batch, counts)
        result = CheckResult(
            spec=spec,
            status=_status(spec, evaluation),
            observed_value=evaluation.observed_value,
            failed_rows=evaluation.failed_rows,
            rows_checked=evaluation.rows_checked,
            details=evaluation.details,
        )
        if evaluation.failed_rows:
            _store_failed_rows(spark, spec, evaluation, batch, checked_at)
        if result.status != PASSED:
            log.warning(
                "dq_check_not_passed",
                extra={
                    "check_id": spec.check_id,
                    "status": result.status,
                    "severity": spec.severity,
                    "observed_value": result.observed_value,
                    "threshold": spec.threshold,
                    "details": result.details,
                },
            )
        results.append(result)

    append_rows(
        spark,
        RESULTS_TABLE,
        _RESULTS_SCHEMA,
        [
            (
                batch.run_id,
                batch.batch_id,
                r.spec.layer,
                r.spec.table,
                r.spec.check_id,
                r.spec.check_type,
                r.spec.severity,
                r.status,
                None if r.observed_value is None else float(r.observed_value),
                float(r.spec.threshold),
                r.failed_rows,
                r.rows_checked,
                r.details,
                checked_at,
            )
            for r in results
        ],
    )
    return results


def enforce(layer: str, results: list[CheckResult]) -> None:
    """Stop the pipeline if any error-severity check failed."""
    blocking = [result for result in results if result.blocks_pipeline]
    if blocking:
        summary = "; ".join(f"{r.spec.check_id} ({r.details})" for r in blocking)
        raise DataQualityError(
            f"{len(blocking)} error-severity check(s) failed in layer {layer}: {summary}"
        )
