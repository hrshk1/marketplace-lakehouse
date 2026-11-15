"""The pipeline's steps, as plain functions.

Airflow tasks and the command-line runner both call these, so the two ways of
running the pipeline cannot drift apart. Every step records itself in
`ops.pipeline_runs` and is safe to repeat for the same batch.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from pyspark.sql import SparkSession

from marketplace.batch import Batch
from marketplace.bronze.extract import ExtractResult, extract_table
from marketplace.config import Settings
from marketplace.dq.loader import load_layer_checks
from marketplace.dq.model import PASSED, CheckResult
from marketplace.dq.runner import enforce, run_checks
from marketplace.gold.models import GOLD_MODELS
from marketplace.lake import ensure_namespaces
from marketplace.ops.runs import record_failure, track_step
from marketplace.silver.build import SilverResult, build_silver_table
from marketplace.silver.specs import SILVER_TABLES, silver_table
from marketplace.sources.readers import (
    SOURCE_TABLES,
    PostgresMongoReader,
    SourceReader,
    source_table,
)
from marketplace.spark import build_spark

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

EXTRACT_TABLES: tuple[str, ...] = tuple(table.name for table in SOURCE_TABLES)
SILVER_TABLE_NAMES: tuple[str, ...] = tuple(table.name for table in SILVER_TABLES)
GOLD_MODEL_NAMES: tuple[str, ...] = tuple(GOLD_MODELS)

# Expected worst-case duration of one step, per layer. A step that takes
# longer still succeeds but is flagged as an SLA breach in ops.pipeline_runs.
SLA_SECONDS = {BRONZE: 600, SILVER: 600, GOLD: 900, "dq": 600, "publish": 900}


@dataclass(frozen=True)
class PipelineContext:
    """What every step needs: a Spark session, settings and a way to read the sources."""

    spark: SparkSession
    settings: Settings
    reader: SourceReader


def open_context(settings: Settings | None = None) -> PipelineContext:
    """Create the context used outside tests: real sources and the configured catalog."""
    settings = settings or Settings.from_env()
    spark = build_spark(settings)
    ensure_namespaces(spark)
    return PipelineContext(spark, settings, PostgresMongoReader(spark, settings))


def extract_to_bronze(ctx: PipelineContext, batch: Batch, table_name: str) -> ExtractResult:
    """Extract one source table into Bronze."""
    with track_step(ctx.spark, batch, BRONZE, table_name, SLA_SECONDS[BRONZE]) as metrics:
        result = extract_table(
            ctx.spark,
            ctx.reader,
            source_table(table_name),
            batch,
            timedelta(minutes=ctx.settings.extract_lookback_minutes),
        )
        metrics.rows_in = result.source_row_count
        metrics.rows_out = result.rows_extracted
    return result


def build_silver(ctx: PipelineContext, batch: Batch, table_name: str) -> SilverResult:
    """Build one Silver table from this batch's Bronze rows."""
    with track_step(ctx.spark, batch, SILVER, table_name, SLA_SECONDS[SILVER]) as metrics:
        result = build_silver_table(ctx.spark, silver_table(table_name), batch)
        metrics.rows_in = result.rows_in
        metrics.rows_out = result.rows_accepted
        metrics.rows_quarantined = result.rows_quarantined
    return result


def build_gold(ctx: PipelineContext, batch: Batch, model_name: str) -> int:
    """Build one Gold dimension, fact or aggregate; returns its row count."""
    with track_step(ctx.spark, batch, GOLD, model_name, SLA_SECONDS[GOLD]) as metrics:
        metrics.rows_out = GOLD_MODELS[model_name](ctx.spark, batch)
    return metrics.rows_out


def check_layer(ctx: PipelineContext, batch: Batch, layer: str) -> list[CheckResult]:
    """Run a layer's quality checks and record them, without gating."""
    checks = load_layer_checks(ctx.settings.config_dir, layer)
    with track_step(ctx.spark, batch, f"dq_{layer}", "checks", SLA_SECONDS["dq"]) as metrics:
        results = run_checks(ctx.spark, checks, batch)
        metrics.rows_in = len(results)
        metrics.rows_out = sum(result.status == PASSED for result in results)
    return results


def quality_gate(ctx: PipelineContext, batch: Batch, layer: str) -> list[CheckResult]:
    """Run a layer's checks and stop the pipeline if an error-severity check fails."""
    results = check_layer(ctx, batch, layer)
    enforce(layer, results)
    return results


def batch_steps(ctx: PipelineContext, batch: Batch) -> list[tuple[str, str, Callable[[], object]]]:
    """Every step of one batch as (layer, step name, action), in execution order.

    This mirrors the task groups of the Airflow DAG.
    """

    def gate(layer: str) -> tuple[str, str, Callable[[], object]]:
        return (f"dq_{layer}", "checks", lambda: quality_gate(ctx, batch, layer))

    return [
        *(
            (BRONZE, name, lambda n=name: extract_to_bronze(ctx, batch, n))
            for name in EXTRACT_TABLES
        ),
        gate(BRONZE),
        *(
            (SILVER, name, lambda n=name: build_silver(ctx, batch, n))
            for name in SILVER_TABLE_NAMES
        ),
        gate(SILVER),
        *((GOLD, name, lambda n=name: build_gold(ctx, batch, n)) for name in GOLD_MODEL_NAMES),
        gate(GOLD),
    ]


def run_batch(ctx: PipelineContext, batch: Batch) -> None:
    """Run every step of one batch in order, stopping at the first failure."""
    for layer, step, action in batch_steps(ctx, batch):
        try:
            action()
        except Exception as error:
            record_failure(ctx.spark, batch.run_id, batch.batch_id, layer, step, repr(error))
            raise
