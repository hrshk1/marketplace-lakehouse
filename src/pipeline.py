"""The pipeline's steps, as plain functions.

Airflow tasks and the command-line runner both call these, so the two ways of
running the pipeline cannot drift apart. Every step records itself in
`ops.pipeline_runs` and is safe to repeat for the same batch.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

import psycopg
from psycopg import sql
from pyspark.sql import SparkSession

from marketplace.batch import Batch
from marketplace.bronze.extract import ExtractResult, extract_table
from marketplace.config import Settings
from marketplace.dq.loader import load_layer_checks
from marketplace.dq.model import PASSED, CheckResult
from marketplace.dq.runner import enforce, run_checks
from marketplace.gold.models import GOLD_MODELS
from marketplace.lake import NAMESPACES, ensure_namespaces
from marketplace.ops.maintenance import MaintenanceResult, list_tables, maintain_table
from marketplace.ops.runs import record_failure, track_step
from marketplace.publish.export import export_path, export_table
from marketplace.publish.load import load_table, open_export
from marketplace.publish.tables import ANALYTICS, OPERATIONS, load_publish_config
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
PUBLISH = "publish"
MAINTENANCE = "maintenance"

EXTRACT_TABLES: tuple[str, ...] = tuple(table.name for table in SOURCE_TABLES)
SILVER_TABLE_NAMES: tuple[str, ...] = tuple(table.name for table in SILVER_TABLES)
GOLD_MODEL_NAMES: tuple[str, ...] = tuple(GOLD_MODELS)

# Expected worst-case duration of one step, per layer. A step that takes
# longer still succeeds but is flagged as an SLA breach in ops.pipeline_runs.
SLA_SECONDS = {BRONZE: 600, SILVER: 600, GOLD: 900, "dq": 600, PUBLISH: 900, MAINTENANCE: 900}


@dataclass(frozen=True)
class PipelineContext:
    """What every step needs: a Spark session, settings and a way to read the sources.

    `dw_conninfo` is the serving warehouse connection. When it is None the
    publish step still exports Parquet but skips the warehouse load, which is
    how the pipeline runs without any database in tests.
    """

    spark: SparkSession
    settings: Settings
    reader: SourceReader
    dw_conninfo: str | None


def open_context(settings: Settings | None = None) -> PipelineContext:
    """Create the context used outside tests: real sources and the configured catalog."""
    settings = settings or Settings.from_env()
    spark = build_spark(settings)
    ensure_namespaces(spark)
    return PipelineContext(
        spark, settings, PostgresMongoReader(spark, settings), settings.dw_conninfo
    )


def reset_lakehouse(ctx: PipelineContext) -> None:
    """Drop every lakehouse table and the serving schema.

    Used when the sources are re-seeded: data loaded from the previous sources
    would otherwise be mixed with rows from the new ones.
    """
    for table in list_tables(ctx.spark):
        ctx.spark.sql(f"DROP TABLE {table} PURGE")
    for namespace in NAMESPACES:
        ctx.spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")
    ensure_namespaces(ctx.spark)
    if ctx.dw_conninfo:
        schema = load_publish_config(ctx.settings.config_dir).schema
        with psycopg.connect(ctx.dw_conninfo) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )


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


def publish(ctx: PipelineContext, batch: Batch, group: str) -> None:
    """Export a group of tables to Parquet and load them into the serving warehouse."""
    config = load_publish_config(ctx.settings.config_dir)
    connection = psycopg.connect(ctx.dw_conninfo) if ctx.dw_conninfo else None
    try:
        for table in config.in_group(group):
            with track_step(
                ctx.spark, batch, PUBLISH, table.target, SLA_SECONDS[PUBLISH]
            ) as metrics:
                exported = export_table(ctx.spark, table, ctx.settings.export_root, batch)
                metrics.rows_in = exported or 0
                if exported is not None and connection is not None:
                    export = open_export(
                        ctx.settings, export_path(ctx.settings.export_root, table, batch)
                    )
                    metrics.rows_out = load_table(connection, config.schema, table, export)
    finally:
        if connection is not None:
            connection.close()


def maintain(ctx: PipelineContext, batch: Batch) -> list[MaintenanceResult]:
    """Compact every lakehouse table, expire old snapshots and remove orphan files."""
    results = []
    for table in list_tables(ctx.spark):
        with track_step(ctx.spark, batch, MAINTENANCE, table, SLA_SECONDS[MAINTENANCE]) as metrics:
            result = maintain_table(ctx.spark, table)
            metrics.rows_in = result.data_files_rewritten
            metrics.rows_out = result.data_files_added
        results.append(result)
    return results


def batch_steps(ctx: PipelineContext, batch: Batch) -> list[tuple[str, str, Callable[[], object]]]:
    """Every gated step of one batch as (layer, step name, action), in execution order.

    This mirrors the task groups of the Airflow DAG.
    """

    def gate(layer: str) -> tuple[str, str, Callable[[], object]]:
        return (f"dq_{layer}", "checks", lambda: quality_gate(ctx, batch, layer))

    def steps(layer: str, names: tuple[str, ...], action: Callable) -> list:
        return [(layer, name, lambda n=name: action(ctx, batch, n)) for name in names]

    return [
        *steps(BRONZE, EXTRACT_TABLES, extract_to_bronze),
        gate(BRONZE),
        *steps(SILVER, SILVER_TABLE_NAMES, build_silver),
        gate(SILVER),
        *steps(GOLD, GOLD_MODEL_NAMES, build_gold),
        gate(GOLD),
        (PUBLISH, ANALYTICS, lambda: publish(ctx, batch, ANALYTICS)),
    ]


def run_batch(ctx: PipelineContext, batch: Batch) -> None:
    """Run every step of one batch in order, stopping at the first failure.

    The operational tables are published whether or not the batch succeeded,
    so a failed run is visible in the warehouse.
    """
    try:
        for layer, step, action in batch_steps(ctx, batch):
            try:
                action()
            except Exception as error:
                record_failure(ctx.spark, batch.run_id, batch.batch_id, layer, step, repr(error))
                raise
    finally:
        publish(ctx, batch, OPERATIONS)
