"""Incremental extraction of one source table into its append-only Bronze table."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from marketplace.batch import Batch
from marketplace.bronze import checkpoints
from marketplace.bronze.schema_evolution import SchemaDriftError, apply_schema_changes, diff_schemas
from marketplace.lake import create_table, table_exists
from marketplace.logging_setup import get_logger
from marketplace.sources.readers import SourceReader, SourceTable

log = get_logger("bronze")


@dataclass(frozen=True)
class ExtractResult:
    """Outcome of extracting one table for one batch."""

    table: str
    rows_extracted: int
    source_row_count: int
    full_snapshot: bool


def bronze_table(name: str) -> str:
    return f"bronze.{name}"


def extract_table(
    spark: SparkSession,
    reader: SourceReader,
    table: SourceTable,
    batch: Batch,
    lookback: timedelta,
) -> ExtractResult:
    """Land the changed rows of one source table for one batch.

    The batch's rows replace whatever the same batch wrote before, so a re-run
    never duplicates data. Bronze tables are partitioned by `_batch_id`, which
    makes that replacement a whole-partition swap in a single Iceberg commit.
    """
    checkpoints.ensure_table(spark)
    target = bronze_table(table.name)
    window = checkpoints.resolve_window(
        checkpoints.previous_high_watermark(spark, table.name, batch.batch_id),
        batch.high_watermark,
        lookback,
    )

    incoming = (
        reader.read_window(table, window.read_from, window.high_watermark)
        .withColumn("_batch_id", F.lit(batch.batch_id))
        .withColumn("_ingested_at", F.lit(datetime.now(UTC)))
        .withColumn("_source_system", F.lit(table.system))
        .withColumn("_extract_watermark", F.lit(window.high_watermark))
    )

    if not table_exists(spark, target):
        create_table(incoming, target, F.col("_batch_id"))
    else:
        diff = diff_schemas(spark.table(target).schema, incoming.schema)
        try:
            apply_schema_changes(spark, target, diff, batch.batch_id)
        except SchemaDriftError as error:
            checkpoints.record(
                spark, table.name, batch, window, checkpoints.QUARANTINED, message=str(error)
            )
            log.error(
                "bronze_batch_quarantined",
                extra={"table": table.name, "batch_id": batch.batch_id, "reason": str(error)},
            )
            raise

    incoming.writeTo(target).overwrite(F.col("_batch_id") == batch.batch_id)

    rows_extracted = spark.table(target).where(F.col("_batch_id") == batch.batch_id).count()
    source_row_count = reader.count_until(table, window.high_watermark)
    checkpoints.record(
        spark,
        table.name,
        batch,
        window,
        checkpoints.COMMITTED,
        rows_extracted=rows_extracted,
        source_row_count=source_row_count,
    )
    log.info(
        "bronze_extract_committed",
        extra={
            "table": table.name,
            "batch_id": batch.batch_id,
            "rows": rows_extracted,
            "full_snapshot": window.is_full_snapshot,
        },
    )
    return ExtractResult(table.name, rows_extracted, source_row_count, window.is_full_snapshot)
