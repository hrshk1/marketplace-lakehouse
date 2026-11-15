"""Persisted extraction watermarks, one row per source table and batch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from marketplace.batch import Batch
from marketplace.lake import epoch_micros_to_utc, scalar, table_exists

CHECKPOINT_TABLE = "ops.extract_checkpoints"
COMMITTED = "committed"
QUARANTINED = "quarantined"

_SCHEMA = (
    "table_name string, batch_id string, low_watermark timestamp, high_watermark timestamp, "
    "rows_extracted bigint, source_row_count bigint, status string, message string, "
    "recorded_at timestamp"
)


@dataclass(frozen=True)
class ExtractWindow:
    """The slice of source changes one batch extracts.

    `low_watermark` is where the previous batch stopped (None on the first
    run). `read_from` starts earlier by the lookback, so rows committed late
    with a slightly old `updated_at` are still picked up.
    """

    low_watermark: datetime | None
    read_from: datetime | None
    high_watermark: datetime

    @property
    def is_full_snapshot(self) -> bool:
        return self.low_watermark is None


def resolve_window(
    previous_high: datetime | None, batch_high: datetime, lookback: timedelta
) -> ExtractWindow:
    """Decide what to extract given where the previous batch stopped."""
    if previous_high is None:
        return ExtractWindow(None, None, batch_high)
    return ExtractWindow(previous_high, previous_high - lookback, batch_high)


def ensure_table(spark: SparkSession) -> None:
    if not table_exists(spark, CHECKPOINT_TABLE):
        spark.sql(f"CREATE TABLE {CHECKPOINT_TABLE} ({_SCHEMA}) USING iceberg")


def previous_high_watermark(spark: SparkSession, table_name: str, batch_id: str) -> datetime | None:
    """High watermark of the latest committed batch strictly before `batch_id`.

    Looking only at earlier batches is what makes a re-run reproducible: the
    batch being re-run never sees its own previous checkpoint.
    """
    value = scalar(
        spark,
        f"""
        SELECT unix_micros(max(high_watermark))
        FROM {CHECKPOINT_TABLE}
        WHERE table_name = '{table_name}' AND status = '{COMMITTED}' AND batch_id < '{batch_id}'
        """,
    )
    return epoch_micros_to_utc(value)


def record(
    spark: SparkSession,
    table_name: str,
    batch: Batch,
    window: ExtractWindow,
    status: str,
    rows_extracted: int = 0,
    source_row_count: int = 0,
    message: str = "",
) -> None:
    """Upsert the checkpoint of one table for one batch."""
    row = (
        table_name,
        batch.batch_id,
        window.low_watermark,
        window.high_watermark,
        rows_extracted,
        source_row_count,
        status,
        message,
        datetime.now(UTC),
    )
    spark.createDataFrame([row], schema=_SCHEMA).createOrReplaceTempView("checkpoint_update")
    spark.sql(
        f"""
        MERGE INTO {CHECKPOINT_TABLE} AS target
        USING checkpoint_update AS source
        ON target.table_name = source.table_name AND target.batch_id = source.batch_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
