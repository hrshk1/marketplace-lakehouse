"""Export lakehouse tables to Parquet files in the export area of the object store."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from marketplace.batch import Batch
from marketplace.lake import table_exists
from marketplace.logging_setup import get_logger
from marketplace.publish.tables import PublishedTable
from marketplace.silver.specs import SILVER_TABLES

log = get_logger("publish")


def quarantine_summary(spark: SparkSession) -> DataFrame:
    """Rejected-row counts per table, rule and batch, across all quarantine tables."""
    per_table = [
        f"""
        SELECT '{table.name}' AS table_name, rule_id, reject_reason, _batch_id AS batch_id,
               count(*) AS rejected_rows
        FROM {table.quarantine}
        GROUP BY rule_id, reject_reason, _batch_id
        """
        for table in SILVER_TABLES
        if table_exists(spark, table.quarantine)
    ]
    if not per_table:
        return spark.createDataFrame(
            [],
            "table_name string, rule_id string, reject_reason string, batch_id string, "
            "rejected_rows bigint",
        )
    return spark.sql(" UNION ALL ".join(per_table))


# Sources that are computed at export time instead of being read from a table.
_DERIVED_SOURCES: dict[str, Callable[[SparkSession], DataFrame]] = {
    "quarantine_summary": quarantine_summary,
}


def export_path(export_root: str, table: PublishedTable, batch: Batch) -> str:
    """Folder holding one table's Parquet export for one batch."""
    return f"{export_root.rstrip('/')}/{table.target}/batch_id={batch.batch_id}"


def export_table(
    spark: SparkSession, table: PublishedTable, export_root: str, batch: Batch
) -> int | None:
    """Write one table as Parquet; returns the row count, or None if the source is absent.

    The folder is overwritten, so exporting the same batch twice leaves one copy.
    """
    if table.source in _DERIVED_SOURCES:
        source = _DERIVED_SOURCES[table.source](spark)
    elif table_exists(spark, table.source):
        source = spark.table(table.source)
    else:
        log.info("export_skipped_missing_source", extra={"source": table.source})
        return None

    path = export_path(export_root, table, batch)
    source.coalesce(1).write.mode("overwrite").parquet(path)
    rows = spark.read.parquet(path).count()
    log.info("table_exported", extra={"source": table.source, "path": path, "rows": rows})
    return rows
