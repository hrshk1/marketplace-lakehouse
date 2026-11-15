"""Iceberg table maintenance: compaction, snapshot expiry and orphan file cleanup."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from marketplace.lake import NAMESPACES, sql_timestamp
from marketplace.logging_setup import get_logger

log = get_logger("maintenance")

# Snapshots newer than this stay available for time travel and rollback.
SNAPSHOT_RETENTION = timedelta(days=7)
MIN_SNAPSHOTS_TO_KEEP = 5
# Iceberg refuses to delete unreferenced files younger than 24 hours, because a
# write that is still in flight could own them. Three days is its default.
ORPHAN_FILE_RETENTION = timedelta(days=3)


@dataclass(frozen=True)
class MaintenanceResult:
    """What maintenance did to one table."""

    table: str
    data_files_rewritten: int
    data_files_added: int
    snapshot_files_deleted: int
    orphan_files_deleted: int


def list_tables(spark: SparkSession) -> list[str]:
    """Every table in the lakehouse namespaces, as `namespace.table`."""
    tables = []
    for namespace in NAMESPACES:
        rows = spark.sql(f"SHOW TABLES IN {namespace}").collect()
        tables.extend(f"{namespace}.{row.tableName}" for row in rows)
    return sorted(tables)


def compact(spark: SparkSession, table: str) -> tuple[int, int]:
    """Merge small data files; returns (files rewritten, files added)."""
    catalog = spark.catalog.currentCatalog()
    row = spark.sql(
        f"""
        CALL {catalog}.system.rewrite_data_files(
            table => '{table}',
            options => map('min-input-files', '2')
        )
        """
    ).collect()[0]
    return row.rewritten_data_files_count, row.added_data_files_count


def expire_snapshots(spark: SparkSession, table: str, now: datetime) -> int:
    """Drop snapshots past retention; returns the number of files deleted with them."""
    catalog = spark.catalog.currentCatalog()
    row = spark.sql(
        f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{table}',
            older_than => {sql_timestamp(now - SNAPSHOT_RETENTION)},
            retain_last => {MIN_SNAPSHOTS_TO_KEEP}
        )
        """
    ).collect()[0]
    return sum(value for name, value in row.asDict().items() if name.startswith("deleted_"))


def remove_orphan_files(spark: SparkSession, table: str, now: datetime) -> int:
    """Delete files no snapshot references; returns how many were deleted."""
    catalog = spark.catalog.currentCatalog()
    rows = spark.sql(
        f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{table}',
            older_than => {sql_timestamp(now - ORPHAN_FILE_RETENTION)}
        )
        """
    ).collect()
    return len(rows)


def maintain_table(spark: SparkSession, table: str) -> MaintenanceResult:
    """Run the three maintenance operations on one table, in the safe order.

    Compaction first, so the snapshots it replaces can be expired; orphan
    cleanup last, so it also sees files that expiry no longer references.
    """
    now = datetime.now(UTC)
    rewritten, added = compact(spark, table)
    result = MaintenanceResult(
        table=table,
        data_files_rewritten=rewritten,
        data_files_added=added,
        snapshot_files_deleted=expire_snapshots(spark, table, now),
        orphan_files_deleted=remove_orphan_files(spark, table, now),
    )
    log.info(
        "table_maintained",
        extra={
            "table": table,
            "data_files_rewritten": rewritten,
            "data_files_added": added,
            "snapshot_files_deleted": result.snapshot_files_deleted,
            "orphan_files_deleted": result.orphan_files_deleted,
        },
    )
    return result
