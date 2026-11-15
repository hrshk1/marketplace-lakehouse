"""Small helpers for working with Iceberg tables through Spark."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession

NAMESPACES = ("bronze", "silver", "silver_quarantine", "gold", "dq", "ops")


def ensure_namespaces(spark: SparkSession) -> None:
    """Create the lakehouse namespaces if they do not exist yet."""
    for namespace in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def table_exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table)


def create_table(df: DataFrame, table: str, *partitioning: object) -> None:
    """Create an empty format-v2 Iceberg table with the schema of `df`."""
    writer = df.limit(0).writeTo(table).using("iceberg").tableProperty("format-version", "2")
    if partitioning:
        writer = writer.partitionedBy(*partitioning)
    writer.create()


def append_rows(spark: SparkSession, table: str, schema_ddl: str, rows: Sequence[tuple]) -> None:
    """Append rows to a log-style table, creating it on first use."""
    df = spark.createDataFrame(list(rows), schema=schema_ddl)
    if not table_exists(spark, table):
        create_table(df, table)
    df.writeTo(table).append()


def scalar(spark: SparkSession, query: str) -> object:
    """Run a query that returns one row with one column and return that value."""
    return spark.sql(query).collect()[0][0]


def epoch_micros_to_utc(value: int | None) -> datetime | None:
    """Convert `unix_micros(ts)` back to an aware datetime.

    Timestamps are read back as integers because collecting a Spark timestamp
    yields a naive datetime in the driver's local time zone.
    """
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC)


def sql_timestamp(moment: datetime) -> str:
    """Render an aware datetime as a Spark SQL timestamp literal."""
    return f"TIMESTAMP '{moment.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}+00:00'"
