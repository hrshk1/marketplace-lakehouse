"""Builders for the Gold star schema.

The transformations themselves are Spark SQL files under `sql/`. This module
decides when each one runs, creates tables on first use and keeps every build
idempotent.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from marketplace.batch import Batch
from marketplace.lake import create_table, scalar, table_exists
from marketplace.logging_setup import get_logger

log = get_logger("gold")

_SQL_DIR = Path(__file__).parent / "sql"
_CUSTOMER_CHANGES_VIEW = "customer_changes"


def _sql(name: str, **params: str) -> str:
    """Load a SQL file, filling in its `{placeholders}` when any are given."""
    text = (_SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
    return text.format(**params) if params else text


def _run_script(spark: SparkSession, name: str) -> None:
    """Run a SQL file that holds several statements separated by semicolons."""
    for statement in _sql(name).split(";\n"):
        if statement.strip():
            spark.sql(statement)


def _row_count(spark: SparkSession, table: str) -> int:
    return spark.table(table).count()


def _build_static(spark: SparkSession, name: str) -> int:
    """Create a dimension whose content never changes, if it is not there yet."""
    table = f"gold.{name}"
    if not table_exists(spark, table):
        spark.sql(_sql(name))
    return _row_count(spark, table)


def build_dim_date(spark: SparkSession, batch: Batch) -> int:
    return _build_static(spark, "dim_date")


def build_dim_payment_method(spark: SparkSession, batch: Batch) -> int:
    return _build_static(spark, "dim_payment_method")


def apply_customer_changes(spark: SparkSession, changes_view: str) -> None:
    """Apply one batch of customer versions to the SCD Type 2 dimension."""
    if not table_exists(spark, "gold.dim_customer"):
        _run_script(spark, "dim_customer_create")
    spark.sql(_sql("dim_customer_merge", changes=changes_view))


def build_dim_customer(spark: SparkSession, batch: Batch) -> int:
    """Merge the current state of every customer into the SCD Type 2 dimension.

    Reading all of `silver.customers`, not only this batch, makes the build
    self-healing: a batch that was skipped is caught up by the next one.
    """
    spark.sql(
        """
        SELECT customer_id, city, state, pincode, loyalty_tier,
               CAST(created_at AS DATE) AS customer_since, updated_at
        FROM silver.customers
        """
    ).createOrReplaceTempView(_CUSTOMER_CHANGES_VIEW)
    apply_customer_changes(spark, _CUSTOMER_CHANGES_VIEW)
    return _row_count(spark, "gold.dim_customer")


def merge_type1(
    spark: SparkSession, target: str, surrogate_key: str, business_key: str, source_sql: str
) -> None:
    """Upsert a Type 1 dimension: overwrite changed attributes, add new members.

    New members are numbered on their own, continuing after the current maximum
    key. Existing members are numbered too, but that value is never used: their
    keys never change, which keeps the fact rows pointing at them valid.
    """
    source = spark.sql(source_sql)
    attributes = [column for column in source.columns if column != business_key]
    source.createOrReplaceTempView("dim_source")
    key_offset = scalar(
        spark, f"SELECT greatest(coalesce(max({surrogate_key}), 0), 0) FROM {target}"
    )

    is_changed = " OR ".join(f"NOT (target.{c} <=> source.{c})" for c in attributes)
    assignments = ", ".join(f"{c} = source.{c}" for c in attributes)
    columns = [surrogate_key, business_key, *attributes]
    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING (
            SELECT dim_source.*,
                   {key_offset} + row_number() OVER (
                       PARTITION BY existing.{business_key} IS NULL
                       ORDER BY dim_source.{business_key}
                   ) AS {surrogate_key}
            FROM dim_source
            LEFT JOIN {target} AS existing
                   ON existing.{business_key} = dim_source.{business_key}
        ) AS source
        ON target.{business_key} = source.{business_key}
        WHEN MATCHED AND ({is_changed}) THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(columns)})
            VALUES ({", ".join(f"source.{c}" for c in columns)})
        """
    )


def _build_type1(spark: SparkSession, name: str, surrogate_key: str, business_key: str) -> int:
    table = f"gold.{name}"
    if not table_exists(spark, table):
        _run_script(spark, f"{name}_create")
    merge_type1(spark, table, surrogate_key, business_key, _sql(f"{name}_source"))
    return _row_count(spark, table)


def build_dim_seller(spark: SparkSession, batch: Batch) -> int:
    return _build_type1(spark, "dim_seller", "seller_sk", "seller_id")


def build_dim_product(spark: SparkSession, batch: Batch) -> int:
    return _build_type1(spark, "dim_product", "product_sk", "product_id")


def affected_order_months(spark: SparkSession, batch: Batch) -> list[str]:
    """Order months with at least one order, item or payment changed by this batch."""
    rows = spark.sql(_sql("affected_months", batch_id=batch.batch_id)).collect()
    return sorted(row.order_month.isoformat() for row in rows)


def _build_fact(spark: SparkSession, name: str, batch: Batch) -> int:
    """Rebuild the fact partitions (order months) this batch touched.

    Whole partitions are replaced in one Iceberg commit, so the build can be
    repeated safely and readers never see a half-written month.
    """
    table = f"gold.{name}"
    months = affected_order_months(spark, batch)
    if not months:
        log.info("gold_fact_unchanged", extra={"table": name, "batch_id": batch.batch_id})
        return _row_count(spark, table) if table_exists(spark, table) else 0

    rebuilt = spark.sql(_sql(name, order_months=", ".join(f"DATE '{month}'" for month in months)))
    if not table_exists(spark, table):
        create_table(rebuilt, table, F.months("order_ts"))
    rebuilt.writeTo(table).overwritePartitions()
    log.info(
        "gold_fact_partitions_rebuilt",
        extra={"table": name, "batch_id": batch.batch_id, "order_months": months},
    )
    return _row_count(spark, table)


def build_fact_order_items(spark: SparkSession, batch: Batch) -> int:
    return _build_fact(spark, "fact_order_items", batch)


def build_fact_payments(spark: SparkSession, batch: Batch) -> int:
    return _build_fact(spark, "fact_payments", batch)


def build_agg_daily_sales(spark: SparkSession, batch: Batch) -> int:
    """Recompute the daily aggregate from the fact table and swap it in atomically."""
    spark.sql(_sql("agg_daily_sales")).writeTo("gold.agg_daily_sales").using(
        "iceberg"
    ).createOrReplace()
    return _row_count(spark, "gold.agg_daily_sales")


# Build order: dimensions first, because facts look up their surrogate keys.
GOLD_MODELS: dict[str, Callable[[SparkSession, Batch], int]] = {
    "dim_date": build_dim_date,
    "dim_payment_method": build_dim_payment_method,
    "dim_customer": build_dim_customer,
    "dim_seller": build_dim_seller,
    "dim_product": build_dim_product,
    "fact_order_items": build_fact_order_items,
    "fact_payments": build_fact_payments,
    "agg_daily_sales": build_agg_daily_sales,
}
