"""The check types of the data quality framework.

Row-level checks return the rows that break the rule; the observed value is
the share of checked rows that fail, compared against the check's threshold.
Table-level checks (freshness, reconciliation) compute their observed value
directly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812

from marketplace.batch import Batch
from marketplace.dq.model import CheckSpec
from marketplace.lake import epoch_micros_to_utc, scalar, sql_timestamp

RECORD_KEY = "record_key"

# Row counts of the tables already counted during one run, keyed by table and
# filter. Many checks share a table, and counting it once per check would
# dominate the run time.
RowCounts = dict[tuple[str, str | None], int]


@dataclass(frozen=True)
class Evaluation:
    """Raw outcome of a check before the threshold is applied."""

    observed_value: float | None
    failed_rows: int | None = None
    rows_checked: int | None = None
    failing: DataFrame | None = None
    details: str = ""


def render(template: str, batch: Batch) -> str:
    """Fill batch placeholders into a filter or SQL snippet from the configuration."""
    return template.format(
        batch_id=batch.batch_id, high_watermark=sql_timestamp(batch.high_watermark)
    )


def scope(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    """The rows a check looks at: the table, narrowed by the table-level filter."""
    df = spark.table(spec.relation)
    return df.where(render(spec.row_filter, batch)) if spec.row_filter else df


def _record_key(spec: CheckSpec) -> Column:
    return F.concat_ws("|", *(F.col(column).cast("string") for column in spec.key)).alias(
        RECORD_KEY
    )


def _not_null(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    return scope(spark, spec, batch).where(F.col(spec.params["column"]).isNull())


def _unique(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    columns = spec.params["columns"]
    occurrences = F.count(F.lit(1)).over(Window.partitionBy(*columns))
    return (
        scope(spark, spec, batch)
        .withColumn("_occurrences", occurrences)
        .where(F.col("_occurrences") > 1)
    )


def _accepted_values(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    column = F.col(spec.params["column"])
    return scope(spark, spec, batch).where(column.isNotNull() & ~column.isin(spec.params["values"]))


def _relationships(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    """Rows whose non-null foreign key has no match in the parent table."""
    child = scope(spark, spec, batch).where(F.col(spec.params["column"]).isNotNull())
    parent_keys = (
        spark.table(spec.params["to_table"])
        .select(F.col(spec.params["to_column"]).alias("_parent_key"))
        .distinct()
    )
    return child.join(
        parent_keys, child[spec.params["column"]] == parent_keys["_parent_key"], "left_anti"
    )


def _range(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    column = F.col(spec.params["column"])
    outside = F.lit(False)
    if "min" in spec.params:
        outside = outside | (column < spec.params["min"])
    if "max" in spec.params:
        outside = outside | (column > spec.params["max"])
    return scope(spark, spec, batch).where(outside)


def _regex(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    column = F.col(spec.params["column"])
    return scope(spark, spec, batch).where(
        column.isNotNull() & ~column.rlike(spec.params["pattern"])
    )


def _custom_sql(spark: SparkSession, spec: CheckSpec, batch: Batch) -> DataFrame:
    """A query written in the configuration; it must return failing rows as `record_key`."""
    return spark.sql(render(spec.params["sql"], batch))


_ROW_CHECKS: dict[str, Callable[[SparkSession, CheckSpec, Batch], DataFrame]] = {
    "not_null": _not_null,
    "unique": _unique,
    "accepted_values": _accepted_values,
    "relationships": _relationships,
    "range": _range,
    "regex": _regex,
    "custom_sql": _custom_sql,
}


def _rows_in_scope(spark: SparkSession, spec: CheckSpec, batch: Batch, counts: RowCounts) -> int:
    key = (spec.relation, spec.row_filter)
    if key not in counts:
        counts[key] = scope(spark, spec, batch).count()
    return counts[key]


def _evaluate_row_check(
    spark: SparkSession, spec: CheckSpec, batch: Batch, counts: RowCounts
) -> Evaluation:
    failing = _ROW_CHECKS[spec.check_type](spark, spec, batch)
    if RECORD_KEY not in failing.columns:
        failing = failing.select(_record_key(spec))
    failing = failing.select(RECORD_KEY)
    failed_rows = failing.count()
    rows_checked = _rows_in_scope(spark, spec, batch, counts)
    share = failed_rows / rows_checked if rows_checked else float(failed_rows > 0)
    return Evaluation(
        observed_value=share,
        failed_rows=failed_rows,
        rows_checked=rows_checked,
        failing=failing,
        details=f"{failed_rows} of {rows_checked} rows fail",
    )


def _evaluate_freshness(
    spark: SparkSession, spec: CheckSpec, batch: Batch, counts: RowCounts
) -> Evaluation:
    """Hours between the end of the batch and the newest value of a timestamp column.

    Freshness is measured against the batch's logical time, not the wall
    clock, so a backfill of an old date is not reported as stale.
    """
    column = spec.params["column"]
    newest = epoch_micros_to_utc(
        scope(spark, spec, batch).agg(F.unix_micros(F.max(column))).collect()[0][0]
    )
    if newest is None:
        return Evaluation(observed_value=None, details=f"no rows to measure {column} on")
    lag_hours = (batch.high_watermark - newest).total_seconds() / 3600
    return Evaluation(
        observed_value=round(lag_hours, 3),
        details=f"newest {column} is {newest.isoformat()}",
    )


def _evaluate_reconciliation(
    spark: SparkSession, spec: CheckSpec, batch: Batch, counts: RowCounts
) -> Evaluation:
    """Relative difference between two counts (or sums) computed by two queries."""
    left = float(scalar(spark, render(spec.params["left_sql"], batch)) or 0)
    right = float(scalar(spark, render(spec.params["right_sql"], batch)) or 0)
    difference = abs(left - right) / max(abs(left), 1.0)
    return Evaluation(observed_value=difference, details=f"left={left:g} right={right:g}")


CHECK_TYPES: dict[str, Callable[[SparkSession, CheckSpec, Batch, RowCounts], Evaluation]] = {
    **dict.fromkeys(_ROW_CHECKS, _evaluate_row_check),
    "freshness": _evaluate_freshness,
    "row_count_reconciliation": _evaluate_reconciliation,
}


def evaluate(
    spark: SparkSession, spec: CheckSpec, batch: Batch, counts: RowCounts | None = None
) -> Evaluation:
    """Run one check and return its raw outcome.

    Pass the same `counts` dictionary to every check of a run to count each
    table only once.
    """
    return CHECK_TYPES[spec.check_type](spark, spec, batch, {} if counts is None else counts)
