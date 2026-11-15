"""Build one Silver table from the current batch of its Bronze table."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from marketplace.batch import Batch
from marketplace.bronze.extract import bronze_table
from marketplace.lake import create_table, table_exists
from marketplace.logging_setup import get_logger
from marketplace.silver.reference import register_reference_views
from marketplace.silver.specs import LATEST_VIEW, Rule, SilverTable

log = get_logger("silver")

_HELPER_PREFIX = "_chk_"
_LINEAGE_COLUMNS = ("_record_key", "_payload")
_RULE_ID = "_rule_id"


@dataclass(frozen=True)
class SilverResult:
    """Row counts of one Silver build."""

    table: str
    rows_in: int
    rows_accepted: int
    rows_quarantined: int


def _latest_versions(spark: SparkSession, spec: SilverTable, batch: Batch) -> DataFrame:
    """The newest Bronze version of each row in this batch, with its raw payload.

    The payload leaves out `_ingested_at`, the one column that differs between
    two loads of the same batch, so a re-run quarantines byte-identical rows.
    """
    source = bronze_table(spec.name)
    payload_columns = ", ".join(c for c in spark.table(source).columns if c != "_ingested_at")
    ranked = spark.sql(
        f"""
        SELECT *,
               CAST({spec.source_key} AS STRING) AS _record_key,
               to_json(struct({payload_columns})) AS _payload,
               row_number() OVER (
                   PARTITION BY {spec.source_key}
                   ORDER BY updated_at DESC, _ingested_at DESC
               ) AS _version_rank
        FROM {source}
        WHERE _batch_id = '{batch.batch_id}'
        """
    )
    return ranked.where(F.col("_version_rank") == 1).drop("_version_rank")


def _first_matching_rule(rules: tuple[Rule, ...]) -> str:
    """SQL expression yielding the id of the first rule a row breaks, or NULL."""
    if not rules:
        return "CAST(NULL AS STRING)"
    branches = " ".join(f"WHEN {rule.condition} THEN '{rule.rule_id}'" for rule in rules)
    return f"CASE {branches} END"


def _write_quarantine(
    spark: SparkSession, spec: SilverTable, rejected: DataFrame, batch: Batch
) -> None:
    reasons = spark.createDataFrame(
        [(rule.rule_id, rule.reason) for rule in spec.rules], "rule_id string, reject_reason string"
    )
    quarantined = rejected.join(F.broadcast(reasons), rejected[_RULE_ID] == reasons.rule_id).select(
        F.col("_record_key").alias("record_key"),
        "rule_id",
        "reject_reason",
        F.lit(batch.batch_id).alias("_batch_id"),
        F.col("_payload").alias("payload"),
        F.lit(datetime.now(UTC)).alias("quarantined_at"),
    )
    if not table_exists(spark, spec.quarantine):
        create_table(quarantined, spec.quarantine, F.col("_batch_id"))
    quarantined.writeTo(spec.quarantine).overwrite(F.col("_batch_id") == batch.batch_id)


def _merge_accepted(spark: SparkSession, spec: SilverTable, accepted: DataFrame) -> None:
    if not table_exists(spark, spec.target):
        create_table(accepted, spec.target)
    accepted.createOrReplaceTempView("silver_accepted")
    # Strictly newer only. An older version arriving late never overwrites a
    # newer one, and a version seen again (a re-run, or the lookback window
    # re-reading it) leaves the row and its `_batch_id` untouched.
    spark.sql(
        f"""
        MERGE INTO {spec.target} AS target
        USING silver_accepted AS source
        ON target.{spec.key} = source.{spec.key}
        WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def build_silver_table(spark: SparkSession, spec: SilverTable, batch: Batch) -> SilverResult:
    """Clean this batch's rows, quarantine rule breakers and merge the rest into Silver."""
    register_reference_views(spark)
    latest = _latest_versions(spark, spec, batch)
    latest.createOrReplaceTempView(LATEST_VIEW)

    staged = spark.sql(spec.staging_sql).withColumn(
        _RULE_ID, F.expr(_first_matching_rule(spec.rules))
    )
    staged.cache()
    try:
        internal = [
            column
            for column in staged.columns
            if column.startswith(_HELPER_PREFIX) or column in (*_LINEAGE_COLUMNS, _RULE_ID)
        ]
        accepted = staged.where(F.col(_RULE_ID).isNull()).drop(*internal)
        rejected = staged.where(F.col(_RULE_ID).isNotNull())

        rows_in = staged.count()
        rows_quarantined = rejected.count()
        _write_quarantine(spark, spec, rejected, batch)
        _merge_accepted(spark, spec, accepted)
    finally:
        staged.unpersist()

    result = SilverResult(spec.name, rows_in, rows_in - rows_quarantined, rows_quarantined)
    log.info(
        "silver_table_built",
        extra={
            "table": spec.name,
            "batch_id": batch.batch_id,
            "rows_in": result.rows_in,
            "rows_accepted": result.rows_accepted,
            "rows_quarantined": result.rows_quarantined,
        },
    )
    return result
