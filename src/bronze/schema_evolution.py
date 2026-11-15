"""Detect and apply source schema changes on Bronze tables.

New columns are additive and safe, so they are applied automatically and
logged. A removed column or a changed type would silently corrupt history if
coerced, so those stop the batch instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from marketplace.lake import append_rows, table_exists

SCHEMA_CHANGES_TABLE = "dq.schema_changes"
_SCHEMA_CHANGES_DDL = (
    "table_name string, column_name string, change_type string, old_type string, "
    "new_type string, batch_id string, detected_at timestamp"
)

ADDED = "column_added"
REMOVED = "column_removed"
RETYPED = "type_changed"


class SchemaDriftError(RuntimeError):
    """A source schema change that cannot be applied without losing meaning."""


@dataclass(frozen=True)
class RetypedColumn:
    name: str
    old_type: str
    new_type: str


@dataclass(frozen=True)
class SchemaDiff:
    """Difference between a Bronze table and the incoming source schema."""

    added: list[StructField] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    retyped: list[RetypedColumn] = field(default_factory=list)

    @property
    def is_breaking(self) -> bool:
        return bool(self.removed or self.retyped)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.retyped)


def ensure_table(spark: SparkSession) -> None:
    """Create the schema change log up front, so it can be published even when empty."""
    if not table_exists(spark, SCHEMA_CHANGES_TABLE):
        spark.sql(f"CREATE TABLE {SCHEMA_CHANGES_TABLE} ({_SCHEMA_CHANGES_DDL}) USING iceberg")


def diff_schemas(existing: StructType, incoming: StructType) -> SchemaDiff:
    """Compare source columns by name; pipeline metadata columns (`_...`) are ignored."""
    current = {f.name: f for f in existing.fields if not f.name.startswith("_")}
    arriving = {f.name: f for f in incoming.fields if not f.name.startswith("_")}
    return SchemaDiff(
        added=[f for name, f in arriving.items() if name not in current],
        removed=[name for name in current if name not in arriving],
        retyped=[
            RetypedColumn(name, current[name].dataType.simpleString(), f.dataType.simpleString())
            for name, f in arriving.items()
            if name in current and current[name].dataType != f.dataType
        ],
    )


def apply_schema_changes(spark: SparkSession, table: str, diff: SchemaDiff, batch_id: str) -> None:
    """Log every detected change, add new columns, and fail on breaking changes."""
    if diff.is_empty:
        return

    detected_at = datetime.now(UTC)
    source_name = table.split(".")[-1]
    changes = [
        *((f.name, ADDED, None, f.dataType.simpleString()) for f in diff.added),
        *((name, REMOVED, None, None) for name in diff.removed),
        *((c.name, RETYPED, c.old_type, c.new_type) for c in diff.retyped),
    ]
    append_rows(
        spark,
        SCHEMA_CHANGES_TABLE,
        _SCHEMA_CHANGES_DDL,
        [
            (source_name, column, kind, old, new, batch_id, detected_at)
            for column, kind, old, new in changes
        ],
    )

    if diff.is_breaking:
        details = [f"column removed: {name}" for name in diff.removed]
        details += [f"type of {c.name} changed {c.old_type} -> {c.new_type}" for c in diff.retyped]
        raise SchemaDriftError(
            f"Breaking schema change on {table} ({'; '.join(details)}). The batch was not loaded. "
            "Follow the schema change runbook in docs/OPERATIONS.md."
        )

    for new_column in diff.added:
        spark.sql(
            f"ALTER TABLE {table} ADD COLUMN {new_column.name} {new_column.dataType.simpleString()}"
        )
