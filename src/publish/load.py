"""Bulk-load exported Parquet files into the serving warehouse (Postgres).

Each table is loaded into a staging table with COPY, indexed, and then swapped
in for the live table inside one transaction. Readers see either the old table
or the new one, never a half-loaded state.
"""

from __future__ import annotations

import io
from urllib.parse import urlparse

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.fs as pafs
from psycopg import sql

from marketplace.config import Settings
from marketplace.logging_setup import get_logger
from marketplace.publish.tables import PublishedTable

log = get_logger("publish")

_STAGING_SUFFIX = "__staging"
_S3A_PREFIX = "s3a://"
_COPY_BATCH_ROWS = 50_000


class UnsupportedTypeError(TypeError):
    """An exported column has a type with no warehouse mapping."""


def postgres_type(arrow_type: pa.DataType) -> str:
    """Map an Arrow column type to the Postgres type used in the warehouse."""
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return "smallint"
    if pa.types.is_int32(arrow_type):
        return "integer"
    if pa.types.is_int64(arrow_type):
        return "bigint"
    if pa.types.is_floating(arrow_type):
        return "double precision"
    if pa.types.is_decimal(arrow_type):
        return f"numeric({arrow_type.precision}, {arrow_type.scale})"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "text"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_timestamp(arrow_type):
        return "timestamptz" if arrow_type.tz else "timestamp"
    raise UnsupportedTypeError(f"No Postgres mapping for Arrow type {arrow_type}")


def open_export(settings: Settings, path: str) -> pads.Dataset:
    """Open a Parquet export folder on the object store or the local file system."""
    if path.startswith(_S3A_PREFIX):
        endpoint = urlparse(settings.s3_endpoint)
        filesystem = pafs.S3FileSystem(
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            endpoint_override=endpoint.netloc,
            scheme=endpoint.scheme,
        )
        return pads.dataset(path[len(_S3A_PREFIX) :], filesystem=filesystem, format="parquet")
    return pads.dataset(path, format="parquet")


def _index_name(table: str, columns: tuple[str, ...], suffix: str = "") -> str:
    return f"ix_{table}_{'_'.join(columns)}{suffix}"


def _column_list(columns: tuple[str, ...]) -> sql.Composable:
    return sql.SQL(", ").join(map(sql.Identifier, columns))


def _create_staging(
    cursor: psycopg.Cursor, schema: str, staging: str, arrow_schema: pa.Schema
) -> None:
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(field.name), sql.SQL(postgres_type(field.type)))
        for field in arrow_schema
    )
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(*map(sql.Identifier, (schema, staging)))
    )
    cursor.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(staging), columns
        )
    )


def _copy_rows(cursor: psycopg.Cursor, schema: str, staging: str, dataset: pads.Dataset) -> int:
    """Stream the export into the staging table as CSV through COPY."""
    statement = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv)").format(
        sql.Identifier(schema), sql.Identifier(staging), _column_list(tuple(dataset.schema.names))
    )
    options = pacsv.WriteOptions(include_header=False, quoting_style="needed")
    rows = 0
    with cursor.copy(statement) as copy:
        for record_batch in dataset.to_batches(batch_size=_COPY_BATCH_ROWS):
            buffer = io.BytesIO()
            pacsv.write_csv(record_batch, buffer, write_options=options)
            copy.write(buffer.getvalue())
            rows += record_batch.num_rows
    return rows


def _add_keys(cursor: psycopg.Cursor, schema: str, staging: str, table: PublishedTable) -> None:
    """Build the primary key and indexes on staging, under temporary names."""
    qualified = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(staging))
    if table.primary_key:
        cursor.execute(
            sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                qualified,
                sql.Identifier(f"pk_{table.target}{_STAGING_SUFFIX}"),
                _column_list(table.primary_key),
            )
        )
    for columns in table.indexes:
        cursor.execute(
            sql.SQL("CREATE INDEX {} ON {} ({})").format(
                sql.Identifier(_index_name(table.target, columns, _STAGING_SUFFIX)),
                qualified,
                _column_list(columns),
            )
        )
    cursor.execute(sql.SQL("ANALYZE {}").format(qualified))


def _swap(cursor: psycopg.Cursor, schema: str, staging: str, table: PublishedTable) -> None:
    """Replace the live table with staging and give keys and indexes their final names."""
    schema_id = sql.Identifier(schema)
    cursor.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(schema_id, sql.Identifier(table.target))
    )
    cursor.execute(
        sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
            schema_id, sql.Identifier(staging), sql.Identifier(table.target)
        )
    )
    if table.primary_key:
        cursor.execute(
            sql.SQL("ALTER TABLE {}.{} RENAME CONSTRAINT {} TO {}").format(
                schema_id,
                sql.Identifier(table.target),
                sql.Identifier(f"pk_{table.target}{_STAGING_SUFFIX}"),
                sql.Identifier(f"pk_{table.target}"),
            )
        )
    for columns in table.indexes:
        cursor.execute(
            sql.SQL("ALTER INDEX {}.{} RENAME TO {}").format(
                schema_id,
                sql.Identifier(_index_name(table.target, columns, _STAGING_SUFFIX)),
                sql.Identifier(_index_name(table.target, columns)),
            )
        )


def load_table(
    connection: psycopg.Connection, schema: str, table: PublishedTable, dataset: pads.Dataset
) -> int:
    """Load one exported table and swap it in atomically; returns the rows loaded."""
    staging = f"{table.target}{_STAGING_SUFFIX}"
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        _create_staging(cursor, schema, staging, dataset.schema)
        rows = _copy_rows(cursor, schema, staging, dataset)
        _add_keys(cursor, schema, staging, table)
        _swap(cursor, schema, staging, table)
    log.info("table_loaded", extra={"table": f"{schema}.{table.target}", "rows": rows})
    return rows
