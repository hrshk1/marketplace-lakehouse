"""Readers that pull a window of changed rows out of each operational source."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from pymongo import MongoClient
from pyspark.sql import DataFrame, SparkSession

from marketplace.config import Settings
from marketplace.generator.stores import MemoryStore

WATERMARK_COLUMN = "updated_at"
POSTGRES = "postgres"
MONGODB = "mongodb"

# Documents are landed as raw JSON so Bronze stays faithful to the source and
# never breaks when a document gains or loses a field.
DOCUMENT_SCHEMA = "doc_id string, payload string, created_at timestamp, updated_at timestamp"


@dataclass(frozen=True)
class SourceTable:
    """A table or collection that is extracted to Bronze."""

    name: str
    system: str
    primary_key: str


SOURCE_TABLES: tuple[SourceTable, ...] = (
    SourceTable("categories", POSTGRES, "category_id"),
    SourceTable("sellers", POSTGRES, "seller_id"),
    SourceTable("customers", POSTGRES, "customer_id"),
    SourceTable("products", POSTGRES, "product_id"),
    SourceTable("orders", POSTGRES, "order_id"),
    SourceTable("order_items", POSTGRES, "order_item_id"),
    SourceTable("payments", POSTGRES, "payment_id"),
    SourceTable("reviews", MONGODB, "doc_id"),
    SourceTable("product_attributes", MONGODB, "doc_id"),
)


def source_table(name: str) -> SourceTable:
    """Look up a source table by name."""
    for table in SOURCE_TABLES:
        if table.name == name:
            return table
    raise KeyError(f"Unknown source table: {name!r}")


class SourceReader(Protocol):
    """Reads changed rows from the sources as Spark DataFrames."""

    def read_window(self, table: SourceTable, start: datetime | None, end: datetime) -> DataFrame:
        """Rows with `start <= updated_at < end`; `start=None` means from the beginning."""

    def count_until(self, table: SourceTable, end: datetime) -> int:
        """Number of source rows with `updated_at < end`, for reconciliation."""


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def documents_to_rows(documents: Iterable[Mapping[str, Any]]) -> list[tuple]:
    """Turn source documents into (doc_id, raw JSON, created_at, updated_at) rows."""
    return [
        (
            str(document["_id"]),
            json.dumps(document, default=_json_default, sort_keys=True),
            document["created_at"],
            document["updated_at"],
        )
        for document in documents
    ]


def _in_window(value: datetime, start: datetime | None, end: datetime) -> bool:
    return (start is None or value >= start) and value < end


class PostgresMongoReader:
    """Reads PostgreSQL tables over JDBC and MongoDB collections with pymongo."""

    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self._spark = spark
        self._settings = settings

    def read_window(self, table: SourceTable, start: datetime | None, end: datetime) -> DataFrame:
        if table.system == MONGODB:
            with self._mongo() as client:
                documents = client[self._settings.mongo_db][table.name].find(
                    {WATERMARK_COLUMN: self._mongo_filter(start, end)}
                )
                rows = documents_to_rows(documents)
            return self._spark.createDataFrame(rows, schema=DOCUMENT_SCHEMA)
        return self._jdbc(f"SELECT * FROM oltp.{table.name} WHERE {self._sql_filter(start, end)}")

    def count_until(self, table: SourceTable, end: datetime) -> int:
        if table.system == MONGODB:
            with self._mongo() as client:
                return client[self._settings.mongo_db][table.name].count_documents(
                    {WATERMARK_COLUMN: self._mongo_filter(None, end)}
                )
        query = f"SELECT count(*) AS n FROM oltp.{table.name} WHERE {self._sql_filter(None, end)}"
        return self._jdbc(query).collect()[0]["n"]

    def _mongo(self) -> MongoClient:
        return MongoClient(self._settings.mongo_uri, tz_aware=True)

    @staticmethod
    def _mongo_filter(start: datetime | None, end: datetime) -> dict[str, datetime]:
        bounds = {"$lt": end}
        if start is not None:
            bounds["$gte"] = start
        return bounds

    @staticmethod
    def _sql_filter(start: datetime | None, end: datetime) -> str:
        clauses = [f"{WATERMARK_COLUMN} < TIMESTAMPTZ '{end.isoformat()}'"]
        if start is not None:
            clauses.append(f"{WATERMARK_COLUMN} >= TIMESTAMPTZ '{start.isoformat()}'")
        return " AND ".join(clauses)

    def _jdbc(self, query: str) -> DataFrame:
        return (
            self._spark.read.format("jdbc")
            .option("url", self._settings.oltp_jdbc_url)
            .option("driver", "org.postgresql.Driver")
            .option("user", self._settings.oltp_pg_user)
            .option("password", self._settings.oltp_pg_password)
            .option("fetchsize", "10000")
            .option("query", query)
            .load()
        )


class MemoryReader:
    """Reads from the generator's in-memory store; used by container-free tests."""

    def __init__(self, spark: SparkSession, store: MemoryStore) -> None:
        self._spark = spark
        self._store = store

    def read_window(self, table: SourceTable, start: datetime | None, end: datetime) -> DataFrame:
        rows = [
            row
            for row in self._store.tables[table.name].values()
            if _in_window(row[WATERMARK_COLUMN], start, end)
        ]
        if table.system == MONGODB:
            return self._spark.createDataFrame(documents_to_rows(rows), schema=DOCUMENT_SCHEMA)
        columns = self._store.columns[table.name]
        schema = ", ".join(f"{name} {spark_type}" for name, spark_type in columns.items())
        return self._spark.createDataFrame(
            [tuple(row.get(name) for name in columns) for row in rows], schema=schema
        )

    def count_until(self, table: SourceTable, end: datetime) -> int:
        return sum(
            1 for row in self._store.tables[table.name].values() if row[WATERMARK_COLUMN] < end
        )
