"""Where the generator writes: real sources, or an in-memory stand-in for tests."""

from __future__ import annotations

from datetime import datetime
from importlib import resources
from typing import Any, Protocol

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from pymongo import MongoClient, UpdateOne

from marketplace.config import Settings
from marketplace.generator import schema

Row = dict[str, Any]

_SPARK_TO_POSTGRES_TYPE = {"boolean": "boolean"}

#Any storage class that wants to work with MarketplaceSimulator MUST provide these 5 functions. Because of this protocol, MarketplaceSimulator doesn't care whether you save rows to a real PostgreSQL database, MongoDB, or a plain Python dictionary
class SourceStore(Protocol):
    """The operations the simulator needs from the operational sources."""

    def reset(self) -> None:
        """Drop all data and recreate empty tables and collections."""

    def insert(self, table: str, rows: list[Row]) -> None:
        """Insert new rows; every row must carry all columns of the table."""

    def update(self, table: str, rows: list[Row]) -> None:
        """Update rows by primary key; each row holds the key and the changed columns."""

    def fetch(
        self,
        table: str,
        columns: list[str],
        since_column: str | None = None,
        since: datetime | None = None,
    ) -> list[Row]:
        """Read selected columns, optionally only rows where `since_column >= since`."""

    def add_column(self, table: str, column: str, spark_type: str) -> None:
        """Add a nullable column to a table (the schema-drift event)."""

# this we have kept for tests
class MemoryStore:
    """Dictionary-backed store. Mirrors source behaviour closely enough for tests."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[Any, Row]] = {}
        self.columns: dict[str, dict[str, str]] = {}
        self.reset()

    def reset(self) -> None:
        self.tables = {name: {} for name in (*schema.POSTGRES_TABLES, *schema.MONGO_COLLECTIONS)}
        self.columns = {name: dict(table.columns) for name, table in schema.POSTGRES_TABLES.items()}

    def insert(self, table: str, rows: list[Row]) -> None:
        key = schema.primary_key(table)
        target = self.tables[table]
        for row in rows:
            if row[key] in target:
                raise KeyError(f"Duplicate primary key {row[key]!r} in {table}")
            target[row[key]] = dict(row)

    def update(self, table: str, rows: list[Row]) -> None:
        key = schema.primary_key(table)
        for row in rows:
            self.tables[table][row[key]].update(row)

    def fetch(
        self,
        table: str,
        columns: list[str],
        since_column: str | None = None,
        since: datetime | None = None,
    ) -> list[Row]:
        rows = self.tables[table].values()
        if since_column is not None and since is not None:
            rows = [row for row in rows if row[since_column] >= since]
        return [{column: row.get(column) for column in columns} for row in rows]

    def add_column(self, table: str, column: str, spark_type: str) -> None:
        self.columns[table][column] = spark_type

#the real store
class PostgresMongoStore:
    """Writes relational tables to PostgreSQL (`oltp` schema) and documents to MongoDB."""

    def __init__(self, settings: Settings) -> None:
        self._pg = psycopg.connect(settings.oltp_conninfo, autocommit=False)
        self._mongo_client: MongoClient = MongoClient(settings.mongo_uri, tz_aware=True)
        self._mongo = self._mongo_client[settings.mongo_db]

    def close(self) -> None:
        self._pg.close()
        self._mongo_client.close()

    def __enter__(self) -> PostgresMongoStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def reset(self) -> None:
        ddl = resources.files("marketplace.generator").joinpath("sql/oltp_schema.sql").read_text()
        with self._pg.cursor() as cursor:
            cursor.execute(ddl)
        self._pg.commit()
        for collection in schema.MONGO_COLLECTIONS:
            self._mongo.drop_collection(collection)
            self._mongo[collection].create_index("updated_at")

    def insert(self, table: str, rows: list[Row]) -> None:
        if not rows:
            return
        if table in schema.MONGO_COLLECTIONS:
            self._mongo[table].insert_many(rows, ordered=False)
            return
        columns = list(rows[0])
        statement = sql.SQL("COPY oltp.{} ({}) FROM STDIN").format(
            sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns))
        )
        with self._pg.cursor() as cursor, cursor.copy(statement) as copy:
            for row in rows:
                copy.write_row([row[column] for column in columns])
        self._pg.commit()

    def update(self, table: str, rows: list[Row]) -> None:
        if not rows:
            return
        key = schema.primary_key(table)
        if table in schema.MONGO_COLLECTIONS:
            self._mongo[table].bulk_write(
                [
                    UpdateOne(
                        {key: row[key]},
                        {"$set": {name: value for name, value in row.items() if name != key}},
                    )
                    for row in rows
                ],
                ordered=False,
            )
            return
        changed = [column for column in rows[0] if column != key]
        statement = sql.SQL("UPDATE oltp.{} SET {} WHERE {} = {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(
                sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder(column))
                for column in changed
            ),
            sql.Identifier(key),
            sql.Placeholder(key),
        )
        with self._pg.cursor() as cursor:
            cursor.executemany(statement, rows)
        self._pg.commit()

    def fetch(
        self,
        table: str,
        columns: list[str],
        since_column: str | None = None,
        since: datetime | None = None,
    ) -> list[Row]:
        statement = sql.SQL("SELECT {} FROM oltp.{}").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(table)
        )
        params: tuple[datetime, ...] = ()
        if since_column is not None and since is not None:
            statement += sql.SQL(" WHERE {} >= %s").format(sql.Identifier(since_column))
            params = (since,)
        with self._pg.cursor(row_factory=dict_row) as cursor:
            cursor.execute(statement, params)
            rows = cursor.fetchall()
        self._pg.commit()
        return rows

    def add_column(self, table: str, column: str, spark_type: str) -> None:
        statement = sql.SQL("ALTER TABLE oltp.{} ADD COLUMN {} {}").format(
            sql.Identifier(table),
            sql.Identifier(column),
            sql.SQL(_SPARK_TO_POSTGRES_TYPE[spark_type]),
        )
        with self._pg.cursor() as cursor:
            cursor.execute(statement)
        self._pg.commit()
