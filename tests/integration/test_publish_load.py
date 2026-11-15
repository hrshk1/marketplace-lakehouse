"""Bulk load of exported Parquet into the serving warehouse, with an atomic swap."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pytest

from marketplace.publish.load import UnsupportedTypeError, load_table, postgres_type
from marketplace.publish.tables import PublishedTable

SCHEMA = "dw_load_test"
TABLE = PublishedTable(
    source="gold.sales",
    target="sales",
    group="analytics",
    primary_key=("sale_id",),
    indexes=(("sold_on",), ("city", "sold_on")),
)


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (pa.int32(), "integer"),
        (pa.int64(), "bigint"),
        (pa.string(), "text"),
        (pa.bool_(), "boolean"),
        (pa.float64(), "double precision"),
        (pa.decimal128(14, 2), "numeric(14, 2)"),
        (pa.date32(), "date"),
        (pa.timestamp("us", tz="UTC"), "timestamptz"),
        (pa.timestamp("us"), "timestamp"),
    ],
)
def test_arrow_types_map_to_postgres_types(arrow_type, expected):
    assert postgres_type(arrow_type) == expected


def test_a_type_without_a_mapping_is_rejected():
    with pytest.raises(UnsupportedTypeError, match="map<string, string>"):
        postgres_type(pa.map_(pa.string(), pa.string()))


def write_export(folder: Path, rows: list[dict]) -> pads.Dataset:
    schema = pa.schema(
        [
            ("sale_id", pa.int64()),
            ("city", pa.string()),
            ("amount", pa.decimal128(12, 2)),
            ("sold_on", pa.date32()),
            ("sold_at", pa.timestamp("us", tz="UTC")),
            ("is_gift", pa.bool_()),
        ]
    )
    folder.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), folder / "part-0.parquet")
    return pads.dataset(folder, format="parquet")


FIRST_LOAD = [
    {
        "sale_id": 1,
        "city": 'Pune, "Camp"',
        "amount": Decimal("1999.50"),
        "sold_on": date(2026, 6, 30),
        "sold_at": datetime(2026, 6, 30, 18, 30, tzinfo=UTC),
        "is_gift": True,
    },
    {"sale_id": 2, "city": "", "amount": None, "sold_on": None, "sold_at": None, "is_gift": False},
    {
        "sale_id": 3,
        "city": None,
        "amount": Decimal("0.01"),
        "sold_on": date(2026, 7, 1),
        "sold_at": None,
        "is_gift": None,
    },
]


@pytest.fixture
def connection(warehouse_dsn: str):
    with psycopg.connect(warehouse_dsn) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        conn.commit()
        yield conn
        conn.rollback()
        conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        conn.commit()


def index_names(conn: psycopg.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s ORDER BY 1", (SCHEMA,)
    ).fetchall()
    return [name for (name,) in rows]


@pytest.mark.postgres
def test_load_preserves_values_nulls_and_empty_strings(connection, tmp_path):
    loaded = load_table(connection, SCHEMA, TABLE, write_export(tmp_path / "v1", FIRST_LOAD))

    assert loaded == 3
    assert connection.execute(f"SELECT * FROM {SCHEMA}.sales ORDER BY sale_id").fetchall() == [
        (
            1,
            'Pune, "Camp"',
            Decimal("1999.50"),
            date(2026, 6, 30),
            datetime(2026, 6, 30, 18, 30, tzinfo=UTC),
            True,
        ),
        (2, "", None, None, None, False),
        (3, None, Decimal("0.01"), date(2026, 7, 1), None, None),
    ]


@pytest.mark.postgres
def test_load_creates_typed_columns_primary_key_and_indexes(connection, tmp_path):
    load_table(connection, SCHEMA, TABLE, write_export(tmp_path / "v1", FIRST_LOAD))

    columns = connection.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = 'sales' ORDER BY ordinal_position",
        (SCHEMA,),
    ).fetchall()
    assert columns == [
        ("sale_id", "bigint"),
        ("city", "text"),
        ("amount", "numeric"),
        ("sold_on", "date"),
        ("sold_at", "timestamp with time zone"),
        ("is_gift", "boolean"),
    ]
    assert index_names(connection) == ["ix_sales_city_sold_on", "ix_sales_sold_on", "pk_sales"]


@pytest.mark.postgres
def test_reload_replaces_the_table_and_leaves_no_staging_objects(connection, tmp_path):
    load_table(connection, SCHEMA, TABLE, write_export(tmp_path / "v1", FIRST_LOAD))
    second = [{**FIRST_LOAD[0], "sale_id": 9, "city": "Delhi"}]

    load_table(connection, SCHEMA, TABLE, write_export(tmp_path / "v2", second))

    assert connection.execute(f"SELECT sale_id, city FROM {SCHEMA}.sales").fetchall() == [
        (9, "Delhi")
    ]
    tables = connection.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s", (SCHEMA,)
    ).fetchall()
    assert tables == [("sales",)]
    assert index_names(connection) == ["ix_sales_city_sold_on", "ix_sales_sold_on", "pk_sales"]


@pytest.mark.postgres
def test_failed_load_keeps_the_previous_table_intact(connection, tmp_path):
    load_table(connection, SCHEMA, TABLE, write_export(tmp_path / "v1", FIRST_LOAD))
    duplicate_keys = [FIRST_LOAD[0], FIRST_LOAD[0]]

    with pytest.raises(psycopg.errors.UniqueViolation):
        load_table(connection, SCHEMA, TABLE, write_export(tmp_path / "bad", duplicate_keys))

    assert connection.execute(f"SELECT count(*) FROM {SCHEMA}.sales").fetchone() == (3,)
