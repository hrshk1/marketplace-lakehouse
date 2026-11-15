"""Generator and extraction against real PostgreSQL and MongoDB.

Uses scratch databases (`TEST_PG_DSN` and `TEST_MONGO_URI`), never the stack's
own sources. The lakehouse side stays on the local file system.
"""

from __future__ import annotations

import dataclasses
import json
import os
from datetime import timedelta
from decimal import Decimal

import pytest
from psycopg.conninfo import conninfo_to_dict
from pyspark.sql.types import DecimalType, TimestampType

from marketplace.bronze.extract import extract_table
from marketplace.generator.ledger import Ledger
from marketplace.generator.model import PROFILES, GeneratorConfig
from marketplace.generator.simulator import MarketplaceSimulator
from marketplace.generator.stores import PostgresMongoStore
from marketplace.sources.readers import SOURCE_TABLES, PostgresMongoReader, source_table
from tests.helpers import batch, rows

pytestmark = [pytest.mark.spark, pytest.mark.postgres, pytest.mark.mongodb]

LOOKBACK = timedelta(minutes=60)
MONGO_TEST_DB = "marketplace_test"


@pytest.fixture(scope="module")
def source_settings(settings, warehouse_dsn, tmp_path_factory):
    mongo_uri = os.environ.get("TEST_MONGO_URI", "").strip()
    if not mongo_uri:
        pytest.skip("TEST_MONGO_URI is not set; skipping tests that need MongoDB")
    postgres = conninfo_to_dict(warehouse_dsn)
    return dataclasses.replace(
        settings,
        oltp_pg_host=postgres["host"],
        oltp_pg_port=int(postgres.get("port", 5432)),
        oltp_pg_db=postgres["dbname"],
        oltp_pg_user=postgres["user"],
        oltp_pg_password=postgres["password"],
        mongo_uri=mongo_uri,
        mongo_db=MONGO_TEST_DB,
        ledger_path=tmp_path_factory.mktemp("sources") / "ledger.sqlite",
    )


@pytest.fixture(scope="module")
def seeded(source_settings):
    """Small profile seeded into the scratch databases, plus the simulator to advance it."""
    config = GeneratorConfig.from_settings(source_settings, PROFILES["small"])
    with (
        PostgresMongoStore(source_settings) as store,
        Ledger(source_settings.ledger_path) as ledger,
    ):
        simulator = MarketplaceSimulator(store, ledger, config)
        summary = simulator.seed()
        yield store, simulator, summary


def test_seed_creates_constrained_tables_and_documents(seeded):
    store, _, summary = seeded

    for table in ("customers", "orders", "order_items", "payments"):
        assert len(store.fetch(table, [source_table(table).primary_key])) == summary.inserted[table]
    foreign_keys = store._pg.execute(
        "SELECT count(*) FROM information_schema.table_constraints "
        "WHERE table_schema = 'oltp' AND constraint_type = 'FOREIGN KEY'"
    ).fetchone()[0]
    store._pg.commit()
    assert foreign_keys == 7
    review = store._mongo["reviews"].find_one({"seller_reply": {"$ne": None}})
    assert set(review["seller_reply"]) == {"text", "replied_at"}


def test_every_source_table_lands_in_bronze_with_matching_counts(lake, seeded, source_settings):
    _, _, summary = seeded
    reader = PostgresMongoReader(lake, source_settings)

    for table in SOURCE_TABLES:
        result = extract_table(lake, reader, table, batch("2026-06-30"), LOOKBACK)
        assert result.full_snapshot
        assert result.rows_extracted == result.source_row_count == summary.inserted[table.name]


def test_jdbc_types_and_document_payloads_survive_extraction(lake, seeded, source_settings):
    reader = PostgresMongoReader(lake, source_settings)
    for name in ("orders", "reviews"):
        extract_table(lake, reader, source_table(name), batch("2026-06-30"), LOOKBACK)

    orders = lake.table("bronze.orders").schema
    assert orders["total_amount"].dataType == DecimalType(12, 2)
    assert isinstance(orders["order_ts"].dataType, TimestampType)
    assert rows(lake, "SELECT DISTINCT _source_system FROM bronze.orders") == [("postgres",)]

    source_total = seeded[0]._pg.execute("SELECT sum(total_amount) FROM oltp.orders").fetchone()[0]
    seeded[0]._pg.commit()
    assert rows(lake, "SELECT sum(total_amount) FROM bronze.orders") == [(Decimal(source_total),)]

    document = lake.sql(
        "SELECT doc_id, payload, _source_system FROM bronze.reviews ORDER BY doc_id LIMIT 1"
    ).collect()[0]
    payload = json.loads(document.payload)
    assert payload["_id"] == document.doc_id
    assert 1 <= payload["rating"] <= 5
    assert document._source_system == "mongodb"


def test_second_batch_extracts_only_what_changed(lake, seeded, source_settings):
    _, simulator, _ = seeded
    reader = PostgresMongoReader(lake, source_settings)
    orders, reviews = source_table("orders"), source_table("reviews")
    for table in (orders, reviews):
        extract_table(lake, reader, table, batch("2026-06-30"), LOOKBACK)

    day = simulator.advance(1)[0]
    changed_orders = extract_table(lake, reader, orders, batch("2026-07-01"), LOOKBACK)
    changed_reviews = extract_table(lake, reader, reviews, batch("2026-07-01"), LOOKBACK)

    assert not changed_orders.full_snapshot
    # New and updated rows, plus at most the few rows the one-hour lookback re-reads.
    touched = day.inserted["orders"] + day.updated["orders"]
    assert touched <= changed_orders.rows_extracted <= touched + 20
    assert changed_orders.rows_extracted == changed_orders.source_row_count
    assert changed_reviews.rows_extracted >= day.inserted["reviews"]
