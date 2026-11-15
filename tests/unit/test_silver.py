"""Silver: conforming, deduplication, quarantine and late-arriving versions."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from marketplace.generator.stores import MemoryStore
from marketplace.silver.build import build_silver_table
from marketplace.silver.specs import silver_table
from tests.helpers import (
    batch,
    customer_row,
    load_to_silver,
    order_item_row,
    order_row,
    payment_row,
    rows,
    ts,
    upsert,
)

pytestmark = pytest.mark.spark

DAY_0 = "2026-06-30"
DAY_1 = "2026-07-01"
NOON = ts("2026-06-30 12:00")


def quarantine(lake, table: str) -> list[tuple]:
    return rows(
        lake,
        f"SELECT record_key, rule_id, _batch_id FROM silver_quarantine.{table} ORDER BY 1, 3",
    )


def test_address_fields_are_trimmed_cased_and_mapped_to_official_names(lake):
    store = MemoryStore()
    store.insert(
        "customers",
        [
            customer_row(1, NOON, city="  mumbai ", state="MH", full_name="  Asha Rao "),
            customer_row(2, NOON, city="Bangalore", state="karnataka", email="  A.B@Mail.Test "),
            customer_row(3, NOON, city="PUNE", state="Maharashtra", loyalty_tier="GOLD"),
        ],
    )

    load_to_silver(lake, store, ["customers"], DAY_0)

    assert rows(
        lake,
        "SELECT customer_id, full_name, email, city, state, loyalty_tier "
        "FROM silver.customers ORDER BY 1",
    ) == [
        (1, "Asha Rao", "customer1@mail.test", "Mumbai", "Maharashtra", "bronze"),
        (2, "Customer 2", "a.b@mail.test", "Bengaluru", "Karnataka", "bronze"),
        (3, "Customer 3", "customer3@mail.test", "Pune", "Maharashtra", "gold"),
    ]


def test_rule_breakers_are_quarantined_with_reason_and_original_payload(lake):
    store = MemoryStore()
    store.insert(
        "customers",
        [customer_row(1, NOON), customer_row(2, NOON, pincode="4110"), customer_row(3, NOON)],
    )

    result = load_to_silver(lake, store, ["customers"], DAY_0)["customers"]

    assert (result.rows_in, result.rows_accepted, result.rows_quarantined) == (3, 2, 1)
    assert rows(lake, "SELECT customer_id FROM silver.customers ORDER BY 1") == [(1,), (3,)]
    rejected = lake.sql(
        "SELECT record_key, rule_id, reject_reason, _batch_id, payload "
        "FROM silver_quarantine.customers"
    ).collect()
    assert len(rejected) == 1
    assert rejected[0].record_key == "2"
    assert rejected[0].rule_id == "CUS_INVALID_PINCODE"
    assert rejected[0].reject_reason == "pincode is not a 6-digit Indian pincode"
    assert rejected[0]._batch_id == DAY_0
    payload = json.loads(rejected[0].payload)
    assert payload["pincode"] == "4110"
    assert payload["_source_system"] == "postgres"


def test_newer_version_replaces_the_row_and_older_late_version_does_not(lake):
    store = MemoryStore()
    store.insert("customers", [customer_row(1, ts("2026-06-30 10:00"), city="Pune")])
    load_to_silver(lake, store, ["customers"], DAY_0)

    upsert(
        store, "customers", "customer_id", [customer_row(1, ts("2026-07-01 09:00"), city="Delhi")]
    )
    load_to_silver(lake, store, ["customers"], DAY_1)
    assert rows(lake, "SELECT city, _batch_id FROM silver.customers") == [("Delhi", DAY_1)]

    # A stale version shows up afterwards carrying an older updated_at.
    upsert(store, "customers", "customer_id", [customer_row(1, ts("2026-07-01 08:00"), city="Goa")])
    load_to_silver(lake, store, ["customers"], "2026-07-02")

    assert rows(lake, "SELECT city, _batch_id FROM silver.customers") == [("Delhi", DAY_1)]


def test_only_the_latest_version_within_a_batch_reaches_silver(lake):
    store = MemoryStore()
    store.insert("customers", [customer_row(1, ts("2026-06-30 23:30"), city="Pune")])
    load_to_silver(lake, store, ["customers"], DAY_0)
    # The lookback re-reads the 23:30 row next to the new version, so the
    # next batch holds two versions of customer 1.
    lake.sql(
        f"""
        INSERT INTO bronze.customers
        SELECT customer_id, full_name, email, phone, 'Delhi', state, pincode, loyalty_tier,
               created_at, TIMESTAMP '2026-07-01 09:00:00', '{DAY_1}', _ingested_at,
               _source_system, _extract_watermark
        FROM bronze.customers
        UNION ALL
        SELECT customer_id, full_name, email, phone, city, state, pincode, loyalty_tier,
               created_at, updated_at, '{DAY_1}', _ingested_at, _source_system, _extract_watermark
        FROM bronze.customers
        """
    )

    result = build_silver_table(lake, silver_table("customers"), batch(DAY_1))

    assert result.rows_in == 1
    assert rows(lake, "SELECT city FROM silver.customers") == [("Delhi",)]


@pytest.fixture
def order_store() -> MemoryStore:
    """Three healthy orders plus one of each order-level defect."""
    store = MemoryStore()
    store.insert(
        "orders",
        [
            order_row(1, NOON),
            order_row(2, NOON, customer_id=None),
            order_row(3, NOON, order_ts=ts("2026-07-09 12:00")),
            order_row(4, ts("2026-06-30 12:00:02"), order_number="MP-000001"),
            order_row(5, NOON, total_amount=Decimal("-10.00")),
            order_row(6, NOON, order_status="teleported"),
        ],
    )
    store.insert(
        "order_items",
        [
            order_item_row(11, 1, NOON, quantity=2, discount_amount=Decimal("50.00")),
            order_item_row(31, 3, NOON),
            order_item_row(991, 99, NOON),
        ],
    )
    store.insert(
        "payments",
        [
            payment_row(11, 1, NOON),
            payment_row(12, 1, NOON, amount=Decimal("0.00")),
            payment_row(21, 2, NOON, amount=Decimal("-500.00")),
            payment_row(31, 3, NOON),
        ],
    )
    return store


def test_order_rules_reject_future_duplicate_negative_and_unknown_status(lake, order_store):
    load_to_silver(lake, order_store, ["orders"], DAY_0)

    assert rows(lake, "SELECT order_id, customer_id FROM silver.orders ORDER BY 1") == [
        (1, 1),
        (2, None),
    ]
    assert quarantine(lake, "orders") == [
        ("3", "ORD_FUTURE_DATE", DAY_0),
        ("4", "ORD_DUPLICATE_NUMBER", DAY_0),
        ("5", "ORD_NEGATIVE_TOTAL", DAY_0),
        ("6", "ORD_INVALID_STATUS", DAY_0),
    ]


def test_order_retry_arriving_in_a_later_batch_is_still_a_duplicate(lake, order_store):
    load_to_silver(lake, order_store, ["orders"], DAY_0)
    order_store.insert("orders", [order_row(7, ts("2026-07-01 08:00"), order_number="MP-000002")])

    load_to_silver(lake, order_store, ["orders"], DAY_1)

    assert ("7", "ORD_DUPLICATE_NUMBER", DAY_1) in quarantine(lake, "orders")
    assert rows(lake, "SELECT count(*) FROM silver.orders WHERE order_id = 7") == [(0,)]


def test_items_are_split_into_accepted_orphans_and_children_of_rejected_orders(lake, order_store):
    load_to_silver(lake, order_store, ["orders", "order_items"], DAY_0)

    assert rows(lake, "SELECT order_item_id, item_total FROM silver.order_items") == [
        (11, Decimal("950.00"))
    ]
    assert quarantine(lake, "order_items") == [
        ("31", "OI_PARENT_REJECTED", DAY_0),
        ("991", "OI_ORPHAN", DAY_0),
    ]


def test_payments_with_non_positive_amount_or_rejected_order_are_quarantined(lake, order_store):
    load_to_silver(lake, order_store, ["orders", "payments"], DAY_0)

    assert rows(lake, "SELECT payment_id FROM silver.payments") == [(11,)]
    assert quarantine(lake, "payments") == [
        ("12", "PAY_NON_POSITIVE_AMOUNT", DAY_0),
        ("21", "PAY_NON_POSITIVE_AMOUNT", DAY_0),
        ("31", "PAY_PARENT_REJECTED", DAY_0),
    ]


def test_rebuilding_the_same_batch_leaves_silver_and_quarantine_unchanged(lake, order_store):
    tables = ["orders", "order_items", "payments"]
    load_to_silver(lake, order_store, tables, DAY_0)
    before = {
        name: (
            rows(lake, f"SELECT * FROM silver.{name} ORDER BY 1"),
            quarantine(lake, name),
        )
        for name in tables
    }

    load_to_silver(lake, order_store, tables, DAY_0)

    for name in tables:
        assert rows(lake, f"SELECT * FROM silver.{name} ORDER BY 1") == before[name][0]
        assert quarantine(lake, name) == before[name][1]


def test_documents_are_flattened_with_nested_reply_and_attribute_map(lake):
    store = MemoryStore()
    store.insert(
        "reviews",
        [
            {
                "_id": "rev-1",
                "order_id": 1,
                "product_id": 7,
                "customer_id": 3,
                "rating": 4,
                "title": " Good purchase ",
                "text": "Works well.",
                "helpful_votes": 2,
                "seller_reply": {"text": "Thank you.", "replied_at": ts("2026-06-30 11:00")},
                "created_at": ts("2026-06-30 09:00"),
                "updated_at": ts("2026-06-30 11:00"),
            },
            {
                "_id": "rev-2",
                "order_id": 2,
                "product_id": 8,
                "customer_id": 4,
                "rating": 9,
                "title": "Broken",
                "text": "Rating is out of range.",
                "helpful_votes": 0,
                "seller_reply": None,
                "created_at": NOON,
                "updated_at": NOON,
            },
        ],
    )
    store.insert(
        "product_attributes",
        [
            {
                "_id": "prod-7",
                "product_id": 7,
                "department": "Fashion",
                "attributes": {"brand": "Kashvi", "color": "Navy", "size": "M"},
                "created_at": NOON,
                "updated_at": NOON,
            }
        ],
    )

    load_to_silver(lake, store, ["reviews", "product_attributes"], DAY_0)

    assert rows(
        lake,
        "SELECT review_id, order_id, product_id, rating, title, has_seller_reply, "
        "seller_reply_text, unix_seconds(seller_replied_at) FROM silver.reviews",
    ) == [
        (
            "rev-1",
            1,
            7,
            4,
            "Good purchase",
            True,
            "Thank you.",
            int(ts("2026-06-30 11:00").timestamp()),
        )
    ]
    assert quarantine(lake, "reviews") == [("rev-2", "REV_INVALID_RATING", DAY_0)]
    attributes = lake.sql(
        "SELECT product_id, department, brand, color, material, attributes "
        "FROM silver.product_attributes"
    ).collect()[0]
    assert (attributes.product_id, attributes.department) == (7, "Fashion")
    assert (attributes.brand, attributes.color, attributes.material) == ("Kashvi", "Navy", None)
    assert attributes.attributes == {"brand": "Kashvi", "color": "Navy", "size": "M"}
