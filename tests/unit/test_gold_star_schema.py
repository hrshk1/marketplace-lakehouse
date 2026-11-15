"""Gold star schema built end to end from a tiny, hand-written marketplace.

Every expected number below was worked out by hand from the rows in `store`.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from marketplace.generator.stores import MemoryStore
from marketplace.gold.models import affected_order_months
from tests.conftest import use_fresh_catalog
from tests.helpers import (
    batch,
    category_row,
    customer_row,
    order_item_row,
    order_row,
    payment_row,
    product_row,
    rows,
    run_day,
    seller_row,
    ts,
    upsert,
)

pytestmark = pytest.mark.spark

DAY_0 = "2026-06-30"
DAY_1 = "2026-07-01"
SEEDED = ts("2026-06-30 08:00")


def build_store() -> MemoryStore:
    """Two customers, two products in two departments, three orders on day 0.

    Order 1 (customer 1, delivered): 2 x product 1 at 1000 less 100, plus 1 x product 2 at 500.
    Order 2 (customer 2, cancelled): 1 x product 2 at 500, shipping 49, paid on second attempt.
    Order 3 (no customer, placed):   1 x product 1 at 1000, cash on delivery.
    """
    store = MemoryStore()
    store.insert(
        "categories",
        [
            category_row(1, "Electronics"),
            category_row(2, "Smartphones", 1),
            category_row(3, "Fashion"),
            category_row(4, "Footwear", 3),
        ],
    )
    store.insert("sellers", [seller_row(1, SEEDED), seller_row(2, SEEDED, city="Bangalore")])
    store.insert(
        "products",
        [
            product_row(1, SEEDED),
            product_row(2, SEEDED, seller_id=2, category_id=4, price=Decimal("500.00")),
        ],
    )
    store.insert(
        "product_attributes",
        [
            {
                "_id": "prod-1",
                "product_id": 1,
                "department": "Electronics",
                "attributes": {"brand": "Voltix", "color": "Black"},
                "created_at": SEEDED,
                "updated_at": SEEDED,
            }
        ],
    )
    store.insert(
        "customers",
        [
            customer_row(1, SEEDED),
            customer_row(2, SEEDED, city="Delhi", state="DL", pincode="110001"),
        ],
    )
    store.insert(
        "orders",
        [
            order_row(
                1,
                ts("2026-06-30 22:00"),
                order_ts=ts("2026-06-30 10:00"),
                created_at=ts("2026-06-30 10:00"),
                order_status="delivered",
                delivered_at=ts("2026-06-30 22:00"),
                total_amount=Decimal("2400.00"),
            ),
            order_row(
                2,
                ts("2026-06-30 11:00"),
                customer_id=2,
                order_status="cancelled",
                total_amount=Decimal("549.00"),
                shipping_fee=Decimal("49.00"),
            ),
            order_row(3, ts("2026-06-30 12:00"), customer_id=None, total_amount=Decimal("1000.00")),
        ],
    )
    store.insert(
        "order_items",
        [
            order_item_row(
                11,
                1,
                SEEDED,
                quantity=2,
                unit_price=Decimal("1000.00"),
                discount_amount=Decimal("100.00"),
            ),
            order_item_row(12, 1, SEEDED, product_id=2, seller_id=2),
            order_item_row(21, 2, SEEDED, product_id=2, seller_id=2),
            order_item_row(31, 3, SEEDED, unit_price=Decimal("1000.00")),
        ],
    )
    store.insert(
        "payments",
        [
            payment_row(11, 1, SEEDED, amount=Decimal("2400.00")),
            payment_row(
                21,
                2,
                SEEDED,
                payment_method="CARD",
                payment_status="failed",
                amount=Decimal("549.00"),
            ),
            payment_row(
                22,
                2,
                SEEDED,
                payment_method="CARD",
                payment_status="refunded",
                attempt_no=2,
                amount=Decimal("549.00"),
            ),
            payment_row(
                31,
                3,
                SEEDED,
                payment_method="COD",
                payment_status="pending",
                amount=Decimal("1000.00"),
            ),
        ],
    )
    return store


def add_day_1(store: MemoryStore) -> None:
    """Day 1: customer 1 moves to Delhi at noon and orders once before and once after.

    Product 2 is repriced, product 3 is listed already retired and seller 1 is renamed.
    """
    upsert(
        store,
        "customers",
        "customer_id",
        [customer_row(1, ts("2026-07-01 12:00"), city="Delhi", state="Delhi", pincode="110002")],
    )
    store.insert(
        "orders",
        [order_row(4, ts("2026-07-01 09:00")), order_row(5, ts("2026-07-01 15:00"))],
    )
    store.insert(
        "order_items",
        [
            order_item_row(41, 4, ts("2026-07-01 09:00")),
            order_item_row(51, 5, ts("2026-07-01 15:00")),
        ],
    )
    store.insert(
        "payments",
        [payment_row(41, 4, ts("2026-07-01 09:01")), payment_row(51, 5, ts("2026-07-01 15:01"))],
    )
    upsert(
        store,
        "products",
        "product_id",
        [
            product_row(
                2, ts("2026-07-01 10:00"), seller_id=2, category_id=4, price=Decimal("450.00")
            ),
            product_row(
                3, ts("2026-07-01 10:00"), deleted_at=ts("2026-07-01 10:00"), is_active=False
            ),
        ],
    )
    upsert(
        store,
        "sellers",
        "seller_id",
        [seller_row(1, ts("2026-07-01 10:00"), seller_name="Renamed Traders")],
    )


# The pipeline runs are the slow part, so the read-only tests share two
# lakehouses: one after day 0 and one after day 1.
@pytest.fixture(scope="module")
def day_0_catalog(spark, settings, tmp_path_factory) -> str:
    name = use_fresh_catalog(spark, settings, tmp_path_factory.mktemp("gold_day_0"))
    run_day(spark, settings, build_store(), DAY_0)
    return name


@pytest.fixture(scope="module")
def day_1_lakehouse(spark, settings, tmp_path_factory) -> tuple[str, MemoryStore]:
    name = use_fresh_catalog(spark, settings, tmp_path_factory.mktemp("gold_day_1"))
    store = build_store()
    run_day(spark, settings, store, DAY_0)
    add_day_1(store)
    run_day(spark, settings, store, DAY_1)
    return name, store


@pytest.fixture
def day_0(spark, day_0_catalog):
    spark.sql(f"USE {day_0_catalog}")
    return spark


@pytest.fixture
def day_1(spark, day_1_lakehouse):
    spark.sql(f"USE {day_1_lakehouse[0]}")
    return spark


def test_dimensions_get_surrogate_keys_and_an_unknown_member(day_0):
    assert rows(day_0, "SELECT seller_sk, seller_id, city FROM gold.dim_seller ORDER BY 1") == [
        (-1, -1, "Unknown"),
        (1, 1, "Mumbai"),
        (2, 2, "Bengaluru"),
    ]
    assert rows(
        day_0,
        "SELECT product_sk, product_id, category_name, department, brand, color, price "
        "FROM gold.dim_product ORDER BY 1",
    ) == [
        (-1, -1, "Unknown", "Unknown", None, None, None),
        (1, 1, "Smartphones", "Electronics", "Voltix", "Black", Decimal("1000.00")),
        (2, 2, "Footwear", "Fashion", None, None, Decimal("500.00")),
    ]
    assert rows(day_0, "SELECT count(*) FROM gold.dim_customer WHERE customer_sk = -1") == [(1,)]
    assert rows(day_0, "SELECT count(*) FROM gold.dim_date WHERE date_key = -1") == [(1,)]
    assert rows(
        day_0, "SELECT payment_method FROM gold.dim_payment_method WHERE payment_method_sk = -1"
    ) == [("UNKNOWN",)]


def test_dim_date_describes_each_calendar_day(day_0):
    assert rows(
        day_0,
        "SELECT date_key, day_of_week, day_name, month, quarter, year, is_weekend, is_month_end "
        "FROM gold.dim_date WHERE date_key IN (20260630, 20260704) ORDER BY 1",
    ) == [
        (20260630, 2, "Tuesday", 6, 2, 2026, False, True),
        (20260704, 6, "Saturday", 7, 3, 2026, True, False),
    ]


def test_fact_order_items_has_one_row_per_line_with_measures_and_keys(day_0):
    assert rows(
        day_0,
        "SELECT order_item_id, order_id, order_date_key, customer_sk, product_sk, seller_sk, "
        "payment_method_sk, quantity, unit_price, discount, item_total, shipping_fee, "
        "order_status, delivery_days FROM gold.fact_order_items ORDER BY 1",
    ) == [
        (11, 1, 20260630, 1, 1, 1, 1, 2, Decimal("1000.00"), Decimal("100.00"), Decimal("1900.00"), Decimal("0.00"), "delivered", Decimal("0.50")),
        (12, 1, 20260630, 1, 2, 2, 1, 1, Decimal("500.00"), Decimal("0.00"), Decimal("500.00"), Decimal("0.00"), "delivered", Decimal("0.50")),
        (21, 2, 20260630, 2, 2, 2, 2, 1, Decimal("500.00"), Decimal("0.00"), Decimal("500.00"), Decimal("49.00"), "cancelled", None),
        (31, 3, 20260630, -1, 1, 1, 5, 1, Decimal("1000.00"), Decimal("0.00"), Decimal("1000.00"), Decimal("0.00"), "placed", None),
    ]  # fmt: skip


def test_fact_payments_has_one_row_per_attempt(day_0):
    assert rows(
        day_0,
        "SELECT payment_id, order_id, payment_date_key, customer_sk, payment_method_sk, attempt_no, "
        "amount, payment_status, is_successful FROM gold.fact_payments ORDER BY 1",
    ) == [
        (11, 1, 20260630, 1, 1, 1, Decimal("2400.00"), "success", True),
        (21, 2, 20260630, 2, 2, 1, Decimal("549.00"), "failed", False),
        (22, 2, 20260630, 2, 2, 2, Decimal("549.00"), "refunded", False),
        (31, 3, 20260630, -1, 5, 1, Decimal("1000.00"), "pending", False),
    ]


def test_agg_daily_sales_excludes_cancelled_orders_from_gmv(day_0):
    assert rows(
        day_0,
        "SELECT date_key, category, city, gmv, orders, units, avg_order_value, cancellation_rate "
        "FROM gold.agg_daily_sales ORDER BY category, city",
    ) == [
        (20260630, "Electronics", "Pune", Decimal("1900.00"), 1, 2, Decimal("1900.00"), Decimal("0.0000")),
        (20260630, "Electronics", "Unknown", Decimal("1000.00"), 1, 1, Decimal("1000.00"), Decimal("0.0000")),
        (20260630, "Fashion", "Delhi", Decimal("0.00"), 1, 0, None, Decimal("1.0000")),
        (20260630, "Fashion", "Pune", Decimal("500.00"), 1, 1, Decimal("500.00"), Decimal("0.0000")),
    ]  # fmt: skip


def test_customer_key_is_the_version_valid_at_order_time(day_1):
    assert rows(
        day_1,
        """
        SELECT f.order_id, c.city, c.is_current
        FROM gold.fact_order_items AS f
        JOIN gold.dim_customer AS c ON c.customer_sk = f.customer_sk
        WHERE c.customer_id = 1
        GROUP BY f.order_id, c.city, c.is_current
        ORDER BY f.order_id
        """,
    ) == [
        (1, "Pune", False),
        (4, "Pune", False),
        (5, "Delhi", True),
    ]
    # Joining on the business key alone would wrongly report every order under Delhi.
    assert rows(
        day_1,
        """
        SELECT c.city, CAST(sum(f.item_total) AS DECIMAL(12, 2))
        FROM gold.fact_order_items AS f
        JOIN gold.dim_customer AS c ON c.customer_sk = f.customer_sk
        WHERE c.customer_id = 1
        GROUP BY c.city ORDER BY c.city
        """,
    ) == [("Delhi", Decimal("500.00")), ("Pune", Decimal("2900.00"))]


def test_only_the_order_months_touched_by_a_batch_are_rebuilt(day_1):
    assert affected_order_months(day_1, batch(DAY_0)) == ["2026-06-01"]
    assert affected_order_months(day_1, batch(DAY_1)) == ["2026-07-01"]
    assert rows(
        day_1,
        "SELECT partition.order_ts_month, sum(record_count) "
        "FROM gold.fact_order_items.files GROUP BY 1 ORDER BY 1",
    ) == [(677, 4), (678, 2)]


def test_type1_dimensions_overwrite_attributes_and_keep_surrogate_keys(day_1):
    assert rows(
        day_1,
        "SELECT product_sk, product_id, price, is_deleted FROM gold.dim_product WHERE product_sk > 0 ORDER BY 1",
    ) == [
        (1, 1, Decimal("1000.00"), False),
        (2, 2, Decimal("450.00"), False),
        (3, 3, Decimal("1000.00"), True),
    ]
    assert rows(
        day_1, "SELECT seller_sk, seller_name FROM gold.dim_seller WHERE seller_id = 1"
    ) == [(1, "Renamed Traders")]
    # Facts recorded the price charged at the time, so they do not move with the dimension.
    assert rows(day_1, "SELECT unit_price FROM gold.fact_order_items WHERE order_item_id = 21") == [
        (Decimal("500.00"),)
    ]


def test_rerunning_a_batch_leaves_gold_unchanged(day_1, settings, day_1_lakehouse):
    store = day_1_lakehouse[1]
    tables = [
        "dim_customer",
        "dim_seller",
        "dim_product",
        "fact_order_items",
        "fact_payments",
        "agg_daily_sales",
    ]
    before = {name: rows(day_1, f"SELECT * FROM gold.{name} ORDER BY 1, 2, 3") for name in tables}

    run_day(day_1, settings, store, DAY_1)
    run_day(day_1, settings, store, DAY_0)

    for name in tables:
        assert rows(day_1, f"SELECT * FROM gold.{name} ORDER BY 1, 2, 3") == before[name], name
