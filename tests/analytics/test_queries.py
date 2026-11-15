"""Every analytics query, run on the hand-built star schema in `fixture.sql`.

The expected rows were worked out by hand from the order list at the top of
the fixture; the comments next to them show the arithmetic.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

pytestmark = pytest.mark.postgres

ROOT = Path(__file__).resolve().parents[2]
QUERIES_DIR = ROOT / "analytics" / "queries"
FIXTURE = Path(__file__).with_name("fixture.sql")


def money(value: str) -> Decimal:
    return Decimal(value)


EXPECTED: dict[str, list[tuple]] = {
    # May 10000 + 2000 (order 3 is cancelled); June 3000 + 4000 + 13000 + 1000;
    # July 1000 + 4000 + 10000 + 3000. Growth: 21000/12000 - 1, 18000/21000 - 1.
    "01_monthly_gmv_growth.sql": [
        ("2026-05", money("12000"), 2, None, None),
        ("2026-06", money("21000"), 4, money("12000"), money("75.0")),
        ("2026-07", money("18000"), 4, money("21000"), money("-14.3")),
    ],
    "02_month_to_date_gmv.sql": [
        (date(2026, 5, 5), money("10000"), money("10000")),
        (date(2026, 5, 10), money("2000"), money("12000")),
        (date(2026, 6, 3), money("3000"), money("3000")),
        (date(2026, 6, 18), money("4000"), money("7000")),
        (date(2026, 6, 20), money("13000"), money("20000")),
        (date(2026, 6, 27), money("1000"), money("21000")),
        (date(2026, 7, 4), money("1000"), money("1000")),
        (date(2026, 7, 12), money("4000"), money("5000")),
        (date(2026, 7, 15), money("10000"), money("15000")),
        (date(2026, 7, 20), money("3000"), money("18000")),
    ],
    # Phone X: orders 1, 6, 10. Earbuds: 4, 6, 11. Sneakers: 5, 9. Kurta: 2, 7. Novel: 8 only.
    "03_top_products_per_department.sql": [
        ("Books", 1, "Novel", money("1000"), 2),
        ("Electronics", 1, "Phone X", money("30000"), 3),
        ("Electronics", 2, "Earbuds", money("9000"), 3),
        ("Fashion", 1, "Sneakers", money("8000"), 2),
        ("Fashion", 2, "Kurta", money("3000"), 3),
    ],
    # Alpha 30000, Beta 9000 + 3000, Gamma 8000 + 1000; total 51000.
    "04_seller_revenue_pareto.sql": [
        ("Alpha Traders", money("30000"), money("58.8"), money("58.8"), True),
        ("Beta Stores", money("12000"), money("23.5"), money("82.4"), True),
        ("Gamma Mart", money("9000"), money("17.6"), money("100.0"), False),
    ],
    # First purchase: 101 and 102 in May, 104 in June, 103 in July (its May order was cancelled).
    # May cohort: both buy again in June, only 102 in July.
    "05_cohort_retention.sql": [
        ("2026-05", 2, 0, 2, money("100.0")),
        ("2026-05", 2, 1, 2, money("100.0")),
        ("2026-05", 2, 2, 1, money("50.0")),
        ("2026-06", 1, 0, 1, money("100.0")),
        ("2026-06", 1, 1, 1, money("100.0")),
        ("2026-07", 1, 0, 1, money("100.0")),
    ],
    "06_new_vs_returning_customers.sql": [
        ("2026-05", 2, 0),
        ("2026-06", 1, 2),
        ("2026-07", 1, 2),
    ],
    # Customer 101 spent 13000 while living in Pune and 4000 after moving to Mumbai.
    # Using the current city moves all 17000 to Mumbai and leaves Pune with 103's 10000.
    "07_revenue_by_city_point_in_time.sql": [
        ("Bengaluru", money("5000"), money("5000")),
        ("Delhi", money("16000"), money("16000")),
        ("Mumbai", money("4000"), money("17000")),
        ("Pune", money("23000"), money("10000")),
        ("Unknown", money("3000"), money("3000")),
    ],
    # 101 becomes silver on 15 June; 104 becomes gold on 10 July.
    "08_loyalty_tiers_as_of_month_end.sql": [
        (date(2026, 5, 31), "bronze", 2),
        (date(2026, 5, 31), "gold", 1),
        (date(2026, 5, 31), "silver", 1),
        (date(2026, 6, 30), "bronze", 1),
        (date(2026, 6, 30), "gold", 1),
        (date(2026, 6, 30), "silver", 2),
        (date(2026, 7, 31), "bronze", 1),
        (date(2026, 7, 31), "gold", 2),
        (date(2026, 7, 31), "silver", 1),
    ],
    # CARD: 4 attempts, 3 succeed; first attempts succeed for orders 6 and 8 but not 2.
    "09_payment_success_by_method.sql": [
        ("CARD", 4, 3, money("75.0"), money("66.7"), money("16000")),
        ("UPI", 4, 4, money("100.0"), money("100.0"), money("21000")),
        ("COD", 2, 0, money("0.0"), money("0.0"), money("0")),
        ("NET_BANKING", 2, 1, money("50.0"), money("0.0"), money("10000")),
        ("WALLET", 1, 0, money("0.0"), money("0.0"), money("0")),
    ],
    # Pune: 2, 1 and 3 days. Delhi: 3, 2 and 1 days (order 6 counted once).
    "10_delivery_time_by_city.sql": [
        ("Bengaluru", 1, money("5.00"), money("5.00"), money("5.00")),
        ("Mumbai", 1, money("4.00"), money("4.00"), money("4.00")),
        ("Delhi", 3, money("2.00"), money("2.00"), money("3.00")),
        ("Pune", 3, money("2.00"), money("2.00"), money("3.00")),
    ],
    # bronze: orders 1, 4, 10. gold: 2, 6, 8, 9 (104 was already gold). silver: 5 and 7.
    "11_order_value_by_loyalty_tier.sql": [
        ("bronze", 3, money("23000"), money("7666.67"), 1),
        ("gold", 4, money("20000"), money("5000.00"), 2),
        ("Unknown", 1, money("3000"), money("3000.00"), 3),
        ("silver", 2, money("5000"), money("2500.00"), 4),
    ],
    # Weekend orders: 2, 6, 7, 8, 9. Weekday orders: 1, 4, 5, 10, 11.
    "12_weekend_vs_weekday_sales.sql": [
        ("Weekday", 5, money("30000"), money("6000.00"), money("58.8")),
        ("Weekend", 5, money("21000"), money("4200.00"), money("41.2")),
    ],
    "13_cancellation_and_return_rates.sql": [
        ("Books", 2, 1, money("50.0"), 0, money("0.0")),
        ("Electronics", 5, 0, money("0.0"), 0, money("0.0")),
        ("Fashion", 4, 0, money("0.0"), 1, money("25.0")),
    ],
    # Electronics: Pune 23000 of 39000. Fashion: Bengaluru 5000 of 11000.
    "14_strongest_city_per_department.sql": [
        ("Books", "Delhi", money("1000"), money("100.0")),
        ("Electronics", "Pune", money("23000"), money("59.0")),
        ("Fashion", "Bengaluru", money("5000"), money("45.5")),
    ],
}

QUERY_FILES = sorted(path.name for path in QUERIES_DIR.glob("*.sql"))


@pytest.fixture(scope="module")
def warehouse(warehouse_dsn: str):
    with psycopg.connect(warehouse_dsn, autocommit=True) as connection:
        connection.execute("SET TIME ZONE 'UTC'")
        connection.execute(FIXTURE.read_text(encoding="utf-8"))
        yield connection
        connection.execute("DROP SCHEMA dw CASCADE")


def test_every_query_has_expected_results_and_at_least_twelve_exist():
    assert len(QUERY_FILES) >= 12
    assert set(QUERY_FILES) == set(EXPECTED)


@pytest.mark.parametrize("name", QUERY_FILES)
def test_query_states_its_business_question(name: str):
    first_line = (QUERIES_DIR / name).read_text(encoding="utf-8").splitlines()[0]

    assert first_line.startswith("-- Business question:")


@pytest.mark.parametrize("name", QUERY_FILES)
def test_query_returns_the_hand_computed_answer(warehouse, name: str):
    rows = warehouse.execute((QUERIES_DIR / name).read_text(encoding="utf-8")).fetchall()

    assert rows == EXPECTED[name]
