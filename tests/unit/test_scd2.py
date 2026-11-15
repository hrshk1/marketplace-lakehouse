"""SCD Type 2 merge on dim_customer: insert, change, no-change, out-of-order, re-run."""

from __future__ import annotations

from datetime import date

import pytest

from marketplace.gold.models import apply_customer_changes
from tests.helpers import rows, ts

pytestmark = pytest.mark.spark

BEGINNING = "1900-01-01 00:00:00"
END = "9999-12-31 00:00:00"
CHANGES_DDL = (
    "customer_id bigint, city string, state string, pincode string, loyalty_tier string, "
    "customer_since date, updated_at timestamp"
)


def change(customer_id: int, updated_at: str, city: str = "Pune", tier: str = "bronze") -> tuple:
    state = {"Pune": "Maharashtra", "Delhi": "Delhi", "Kochi": "Kerala"}[city]
    return (customer_id, city, state, "411001", tier, date(2026, 1, 1), ts(updated_at))


def apply(lake, *changes: tuple) -> None:
    lake.createDataFrame(list(changes), CHANGES_DDL).createOrReplaceTempView("changes")
    apply_customer_changes(lake, "changes")


def history(lake, customer_id: int | None = None) -> list[tuple]:
    where = "customer_id > 0" if customer_id is None else f"customer_id = {customer_id}"
    return rows(
        lake,
        f"""
        SELECT customer_sk, customer_id, city, loyalty_tier,
               date_format(valid_from, 'yyyy-MM-dd HH:mm:ss'),
               date_format(valid_to, 'yyyy-MM-dd HH:mm:ss'), is_current
        FROM gold.dim_customer WHERE {where} ORDER BY customer_id, valid_from
        """,
    )


def test_dimension_starts_with_the_unknown_member(lake):
    apply(lake)

    assert rows(
        lake, "SELECT customer_sk, customer_id, city, is_current FROM gold.dim_customer"
    ) == [(-1, -1, "Unknown", True)]


def test_new_customers_get_one_open_ended_version(lake):
    apply(lake, change(1, "2026-06-30 10:00"), change(2, "2026-06-30 11:00", city="Delhi"))

    assert history(lake) == [
        (1, 1, "Pune", "bronze", BEGINNING, END, True),
        (2, 2, "Delhi", "bronze", BEGINNING, END, True),
    ]


def test_changed_attribute_closes_the_old_version_and_opens_a_new_one(lake):
    apply(lake, change(1, "2026-06-30 10:00"), change(2, "2026-06-30 11:00"))

    apply(lake, change(1, "2026-07-01 15:30", city="Delhi"), change(2, "2026-06-30 11:00"))

    assert history(lake) == [
        (1, 1, "Pune", "bronze", BEGINNING, "2026-07-01 15:30:00", False),
        (3, 1, "Delhi", "bronze", "2026-07-01 15:30:00", END, True),
        (2, 2, "Pune", "bronze", BEGINNING, END, True),
    ]


def test_tier_change_is_tracked_like_an_address_change(lake):
    apply(lake, change(1, "2026-06-30 10:00"))

    apply(lake, change(1, "2026-07-02 09:00", tier="gold"))

    assert [(row[3], row[6]) for row in history(lake, 1)] == [("bronze", False), ("gold", True)]


def test_unchanged_attributes_create_no_new_version_even_with_a_newer_timestamp(lake):
    apply(lake, change(1, "2026-06-30 10:00"))

    apply(lake, change(1, "2026-07-05 10:00"))

    assert history(lake) == [(1, 1, "Pune", "bronze", BEGINNING, END, True)]


def test_reapplying_the_same_changes_gives_the_same_dimension(lake):
    apply(lake, change(1, "2026-06-30 10:00"))
    apply(lake, change(1, "2026-07-01 15:30", city="Delhi"))
    before = history(lake)

    apply(lake, change(1, "2026-07-01 15:30", city="Delhi"))

    assert history(lake) == before


def test_out_of_order_version_older_than_the_current_one_is_ignored(lake):
    apply(lake, change(1, "2026-06-30 10:00"))
    apply(lake, change(1, "2026-07-03 12:00", city="Delhi"))
    before = history(lake)

    # A stale version stamped before the Delhi move arrives afterwards.
    apply(lake, change(1, "2026-07-02 08:00", city="Kochi"))

    assert history(lake) == before


def test_successive_changes_build_a_gapless_history_with_one_current_row(lake):
    apply(lake, change(1, "2026-06-30 10:00"))
    apply(lake, change(1, "2026-07-01 09:00", city="Delhi"))
    apply(lake, change(1, "2026-07-02 18:00", city="Kochi", tier="silver"))

    versions = history(lake, 1)

    assert [row[2] for row in versions] == ["Pune", "Delhi", "Kochi"]
    assert [row[6] for row in versions] == [False, False, True]
    # Each version ends exactly where the next begins.
    assert [row[5] for row in versions[:-1]] == [row[4] for row in versions[1:]]
    assert len({row[0] for row in versions}) == 3


def test_row_hash_changes_only_when_a_tracked_attribute_changes(lake):
    apply(lake, change(1, "2026-06-30 10:00"))
    apply(lake, change(1, "2026-07-01 09:00", city="Delhi"))
    apply(lake, change(1, "2026-07-02 09:00", city="Pune"))

    hashes = [
        row[0]
        for row in rows(
            lake, "SELECT row_hash FROM gold.dim_customer WHERE customer_id = 1 ORDER BY valid_from"
        )
    ]

    assert hashes[0] == hashes[2] != hashes[1]
