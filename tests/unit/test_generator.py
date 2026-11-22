"""Generator: determinism, order lifecycle, injected defects and the ledger."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pytest

from marketplace.generator.ledger import Ledger
from marketplace.generator.lifecycle import plan_order, state_at
from marketplace.generator.model import PROFILES, GeneratorConfig
from marketplace.generator.simulator import MarketplaceSimulator, StateMismatchError
from marketplace.generator.stores import MemoryStore
from tests.helpers import ts

SMALL = PROFILES["small"]
START = date(2026, 6, 30)
DRIFT_DAY = 2


def config(
    seed: int = 42, defect_rate: float = 0.02, invalid_quantity_rate: float = 0.0
) -> GeneratorConfig:
    return GeneratorConfig(seed, START, SMALL, defect_rate, DRIFT_DAY, invalid_quantity_rate)


def simulate(
    tmp_path: Path, name: str, steps: tuple[int, ...] = (), **overrides
) -> tuple[MemoryStore, Ledger, MarketplaceSimulator]:
    """Seed a fresh in-memory marketplace and advance it by each step in turn."""
    store, ledger = MemoryStore(), Ledger(tmp_path / f"{name}.sqlite")
    simulator = MarketplaceSimulator(store, ledger, config(**overrides))
    simulator.seed()
    for days in steps:
        simulator.advance(days)
    return store, ledger, simulator


def fingerprint(store: MemoryStore, ledger: Ledger) -> str:
    """One hash over every source row and every ledger entry."""
    content = {
        "tables": {name: sorted(map(str, rows.values())) for name, rows in store.tables.items()},
        "columns": store.columns,
        "defects": [str(defect) for defect in ledger.defects()],
    }
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


def test_same_seed_produces_identical_sources_and_ledger(tmp_path):
    first = simulate(tmp_path, "first", (3,))
    second = simulate(tmp_path, "second", (3,))

    assert fingerprint(*first[:2]) == fingerprint(*second[:2])


def test_different_seed_produces_different_data(tmp_path):
    first = simulate(tmp_path, "first")
    other = simulate(tmp_path, "other", seed=7)

    assert fingerprint(*first[:2]) != fingerprint(*other[:2])


def test_advancing_in_several_steps_equals_one_big_step(tmp_path):
    in_steps = simulate(tmp_path, "steps", (1, 2, 1))
    at_once = simulate(tmp_path, "once", (4,))

    assert fingerprint(*in_steps[:2]) == fingerprint(*at_once[:2])


def test_seed_matches_the_profile_scale(tmp_path):
    store, _, _ = simulate(tmp_path, "scale", defect_rate=0)

    counts = {name: len(rows) for name, rows in store.tables.items()}
    assert counts["customers"] == SMALL.customers
    assert counts["sellers"] == SMALL.sellers
    assert counts["products"] == counts["product_attributes"] == SMALL.products
    assert counts["categories"] == 48
    # Daily volume has weekday seasonality and noise, so the total is approximate.
    assert counts["orders"] == pytest.approx(SMALL.orders, rel=0.15)


def test_order_totals_equal_their_items_plus_shipping(tmp_path):
    store, _, _ = simulate(tmp_path, "totals", defect_rate=0)

    line_totals: Counter = Counter()
    for item in store.tables["order_items"].values():
        line_totals[item["order_id"]] += (
            item["quantity"] * item["unit_price"] - item["discount_amount"]
        )

    for order in store.tables["orders"].values():
        assert order["total_amount"] == line_totals[order["order_id"]] + order["shipping_fee"]


def test_order_plan_moves_forward_in_time_through_valid_statuses():
    valid_next = {
        "placed": {"paid", "packed", "cancelled"},
        "paid": {"packed", "cancelled"},
        "packed": {"shipped"},
        "shipped": {"delivered"},
        "delivered": {"returned"},
    }
    for order_id in range(1, 400):
        plan = plan_order(42, order_id, ts("2026-06-30 10:00"))
        times = [event.at for event in plan.events]
        assert times == sorted(times)
        assert plan.events[0].status == "placed"
        for previous, current in zip(plan.events, plan.events[1:], strict=False):
            assert current.status in valid_next[previous.status]


def test_state_at_returns_the_latest_event_not_after_the_moment():
    plan = plan_order(42, 5, ts("2026-06-30 10:00"))

    assert state_at(plan.events, ts("2026-06-30 09:59")) is None
    assert state_at(plan.events, ts("2026-06-30 10:00")).status == "placed"
    assert state_at(plan.events, ts("2027-01-01 00:00")) == plan.events[-1]


def test_every_defect_type_is_injected_and_recorded(tmp_path):
    _, ledger, _ = simulate(tmp_path, "defects", (3,))

    by_type = Counter(defect.defect_type for defect in ledger.defects())
    assert set(by_type) == {
        "duplicate_order",
        "null_customer_id",
        "future_order_date",
        "orphan_order_item",
        "non_positive_amount",
        "late_arriving_update",
        "schema_drift",
    }
    assert by_type["schema_drift"] == 1


def test_no_defects_are_injected_when_the_rate_is_zero(tmp_path):
    store, ledger, _ = simulate(tmp_path, "clean", (1,), defect_rate=0)

    assert ledger.defects() == []
    assert all(order["customer_id"] is not None for order in store.tables["orders"].values())
    assert all(payment["amount"] > 0 for payment in store.tables["payments"].values())


def test_recorded_defects_describe_the_damaged_rows(tmp_path):
    store, ledger, _ = simulate(tmp_path, "damage", (3,))
    orders, items, payments = (store.tables[t] for t in ("orders", "order_items", "payments"))
    keys = ledger.keys_by_type()

    assert all(orders[int(key)]["customer_id"] is None for key in keys["null_customer_id"])
    assert all(payments[int(key)]["amount"] <= 0 for key in keys["non_positive_amount"])
    assert all(items[int(key)]["order_id"] not in orders for key in keys["orphan_order_item"])
    for key in keys["future_order_date"]:
        order = orders[int(key)]
        assert order["order_ts"] > order["created_at"] + timedelta(days=1)
    numbers = Counter(order["order_number"] for order in orders.values())
    assert all(numbers[orders[int(key)]["order_number"]] == 2 for key in keys["duplicate_order"])


def test_defective_orders_never_progress_through_the_lifecycle(tmp_path):
    store, ledger, _ = simulate(tmp_path, "frozen", (4,))

    for order_id in ledger.frozen_order_ids():
        assert store.tables["orders"][order_id]["order_status"] == "placed"


def test_late_updates_are_stamped_before_the_start_of_their_day(tmp_path):
    store, ledger, _ = simulate(tmp_path, "late", (1,))
    day_one_start = config().day_start(1)

    late = ledger.defects("late_arriving_update")

    assert late
    for defect in late:
        updated_at = store.tables["orders"][int(defect.record_key)]["updated_at"]
        assert day_one_start - timedelta(hours=1) < updated_at < day_one_start


def test_schema_drift_adds_the_column_on_the_configured_day(tmp_path):
    store, ledger, simulator = simulate(tmp_path, "drift", (DRIFT_DAY - 1,))
    assert "gift_wrap" not in store.columns["order_items"]

    simulator.advance(1)

    assert store.columns["order_items"]["gift_wrap"] == "boolean"
    drift = ledger.defects("schema_drift")
    assert [(d.sim_day, d.record_key) for d in drift] == [(DRIFT_DAY, "order_items.gift_wrap")]
    newest_item = max(store.tables["order_items"].values(), key=lambda row: row["created_at"])
    assert isinstance(newest_item["gift_wrap"], bool)


def test_keys_by_type_can_be_limited_to_processed_days(tmp_path):
    _, ledger, _ = simulate(tmp_path, "through", (3,))

    assert "schema_drift" not in ledger.keys_by_type(through_day=DRIFT_DAY - 1)
    assert "schema_drift" in ledger.keys_by_type(through_day=DRIFT_DAY)


def test_advance_refuses_to_continue_a_simulation_seeded_differently(tmp_path):
    store, ledger, _ = simulate(tmp_path, "mismatch")

    with pytest.raises(StateMismatchError, match="seed 42"):
        MarketplaceSimulator(store, ledger, config(seed=99)).advance(1)


def test_advance_requires_a_seeded_simulation(tmp_path):
    simulator = MarketplaceSimulator(MemoryStore(), Ledger(tmp_path / "empty.sqlite"), config())

    with pytest.raises(StateMismatchError, match="Run `seed` first"):
        simulator.advance(1)


def test_invalid_quantity_is_off_by_default(tmp_path):
    store, ledger, _ = simulate(tmp_path, "qty_off", (2,))

    assert ledger.defects("invalid_quantity") == []
    assert all(item["quantity"] > 0 for item in store.tables["order_items"].values())


def test_invalid_quantity_is_recorded_and_present_in_the_source(tmp_path):
    store, ledger, _ = simulate(tmp_path, "qty_on", (2,), invalid_quantity_rate=0.05)

    recorded = ledger.defects("invalid_quantity")
    assert recorded
    items = store.tables["order_items"]
    assert all(items[int(defect.record_key)]["quantity"] <= 0 for defect in recorded)


def test_invalid_quantity_does_not_change_the_other_defects(tmp_path):
    def other_defects(name: str, rate: float) -> list[str]:
        _, ledger, _ = simulate(tmp_path, name, (2,), invalid_quantity_rate=rate)
        return [str(d) for d in ledger.defects() if d.defect_type != "invalid_quantity"]

    assert other_defects("others_off", 0.0) == other_defects("others_on", 0.05)


def test_invalid_quantity_is_deterministic(tmp_path):
    first = simulate(tmp_path, "qty_a", (2,), invalid_quantity_rate=0.05)
    second = simulate(tmp_path, "qty_b", (2,), invalid_quantity_rate=0.05)

    assert fingerprint(*first[:2]) == fingerprint(*second[:2])
