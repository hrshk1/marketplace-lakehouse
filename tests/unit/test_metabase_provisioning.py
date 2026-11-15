"""Metabase provisioning: dashboard definitions, layout and idempotent upserts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from marketplace.bi import metabase
from marketplace.bi.metabase import CardSpec, DashboardSpec, layout, load_dashboards
from marketplace.config import Settings

DASHBOARDS_DIR = Path(__file__).resolve().parents[2] / "analytics" / "dashboards"


def card(name: str, width: int, height: int = 4) -> CardSpec:
    return CardSpec(name, "table", f"SELECT '{name}'", width, height, {})


def test_layout_fills_rows_left_to_right_and_wraps_at_the_grid_width():
    cards = (
        card("a", 12, 4),
        card("b", 12, 6),
        card("c", 16, 3),
        card("d", 10, 3),
        card("e", 8, 3),
    )

    # Row 1: a and b (12 + 12). Row 2 starts below the taller card b.
    # d does not fit next to c (16 + 10 > 24), so it starts a third row with e.
    assert layout(cards) == [(0, 0), (0, 12), (6, 0), (9, 0), (9, 10)]


def test_shipped_dashboards_cover_sales_customers_and_pipeline_health():
    dashboards = load_dashboards(DASHBOARDS_DIR)

    assert [d.name for d in dashboards] == [
        "Sales Overview",
        "Customer and Retention",
        "Pipeline Health",
    ]
    for dashboard in dashboards:
        assert dashboard.description
        assert len(dashboard.cards) >= 4
        names = [c.name for c in dashboard.cards]
        assert len(names) == len(set(names))
        for spec in dashboard.cards:
            assert 1 <= spec.width <= metabase.GRID_WIDTH
            assert "dw." in spec.sql


def test_card_names_are_unique_across_dashboards():
    names = [c.name for d in load_dashboards(DASHBOARDS_DIR) for c in d.cards]

    assert len(names) == len(set(names))


class FakeMetabase:
    """In-memory stand-in for the Metabase API, recording what was created or updated."""

    def __init__(self) -> None:
        self.databases: list[dict] = []
        self.collections: list[dict] = []
        self.cards: list[dict] = []
        self.dashboards: list[dict] = []
        self.calls: list[tuple[str, str]] = []
        self.signed_in_as: str | None = None

    def wait_until_ready(self) -> None:
        pass

    def sign_in(self, email: str, password: str) -> None:
        self.signed_in_as = email

    def _create(self, store: list[dict], payload: dict, **extra: Any) -> dict:
        item = {**payload, **extra, "id": len(store) + 1}
        store.append(item)
        return item

    def request(self, method: str, path: str, payload: dict | None = None) -> Any:
        self.calls.append((method, path))
        parts = path.strip("/").split("/")[1:]
        resource = parts[0]
        store = {
            "database": self.databases,
            "collection": self.collections,
            "card": self.cards,
            "dashboard": self.dashboards,
        }[resource]
        if method == "GET" and parts[-1] == "items":
            return {"data": [*self.cards, *self.dashboards]}
        if method == "GET":
            return {"data": store} if resource == "database" else store
        if method == "POST" and len(parts) == 1:
            model = {"model": resource} if resource in ("card", "dashboard") else {}
            return self._create(store, payload, **model)
        if method == "PUT":
            existing = next(item for item in store if item["id"] == int(parts[1]))
            existing.update(payload)
            return existing
        return None


@pytest.fixture
def fake(monkeypatch) -> FakeMetabase:
    fake = FakeMetabase()
    monkeypatch.setattr(metabase, "MetabaseClient", lambda base_url: fake)
    return fake


def test_provisioning_creates_connection_questions_and_dashboards(fake):
    settings = Settings.from_env()
    dashboards = load_dashboards(DASHBOARDS_DIR)

    result = metabase.provision(settings)

    assert fake.signed_in_as == settings.metabase_admin_email
    assert [db["name"] for db in fake.databases] == ["Marketplace Warehouse"]
    assert fake.databases[0]["details"]["schema-filters-patterns"] == "dw"
    assert ("POST", "/api/database/1/sync_schema") in fake.calls
    assert len(fake.cards) == sum(len(d.cards) for d in dashboards)
    assert set(result) == {d.name for d in dashboards}
    sales = next(d for d in fake.dashboards if d["name"] == "Sales Overview")
    assert len(sales["dashcards"]) == len(dashboards[0].cards)
    assert all(c["dataset_query"]["database"] == 1 for c in fake.cards)


def test_provisioning_twice_updates_in_place_instead_of_duplicating(fake):
    settings = Settings.from_env()
    first = metabase.provision(settings)
    counts = (len(fake.databases), len(fake.collections), len(fake.cards), len(fake.dashboards))

    second = metabase.provision(settings)

    assert second == first
    assert (
        len(fake.databases),
        len(fake.collections),
        len(fake.cards),
        len(fake.dashboards),
    ) == counts


def test_dashboard_placements_follow_the_layout():
    spec = DashboardSpec("Demo", "demo", (card("a", 16), card("b", 16)))
    fake = FakeMetabase()

    dashboard_id = metabase.ensure_dashboard(
        fake, spec, [7, 8], collection_id=1, existing_dashboards=[]
    )

    placed = fake.dashboards[dashboard_id - 1]["dashcards"]
    assert [(p["card_id"], p["row"], p["col"], p["size_x"]) for p in placed] == [
        (7, 0, 0, 16),
        (8, 4, 0, 16),
    ]
