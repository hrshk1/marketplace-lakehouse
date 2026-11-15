"""Provision Metabase from the dashboard definitions in `analytics/dashboards/`.

Everything is looked up by name before it is created, so running the
provisioning again updates the existing database connection, questions and
dashboards instead of duplicating them.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml

from marketplace.config import Settings
from marketplace.logging_setup import get_logger

log = get_logger("bi")

DATABASE_NAME = "Marketplace Warehouse"
COLLECTION_NAME = "Marketplace Lakehouse"
GRID_WIDTH = 24
_READY_TIMEOUT_SECONDS = 300
_REQUEST_TIMEOUT_SECONDS = 60


class MetabaseError(RuntimeError):
    """Metabase rejected a request or did not become ready."""


@dataclass(frozen=True)
class CardSpec:
    """One question on a dashboard: a native SQL query and how to draw it."""

    name: str
    display: str
    sql: str
    width: int
    height: int
    visualization: dict[str, Any]


@dataclass(frozen=True)
class DashboardSpec:
    name: str
    description: str
    cards: tuple[CardSpec, ...]


def load_dashboards(dashboards_dir: Path) -> list[DashboardSpec]:
    """Read every dashboard definition, in file-name order."""
    dashboards = []
    for path in sorted(dashboards_dir.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        cards = tuple(
            CardSpec(
                name=card["name"],
                display=card["display"],
                sql=card["sql"],
                width=card["width"],
                height=card["height"],
                visualization=card.get("visualization", {}),
            )
            for card in document["cards"]
        )
        dashboards.append(DashboardSpec(document["name"], document["description"], cards))
    return dashboards


def layout(cards: tuple[CardSpec, ...]) -> list[tuple[int, int]]:
    """Place cards left to right, wrapping to a new row when the grid is full.

    Returns the (row, column) of each card's top-left corner.
    """
    positions = []
    row = column = row_height = 0
    for card in cards:
        if column + card.width > GRID_WIDTH:
            row += row_height
            column = row_height = 0
        positions.append((row, column))
        column += card.width
        row_height = max(row_height, card.height)
    return positions


class MetabaseClient:
    """Minimal client for the parts of the Metabase API that provisioning needs."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()

    def request(self, method: str, path: str, payload: dict | None = None) -> Any:
        response = self._session.request(
            method, f"{self._base_url}{path}", json=payload, timeout=_REQUEST_TIMEOUT_SECONDS
        )
        if response.status_code >= 400:
            raise MetabaseError(
                f"{method} {path} failed with {response.status_code}: {response.text[:500]}"
            )
        return response.json() if response.content else None

    def wait_until_ready(self) -> None:
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                if self.request("GET", "/api/health").get("status") == "ok":
                    return
            except (requests.RequestException, MetabaseError):
                pass
            time.sleep(5)
        raise MetabaseError(f"Metabase at {self._base_url} did not become ready in time")

    def sign_in(self, email: str, password: str) -> None:
        """Create the admin user on a fresh instance, then open a session."""
        properties = self.request("GET", "/api/session/properties")
        if not properties["has-user-setup"]:
            self.request(
                "POST",
                "/api/setup",
                {
                    "token": properties["setup-token"],
                    "user": {
                        "email": email,
                        "password": password,
                        "first_name": "Lakehouse",
                        "last_name": "Admin",
                        "site_name": COLLECTION_NAME,
                    },
                    "prefs": {"site_name": COLLECTION_NAME, "allow_tracking": False},
                },
            )
        session = self.request("POST", "/api/session", {"username": email, "password": password})
        self._session.headers["X-Metabase-Session"] = session["id"]


def _find(items: list[dict], name: str) -> dict | None:
    return next((item for item in items if item["name"] == name), None)


def ensure_database(client: MetabaseClient, settings: Settings) -> int:
    """Create or update the connection to the `dw` schema; returns its Metabase id."""
    payload = {
        "name": DATABASE_NAME,
        "engine": "postgres",
        "details": {
            "host": settings.platform_pg_host,
            "port": settings.platform_pg_port,
            "dbname": settings.dw_db,
            "user": settings.platform_pg_user,
            "password": settings.platform_pg_password,
            "ssl": False,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": "dw",
        },
    }
    existing = _find(client.request("GET", "/api/database")["data"], DATABASE_NAME)
    if existing:
        client.request("PUT", f"/api/database/{existing['id']}", payload)
        database_id = existing["id"]
    else:
        database_id = client.request("POST", "/api/database", payload)["id"]
    client.request("POST", f"/api/database/{database_id}/sync_schema")
    return database_id


def ensure_collection(client: MetabaseClient) -> int:
    existing = _find(client.request("GET", "/api/collection"), COLLECTION_NAME)
    if existing:
        return existing["id"]
    return client.request("POST", "/api/collection", {"name": COLLECTION_NAME})["id"]


def ensure_card(
    client: MetabaseClient,
    card: CardSpec,
    database_id: int,
    collection_id: int,
    existing_cards: list[dict],
) -> int:
    payload = {
        "name": card.name,
        "display": card.display,
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {"query": card.sql},
        },
        "visualization_settings": card.visualization,
    }
    existing = _find(existing_cards, card.name)
    if existing:
        client.request("PUT", f"/api/card/{existing['id']}", payload)
        return existing["id"]
    return client.request("POST", "/api/card", payload)["id"]


def ensure_dashboard(
    client: MetabaseClient,
    dashboard: DashboardSpec,
    card_ids: list[int],
    collection_id: int,
    existing_dashboards: list[dict],
) -> int:
    existing = _find(existing_dashboards, dashboard.name)
    dashboard_id = (
        existing["id"]
        if existing
        else client.request(
            "POST", "/api/dashboard", {"name": dashboard.name, "collection_id": collection_id}
        )["id"]
    )
    # Sending the full list replaces whatever cards the dashboard had before.
    # Negative ids tell Metabase these are new placements.
    dashcards = [
        {
            "id": -(index + 1),
            "card_id": card_id,
            "row": row,
            "col": column,
            "size_x": card.width,
            "size_y": card.height,
        }
        for index, (card, card_id, (row, column)) in enumerate(
            zip(dashboard.cards, card_ids, layout(dashboard.cards), strict=True)
        )
    ]
    client.request(
        "PUT",
        f"/api/dashboard/{dashboard_id}",
        {"description": dashboard.description, "dashcards": dashcards},
    )
    return dashboard_id


def provision(settings: Settings) -> dict[str, int]:
    """Set up Metabase end to end; returns dashboard ids keyed by dashboard name."""
    client = MetabaseClient(settings.metabase_url)
    client.wait_until_ready()
    client.sign_in(settings.metabase_admin_email, settings.metabase_admin_password)
    database_id = ensure_database(client, settings)
    collection_id = ensure_collection(client)

    items = client.request("GET", f"/api/collection/{collection_id}/items")["data"]
    existing_cards = [item for item in items if item["model"] == "card"]
    existing_dashboards = [item for item in items if item["model"] == "dashboard"]

    dashboard_ids = {}
    for dashboard in load_dashboards(settings.project_root / "analytics" / "dashboards"):
        card_ids = [
            ensure_card(client, card, database_id, collection_id, existing_cards)
            for card in dashboard.cards
        ]
        dashboard_ids[dashboard.name] = ensure_dashboard(
            client, dashboard, card_ids, collection_id, existing_dashboards
        )
        log.info(
            "dashboard_provisioned",
            extra={"dashboard": dashboard.name, "cards": len(card_ids)},
        )
    return dashboard_ids
