"""SQLite ledger of injected defects and of the simulator's own position.

The ledger is the answer key for the data quality framework: every defect the
generator plants is written here, so detection can be scored against it.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from marketplace.generator.model import Defect

_SCHEMA = """
CREATE TABLE IF NOT EXISTS defects (
    defect_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    sim_day     INTEGER NOT NULL,
    defect_type TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    record_key  TEXT NOT NULL,
    detail      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_defects_type ON defects (defect_type);
CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# Defective orders that must never progress through the order lifecycle,
# so their defect stays visible in the source for as long as the data lives.
FROZEN_DEFECT_TYPES = ("duplicate_order", "future_order_date")


class Ledger:
    """Thin wrapper around the ledger database file."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Ledger:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def reset(self) -> None:
        """Forget all defects and simulator state (used when re-seeding)."""
        with self._conn:
            self._conn.execute("DELETE FROM defects")
            self._conn.execute("DELETE FROM state")

    def record(self, defects: Iterable[Defect]) -> None:
        """Append injected defects."""
        rows = [(d.sim_day, d.defect_type, d.table_name, d.record_key, d.detail) for d in defects]
        with self._conn:
            self._conn.executemany(
                "INSERT INTO defects (sim_day, defect_type, table_name, record_key, detail) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )

    def defects(self, defect_type: str | None = None) -> list[Defect]:
        """Return recorded defects in insertion order, optionally of one type."""
        query = "SELECT sim_day, defect_type, table_name, record_key, detail FROM defects"
        params: tuple[str, ...] = ()
        if defect_type is not None:
            query += " WHERE defect_type = ?"
            params = (defect_type,)
        rows = self._conn.execute(query + " ORDER BY defect_id", params).fetchall()
        return [Defect(*row) for row in rows]

    def keys_by_type(self, through_day: int | None = None) -> dict[str, set[str]]:
        """Return the defective record keys for each defect type.

        `through_day` limits the answer to defects injected on or before that
        simulated day, for scoring a pipeline that has not processed later days.
        """
        result: dict[str, set[str]] = {}
        for defect in self.defects():
            if through_day is None or defect.sim_day <= through_day:
                result.setdefault(defect.defect_type, set()).add(defect.record_key)
        return result

    def frozen_order_ids(self) -> set[int]:
        """Order ids whose lifecycle is frozen because the row itself is a defect."""
        placeholders = ", ".join("?" for _ in FROZEN_DEFECT_TYPES)
        rows = self._conn.execute(
            f"SELECT record_key FROM defects WHERE defect_type IN ({placeholders})",
            FROZEN_DEFECT_TYPES,
        ).fetchall()
        return {int(key) for (key,) in rows}

    def get_state(self, key: str, default: str | None = None) -> str | None:
        row = self._conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def set_state(self, key: str, value: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO state (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
