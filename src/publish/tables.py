"""The list of tables published to the serving warehouse, read from `config/publish.yml`."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

ANALYTICS = "analytics"
OPERATIONS = "operations"


@dataclass(frozen=True)
class PublishedTable:
    """One lakehouse table and how it should look in the warehouse."""

    source: str
    target: str
    group: str
    primary_key: tuple[str, ...]
    indexes: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class PublishConfig:
    schema: str
    tables: tuple[PublishedTable, ...]

    def in_group(self, group: str) -> tuple[PublishedTable, ...]:
        return tuple(table for table in self.tables if table.group == group)


def load_publish_config(config_dir: Path) -> PublishConfig:
    """Read the publish configuration."""
    document = yaml.safe_load((config_dir / "publish.yml").read_text(encoding="utf-8"))
    tables = tuple(
        PublishedTable(
            source=entry["source"],
            target=entry["target"],
            group=entry["group"],
            primary_key=tuple(entry.get("primary_key", ())),
            indexes=tuple(tuple(columns) for columns in entry.get("indexes", ())),
        )
        for entry in document["tables"]
    )
    return PublishConfig(schema=document["schema"], tables=tables)
