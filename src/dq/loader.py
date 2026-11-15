"""Load and validate check declarations from `config/dq/<layer>.yml`.

A layer file lists checks per table. Checks that every table needs (key not
null, reconciliation) are declared once under `common_checks` and expanded for
each table, with `{table}`, `{key}` and `{source_key}` filled in.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from marketplace.dq.checks import CHECK_TYPES
from marketplace.dq.model import SEVERITIES, CheckSpec

LAYERS = ("bronze", "silver", "gold")
_REQUIRED_KEYS = {"id", "type", "severity", "threshold", "why"}
_SELECTOR_KEY = "except_tables"


class CheckConfigError(ValueError):
    """The YAML declaration of a check is incomplete or inconsistent."""


def _as_tuple(value: str | list[str]) -> tuple[str, ...]:
    return (value,) if isinstance(value, str) else tuple(value)


def _fill(value: Any, placeholders: dict[str, str]) -> Any:
    """Substitute table placeholders in every string of a nested structure."""
    if isinstance(value, str):
        for name, replacement in placeholders.items():
            value = value.replace(f"{{{name}}}", replacement)
        return value
    if isinstance(value, list):
        return [_fill(item, placeholders) for item in value]
    if isinstance(value, dict):
        return {key: _fill(item, placeholders) for key, item in value.items()}
    return value


def _parse_check(layer: str, table: str, table_config: dict[str, Any], raw: dict) -> CheckSpec:
    missing = _REQUIRED_KEYS - raw.keys()
    if missing:
        raise CheckConfigError(f"Check {raw.get('id', '<no id>')} is missing {sorted(missing)}")
    if raw["type"] not in CHECK_TYPES:
        raise CheckConfigError(f"Check {raw['id']}: unknown type {raw['type']!r}")
    if raw["severity"] not in SEVERITIES:
        raise CheckConfigError(f"Check {raw['id']}: unknown severity {raw['severity']!r}")
    return CheckSpec(
        check_id=raw["id"],
        layer=layer,
        table=table,
        check_type=raw["type"],
        severity=raw["severity"],
        threshold=float(raw["threshold"]),
        why=raw["why"],
        key=_as_tuple(table_config["key"]),
        row_filter=table_config.get("filter"),
        params={name: value for name, value in raw.items() if name not in _REQUIRED_KEYS},
    )


def _table_checks(table: str, table_config: dict[str, Any], common: list[dict]) -> list[dict]:
    key = _as_tuple(table_config["key"])[0]
    placeholders = {
        "table": table,
        "key": key,
        "source_key": table_config.get("source_key", key),
    }
    expanded = [
        _fill({name: value for name, value in check.items() if name != _SELECTOR_KEY}, placeholders)
        for check in common
        if table not in check.get(_SELECTOR_KEY, [])
    ]
    return expanded + (table_config.get("checks") or [])


def load_layer_checks(config_dir: Path, layer: str) -> list[CheckSpec]:
    """Return every check declared for a layer, in file order."""
    if layer not in LAYERS:
        raise CheckConfigError(f"Unknown layer {layer!r}; expected one of {LAYERS}")
    document = yaml.safe_load((config_dir / "dq" / f"{layer}.yml").read_text(encoding="utf-8"))
    common = document.get("common_checks") or []

    checks = [
        _parse_check(layer, table, table_config, raw)
        for table, table_config in document["tables"].items()
        for raw in _table_checks(table, table_config, common)
    ]
    ids = [check.check_id for check in checks]
    duplicates = sorted({check_id for check_id in ids if ids.count(check_id) > 1})
    if duplicates:
        raise CheckConfigError(f"Duplicate check ids in {layer}.yml: {duplicates}")
    return checks
