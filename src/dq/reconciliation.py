"""Score defect detection against the generator's ledger.

The generator records every defect it injects. This module collects what the
pipeline flagged (quarantine rules, failed checks, logged schema changes) and
reports precision and recall per defect type.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession

from marketplace.bronze.schema_evolution import SCHEMA_CHANGES_TABLE
from marketplace.dq.runner import FAILED_ROWS_TABLE
from marketplace.lake import table_exists


@dataclass(frozen=True)
class DefectScore:
    """Detection quality for one defect type."""

    defect_type: str
    injected: int
    detected: int
    true_positives: int

    @property
    def precision(self) -> float | None:
        """Share of flagged records that really were injected defects."""
        return self.true_positives / self.detected if self.detected else None

    @property
    def recall(self) -> float | None:
        """Share of injected defects that were flagged."""
        return self.true_positives / self.injected if self.injected else None


def load_detectors(config_dir: Path) -> dict[str, dict[str, Any]]:
    """Read which quarantine rule or check is responsible for each defect type."""
    path = config_dir / "dq" / "defect_detectors.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["detectors"]


def _keys(spark: SparkSession, table: str, query: str) -> set[str]:
    if not table_exists(spark, table):
        return set()
    return {row[0] for row in spark.sql(query).collect()}


def detected_keys(spark: SparkSession, detector: dict[str, Any]) -> set[str]:
    """Record keys the pipeline flagged through one detector."""
    kind = detector["kind"]
    if kind == "quarantine":
        table = f"silver_quarantine.{detector['table']}"
        return _keys(
            spark,
            table,
            f"SELECT DISTINCT record_key FROM {table} WHERE rule_id = '{detector['rule_id']}'",
        )
    if kind == "check":
        return _keys(
            spark,
            FAILED_ROWS_TABLE,
            f"SELECT DISTINCT record_key FROM {FAILED_ROWS_TABLE} "
            f"WHERE check_id = '{detector['check_id']}'",
        )
    if kind == "schema_change":
        return _keys(
            spark,
            SCHEMA_CHANGES_TABLE,
            f"SELECT DISTINCT concat(table_name, '.', column_name) FROM {SCHEMA_CHANGES_TABLE} "
            f"WHERE change_type = '{detector['change_type']}'",
        )
    raise ValueError(f"Unknown detector kind: {kind!r}")


def score_detection(
    spark: SparkSession,
    injected_by_type: dict[str, set[str]],
    detectors: dict[str, dict[str, Any]],
) -> list[DefectScore]:
    """Compare injected and detected keys for every defect type that has a detector."""
    scores = []
    for defect_type, detector in detectors.items():
        injected = injected_by_type.get(defect_type, set())
        detected = detected_keys(spark, detector)
        scores.append(
            DefectScore(defect_type, len(injected), len(detected), len(injected & detected))
        )
    return scores


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def render_markdown(scores: list[DefectScore]) -> str:
    """Render the scores as a Markdown table, with a totals row."""
    total = DefectScore(
        "all types",
        sum(s.injected for s in scores),
        sum(s.detected for s in scores),
        sum(s.true_positives for s in scores),
    )
    lines = [
        "| Defect type | Injected | Detected | True positives | Precision | Recall |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    lines += [
        f"| {s.defect_type} | {s.injected} | {s.detected} | {s.true_positives} "
        f"| {_percent(s.precision)} | {_percent(s.recall)} |"
        for s in [*scores, total]
    ]
    return "\n".join(lines) + "\n"


def write_report(scores: list[DefectScore], reports_dir: Path) -> None:
    """Save the scores as Markdown and JSON under `reports/`."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "defect_reconciliation.md").write_text(render_markdown(scores), encoding="utf-8")
    payload = [{**asdict(s), "precision": s.precision, "recall": s.recall} for s in scores]
    (reports_dir / "defect_reconciliation.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
