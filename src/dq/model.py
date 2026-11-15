"""Value types of the data quality framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

WARN = "warn"
ERROR = "error"
SEVERITIES = (WARN, ERROR)

PASSED = "pass"
WARNED = "warn"
FAILED = "fail"


@dataclass(frozen=True)
class CheckSpec:
    """One declared check, as read from the YAML configuration."""

    check_id: str
    layer: str
    table: str
    check_type: str
    severity: str
    threshold: float
    why: str
    key: tuple[str, ...]
    row_filter: str | None
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def relation(self) -> str:
        """Fully qualified name of the table the check reads."""
        return f"{self.layer}.{self.table}"


@dataclass(frozen=True)
class CheckResult:
    """Outcome of running one check for one batch."""

    spec: CheckSpec
    status: str
    observed_value: float | None
    failed_rows: int | None
    rows_checked: int | None
    details: str

    @property
    def blocks_pipeline(self) -> bool:
        return self.status == FAILED


class DataQualityError(RuntimeError):
    """Raised when at least one error-severity check fails for a layer."""
