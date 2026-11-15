"""The unit of work of the pipeline: one logical date."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta


@dataclass(frozen=True)
class Batch:
    """One pipeline run for one logical date.

    The batch covers source changes up to the end of `batch_date` (UTC). Two
    runs for the same date share a `batch_id` and therefore replace each
    other's output; `run_id` tells them apart in the operational tables.
    """

    batch_date: date
    run_id: str

    @property
    def batch_id(self) -> str:
        return self.batch_date.isoformat()

    @property
    def high_watermark(self) -> datetime:
        """Exclusive upper bound of the batch: midnight UTC after `batch_date`."""
        day = self.batch_date
        return datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)


def new_run_id(prefix: str = "cli") -> str:
    """A unique, sortable run id for runs started outside the scheduler."""
    return f"{prefix}__{datetime.now(UTC):%Y%m%dT%H%M%S%f}"
