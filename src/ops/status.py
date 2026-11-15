"""Where the simulation and the pipeline currently stand."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import SparkSession

from marketplace.lake import scalar, table_exists
from marketplace.ops.runs import RUNS_TABLE, SUCCESS

# A batch counts as completed once its Gold quality gate has passed.
_COMPLETION_LAYER = "dq_gold"


@dataclass(frozen=True)
class PipelineStatus:
    """Simulated dates available in the sources and how far the pipeline has got."""

    simulated_dates: tuple[date, ...]
    latest_completed_batch: date | None

    @property
    def latest_simulated_date(self) -> date | None:
        return self.simulated_dates[-1] if self.simulated_dates else None

    @property
    def pending_dates(self) -> tuple[date, ...]:
        """Simulated dates the pipeline has not completed yet, oldest first."""
        done = self.latest_completed_batch
        return tuple(day for day in self.simulated_dates if done is None or day > done)


def latest_completed_batch(spark: SparkSession) -> date | None:
    """Logical date of the newest batch that passed the Gold quality gate."""
    if not table_exists(spark, RUNS_TABLE):
        return None
    batch_id = scalar(
        spark,
        f"SELECT max(batch_id) FROM {RUNS_TABLE} "
        f"WHERE layer = '{_COMPLETION_LAYER}' AND status = '{SUCCESS}'",
    )
    return date.fromisoformat(batch_id) if batch_id else None


def pipeline_status(
    spark: SparkSession, sim_start_date: date, current_sim_day: int | None
) -> PipelineStatus:
    """Combine the generator's position with the pipeline's run log."""
    days = range(current_sim_day + 1) if current_sim_day is not None else ()
    return PipelineStatus(
        simulated_dates=tuple(sim_start_date + timedelta(days=day) for day in days),
        latest_completed_batch=latest_completed_batch(spark),
    )
