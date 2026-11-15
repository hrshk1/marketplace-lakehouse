"""Configuration and small value types shared across the generator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from marketplace.config import Settings


@dataclass(frozen=True)
class Profile:
    """Scale of the initial state created by `seed`."""

    name: str
    customers: int
    sellers: int
    products: int
    orders: int
    history_days: int

    @property
    def orders_per_day(self) -> int:
        return max(1, self.orders // self.history_days)


PROFILES: dict[str, Profile] = {
    "default": Profile("default", 20_000, 500, 5_000, 200_000, 180),
    "small": Profile("small", 300, 20, 150, 1_500, 30),
}


@dataclass(frozen=True)
class GeneratorConfig:
    """Everything that determines the generated data. Same config, same data."""

    seed: int
    start_date: date
    profile: Profile
    defect_rate: float
    schema_drift_day: int

    @classmethod
    def from_settings(cls, settings: Settings, profile: Profile) -> GeneratorConfig:
        return cls(
            seed=settings.generator_seed,
            start_date=settings.sim_start_date,
            profile=profile,
            defect_rate=settings.defect_rate,
            schema_drift_day=settings.schema_drift_day,
        )

    def day_start(self, day: int) -> datetime:
        """Midnight UTC at the start of simulated day `day` (day 0 is `start_date`)."""
        start = datetime(
            self.start_date.year, self.start_date.month, self.start_date.day, tzinfo=UTC
        )
        return start + timedelta(days=day)

    def day_end(self, day: int) -> datetime:
        """The instant simulated day `day` ends, which is when the next one starts."""
        return self.day_start(day + 1)


@dataclass(frozen=True)
class Defect:
    """One deliberately injected data problem, as recorded in the ledger."""

    sim_day: int
    defect_type: str
    table_name: str
    record_key: str
    detail: str


@dataclass
class DaySummary:
    """Row counts produced while simulating one day (or the seed)."""

    day: int
    inserted: dict[str, int]
    updated: dict[str, int]
    defects: dict[str, int]
