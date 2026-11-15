"""Runtime settings, read once from environment variables.

Every tunable lives here so no module reads `os.environ` on its own. The
defaults describe a container-free setup (file-system Iceberg catalog under
`data/`), which is what the test suite uses; `.env.example` holds the values
for the Docker stack.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

_DEFAULT_ROOT = Path(__file__).resolve().parents[1]


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


@dataclass(frozen=True)
class Settings:
    """All configuration for the generator, the pipeline and the BI provisioning."""

    project_root: Path

    oltp_pg_host: str
    oltp_pg_port: int
    oltp_pg_db: str
    oltp_pg_user: str
    oltp_pg_password: str

    mongo_uri: str
    mongo_db: str

    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str

    catalog_type: str
    catalog_uri: str
    warehouse: str
    export_root: str

    platform_pg_host: str
    platform_pg_port: int
    platform_pg_user: str
    platform_pg_password: str
    dw_db: str

    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: int
    spark_jars_dir: Path

    generator_seed: int
    sim_start_date: date
    defect_rate: float
    schema_drift_day: int
    ledger_path: Path

    extract_lookback_minutes: int

    metabase_url: str
    metabase_admin_email: str
    metabase_admin_password: str

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from the process environment, applying local defaults."""
        root = Path(_env("PROJECT_ROOT", str(_DEFAULT_ROOT)))
        data_dir = root / "data"
        return cls(
            project_root=root,
            oltp_pg_host=_env("OLTP_PG_HOST", "localhost"),
            oltp_pg_port=int(_env("OLTP_PG_PORT", "5432")),
            oltp_pg_db=_env("OLTP_PG_DB", "marketplace"),
            oltp_pg_user=_env("OLTP_PG_USER", "oltp"),
            oltp_pg_password=_env("OLTP_PG_PASSWORD", ""),
            mongo_uri=_env("MONGO_URI", "mongodb://localhost:27017"),
            mongo_db=_env("MONGO_DB", "marketplace"),
            s3_endpoint=_env("S3_ENDPOINT", ""),
            s3_access_key=_env("S3_ACCESS_KEY", ""),
            s3_secret_key=_env("S3_SECRET_KEY", ""),
            catalog_type=_env("ICEBERG_CATALOG_TYPE", "hadoop"),
            catalog_uri=_env("ICEBERG_CATALOG_URI", ""),
            warehouse=_env("ICEBERG_WAREHOUSE", str(data_dir / "warehouse")),
            export_root=_env("EXPORT_ROOT", str(data_dir / "exports")),
            platform_pg_host=_env("PLATFORM_PG_HOST", "localhost"),
            platform_pg_port=int(_env("PLATFORM_PG_PORT", "5432")),
            platform_pg_user=_env("PLATFORM_PG_USER", "platform"),
            platform_pg_password=_env("PLATFORM_PG_PASSWORD", ""),
            dw_db=_env("DW_PG_DB", "warehouse"),
            spark_master=_env("SPARK_MASTER", "local[2]"),
            spark_driver_memory=_env("SPARK_DRIVER_MEMORY", "1g"),
            spark_shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "4")),
            spark_jars_dir=Path(_env("SPARK_JARS_DIR", str(root / "jars"))),
            generator_seed=int(_env("GENERATOR_SEED", "42")),
            sim_start_date=date.fromisoformat(_env("SIM_START_DATE", "2026-06-30")),
            defect_rate=float(_env("DEFECT_RATE", "0.005")),
            schema_drift_day=int(_env("SCHEMA_DRIFT_DAY", "2")),
            ledger_path=Path(_env("LEDGER_PATH", str(data_dir / "generator_ledger.sqlite"))),
            extract_lookback_minutes=int(_env("EXTRACT_LOOKBACK_MINUTES", "60")),
            metabase_url=_env("METABASE_URL", "http://localhost:3000"),
            metabase_admin_email=_env("METABASE_ADMIN_EMAIL", "admin@marketplace.local"),
            metabase_admin_password=_env("METABASE_ADMIN_PASSWORD", ""),
        )

    @property
    def oltp_conninfo(self) -> str:
        """libpq connection string for the OLTP source database."""
        return (
            f"host={self.oltp_pg_host} port={self.oltp_pg_port} dbname={self.oltp_pg_db} "
            f"user={self.oltp_pg_user} password={self.oltp_pg_password}"
        )

    @property
    def oltp_jdbc_url(self) -> str:
        """JDBC URL Spark uses to read the OLTP source database."""
        return f"jdbc:postgresql://{self.oltp_pg_host}:{self.oltp_pg_port}/{self.oltp_pg_db}"

    @property
    def dw_conninfo(self) -> str:
        """libpq connection string for the serving warehouse database."""
        return (
            f"host={self.platform_pg_host} port={self.platform_pg_port} dbname={self.dw_db} "
            f"user={self.platform_pg_user} password={self.platform_pg_password}"
        )

    @property
    def config_dir(self) -> Path:
        """Directory holding the declarative YAML configuration."""
        return self.project_root / "config"
