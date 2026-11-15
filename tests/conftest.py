"""Shared fixtures: a local Spark session and throwaway Iceberg catalogs.

Tests never touch MinIO or the JDBC catalog. Each test that needs tables gets
its own file-system catalog in a temporary directory, made the session default
so production code can keep using two-part names such as `bronze.orders`.
"""

from __future__ import annotations

import dataclasses
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from marketplace.config import Settings
from marketplace.lake import ensure_namespaces
from marketplace.spark import build_spark, catalog_conf

os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()


@pytest.fixture(scope="session")
def settings(tmp_path_factory: pytest.TempPathFactory) -> Settings:
    """Settings for a container-free run: file-system catalog, no object store."""
    base = tmp_path_factory.mktemp("lakehouse")
    return dataclasses.replace(
        Settings.from_env(),
        catalog_type="hadoop",
        catalog_uri="",
        warehouse=str(base / "warehouse"),
        export_root=str(base / "exports"),
        s3_endpoint="",
        ledger_path=base / "ledger.sqlite",
        spark_master="local[1]",
        spark_shuffle_partitions=1,
        generator_seed=42,
        defect_rate=0.02,
        schema_drift_day=2,
    )


@pytest.fixture(scope="session")
def spark(settings: Settings) -> Iterator[SparkSession]:
    session = build_spark(settings, app_name="marketplace-tests")
    yield session
    session.stop()


def use_fresh_catalog(spark: SparkSession, settings: Settings, warehouse: Path) -> str:
    """Register an empty Iceberg catalog, make it the session default and return its name."""
    name = f"lake_{uuid.uuid4().hex[:10]}"
    catalog_settings = dataclasses.replace(settings, warehouse=str(warehouse))
    for key, value in catalog_conf(catalog_settings, name).items():
        spark.conf.set(key, value)
    spark.sql(f"USE {name}")
    ensure_namespaces(spark)
    return name


@pytest.fixture
def lake(spark: SparkSession, settings: Settings, tmp_path: Path) -> SparkSession:
    """A Spark session pointed at an empty lakehouse that only this test sees."""
    use_fresh_catalog(spark, settings, tmp_path / "warehouse")
    return spark
