"""Spark session construction for the lakehouse.

The Iceberg catalog is always registered under the name `lake` and made the
session default, so SQL throughout the project uses two-part names such as
`bronze.orders`. Only the catalog backend changes between environments.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from marketplace.config import Settings

CATALOG = "lake"


def catalog_conf(settings: Settings, catalog: str = CATALOG) -> dict[str, str]:
    """Return the Spark properties that register one Iceberg catalog."""
    prefix = f"spark.sql.catalog.{catalog}"
    conf = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.warehouse": settings.warehouse,
    }
    if settings.catalog_type == "jdbc":
        conf.update(
            {
                f"{prefix}.type": "jdbc",
                f"{prefix}.uri": settings.catalog_uri,
                f"{prefix}.jdbc.user": settings.platform_pg_user,
                f"{prefix}.jdbc.password": settings.platform_pg_password,
                f"{prefix}.jdbc.schema-version": "V1",
            }
        )
    elif settings.catalog_type == "hadoop":
        conf[f"{prefix}.type"] = "hadoop"
    else:
        raise ValueError(f"Unsupported ICEBERG_CATALOG_TYPE: {settings.catalog_type!r}")
    return conf


def _s3a_conf(settings: Settings) -> dict[str, str]:
    """S3A properties for an S3-compatible store addressed by path, not by virtual host."""
    return {
        "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint,
        "spark.hadoop.fs.s3a.access.key": settings.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.s3_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
            settings.s3_endpoint.startswith("https")
        ).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }


def build_spark(settings: Settings, app_name: str = "marketplace-lakehouse") -> SparkSession:
    """Create (or reuse) the local-mode Spark session configured for Iceberg."""
    jars = sorted(str(path) for path in settings.spark_jars_dir.glob("*.jar"))
    if not jars:
        raise FileNotFoundError(
            f"No jars found in {settings.spark_jars_dir}. Run `python scripts/fetch_jars.py`."
        )

    conf = {
        "spark.jars": ",".join(jars),
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(settings.spark_shuffle_partitions),
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.defaultCatalog": CATALOG,
        # Microsecond timestamps keep exported Parquet readable by Arrow
        # without the legacy INT96 encoding.
        "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
        **catalog_conf(settings),
    }
    if settings.s3_endpoint:
        conf.update(_s3a_conf(settings))

    builder = SparkSession.builder.master(settings.spark_master).appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # py4j logs every gateway call at INFO, which drowns the pipeline's own logs.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    return spark
