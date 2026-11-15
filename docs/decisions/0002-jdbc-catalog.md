# 2. JDBC catalog backed by PostgreSQL

Status: accepted

## Context

An Iceberg catalog stores one thing per table: a pointer to the current metadata file. Commits
are safe only if swapping that pointer is atomic. The options that run locally without extra
licences are:

- Hadoop catalog: the pointer is a file. Atomic rename is not guaranteed on S3-compatible object
  stores, so concurrent commits can lose data.
- Hive Metastore: works, but needs a metastore service and its own database.
- REST catalog: the cleanest interface, but one more service to run and size.
- JDBC catalog: the pointer is a row in a relational database, swapped with a compare-and-set
  `UPDATE`.

## Decision

Use the Iceberg JDBC catalog with a dedicated PostgreSQL database (`iceberg_catalog`) on the
platform Postgres server. The test suite uses the Hadoop catalog on the local file system, where
rename is atomic and no service is needed.

## Consequences

- Commits are atomic and safe under concurrency, with no extra container.
- The catalog database is small but critical: losing it loses the pointers to every table, so it
  must be backed up together with the bucket.
- The catalog is selected by configuration (`ICEBERG_CATALOG_TYPE`); pipeline code never names it.
- On AWS this maps to the Glue Data Catalog, which plays the same role.
