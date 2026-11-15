# 6. PostgreSQL schema `dw` as the serving warehouse

Status: accepted

## Context

Dashboards need low-latency SQL over the Gold star schema. In production this is the job of a
columnar warehouse such as Redshift, BigQuery or Snowflake. None of those run on a laptop without
an account, which the project rules out.

## Decision

Publish Gold to a PostgreSQL schema named `dw`. The publish step mirrors how a cloud warehouse is
loaded: export each table to Parquet in the object store, bulk-load it with `COPY` into a staging
table, build keys and indexes, then swap staging in for the live table inside one transaction.

## Consequences

- Metabase connects with a stock driver and the analytics queries are standard SQL.
- The load pattern (object store export, bulk copy, atomic swap) carries over to Redshift
  unchanged in shape; only the `COPY` syntax and the distribution and sort keys differ.
- PostgreSQL is a row store. Wide scans over the 340k-row fact are fast enough here, but this
  choice would not hold at warehouse scale; it stands in for a columnar engine, it does not
  replace one.
- Every batch reloads each table in full. That is simple and atomic, and wasteful once facts are
  large; an incremental load by order month is the obvious next step and is listed under
  limitations.
- Trino or DuckDB reading Iceberg directly would avoid the copy altogether. They were not used
  because the brief asks for a warehouse load step, and because a second query engine adds
  memory pressure on an 8 GB machine.
