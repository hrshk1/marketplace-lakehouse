# 1. Apache Iceberg as the lakehouse table format

Status: accepted

## Context

Bronze, Silver and Gold live as files in an object store. Plain Parquet folders would be enough to
read the data, but the pipeline needs more than reads:

- a batch must be replaceable in one atomic step, so a re-run never leaves duplicates or a
  half-written table;
- Silver and the SCD2 dimension need row-level `MERGE`;
- a new source column must be addable without rewriting existing files;
- operators need a way to look at, and go back to, an earlier state of a table.

## Decision

Use Apache Iceberg (format version 2) for every lakehouse table, accessed through Spark with the
Iceberg SQL extensions.

## Consequences

- Batch replacement is a partition overwrite or `MERGE` committed as one snapshot.
- Schema evolution is a metadata change (`ALTER TABLE ... ADD COLUMN`); old files stay valid.
- Hidden partitioning (`months(order_ts)`) lets queries filter on `order_ts` without knowing how
  the table is laid out.
- Snapshots give time travel and rollback, at the price of maintenance: without compaction and
  snapshot expiry, small files and metadata pile up. That is why `iceberg_maintenance` exists.
- Delta Lake and Apache Hudi would have met the same needs. Iceberg was chosen because its catalog
  and file layout are engine-neutral, which keeps the door open to Trino or Athena as query
  engines later.
