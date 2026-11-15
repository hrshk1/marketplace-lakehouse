# 8. Spark in local mode, and what that limits

Status: accepted

## Context

The whole stack must fit in about 8 GB of memory given to Docker, next to two source databases,
an object store, three Airflow services and Metabase. A standalone Spark cluster needs a master
and at least one worker, each a JVM with its own overhead.

## Decision

Run Spark 3.5.9 in local mode: driver and executor share one JVM inside the process that runs the
step (an Airflow task or the `tools` container). The driver gets 1 GB and two cores, with four
shuffle partitions.

## Consequences

What still holds in local mode:

- The code is the same PySpark and Spark SQL that would run on a cluster. Moving to EMR or Glue
  changes the master URL and the configuration, not the transformations.
- Iceberg commits, `MERGE` and hidden partitioning behave as they do on a cluster.

What local mode cannot show:

- No real shuffle across machines, so nothing here proves anything about skew, spill or network
  cost.
- No executor failures, so retries and speculative execution are never exercised.
- All data for a stage must fit in one JVM. The default profile (about 800k source rows) uses a
  fraction of the heap; at 100 times that volume the driver would run out of memory.
- Every Airflow task pays a JVM start of several seconds, which dominates run time at this data
  size. On a cluster the same step would submit to a long-lived application.
- One task at a time. Parallel extraction of the nine source tables, which the DAG allows, is
  switched off by `AIRFLOW__CORE__PARALLELISM=1`.

Small sources are also read without partitioning: the JDBC reader uses a single connection per
table. With a cluster, `partitionColumn` on the primary key would parallelise large tables.
