# Operations

All commands run from the repository root on the host. They need Python and Docker only.

## Everyday commands

| Command | What it does |
| --- | --- |
| `python scripts/dev.py up` | Build the image if needed and start every service |
| `python scripts/dev.py down` | Stop the services and keep their data |
| `python scripts/dev.py reset` | Stop the services and delete all volumes and local state |
| `python scripts/dev.py seed [--small]` | Recreate the sources as of day 0 and clear the lakehouse |
| `python scripts/dev.py advance --days N` | Simulate N more days in the sources |
| `python scripts/dev.py run` | Run the pipeline for every simulated date not completed yet |
| `python scripts/dev.py run --date 2026-07-01` | Run (or re-run) one logical date |
| `python scripts/dev.py run --airflow --date 2026-07-01` | Run one logical date through the Airflow DAG |
| `python scripts/dev.py dq` | Re-run all checks for the latest batch and print the defect report |
| `python scripts/dev.py test` | Run the test suite with coverage |
| `python scripts/dev.py lint [--fix]` | Run ruff lint and format checks |
| `python scripts/dev.py demo [--small]` | Run the whole story end to end |

`docker compose run --rm tools python -m marketplace.cli status` shows the simulated dates, the
latest completed batch and the dates still pending. `... marketplace.cli maintain` runs table
maintenance, and `... marketplace.cli provision-bi` creates or updates the Metabase dashboards.

## How a run is recorded

Every step writes one row to `ops.pipeline_runs` (published to `dw.pipeline_runs`):

```sql
SELECT layer, step, status, rows_in, rows_out, rows_quarantined, duration_seconds, sla_breached
FROM dw.pipeline_runs
WHERE batch_id = '2026-07-01'
ORDER BY started_at;
```

A batch counts as completed once its Gold quality gate has passed (`layer = 'dq_gold'`,
`status = 'success'`).

## Backfill

A batch is identified by its logical date, so backfilling is running that date.

One date:

```bash
python scripts/dev.py run --date 2026-07-01
```

Through Airflow, one date or a range:

```bash
docker compose exec airflow-scheduler airflow dags trigger daily_marketplace_pipeline --logical-date 2026-07-01T00:00:00+00:00
```

```bash
docker compose exec airflow-scheduler airflow backfill create --dag-id daily_marketplace_pipeline --from-date 2026-07-01 --to-date 2026-07-05
```

Run dates in ascending order. `max_active_runs=1` enforces this in Airflow. Each batch takes its
lower watermark from the latest earlier batch, so a gap is not an error: the next batch simply
covers a longer window.

What a backfill can and cannot recover:

- Silver and Gold end up correct. Silver keeps the newest version of each row no matter in which
  order batches arrive, and fact partitions are rebuilt from Silver.
- Bronze reflects the source as it is at extraction time. Extraction is a query against a
  mutable database, so re-extracting an old date finds each row in its current state. A row that
  has changed since is no longer inside the old date's window and appears only in the batch that
  covers its latest change. Intermediate versions that were never extracted are gone. This is
  the main cost of watermark extraction (see ADR 3).

## Re-run

Re-running a batch is safe at every layer:

| Layer | Why a re-run does not duplicate or corrupt |
| --- | --- |
| Bronze | The batch overwrites its own `_batch_id` partition in one commit |
| Quarantine | Same: partitioned by `_batch_id` and overwritten |
| Silver | `MERGE` updates only when the incoming `updated_at` is strictly newer |
| Dimensions | `MERGE` by business key; SCD2 compares row hashes, so an unchanged customer is untouched |
| Facts | The order months touched by the batch are replaced as whole partitions |
| Aggregate | Rebuilt from the fact table and swapped in |
| `dw` | Each table is reloaded into staging and swapped in one transaction |

`dq.check_results` and `ops.pipeline_runs` are logs: a re-run appends new rows under a new
`run_id`. Dashboards read the latest run of each batch.

## When a run fails

1. Find the failed step. In Airflow the task is red and the run is marked failed by the watcher
   task. From SQL:

   ```sql
   SELECT started_at, batch_id, layer, step, error_message
   FROM dw.pipeline_runs
   WHERE status = 'failed'
   ORDER BY started_at DESC;
   ```

   The operational tables are published even when the run fails, so this works after a failure.

2. Decide by layer:

   | Failed step | Usual cause | Action |
   | --- | --- | --- |
   | `bronze/<table>` | Source unreachable, credentials, breaking schema change | Fix access and re-run; for a schema error follow the runbook below |
   | `dq_bronze/checks` | Rows did not land (`*_source_reconciliation`), null keys | Compare the source count with the Bronze batch, then re-run |
   | `silver/<table>` | A staging query broke, often after a source type change | Fix the query in `src/silver/specs.py` and re-run |
   | `dq_silver/checks` | A rule is missing, so bad rows reached Silver | Add or tighten a Silver rule so the rows are quarantined; re-run |
   | `gold/<model>` | A dimension or fact query failed | Fix and re-run; dimensions and facts are idempotent |
   | `dq_gold/checks` | Referential integrity or SCD2 invariant broken | Do not publish; inspect `dq.failed_rows`, fix the model and re-run |
   | `publish/<table>` | Warehouse unreachable or a type without a mapping | The live `dw` table is unchanged; fix and re-run |

3. Inspect what a check flagged. Every failing key is stored:

   ```sql
   SELECT check_id, record_key
   FROM dq.failed_rows
   WHERE batch_id = '2026-07-01' AND check_id = 'slv_orders_customer_exists';
   ```

4. Re-run the same date. Layers that already succeeded are repeated safely.

Warnings do not stop a run. They are listed by `python scripts/dev.py dq` and on the Pipeline
Health dashboard.

## Quarantined rows

Rows that break a Silver rule are kept in `silver_quarantine.<table>` with the rule, the reason,
the batch and the original row as JSON:

```sql
SELECT record_key, rule_id, reject_reason, _batch_id, payload
FROM silver_quarantine.orders
WHERE rule_id = 'ORD_FUTURE_DATE';
```

A quarantined row re-enters the pipeline by itself once it is corrected in the source: the fix
moves its `updated_at`, the next batch extracts it, and it passes the rule. If the rule was
wrong, fix the rule and re-run the batches that rejected the rows.

## Schema change runbook

### A column was added in the source

This is handled automatically. On the next extraction the column is added to the Bronze table,
existing rows read it as null, and the change is logged:

```sql
SELECT detected_at, batch_id, table_name, column_name, change_type, new_type
FROM dq.schema_changes;
```

The column stays in Bronze until someone decides it is needed. To promote it:

1. Add it to the table's staging query in `src/silver/specs.py`.
2. Add the column to the existing Silver table: `ALTER TABLE silver.<table> ADD COLUMN ...`.
3. Re-run the batches since the column appeared, so Silver picks up its values.
4. If it belongs in Gold, add it to the model's SQL and DDL under `src/gold/sql/`, to
   `docs/DATA_DICTIONARY.md`, and re-run.

### A column was removed or its type changed

The extraction fails with a `SchemaDriftError`, nothing is loaded for that table, and its
checkpoint for the batch is marked `quarantined` with the reason:

```sql
SELECT table_name, batch_id, status, message
FROM ops.extract_checkpoints
WHERE status = 'quarantined';
```

A quarantined checkpoint does not advance the watermark, so no data is skipped. To resolve:

1. Confirm the change with the team that owns the source. It may be a mistake on their side.
2. If the new type is a safe widening (`int` to `bigint`, `float` to `double`, a larger decimal
   precision), apply it to Bronze explicitly, for example
   `ALTER TABLE bronze.<table> ALTER COLUMN <column> TYPE bigint`. Iceberg changes the schema
   without rewriting files. Any other type change needs a new column and a decision about how
   to convert old values.
3. If the column is gone for good, remove it from the Silver staging query if it is used there,
   then drop it from Bronze: `ALTER TABLE bronze.<table> DROP COLUMN <column>`. Iceberg removes it
   from the schema without rewriting files. Earlier snapshots still hold the values until they
   expire, so export them first if they must be kept.
4. Re-run the batch. It extracts the full window since the last committed batch.

## Table maintenance

The `iceberg_maintenance` DAG runs weekly. For every lakehouse table it:

1. compacts small data files (`rewrite_data_files`);
2. expires snapshots older than 7 days, always keeping the last 5 (`expire_snapshots`);
3. deletes files no snapshot references and that are older than 3 days (`remove_orphan_files`).

The order matters: compaction creates the snapshots that make old files unreferenced, and expiry
is what finally releases them. Run it by hand with
`docker compose run --rm tools python -m marketplace.cli maintain`.

The log-style tables (`ops.pipeline_runs`, `dq.check_results`) benefit most: every step appends
one small file to them.

## Time travel and rollback

Every commit to an Iceberg table creates a snapshot. Until maintenance expires it, a snapshot can
be queried or made current again. The output below is from the demo run, in a Spark SQL session
(`docker compose run --rm tools python` with `marketplace.spark.build_spark`).

List the snapshots of the customer dimension. The first two are the table's creation and the
first load; each `overwrite` is a daily SCD2 merge adding 180 rows; `replace` is compaction:

```sql
SELECT snapshot_id, committed_at, operation,
       summary['added-records'] AS added_records, summary['total-records'] AS total_records
FROM gold.dim_customer.snapshots
ORDER BY committed_at;
```

```
+-------------------+-----------------------+---------+-------------+-------------+
|snapshot_id        |committed_at           |operation|added_records|total_records|
+-------------------+-----------------------+---------+-------------+-------------+
|5820509839771044984|2026-11-15 17:15:00.974|append   |1            |1            |
|3363270358620560007|2026-11-15 17:15:02.17 |append   |20000        |20001        |
|2936302459472745315|2026-11-15 17:51:27.466|overwrite|20180        |20181        |
|1119284847305928070|2026-11-15 17:53:26.417|overwrite|20360        |20361        |
|5656983108599725147|2026-11-15 17:55:15.481|overwrite|20540        |20541        |
|8606329514487640963|2026-11-15 18:04:03.646|overwrite|20720        |20721        |
|7706670238621043632|2026-11-15 18:08:13.778|replace  |20721        |20721        |
+-------------------+-----------------------+---------+-------------+-------------+
```

Query the dimension as it was after the first load, before any customer had changed:

```sql
SELECT count(*) AS versions, sum(CAST(NOT is_current AS INT)) AS closed_versions
FROM gold.dim_customer VERSION AS OF 3363270358620560007;
```

```
+--------+---------------+
|versions|closed_versions|
+--------+---------------+
|20001   |0              |
+--------+---------------+
```

The same query without `VERSION AS OF` returns 20721 versions, 560 of them closed.
`TIMESTAMP AS OF '2026-11-15 17:30:00'` works the same way with a point in time.

Roll the table back to that snapshot, then return to the latest one:

```sql
CALL lake.system.rollback_to_snapshot('gold.dim_customer', 3363270358620560007);
```

```
+--------------------+-------------------+
|previous_snapshot_id|current_snapshot_id|
+--------------------+-------------------+
|7706670238621043632 |3363270358620560007|
+--------------------+-------------------+
```

`SELECT count(*) FROM gold.dim_customer` now returns 20001.

```sql
CALL lake.system.set_current_snapshot('gold.dim_customer', 7706670238621043632);
```

The count is 20721 again. Rollback only moves the table's pointer; no data files are rewritten
or deleted, which is why it can be undone.

When to use it: a bad load reached a Gold table and the fix will take time. Roll the table back
to the last good snapshot, re-publish, then fix and re-run the batch. A rollback affects one
table, so tables derived from it (facts built on a dimension, the aggregate built on a fact)
must be rolled back or rebuilt with it.
