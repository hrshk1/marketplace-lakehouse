# 3. Watermark extraction on `updated_at` instead of CDC

Status: accepted

## Context

The pipeline has to pick up inserts and updates from PostgreSQL and MongoDB once a day. Two
families of techniques exist:

- query-based extraction: ask each table for rows whose `updated_at` moved past a stored
  watermark;
- log-based change data capture (Debezium reading the PostgreSQL WAL and the MongoDB oplog).

## Decision

Extract with a persisted watermark per table (`ops.extract_checkpoints`), a one-hour lookback
window and a full snapshot on the first run. Every source table carries `created_at` and
`updated_at`, which is the only requirement.

## Consequences

What this gives:

- No extra infrastructure: no Kafka, no connectors, no replication slots to monitor.
- Extraction is a plain, repeatable query, which makes batches easy to re-run and to test.

What it cannot do, and CDC would add:

- Hard deletes are invisible. The sources use soft deletes (`deleted_at`), so this is acceptable
  here, but it is a contract the source teams must keep.
- Intermediate states are lost. If a row changes twice between two runs, only the last state is
  seen. Running the pipeline late has the same effect: a batch re-read from a source that has
  moved on no longer finds the old versions.
- A row committed late with an `updated_at` older than the watermark is missed unless it falls
  inside the lookback window. The window covers one hour; the `brz_orders_late_arriving` check
  reports how often it is needed.
- A change that does not touch `updated_at` is never extracted.
- Latency is bounded by the schedule, not by the commit.

CDC would be the right move once deletes, every intermediate state, or minute-level freshness
become requirements.
