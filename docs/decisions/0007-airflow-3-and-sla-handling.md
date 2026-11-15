# 7. Airflow 3.3.2, and how SLAs are handled without `sla`

Status: accepted

## Context

The orchestrator has to be pinned to a release that is supported and that was actually run on
the target machine. When this was built:

- Airflow 2.x had reached end of life (April 2026);
- Airflow 3.4.0 had just been released;
- Airflow 3.3.2 was the latest patch of the previous minor line.

Airflow 3 changed two things that matter here. The scheduler no longer parses DAG files (a
separate `dag-processor` does) and tasks talk to an API server instead of the metadata database.
And the task-level `sla` parameter was removed.

## Decision

- Pin Airflow 3.3.2 on Python 3.11, using the official image and its constraints file for every
  shared dependency.
- Run `api-server`, `dag-processor` and `scheduler` with the `LocalExecutor`, limited to one task
  at a time, because each task starts its own Spark driver.
- Replace `sla` with two mechanisms:
  - `execution_timeout` on every task is the hard limit; a task that exceeds it fails and
    retries;
  - each pipeline step compares its duration with a per-layer SLA and records `sla_seconds` and
    `sla_breached` in `ops.pipeline_runs`, which the Pipeline Health dashboard charts.

## Consequences

- A patch release of a mature minor line was preferred over a fresh `.0`.
- A slow but successful step is visible as an SLA breach without failing the run, which is what
  the old `sla` did. What is missing is a push notification at the moment of the breach.
- Airflow 3 Deadline Alerts would add that notification. They need the triggerer service, which
  would not fit the memory budget, so they are not used.
- Three Airflow services cost about 650 MB at idle. This is the largest fixed cost of the stack.
- The daily DAG's schedule is read from `PIPELINE_SCHEDULE`. The local stack sets it to `manual`
  because the sources live in simulated time; with real sources it defaults to `@daily`.
