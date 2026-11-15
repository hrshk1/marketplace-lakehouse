# 5. A small data quality framework of our own

Status: accepted

## Context

Every layer needs checks that can block the next layer, and the results have to be queryable so
they can be charted and compared against the generator's defect ledger. Great Expectations, Soda
Core and dbt tests all cover the check types needed.

## Decision

Write a small framework (`src/dq/`, about 600 lines) instead of adopting a library:

- checks are declared in YAML per layer and table, each with an id, a severity and a threshold;
- nine check types: `not_null`, `unique`, `accepted_values`, `relationships`, `range`, `regex`,
  `freshness`, `row_count_reconciliation` and `custom_sql`;
- results go to the Iceberg table `dq.check_results` and failing keys to `dq.failed_rows`;
- a failed `error` check raises and stops the pipeline; a failed `warn` check is recorded.

## Consequences

- Results live in the lakehouse in a schema we control, so the same tables feed the pipeline
  gate, the Metabase dashboard and the precision and recall report.
- Failing keys are stored, not only counts. That is what makes it possible to score detection
  record by record against the ledger.
- No extra dependency tree inside the Airflow image, where version conflicts are already the
  main risk.
- We own the maintenance. There is no profiling, no data docs site and no anomaly detection;
  thresholds are fixed numbers. If the rule set grows past what YAML and nine check types can
  express, moving to Soda or dbt tests would be the next step.
