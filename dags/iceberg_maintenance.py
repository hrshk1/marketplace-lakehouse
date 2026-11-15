"""Weekly Iceberg maintenance for every lakehouse table.

Compacts small data files, expires snapshots past the retention window and
removes files no snapshot references. Runs separately from the daily pipeline
so a long compaction never delays data delivery.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from marketplace import orchestration

with DAG(
    dag_id="iceberg_maintenance",
    description="Compaction, snapshot expiry and orphan file cleanup",
    schedule="@weekly",
    start_date=datetime(2026, 6, 30),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": orchestration.record_task_failure,
    },
    doc_md=__doc__,
    tags=["lakehouse", "maintenance"],
) as dag:
    PythonOperator(
        task_id="maintain_tables",
        python_callable=orchestration.maintain_tables,
        op_kwargs={
            "batch_date": "{{ (dag_run.logical_date or dag_run.run_after) | ds }}",
            "run_id": "{{ run_id }}",
        },
    )
