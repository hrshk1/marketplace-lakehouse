"""Daily batch pipeline: sources -> Bronze -> Silver -> Gold -> serving warehouse.

Each layer is followed by a quality gate. A failed error-severity check fails
the gate, which blocks every later layer. The run log and quality results are
published even when the run fails.

Backfill one date:
    airflow dags trigger daily_marketplace_pipeline --logical-date 2026-07-01T00:00:00+00:00
Backfill a range:
    airflow backfill create --dag-id daily_marketplace_pipeline \
        --from-date 2026-07-01 --to-date 2026-07-05
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup, TriggerRule, chain

from marketplace import orchestration
from marketplace.pipeline import EXTRACT_TABLES, GOLD_MODEL_NAMES, SILVER_TABLE_NAMES

# The batch is the run's logical date. Runs triggered without one fall back
# to the date they were asked to run on.
BATCH = {
    "batch_date": "{{ (dag_run.logical_date or dag_run.run_after) | ds }}",
    "run_id": "{{ run_id }}",
}

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": orchestration.record_task_failure,
}


def step(task_id: str, callable_, **kwargs: str) -> PythonOperator:
    return PythonOperator(task_id=task_id, python_callable=callable_, op_kwargs={**kwargs, **BATCH})


def quality_gate(layer: str) -> TaskGroup:
    """A one-task group, so the gate shows up as its own stage in the graph."""
    with TaskGroup(f"dq_{layer}") as group:
        # A failed check does not pass on a second try, so gates never retry.
        PythonOperator(
            task_id="checks",
            python_callable=orchestration.quality_gate,
            op_kwargs={"layer": layer, **BATCH},
            retries=0,
        )
    return group


with DAG(
    dag_id="daily_marketplace_pipeline",
    description="Incremental load of the marketplace lakehouse and star schema",
    schedule=orchestration.daily_schedule(),
    start_date=datetime(2026, 6, 30),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["lakehouse", "daily"],
) as dag:
    with TaskGroup("extract_to_bronze") as extract_to_bronze:
        for table in EXTRACT_TABLES:
            step(table, orchestration.extract_table, table_name=table)

    # Silver and Gold steps run in a fixed order: order items and payments
    # check their parent order, and facts look up dimension keys.
    with TaskGroup("build_silver") as build_silver:
        chain(
            *(step(t, orchestration.build_silver_table, table_name=t) for t in SILVER_TABLE_NAMES)
        )

    with TaskGroup("build_gold") as build_gold:
        chain(*(step(m, orchestration.build_gold_model, model_name=m) for m in GOLD_MODEL_NAMES))

    with TaskGroup("publish_to_dw") as publish_to_dw:
        analytics = step("analytics_tables", orchestration.publish_analytics_tables)
        operations = PythonOperator(
            task_id="operational_tables",
            python_callable=orchestration.publish_operational_tables,
            op_kwargs=BATCH,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        analytics >> operations

    chain(
        extract_to_bronze,
        quality_gate("bronze"),
        build_silver,
        quality_gate("silver"),
        build_gold,
        quality_gate("gold"),
        publish_to_dw,
    )

    # `operational_tables` runs after failures too and would otherwise make a
    # failed run look successful. The watcher fails the run in that case.
    watcher = PythonOperator(
        task_id="fail_run_if_any_task_failed",
        python_callable=orchestration.fail_run,
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
        on_failure_callback=None,
    )
    [extract_to_bronze, build_silver, build_gold, publish_to_dw] >> watcher
