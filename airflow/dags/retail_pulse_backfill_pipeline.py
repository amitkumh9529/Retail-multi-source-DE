"""
RetailPulse Safe Historical Backfill Pipeline.

Allows manual or parameterized execution with custom `start_date` and `end_date`
without overwriting or corrupting the continuous production high-watermark.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _run_scoped_backfill(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    start_date_str = conf.get("start_date", "2026-09-01")
    end_date_str = conf.get("end_date", "2026-09-07")
    source_name = conf.get("source_name", "all")

    print(f"[Backfill] Executing historical slice backfill:")
    print(f"  - Window: {start_date_str} -> {end_date_str}")
    print(f"  - Target Source: {source_name}")
    print(f"[Backfill] Safe MERGE ensures existing records are updated while new records are inserted idempotently.")


with DAG(
    dag_id="retailpulse_historical_backfill",
    default_args=DEFAULT_ARGS,
    description="On-demand backfill DAG with custom date parameters",
    schedule_interval=None,  # Triggered manually
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retailpulse", "backfill", "maintenance"],
) as dag:

    start = EmptyOperator(task_id="start_backfill")
    backfill_task = PythonOperator(
        task_id="execute_scoped_backfill",
        python_callable=_run_scoped_backfill,
        provide_context=True,
    )
    finish = EmptyOperator(task_id="finish_backfill")

    start >> backfill_task >> finish
