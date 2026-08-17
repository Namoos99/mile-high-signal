"""Denver 311 pipeline DAG.

Task dependency chain: ingest -> transform (quality gate runs inside this task,
see spark_jobs/run_transform.py) -> load_warehouse.

ARCHITECTURE DECISIONS (see docs/DECISIONS.md AD-016 for the full reasoning):
  - Each task shells out to the same CLI entrypoints a human runs manually
    (`python -m denver311.ingestion.run_ingest`, etc.) rather than importing
    Python functions directly into the DAG file. This means "does the pipeline
    work" has exactly one code path, whether triggered by a human at a
    terminal or by Airflow on a schedule — no separate "Airflow version" of
    the logic to drift out of sync with the manually-run version.
  - Retries are set per-task, not globally, because failure modes differ:
    ingestion hits a flaky third-party API and benefits from retries; a
    quality-gate failure means the data is genuinely bad and retrying the
    same input will fail identically, so transform does NOT retry blindly —
    see the note on the transform task below.
  - No `catchup`: this DAG intentionally does not backfill by running once
    per historical schedule interval. The ingestion watermark (see AD-004,
    AD-005) already handles "how far to reach back," so Airflow re-running
    old intervals would just duplicate work the watermark already prevents,
    while adding confusing repeated log noise.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "denver311-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="denver311_pipeline",
    description="Ingest, clean, and load Denver 311 service requests",
    schedule="0 6 * * *",  # daily at 6am — matches the 30-day lookback window
    start_date=datetime(2026, 8, 1),
    catchup=False,  # see module docstring: the watermark already handles backfill
    default_args=default_args,
    tags=["denver311", "etl"],
)
def denver311_pipeline():
    ingest = BashOperator(
        task_id="ingest",
        bash_command="cd {{ var.value.get('denver311_repo_path', '/opt/airflow/project') }} "
        "&& python -m denver311.ingestion.run_ingest",
        retries=3,  # the ArcGIS API is the most likely transient failure in this DAG
        retry_delay=timedelta(minutes=2),
    )

    transform = BashOperator(
        task_id="transform_and_quality_check",
        bash_command="cd {{ var.value.get('denver311_repo_path', '/opt/airflow/project') }} "
        "&& python -m spark_jobs.run_transform",
        # Deliberately 0 retries here, overriding default_args: a data quality
        # failure (spark_jobs/quality_checks.py raising DataQualityError) means
        # the *data* is bad, not that the task was flaky. Retrying immediately
        # against the same raw input would fail identically and just delay the
        # alert. A human should look before this re-runs.
        retries=0,
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command="cd {{ var.value.get('denver311_repo_path', '/opt/airflow/project') }} "
        "&& python -m warehouse.load_warehouse",
        retries=2,
    )

    @task(trigger_rule="all_done")
    def notify_completion(**context):
        """Placeholder for real alerting (Slack, PagerDuty, email). Runs
        regardless of upstream success/failure (`trigger_rule="all_done"`) so a
        failed run is visible, not just a silently red square in the Airflow UI.
        At scale this is exactly where the severity-tiered alerting described in
        the README's "what I'd do differently" section would plug in."""
        ti = context["ti"]
        states = {
            t: ti.get_dagrun().get_task_instance(t).state
            for t in ["ingest", "transform_and_quality_check", "load_warehouse"]
        }
        print(f"Pipeline run complete. Task states: {states}")

    ingest >> transform >> load_warehouse >> notify_completion()


denver311_pipeline()
