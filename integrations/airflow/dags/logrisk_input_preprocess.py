from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:
    from airflow import DAG
    from airflow.decorators import task
except ImportError:  # Airflow is installed only in the external scheduler image.
    dag = None
else:
    from integrations.airflow.tasks import preprocess_uploaded_input

    _START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def _conf() -> dict[str, str]:
        from airflow.operators.python import get_current_context

        value = dict(get_current_context()["dag_run"].conf or {})
        return {
            "input_job_id": str(value.get("input_job_id") or ""),
            "input_orchestration_run_id": str(value.get("input_orchestration_run_id") or ""),
            "request_id": str(value.get("request_id") or ""),
        }

    with DAG(
        dag_id="logrisk_input_preprocess",
        start_date=_START_DATE,
        schedule_interval=None,
        catchup=False,
        max_active_runs=4,
        default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    ) as dag:
        @task(pool="logrisk_cpu_pool", queue="logrisk_cpu")
        def preprocess_uploaded() -> dict[str, str]:
            return preprocess_uploaded_input(**_conf())

        preprocess_uploaded()
