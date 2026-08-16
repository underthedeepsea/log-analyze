from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:
    from airflow import DAG
    from airflow.decorators import task
except ImportError:
    dag = None
else:
    from integrations.airflow.tasks import execute_agent_workflow

    with DAG(
        dag_id="logrisk_agent_workflow",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        schedule_interval=None,
        catchup=False,
        max_active_runs=4,
        default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    ) as dag:
        @task(pool="logrisk_llm_pool", queue="logrisk_llm")
        def execute() -> dict[str, str]:
            from airflow.operators.python import get_current_context

            conf = dict(get_current_context()["dag_run"].conf or {})
            return execute_agent_workflow(
                workflow_run_id=str(conf.get("workflow_run_id") or ""),
                request_id=str(conf.get("request_id") or ""),
            )

        execute()
