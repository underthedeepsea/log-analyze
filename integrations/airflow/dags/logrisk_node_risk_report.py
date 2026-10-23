from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:
    from airflow import DAG
    from airflow.decorators import task
except ImportError:
    dag = None
else:
    with DAG(dag_id="logrisk_node_risk_report", start_date=datetime(2024, 1, 1, tzinfo=timezone.utc), schedule_interval=None, catchup=False, max_active_runs=1, default_args={"retries": 0}) as dag:
        @task(pool="logrisk_llm_pool", queue="logrisk_llm", execution_timeout=timedelta(seconds=120))
        def execute() -> dict[str, str]:
            from airflow.operators.python import get_current_context
            from logrisk.server_risk.airflow import execute_report
            return execute_report(dict(get_current_context()["dag_run"].conf or {}))
        execute()
