from __future__ import annotations

from datetime import timedelta

try:
    from airflow import DAG
    from airflow.decorators import task
except ImportError:  # Airflow is installed only in the external scheduler image.
    dag = None
else:
    from integrations.airflow.tasks import (
        extract_feature_batch,
        finalize_job,
        prepare_job,
        score_and_reuse,
        validate_candidates,
    )

    def _conf() -> dict[str, str]:
        from airflow.operators.python import get_current_context

        value = dict(get_current_context()["dag_run"].conf or {})
        return {
            "job_id": str(value.get("job_id") or ""),
            "orchestration_run_id": str(value.get("orchestration_run_id") or ""),
            "request_id": str(value.get("request_id") or ""),
        }

    with DAG(
        dag_id="logrisk_analysis",
        schedule_interval=None,
        catchup=False,
        max_active_runs=4,
        default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    ) as dag:
        @task
        def prepare() -> dict[str, str]:
            value = _conf()
            return prepare_job(**value)

        @task
        def score(prepared: dict[str, str]) -> dict[str, str]:
            return score_and_reuse(prepared["job_id"], prepared["orchestration_run_id"])

        @task
        def extract(scored: dict[str, str]) -> dict[str, object]:
            return extract_feature_batch(scored["job_id"], scored["orchestration_run_id"])

        @task
        def validate(extracted: dict[str, object]) -> dict[str, object]:
            return validate_candidates(str(extracted["job_id"]), str(extracted["orchestration_run_id"]))

        @task
        def finalize(validated: dict[str, object]) -> dict[str, str]:
            return finalize_job(str(validated["job_id"]), str(validated["orchestration_run_id"]))

        finalize(validate(extract(score(prepare()))))
