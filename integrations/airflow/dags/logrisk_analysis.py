from __future__ import annotations

from datetime import timedelta
from typing import Any

try:
    from airflow import DAG
    from airflow.decorators import task
except ImportError:  # Airflow is installed only in the external scheduler image.
    dag = None
else:
    from integrations.airflow.tasks import (
        drain_partition,
        extract_feature_batch,
        finalize_job,
        list_drain_partitions,
        list_feature_batches,
        merge_templates,
        prepare_job,
        preprocess_input,
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

    def _identity(value: dict[str, Any]) -> dict[str, str]:
        """Keep cross-task values to stable identifiers and statuses only."""
        return {
            "job_id": str(value["job_id"]),
            "orchestration_run_id": str(value["orchestration_run_id"]),
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
            return prepare_job(**_conf())

        @task
        def preprocess(prepared: dict[str, str]) -> dict[str, str]:
            values = _identity(prepared)
            return preprocess_input(**values)

        @task
        def partitions(preprocessed: dict[str, str]) -> list[dict[str, str]]:
            values = _identity(preprocessed)
            listed = list_drain_partitions(**values)
            return [
                {**values, "partition_id": str(partition_id)}
                for partition_id in listed["partition_ids"]
            ]

        @task
        def drain(partition: dict[str, str]) -> dict[str, str]:
            return drain_partition(**partition)

        @task
        def merge(preprocessed: dict[str, str], _drained: list[dict[str, str]]) -> dict[str, str]:
            return merge_templates(**_identity(preprocessed))

        @task
        def score(merged: dict[str, str]) -> dict[str, str]:
            return score_and_reuse(**_identity(merged))

        @task
        def batches(scored: dict[str, str]) -> list[dict[str, str]]:
            values = _identity(scored)
            listed = list_feature_batches(**values)
            return [
                {**values, "batch_id": str(batch_id)}
                for batch_id in listed["batch_ids"]
            ]

        @task
        def extract(batch: dict[str, str]) -> dict[str, object]:
            return extract_feature_batch(**batch)

        @task
        def validate(scored: dict[str, str], _extracted: list[dict[str, object]]) -> dict[str, object]:
            return validate_candidates(**_identity(scored))

        @task
        def finalize(validated: dict[str, object]) -> dict[str, str]:
            return finalize_job(**_identity(validated))

        prepared = prepare()
        preprocessed = preprocess(prepared)
        partition_list = partitions(preprocessed)
        drained = drain.partial(pool="logrisk_cpu_pool", queue="logrisk_cpu").expand(partition=partition_list)
        merged = merge(preprocessed, drained)
        scored = score(merged)
        batch_list = batches(scored)
        extracted = extract.partial(pool="logrisk_llm_pool", queue="logrisk_llm").expand(batch=batch_list)
        finalize(validate(scored, extracted))
