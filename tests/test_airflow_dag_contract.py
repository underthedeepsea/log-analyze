from __future__ import annotations

from pathlib import Path


def test_airflow_dag_is_lazy_and_keeps_raw_payloads_out_of_conf_and_xcom() -> None:
    source = Path("integrations/airflow/dags/logrisk_analysis.py").read_text(encoding="utf-8")

    assert 'dag_id="logrisk_analysis"' in source
    assert "start_date=" in source
    assert "schedule_interval=None" in source
    assert "max_active_runs=4" in source
    assert "prepare_job" in source
    assert "preprocess_input" in source
    assert "list_drain_partitions" in source
    assert "drain_partition" in source
    assert "merge_templates" in source
    assert "list_feature_batches" in source
    assert "extract_feature_batch" in source
    assert 'pool="logrisk_cpu_pool"' in source
    assert 'queue="logrisk_cpu"' in source
    assert 'pool="logrisk_llm_pool"' in source
    assert 'queue="logrisk_llm"' in source
    assert '@task(pool="logrisk_cpu_pool", queue="logrisk_cpu")' in source
    assert '@task(pool="logrisk_llm_pool", queue="logrisk_llm")' in source
    assert source.count('pool="logrisk_cpu_pool"') == 9
    assert source.count('queue="logrisk_cpu"') == 9
    assert source.count('pool="logrisk_llm_pool"') == 1
    assert source.count('queue="logrisk_llm"') == 1
    assert "from airflow.utils.trigger_rule import TriggerRule" in source
    assert source.count("trigger_rule=TriggerRule.NONE_FAILED") == 2
    assert "def merge(preprocessed: dict[str, str])" in source
    assert "def validate(scored: dict[str, str])" in source
    assert "drained >> merged" in source
    assert "extracted >> validated" in source
    assert "drain.partial(" not in source
    assert "extract.partial(" not in source
    assert ".expand(" in source
    assert "raw_log" not in source
    assert "samples" not in source
    assert "message_core" not in source


def test_input_preprocess_dag_only_passes_input_and_orchestration_ids() -> None:
    source = Path("integrations/airflow/dags/logrisk_input_preprocess.py").read_text(encoding="utf-8")

    assert 'dag_id="logrisk_input_preprocess"' in source
    assert "start_date=" in source
    assert "preprocess_uploaded_input" in source
    assert '"input_job_id"' in source
    assert '"input_orchestration_run_id"' in source
    assert "raw_log" not in source
    assert "samples" not in source
