from __future__ import annotations

from pathlib import Path


def test_airflow_dag_is_lazy_and_keeps_raw_payloads_out_of_conf_and_xcom() -> None:
    source = Path("integrations/airflow/dags/logrisk_analysis.py").read_text(encoding="utf-8")

    assert 'dag_id="logrisk_analysis"' in source
    assert "schedule_interval=None" in source
    assert "max_active_runs=4" in source
    assert "prepare_job" in source
    assert "extract_feature_batch" in source
    assert "raw_log" not in source
    assert "samples" not in source
    assert "message_core" not in source
