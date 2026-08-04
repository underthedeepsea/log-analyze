from __future__ import annotations

from pathlib import Path


def test_django_airflow_deployment_docs_cover_security_boundaries() -> None:
    text = Path("DJANGO_AIRFLOW_DEPLOYMENT_GUIDE.md").read_text(encoding="utf-8")
    for required in (
        "Django 4.2.16",
        "Airflow 2.3.2",
        "CeleryExecutor",
        "LOGRISK_SHARED_ROOT",
        "logrisk_input_preprocess",
        "logrisk_migrate --check",
        "原始日志不得进入 DAG conf 或 XCom",
        "回滚",
    ):
        assert required in text


def test_django_airflow_examples_keep_secrets_in_environment_variables() -> None:
    settings = Path("examples/django_integration/settings_example.py").read_text(encoding="utf-8")
    airflow = Path("examples/airflow/logrisk_analysis.py").read_text(encoding="utf-8")
    input_airflow = Path("examples/airflow/logrisk_input_preprocess.py").read_text(encoding="utf-8")

    assert "os.environ" in settings
    assert "LOGRISK_DATABASE_URL" in settings
    assert "logrisk_analysis" in airflow
    assert "logrisk_input_preprocess" in input_airflow
    assert "postgresql://" not in settings
