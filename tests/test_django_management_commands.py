from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import django
from django.core.management import call_command
from django.test import override_settings


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def _config(tmp_path: Path) -> dict[str, object]:
    return {
        "project_root": str(Path(__file__).resolve().parents[1]),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "write_roles": ["logrisk:operator"],
    }


def test_django_check_reports_pending_migrations_without_applying(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container()
        result = json.loads(call_command("logrisk_check", "--json"))
        clear_cached_container()

    assert result["database"]["pending_migrations"] > 0
    assert result["database"]["applied_migrations"] == 0
    with sqlite3.connect(tmp_path / "state" / "logrisk.sqlite3") as connection:
        row = connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone()
    assert row is None


def test_django_migrate_is_explicit_and_idempotent(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container()
        first = json.loads(call_command("logrisk_migrate", "--json"))
        second = json.loads(call_command("logrisk_migrate", "--json"))
        checked = json.loads(call_command("logrisk_check", "--json"))
        clear_cached_container()

    assert first["database"]["pending_migrations"] == 0
    assert second["database"]["pending_migrations"] == 0
    assert checked["database"]["pending_migrations"] == 0
    assert checked["ready"] is True


def test_reconcile_dispatch_retries_only_reconcilable_runs(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration.airflow import AirflowRun
    from logrisk_django.management.commands import logrisk_reconcile_dispatch
    from logrisk_django.service_factory import clear_cached_container, get_container

    class ReadyAirflow:
        dag_id = "logrisk_analysis"

        def trigger(self, job_id: str, run_id: str, request_id: str) -> AirflowRun:
            return AirflowRun("logrisk__" + job_id, "queued", job_id, run_id, request_id)

    monkeypatch.setattr(logrisk_reconcile_dispatch, "get_airflow_orchestrator", lambda: ReadyAirflow())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = get_container()
        job_id = container.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="test")
        pending = container.orchestration.create_pending(job_id, "request-1", "pacas-alice")
        result = json.loads(call_command("logrisk_reconcile_dispatch", "--json"))
        updated = container.orchestration.get(pending["orchestration_run_id"])
        clear_cached_container()

    assert result["retried"] == [pending["orchestration_run_id"]]
    assert result["failed"] == []
    assert updated["status"] == "dispatched"
