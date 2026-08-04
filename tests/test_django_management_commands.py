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

    class ReadyInputAirflow:
        dag_id = "logrisk_input_preprocess"

        def trigger_input(self, input_job_id: str, run_id: str, request_id: str) -> AirflowRun:
            return AirflowRun(
                "logrisk_input__" + input_job_id,
                "queued",
                None,
                None,
                request_id,
                input_job_id,
                run_id,
            )

    monkeypatch.setattr(logrisk_reconcile_dispatch, "get_airflow_orchestrator", lambda: ReadyAirflow())
    monkeypatch.setattr(logrisk_reconcile_dispatch, "get_input_airflow_orchestrator", lambda: ReadyInputAirflow())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = get_container()
        job_id = container.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="test")
        pending = container.orchestration.create_pending(job_id, "request-1", "pacas-alice")
        source = container.artifact_store.resolve("uploads/reconcile.log")
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("sanitized", encoding="utf-8")
        upload = container.upload_store.create(filename="messages", size_bytes=len(b"sanitized"))
        input_job = container.input_jobs.create(upload_id=upload["upload_id"], filename="messages", source_path=str(source))
        input_pending = container.input_orchestration.create_pending(input_job["input_job_id"], "request-2", "pacas-alice")
        result = json.loads(call_command("logrisk_reconcile_dispatch", "--json"))
        updated = container.orchestration.get(pending["orchestration_run_id"])
        input_updated = container.input_orchestration.get(input_pending["input_orchestration_run_id"])
        clear_cached_container()

    assert result["retried"] == [pending["orchestration_run_id"]]
    assert result["input_retried"] == [input_pending["input_orchestration_run_id"]]
    assert result["failed"] == []
    assert result["input_failed"] == []
    assert updated["status"] == "dispatched"
    assert input_updated["status"] == "dispatched"


def test_reconcile_runs_syncs_active_airflow_states_and_supports_dry_run(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration.airflow import AirflowRun
    from logrisk_django.management.commands import logrisk_reconcile_runs
    from logrisk_django.service_factory import clear_cached_container, get_container

    expected_job_id = ""
    expected_run_id = ""
    expected_input_job_id = ""
    expected_input_run_id = ""

    class ReadyAirflow:
        dag_id = "logrisk_analysis"

        def get_run(self, external_run_id: str) -> AirflowRun:
            return AirflowRun(external_run_id, "success", expected_job_id, expected_run_id, "request-1")

    class ReadyInputAirflow:
        dag_id = "logrisk_input_preprocess"

        def get_run(self, external_run_id: str) -> AirflowRun:
            return AirflowRun(
                external_run_id, "failed", None, None, "request-2", expected_input_job_id, expected_input_run_id,
            )

    monkeypatch.setattr(logrisk_reconcile_runs, "get_airflow_orchestrator", lambda: ReadyAirflow())
    monkeypatch.setattr(logrisk_reconcile_runs, "get_input_airflow_orchestrator", lambda: ReadyInputAirflow())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = get_container()
        job_id = container.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="test")
        run = container.orchestration.create_pending(job_id, "request-1", "pacas-alice")
        dispatched = container.orchestration.mark_dispatched(
            run["orchestration_run_id"], "logrisk_analysis", "airflow-run", expected_version=run["state_version"]
        )
        expected_job_id = job_id
        expected_run_id = dispatched["orchestration_run_id"]
        source = container.artifact_store.resolve("uploads/reconcile-runs.log")
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("sanitized", encoding="utf-8")
        upload = container.upload_store.create(filename="messages", size_bytes=len(b"sanitized"))
        input_job = container.input_jobs.create(upload_id=upload["upload_id"], filename="messages", source_path=str(source))
        input_run = container.input_orchestration.create_pending(input_job["input_job_id"], "request-2", "pacas-alice")
        input_dispatched = container.input_orchestration.mark_dispatched(
            input_run["input_orchestration_run_id"], "logrisk_input_preprocess", "input-airflow-run", expected_version=input_run["state_version"]
        )
        expected_input_job_id = input_job["input_job_id"]
        expected_input_run_id = input_dispatched["input_orchestration_run_id"]
        dry = json.loads(call_command("logrisk_reconcile_runs", "--dry-run", "--json"))
        assert container.orchestration.get(dispatched["orchestration_run_id"])["status"] == "dispatched"
        result = json.loads(call_command("logrisk_reconcile_runs", "--json"))
        updated = container.orchestration.get(dispatched["orchestration_run_id"])
        input_updated = container.input_orchestration.get(input_dispatched["input_orchestration_run_id"])
        clear_cached_container()

    assert len(dry["unchanged"]) == 2
    assert dry["synced"] == []
    assert {item["status"] for item in result["synced"]} == {"completed", "failed"}
    assert updated["status"] == "completed"
    assert input_updated["status"] == "failed"
