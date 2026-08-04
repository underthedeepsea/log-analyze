from __future__ import annotations

import json
import os
from pathlib import Path

import django
from django.core.management import call_command
from django.test import Client, override_settings


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def _config(tmp_path: Path, resolver: str) -> dict[str, object]:
    return {
        "project_root": str(Path(__file__).resolve().parents[1]),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "airflow_input_dag_id": "logrisk_input_preprocess",
        "identity_resolver": resolver,
        "write_roles": ["logrisk:operator"],
    }


def _result() -> dict[str, object]:
    return {
        "summary": {"total_raw_logs": 1},
        "risk_entities": [{
            "entity_id": "node-1",
            "entity_type": "node",
            "cluster": "prod-a",
            "risk_score": 80,
            "risk_level": "high",
            "window_start": "2026-08-03T00:00:00+00:00",
            "window_end": "2026-08-03T00:05:00+00:00",
            "top_templates": [{"template_hash": "hash-1", "count": 1}],
            "affected_entities": [],
        }],
    }


def test_django_syncs_external_airflow_success_without_exposing_payload(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration import AirflowRun
    from logrisk_django.service_factory import clear_cached_container, get_container

    expected_job_id = ""
    expected_run_id = ""

    class FakeAirflow:
        dag_id = "logrisk_analysis"

        def get_run(self, external_run_id: str) -> AirflowRun:
            return AirflowRun(external_run_id, "success", expected_job_id, expected_run_id, "request-1")

    monkeypatch.setattr("logrisk_django.service_factory.get_airflow_orchestrator", lambda: FakeAirflow())
    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = get_container()
        job_id = container.feature_jobs.create_job(_result(), model="test-model")
        run = container.orchestration.create_pending(job_id, "request-1", "scheduler", ["logrisk:operator"])
        expected_job_id = job_id
        expected_run_id = run["orchestration_run_id"]
        dispatched = container.orchestration.mark_dispatched(
            run["orchestration_run_id"], "logrisk_analysis", "airflow-run-1", expected_version=run["state_version"]
        )
        response = Client().post(
            f"/api/orchestration/runs/{dispatched['orchestration_run_id']}/sync",
            data=json.dumps({"expected_version": dispatched["state_version"]}),
            content_type="application/json",
        )
        stored = container.orchestration.get(dispatched["orchestration_run_id"])
        audits = container.runtime_repository.list_audits(limit=20)["items"]
        clear_cached_container()

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert stored["status"] == "completed"
    assert response.json()["airflow_state"] == "success"
    assert "dag_run_conf" not in json.dumps(response.json(), ensure_ascii=False)
    assert any(item["action"] == "orchestration.reconciled" for item in audits)


def test_django_syncs_input_airflow_failure_and_exposes_health_separately(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration import AirflowRun
    from logrisk_django.service_factory import clear_cached_container, get_container

    expected_input_job_id = ""
    expected_input_run_id = ""

    class FakeAirflow:
        dag_id = "logrisk_input_preprocess"

        def get_run(self, external_run_id: str) -> AirflowRun:
            return AirflowRun(
                external_run_id, "failed", None, None, "request-input", expected_input_job_id, expected_input_run_id,
            )

    monkeypatch.setattr("logrisk_django.service_factory.get_input_airflow_orchestrator", lambda: FakeAirflow())
    monkeypatch.setattr(
        "logrisk_django.service_factory.get_airflow_readiness",
        lambda: {"status": "ready", "ready": True, "online": True, "dag_registered": True, "dags": []},
    )
    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        container = get_container()
        upload = container.upload_store.create(filename="input.log", size_bytes=1, chunk_size_bytes=1)
        container.upload_store.append_chunk(upload_id=upload["upload_id"], index=0, data=b"x")
        container.upload_store.complete(upload_id=upload["upload_id"])
        input_job = container.input_jobs.create(
            upload_id=upload["upload_id"], filename="input.log", source_path=str(container.upload_store.source_path(upload["upload_id"])),
        )
        run = container.input_orchestration.create_pending(input_job["input_job_id"], "request-input", "scheduler", ["logrisk:operator"])
        expected_input_job_id = input_job["input_job_id"]
        expected_input_run_id = run["input_orchestration_run_id"]
        dispatched = container.input_orchestration.mark_dispatched(
            run["input_orchestration_run_id"], "logrisk_input_preprocess", "input-airflow-run", expected_version=run["state_version"]
        )
        response = Client().post(
            f"/api/input-orchestration/runs/{dispatched['input_orchestration_run_id']}/sync",
            data=json.dumps({"expected_version": dispatched["state_version"]}),
            content_type="application/json",
        )
        detail = Client().get(f"/api/input-orchestration/runs/{dispatched['input_orchestration_run_id']}")
        health = Client().get("/api/runtime/airflow")
        clear_cached_container()

    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert detail.status_code == 200
    assert detail.json()["status"] == "failed"
    assert health.status_code == 200
    assert health.json()["ready"] is True
