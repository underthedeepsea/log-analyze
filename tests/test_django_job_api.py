from __future__ import annotations

import os
from pathlib import Path

import django
from django.test import Client, override_settings
from django.core.management import call_command


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


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


def _config(tmp_path: Path, resolver: str) -> dict[str, object]:
    return {
        "project_root": str(Path(__file__).resolve().parents[1]),
        "state_root": str(tmp_path / "state"),
        "output_root": str(tmp_path / "output"),
        "database_provider": "sqlite",
        "shared_root": str(tmp_path / "shared"),
        "airflow_base_url": "http://127.0.0.1:18080",
        "airflow_dag_id": "logrisk_analysis",
        "identity_resolver": resolver,
        "write_roles": ["logrisk:operator"],
    }


def test_django_job_is_durable_when_airflow_trigger_fails(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestratorError
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import jobs

    class FailingAirflow:
        def trigger(self, *_args: object):
            raise AirflowOrchestratorError("Airflow 不可用", code="airflow_unavailable", status_code=503)

    monkeypatch.setattr(jobs, "get_airflow_orchestrator", lambda: FailingAirflow())
    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post("/api/jobs", data={"result": _result(), "model_profile_id": "qwen3_1_7b_fast"}, content_type="application/json")
        container = get_container()
        run = container.orchestration.for_job(response.json()["job_id"])
        clear_cached_container()

    assert response.status_code == 503
    assert run and run["status"] == "dispatch_failed"
    assert run["error_code"] == "airflow_unavailable"


def test_django_job_dispatches_after_persisting_with_authenticated_pacas_identity(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration.airflow import AirflowRun
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import jobs

    class ReadyAirflow:
        dag_id = "logrisk_analysis"

        def trigger(self, job_id: str, orchestration_run_id: str, request_id: str) -> AirflowRun:
            assert orchestration_run_id.startswith("orchestration-")
            assert request_id == "request-django-test"
            return AirflowRun("logrisk__" + job_id, "queued", job_id, orchestration_run_id, request_id)

    monkeypatch.setattr(jobs, "get_airflow_orchestrator", lambda: ReadyAirflow())
    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.OperatorIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post("/api/jobs", data={"result": _result(), "model_profile_id": "qwen3_1_7b_fast"}, content_type="application/json")
        run = get_container().orchestration.for_job(response.json()["job_id"])
        clear_cached_container()

    assert response.status_code == 202
    assert run and run["status"] == "dispatched"
    assert run["actor"] == "pacas-alice"


def test_django_job_write_fails_closed_without_authenticated_identity(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    with override_settings(LOGRISK=_config(tmp_path, "tests.django_test_project.resolver.AnonymousIdentityResolver")):
        clear_cached_container()
        call_command("logrisk_migrate", "--json")
        response = Client().post("/api/jobs", data={"result": _result(), "model_profile_id": "qwen3_1_7b_fast"}, content_type="application/json")
        clear_cached_container()

    assert response.status_code == 403
    assert response.json()["code"] == "runtime_identity_required"
