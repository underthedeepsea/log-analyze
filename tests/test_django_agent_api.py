from __future__ import annotations

import os
from pathlib import Path

import django
from django.core.management import call_command
from django.test import Client, override_settings


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
        "airflow_agent_dag_id": "logrisk_agent_run",
        "agentic_enabled": True,
        "identity_resolver": "tests.django_test_project.resolver.OperatorIdentityResolver",
        "write_roles": ["logrisk:operator"],
    }


def _result() -> dict:
    return {"summary": {}, "risk_entities": [{
        "entity_id": "node-a", "entity_type": "node", "cluster": "prod-a",
        "risk_score": 80, "risk_level": "high",
        "window_start": "2026-08-12T00:00:00+00:00", "window_end": "2026-08-12T00:05:00+00:00",
        "top_templates": [{"template_hash": "hash-a", "component": "kernel", "template": "OOM <NUM>", "count": 2}],
        "affected_entities": [],
    }]}


def test_django_agent_create_persists_before_airflow_dispatch(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration import AirflowRun
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import agentic

    class ReadyAirflow:
        dag_id = "logrisk_agent_run"

        def trigger_agent(self, agent_run_id: str, request_id: str) -> AirflowRun:
            return AirflowRun("logrisk_agent__" + agent_run_id, "queued", None, None, request_id, agent_run_id=agent_run_id)

    monkeypatch.setattr(agentic, "get_agent_airflow_orchestrator", lambda: ReadyAirflow())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        job_id = get_container().feature_jobs.create_job(_result(), model="qwen3.5:9b-mlx")
        response = Client().post("/api/agent-runs", data={
            "source_job_id": job_id, "entity_id": "node-a", "model_profile_id": "qwen3_5_9b_mlx",
            "prompt_id": "agent_plan_v1", "idempotency_key": "django-agent-1",
        }, content_type="application/json")
        run = get_container().agent_runs.get_run(response.json()["run_id"])
        detail = Client().get(f"/api/agent-runs/{run['run_id']}")
        clear_cached_container()

    assert response.status_code == 202
    assert response.json()["external_dag_id"] == "logrisk_agent_run"
    assert run["status"] == "queued"
    assert detail.status_code == 200
    assert "raw_sample" not in detail.content.decode()


def test_django_agent_api_is_hidden_when_disabled(tmp_path) -> None:
    from logrisk_django.service_factory import clear_cached_container

    config = _config(tmp_path)
    config["agentic_enabled"] = False
    with override_settings(LOGRISK=config):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        response = Client().get("/api/agent-runs")
        clear_cached_container()
    assert response.status_code == 404
    assert response.json()["code"] == "agentic_disabled"


def test_django_agent_dispatch_failure_is_persisted_and_replay_is_read_only(tmp_path, monkeypatch) -> None:
    from logrisk.orchestration import AirflowOrchestratorError
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import agentic

    class FailingAirflow:
        dag_id = "logrisk_agent_run"

        def trigger_agent(self, agent_run_id: str, request_id: str):
            raise AirflowOrchestratorError("不可用", code="airflow_unavailable", status_code=503)

    monkeypatch.setattr(agentic, "get_agent_airflow_orchestrator", lambda: FailingAirflow())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        job_id = get_container().feature_jobs.create_job(_result(), model="qwen3.5:9b-mlx")
        response = Client().post("/api/agent-runs", data={
            "source_job_id": job_id, "entity_id": "node-a", "model_profile_id": "qwen3_5_9b_mlx",
            "prompt_id": "agent_plan_v1", "idempotency_key": "dispatch-fails",
        }, content_type="application/json")
        run = get_container().agent_runs.list_runs()[0]
        replay = Client().post(f"/api/agent-runs/{run['run_id']}/replay", data="{}", content_type="application/json")
        clear_cached_container()

    assert response.status_code == 503
    assert run["status"] == "failed"
    assert run["error_code"] == "airflow_unavailable"
    assert replay.status_code == 200
    assert replay.json()["mode"] == "read_only"
