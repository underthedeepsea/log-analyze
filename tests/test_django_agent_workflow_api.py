from __future__ import annotations

import os
from pathlib import Path

import django
from django.core.management import call_command
from django.test import Client, override_settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def _config(tmp_path: Path) -> dict[str, object]:
    return {"project_root": str(Path(__file__).resolve().parents[1]), "state_root": str(tmp_path / "state"), "output_root": str(tmp_path / "output"), "database_provider": "sqlite", "shared_root": str(tmp_path / "shared"), "airflow_base_url": "http://127.0.0.1:18080", "airflow_dag_id": "logrisk_analysis", "airflow_agent_dag_id": "logrisk_agent_run", "airflow_agent_workflow_dag_id": "logrisk_agent_workflow", "agentic_enabled": True, "agent_workflows_enabled": True, "identity_resolver": "tests.django_test_project.resolver.OperatorIdentityResolver", "write_roles": ["logrisk:operator"]}


def _definition():
    return {"schema_version": "1.0", "name": "协作", "description": "test", "nodes": [{"node_id": "evidence", "role_id": "evidence_specialist", "depends_on": []}], "budget": {"max_nodes": 1, "max_concurrency": 1, "max_tool_calls": 4, "timeout_seconds": 60}, "retry_policy": {"max_attempts": 1}, "idempotency_key": "workflow-1"}


def _result():
    return {"summary": {}, "risk_entities": [{"entity_id": "node-a", "entity_type": "node", "cluster": "prod", "risk_score": 80, "risk_level": "high", "window_start": "2026-08-13T00:00:00Z", "window_end": "2026-08-13T00:05:00Z", "top_templates": [{"template_hash": "h1", "component": "kernel", "template": "OOM <NUM>", "count": 2}], "affected_entities": []}]}


def _runtime_snapshot(profile_id="qwen3_5_9b_mlx"):
    return {"profile_snapshot": {"profile_id": profile_id, "connection_id": "ollama-local", "enabled": True}, "connection_snapshot": {"connection_id": "ollama-local", "provider": "ollama", "enabled": True}, "prompt_id": "agent_plan_v1", "prompt_sha256": "sha"}


def test_django_workflow_persists_before_airflow_dispatch(tmp_path, monkeypatch):
    from logrisk.orchestration import AirflowRun
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import agent_workflows

    class Ready:
        dag_id = "logrisk_agent_workflow"
        def trigger_agent_workflow(self, workflow_run_id, request_id): return AirflowRun("logrisk_workflow__" + workflow_run_id, "queued", None, None, request_id, workflow_run_id=workflow_run_id)

    monkeypatch.setattr(agent_workflows, "get_agent_workflow_airflow_orchestrator", lambda: Ready())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        workflow = Client().post("/api/agent-workflows", data=_definition(), content_type="application/json").json()
        job_id = get_container().feature_jobs.create_job(_result(), model="qwen3.5:9b-mlx")
        response = Client().post(f"/api/agent-workflows/{workflow['workflow_id']}/runs", data={"source_job_id": job_id, "entity_id": "node-a", "model_profile_id": "qwen3_5_9b_mlx", "prompt_id": "agent_plan_v1", "idempotency_key": "run-1"}, content_type="application/json")
        persisted = get_container().agent_workflows.get_run(response.json()["workflow_run_id"])
        clear_cached_container()

    assert response.status_code == 202
    assert response.json()["external_dag_id"] == "logrisk_agent_workflow"
    assert persisted["status"] == "queued"


def test_django_workflow_is_hidden_when_disabled(tmp_path):
    from logrisk_django.service_factory import clear_cached_container
    config = _config(tmp_path); config["agent_workflows_enabled"] = False
    with override_settings(LOGRISK=config):
        clear_cached_container(); call_command("logrisk_migrate", "--json"); response = Client().get("/api/agent-workflows"); clear_cached_container()
    assert response.status_code == 404 and response.json()["code"] == "agent_workflows_disabled"


def test_django_workflow_dispatch_failure_is_persisted_for_resume(tmp_path, monkeypatch):
    from logrisk.orchestration import AirflowOrchestratorError
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import agent_workflows

    class Failing:
        dag_id = "logrisk_agent_workflow"
        def trigger_agent_workflow(self, workflow_run_id, request_id):
            raise AirflowOrchestratorError("Airflow unavailable", code="airflow_unavailable", status_code=503)

    monkeypatch.setattr(agent_workflows, "get_agent_workflow_airflow_orchestrator", lambda: Failing())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        workflow = Client().post("/api/agent-workflows", data=_definition(), content_type="application/json").json()
        job_id = get_container().feature_jobs.create_job(_result(), model="qwen3.5:9b-mlx")
        created = get_container().agent_workflows.create_run(workflow["workflow_id"], source_job_id=job_id, entity_id="node-a", entity_type="node", model_profile_id="qwen3_5_9b_mlx", prompt_id="agent_plan_v1", actor="alice", roles=("logrisk:operator",), request_id="req-1", idempotency_key="run-1", evidence_summary={"entity": {"id": "node-a"}}, runtime_snapshot=_runtime_snapshot())
        get_container().agent_workflows.repository.transition_run(created["workflow_run_id"], "paused", allowed_from={"queued"})
        response = Client().post(f"/api/agent-workflow-runs/{created['workflow_run_id']}/actions/resume", data={"idempotency_key": "resume-1"}, content_type="application/json")
        persisted = get_container().agent_workflows.get_run(created["workflow_run_id"])
        clear_cached_container()

    assert response.status_code == 503
    assert persisted["status"] == "failed"
    assert persisted["error_code"] == "airflow_unavailable"


def test_django_workflow_actions_route_to_control_view(tmp_path, monkeypatch):
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import agent_workflows

    class Ready:
        dag_id = "logrisk_agent_workflow"
        def trigger_agent_workflow(self, workflow_run_id, request_id):
            raise AssertionError("pause must not dispatch Airflow")

    monkeypatch.setattr(agent_workflows, "get_agent_workflow_airflow_orchestrator", lambda: Ready())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        workflow = Client().post("/api/agent-workflows", data=_definition(), content_type="application/json").json()
        created = get_container().agent_workflows.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen3_5_9b_mlx", prompt_id="agent_plan_v1", actor="alice", roles=("logrisk:operator",), request_id="req-1", idempotency_key="run-1", evidence_summary={"entity": {"id": "node-a"}}, runtime_snapshot=_runtime_snapshot())
        response = Client().post(f"/api/agent-workflow-runs/{created['workflow_run_id']}/actions/pause", data={"idempotency_key": "pause-1"}, content_type="application/json")
        clear_cached_container()

    assert response.status_code == 200
    assert response.json()["status"] == "paused"


def test_django_node_retry_dispatch_failure_is_persisted(tmp_path, monkeypatch):
    from logrisk.orchestration import AirflowOrchestratorError
    from logrisk_django.service_factory import clear_cached_container, get_container
    from logrisk_django.views import agent_workflows

    class Failing:
        dag_id = "logrisk_agent_workflow"
        def trigger_agent_workflow(self, workflow_run_id, request_id):
            raise AirflowOrchestratorError("Airflow unavailable", code="airflow_unavailable", status_code=503)

    monkeypatch.setattr(agent_workflows, "get_agent_workflow_airflow_orchestrator", lambda: Failing())
    with override_settings(LOGRISK=_config(tmp_path)):
        clear_cached_container(); call_command("logrisk_migrate", "--json")
        workflow = Client().post("/api/agent-workflows", data=_definition(), content_type="application/json").json()
        service = get_container().agent_workflows
        created = service.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen3_5_9b_mlx", prompt_id="agent_plan_v1", actor="alice", roles=("logrisk:operator",), request_id="req-1", idempotency_key="run-1", evidence_summary={"entity": {"id": "node-a"}}, runtime_snapshot=_runtime_snapshot())
        service.repository.transition_run(created["workflow_run_id"], "running", allowed_from={"queued"})
        service.repository.claim_node(created["workflow_run_id"], "evidence")
        service.repository.finish_node(created["workflow_run_id"], "evidence", status="failed", error_code="temporary", error_summary="failed")
        response = Client().post(f"/api/agent-workflow-runs/{created['workflow_run_id']}/nodes/evidence/retry", data={"idempotency_key": "node-retry-1"}, content_type="application/json")
        persisted = service.get_run(created["workflow_run_id"])
        clear_cached_container()

    assert response.status_code == 503
    assert persisted["status"] == "failed"
    assert persisted["error_code"] == "airflow_unavailable"
