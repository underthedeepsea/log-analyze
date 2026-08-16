from __future__ import annotations

from logrisk.agentic.compiler import WorkflowLimits
from logrisk.agentic.roles import build_role_registry
from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.agentic.workflow_scheduler import WorkflowScheduler
from logrisk.agentic.workflow_service import WorkflowService
from logrisk.airflow_tasks import execute_agent_workflow
from logrisk.database import SQLiteDatabase


class StubScheduler(WorkflowScheduler):
    def __init__(self, repository): self.repository = repository
    def execute(self, run_id):
        run = self.repository.get_run(run_id)
        if run["status"] == "queued": run = self.repository.transition_run(run_id, "running", allowed_from={"queued"})
        return self.repository.transition_run(run_id, "awaiting_human", allowed_from={"running"})


def _service(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    service = WorkflowService(repository, StubScheduler(repository), build_role_registry(), WorkflowLimits())
    workflow = service.create_workflow({"schema_version": "1.0", "name": "w", "description": "x", "nodes": [{"node_id": "e", "role_id": "evidence_specialist", "depends_on": []}], "budget": {"max_nodes": 1, "max_concurrency": 1, "max_tool_calls": 4, "timeout_seconds": 60}, "retry_policy": {"max_attempts": 1}}, actor="alice", idempotency_key="w-1")
    run = service.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="r-1", evidence_summary={}, runtime_snapshot={"profile_snapshot": {"profile_id": "qwen", "connection_id": "local", "enabled": True}, "connection_snapshot": {"connection_id": "local", "provider": "ollama", "enabled": True}, "prompt_id": "agent_plan_v1", "prompt_sha256": "sha"})
    return service, run


def test_airflow_task_recovers_only_stable_workflow_ids(tmp_path):
    service, run = _service(tmp_path)
    container = type("Container", (), {"agent_workflows": service})()
    assert execute_agent_workflow(run["workflow_run_id"], "req-1", container=container) == {"workflow_run_id": run["workflow_run_id"], "status": "awaiting_human"}


def test_airflow_task_rejects_request_identity_mismatch(tmp_path):
    service, run = _service(tmp_path)
    container = type("Container", (), {"agent_workflows": service})()
    try:
        execute_agent_workflow(run["workflow_run_id"], "wrong", container=container)
    except ValueError as exc:
        assert "请求标识" in str(exc)
    else:
        raise AssertionError("request mismatch must fail")
