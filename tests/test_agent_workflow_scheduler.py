from __future__ import annotations

import threading
import time

import pytest

from logrisk.agentic.compiler import WorkflowLimits, compile_workflow
from logrisk.agentic.models import AgentRunRequest
from logrisk.agentic.roles import build_role_registry
from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.agentic.workflow_scheduler import WorkflowScheduler
from logrisk.agentic.workflow_service import WorkflowService
from logrisk.agentic.workflow_worker import WorkflowWorker
from logrisk.agentic.repository import AgentRepository
from logrisk.database import SQLiteDatabase


def _definition(*, max_attempts: int = 2, max_tool_calls: int = 20) -> dict:
    return {
        "schema_version": "1.0", "name": "协作", "description": "parallel",
        "nodes": [
            {"node_id": "evidence", "role_id": "evidence_specialist", "depends_on": []},
            {"node_id": "rules", "role_id": "rule_specialist", "depends_on": []},
            {"node_id": "feature", "role_id": "feature_specialist", "depends_on": ["evidence", "rules"]},
        ],
        "budget": {"max_nodes": 3, "max_concurrency": 2, "max_tool_calls": max_tool_calls, "timeout_seconds": 30},
        "retry_policy": {"max_attempts": max_attempts},
    }


def _runtime_snapshot():
    return {"profile_snapshot": {"profile_id": "qwen", "connection_id": "local", "enabled": True}, "connection_snapshot": {"connection_id": "local", "provider": "ollama", "enabled": True}, "prompt_id": "agent_plan_v1", "prompt_sha256": "sha"}


class FakeChildAgentService:
    def __init__(self, *, fail_once: str | None = None, cost: int = 1, delay: float = 0.04) -> None:
        self.fail_once = fail_once
        self.cost = cost
        self.delay = delay
        self.created: dict[str, dict] = {}
        self.intervals: dict[str, list[tuple[float, float]]] = {}
        self.controls: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def create_run(self, request, *, locked_snapshot):
        persisted = self.repository.create_run(request, locked_snapshot=locked_snapshot)
        run_id = persisted["run_id"]
        with self._lock:
            self.created[run_id] = {"request": request, "locked_snapshot": locked_snapshot, "attempt": len(self.intervals.get(locked_snapshot["workflow_node_id"], [])) + 1}
        return persisted

    def execute_run(self, run_id):
        item = self.created[run_id]
        node_id = item["locked_snapshot"]["workflow_node_id"]
        started = time.monotonic()
        time.sleep(self.delay)
        finished = time.monotonic()
        with self._lock:
            self.intervals.setdefault(node_id, []).append((started, finished))
        if self.fail_once == node_id and len(self.intervals[node_id]) == 1:
            return {"run_id": run_id, "status": "failed", "used_tool_calls": self.cost, "artifacts": [], "error_code": "temporary"}
        artifacts = [{"artifact_id": "candidate-1", "artifact_type": "candidate"}] if node_id == "feature" else []
        return {"run_id": run_id, "status": "awaiting_human", "used_tool_calls": self.cost, "artifacts": artifacts, "error_code": None}

    def pause(self, run_id, *, idempotency_key):
        self.controls.append(("pause", run_id)); return {"run_id": run_id, "status": "paused"}

    def cancel(self, run_id, *, idempotency_key):
        self.controls.append(("cancel", run_id)); return {"run_id": run_id, "status": "cancelled"}


def _service(tmp_path, child, definition=None):
    database = SQLiteDatabase(tmp_path / "state.sqlite3")
    repository = WorkflowRepository(database)
    child.repository = AgentRepository(database)
    service = WorkflowService(repository, WorkflowScheduler(repository, WorkflowWorker(repository, child)), build_role_registry(), WorkflowLimits())
    workflow = service.create_workflow(definition or _definition(), actor="alice", idempotency_key="workflow-1")
    run = service.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", evidence_summary={"entity": {"id": "node-a"}, "template_count": 2}, runtime_snapshot=_runtime_snapshot())
    return service, run


def test_scheduler_runs_independent_nodes_in_parallel_then_dependency(tmp_path):
    child = FakeChildAgentService()
    service, run = _service(tmp_path, child)

    result = service.execute_run(run["workflow_run_id"])

    assert result["status"] == "awaiting_human"
    evidence, rules = child.intervals["evidence"][0], child.intervals["rules"][0]
    feature = child.intervals["feature"][0]
    assert max(evidence[0], rules[0]) < min(evidence[1], rules[1])
    assert feature[0] >= max(evidence[1], rules[1])
    feature_run = next(item for item in child.created.values() if item["locked_snapshot"]["workflow_node_id"] == "feature")
    assert feature_run["locked_snapshot"]["dependency_nodes"] == ["evidence", "rules"]
    assert set(feature_run["locked_snapshot"]["evidence_summary"]["dependency_artifacts"]) == {"evidence", "rules"}
    assert feature_run["locked_snapshot"]["prompt_sha256"] == "sha"
    assert feature_run["locked_snapshot"]["profile_snapshot"]["profile_id"] == "qwen"


def test_scheduler_retries_same_locked_node_without_recompiling(tmp_path):
    child = FakeChildAgentService(fail_once="rules")
    service, run = _service(tmp_path, child)

    result = service.execute_run(run["workflow_run_id"])

    rules = next(node for node in result["nodes"] if node["node_id"] == "rules")
    assert result["status"] == "awaiting_human"
    assert rules["attempt"] == 2
    assert len(child.intervals["rules"]) == 2
    assert len([event for event in result["events"] if event["type"] == "workflow_node_retry_queued"]) == 1


def test_scheduler_fails_closed_before_exceeding_global_tool_budget(tmp_path):
    child = FakeChildAgentService(cost=2)
    service, run = _service(tmp_path, child, _definition(max_tool_calls=3))

    result = service.execute_run(run["workflow_run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "workflow_budget_exhausted"
    assert result["used_tool_calls"] <= 3
    assert next(node for node in result["nodes"] if node["node_id"] == "feature")["status"] != "completed"


def test_pause_and_cancel_propagate_to_active_child_runs(tmp_path):
    child = FakeChildAgentService(delay=0)
    service, run = _service(tmp_path, child)
    repository = service.repository
    repository.transition_run(run["workflow_run_id"], "running", allowed_from={"queued"})
    repository.claim_node(run["workflow_run_id"], "evidence")
    child_evidence = child.repository.create_run(
        AgentRunRequest("job-1", "node-a", "node", "qwen", "agent_plan_v1", 1, 1, 30, ("get_sanitized_evidence",), "manual-evidence", "alice", ("operator",), "req-1"),
        locked_snapshot={"workflow_node_id": "evidence"},
    )["run_id"]
    repository.attach_child_run(run["workflow_run_id"], "evidence", child_evidence)

    paused = service.pause(run["workflow_run_id"], idempotency_key="pause-1")
    resumed = service.resume(run["workflow_run_id"], idempotency_key="resume-1")
    repository.claim_node(run["workflow_run_id"], "rules")
    child_rules = child.repository.create_run(
        AgentRunRequest("job-1", "node-a", "node", "qwen", "agent_plan_v1", 1, 1, 30, ("find_approved_rules",), "manual-rules", "alice", ("operator",), "req-1"),
        locked_snapshot={"workflow_node_id": "rules"},
    )["run_id"]
    repository.attach_child_run(run["workflow_run_id"], "rules", child_rules)
    cancelled = service.cancel(run["workflow_run_id"], idempotency_key="cancel-1")

    assert paused["status"] == "paused"
    assert resumed["status"] == "running"
    assert cancelled["status"] == "cancelled"
    assert ("pause", child_evidence) in child.controls
    assert ("cancel", child_rules) in child.controls


def test_node_retry_action_is_idempotent_and_resumes_failed_run(tmp_path):
    child = FakeChildAgentService(fail_once="rules")
    service, run = _service(tmp_path, child, _definition(max_attempts=1))
    failed = service.execute_run(run["workflow_run_id"])
    assert failed["status"] == "failed"

    first = service.retry_node(run["workflow_run_id"], "rules", idempotency_key="node-retry-1")
    second = service.retry_node(run["workflow_run_id"], "rules", idempotency_key="node-retry-1")

    assert first["status"] == second["status"] == "queued"
    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    assert len([event for event in second["events"] if event["type"] == "workflow_node_retry_queued"]) == 1


def test_recovery_replays_interrupted_node_with_same_locked_workflow(tmp_path):
    child = FakeChildAgentService()
    service, run = _service(tmp_path, child)
    service.repository.transition_run(run["workflow_run_id"], "running", allowed_from={"queued"})
    service.repository.claim_node(run["workflow_run_id"], "evidence")

    recovered = service.recover_active_runs()
    result = service.execute_run(run["workflow_run_id"])

    assert recovered == [run["workflow_run_id"]]
    assert result["status"] == "awaiting_human"
    assert next(node for node in result["nodes"] if node["node_id"] == "evidence")["attempt"] == 2


def test_workflow_rejects_sensitive_evidence_before_persistence(tmp_path):
    child = FakeChildAgentService()
    database = SQLiteDatabase(tmp_path / "state.sqlite3")
    repository = WorkflowRepository(database)
    child.repository = AgentRepository(database)
    service = WorkflowService(repository, WorkflowScheduler(repository, WorkflowWorker(repository, child)), build_role_registry(), WorkflowLimits())
    workflow = service.create_workflow(_definition(), actor="alice", idempotency_key="workflow-1")

    with pytest.raises(Exception, match="敏感|原始"):
        service.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", evidence_summary={"samples": ["raw log"]})

    assert repository.list_runs() == []
