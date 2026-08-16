from __future__ import annotations

from logrisk.agentic.compiler import WorkflowLimits, compile_workflow
from logrisk.agentic.roles import build_role_registry
from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.database import SQLiteDatabase


def _compiled():
    return compile_workflow({
        "schema_version": "1.0", "name": "协作", "description": "test",
        "nodes": [
            {"node_id": "a", "role_id": "evidence_specialist", "depends_on": []},
            {"node_id": "b", "role_id": "rule_specialist", "depends_on": []},
            {"node_id": "c", "role_id": "feature_specialist", "depends_on": ["a", "b"]},
        ],
        "budget": {"max_nodes": 3, "max_concurrency": 2, "max_tool_calls": 20, "timeout_seconds": 300},
        "retry_policy": {"max_attempts": 2},
    }, build_role_registry(), WorkflowLimits())


def _runtime_snapshot(profile_id="qwen", prompt_id="agent_plan_v1"):
    return {
        "profile_snapshot": {"profile_id": profile_id, "connection_id": "local", "enabled": True},
        "connection_snapshot": {"connection_id": "local", "provider": "ollama", "enabled": True},
        "prompt_id": prompt_id,
        "prompt_sha256": "sha-1",
    }


def test_repository_persists_definition_run_nodes_and_monotonic_events(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    replayed = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    run = repository.create_run(
        workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node",
        model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",),
        request_id="req-1", idempotency_key="run-1", runtime_snapshot=_runtime_snapshot(),
    )

    assert replayed["workflow_id"] == workflow["workflow_id"]
    assert replayed["idempotent_replay"] is True
    assert [node["node_id"] for node in run["nodes"]] == ["a", "b", "c"]
    assert [event["sequence"] for event in run["events"]] == [0]
    assert run["locked_snapshot"]["parallel_layers"] == [["a", "b"], ["c"]]


def test_repository_claims_only_ready_nodes_and_recovers_running_nodes(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    run = repository.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", runtime_snapshot=_runtime_snapshot())
    repository.transition_run(run["workflow_run_id"], "running", allowed_from={"queued"})

    ready = repository.ready_nodes(run["workflow_run_id"])
    assert [node["node_id"] for node in ready] == ["a", "b"]
    repository.claim_node(run["workflow_run_id"], "a")
    repository.finish_node(run["workflow_run_id"], "a", status="completed", used_tool_calls=2, result={"ok": True})
    assert [node["node_id"] for node in repository.ready_nodes(run["workflow_run_id"])] == ["b"]
    repository.claim_node(run["workflow_run_id"], "b")

    recovered = repository.recover_active_runs()
    restored = repository.get_run(run["workflow_run_id"])
    assert recovered == [run["workflow_run_id"]]
    assert next(node for node in restored["nodes"] if node["node_id"] == "b")["status"] == "pending"
    assert restored["used_tool_calls"] == 2


def test_repository_control_idempotency_does_not_duplicate_events(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    run = repository.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", runtime_snapshot=_runtime_snapshot())

    first = repository.transition_run(run["workflow_run_id"], "paused", allowed_from={"queued"}, idempotency_key="pause-1")
    second = repository.transition_run(run["workflow_run_id"], "paused", allowed_from={"queued"}, idempotency_key="pause-1")

    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    assert len([event for event in second["events"] if event["type"] == "workflow_run_paused"]) == 1


def test_repository_recovers_only_the_requested_run(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    first = repository.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", runtime_snapshot=_runtime_snapshot())
    second = repository.create_run(workflow["workflow_id"], source_job_id="job-2", entity_id="node-b", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-2", idempotency_key="run-2", runtime_snapshot=_runtime_snapshot())
    for run in (first, second):
        repository.transition_run(run["workflow_run_id"], "running", allowed_from={"queued"})
        repository.claim_node(run["workflow_run_id"], "a")

    repository.recover_run(first["workflow_run_id"])

    assert next(node for node in repository.get_run(first["workflow_run_id"])["nodes"] if node["node_id"] == "a")["status"] == "pending"
    assert next(node for node in repository.get_run(second["workflow_run_id"])["nodes"] if node["node_id"] == "a")["status"] == "running"


def test_repository_rejects_sensitive_evidence_summary_before_persistence(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")

    try:
        repository.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", evidence_summary={"raw_log": "secret"})
    except Exception as exc:
        assert "敏感" in str(exc)
    else:
        raise AssertionError("sensitive evidence must be rejected")
    assert repository.list_runs() == []


def test_repository_lists_runs_by_workflow_and_flattens_artifacts(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    run = repository.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", runtime_snapshot=_runtime_snapshot())
    repository.transition_run(run["workflow_run_id"], "running", allowed_from={"queued"})
    repository.claim_node(run["workflow_run_id"], "a")
    repository.finish_node(run["workflow_run_id"], "a", status="completed", result={"child_agent_run_id": "child-a", "artifacts": [{"artifact_id": "candidate-1", "artifact_type": "candidate", "payload": {"candidate_id": "candidate-1"}}]})

    assert [item["workflow_run_id"] for item in repository.list_runs(workflow_id=workflow["workflow_id"])] == [run["workflow_run_id"]]
    assert repository.get_run(run["workflow_run_id"])["artifacts"][0]["artifact_type"] == "candidate"


def test_repository_requeues_paused_node_for_resume(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    run = repository.create_run(workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node", model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",), request_id="req-1", idempotency_key="run-1", runtime_snapshot=_runtime_snapshot())
    repository.transition_run(run["workflow_run_id"], "running", allowed_from={"queued"})
    repository.claim_node(run["workflow_run_id"], "a")
    node = repository.requeue_node(run["workflow_run_id"], "a", reason="workflow_paused")
    assert node["status"] == "pending"
    assert node["child_agent_run_id"] is None
    assert any(event["type"] == "workflow_node_pending" for event in repository.get_run(run["workflow_run_id"])["events"])


def test_repository_locks_runtime_model_snapshot_at_run_creation(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    run = repository.create_run(
        workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node",
        model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",),
        request_id="req-1", idempotency_key="run-1", evidence_summary={"entity": {"id": "node-a"}},
        runtime_snapshot=_runtime_snapshot(),
    )
    snapshot = repository.get_run(run["workflow_run_id"])["locked_snapshot"]
    assert snapshot["profile_snapshot"]["profile_id"] == "qwen"
    assert snapshot["profile_snapshot"]["connection_id"] == "local"
    assert snapshot["connection_snapshot"]["enabled"] is True
    assert snapshot["prompt_sha256"] == "sha-1"


def test_repository_requires_a_complete_runtime_snapshot(tmp_path):
    repository = WorkflowRepository(SQLiteDatabase(tmp_path / "state.sqlite3"))
    workflow = repository.create_workflow(_compiled(), actor="alice", idempotency_key="workflow-1")
    try:
        repository.create_run(
            workflow["workflow_id"], source_job_id="job-1", entity_id="node-a", entity_type="node",
            model_profile_id="qwen", prompt_id="agent_plan_v1", actor="alice", roles=("operator",),
            request_id="req-1", idempotency_key="run-1", runtime_snapshot=None,
        )
    except Exception as exc:
        assert "快照" in str(exc)
    else:
        raise AssertionError("workflow runtime snapshot must be mandatory")
    assert repository.list_runs() == []
