from __future__ import annotations

from logrisk.agentic.models import AgentPlan, AgentRunRequest, AgentStepPlan
from logrisk.agentic.planner import FakeAgentPlanner
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.runtime import AgentRuntime
from logrisk.agentic.service import AgentService
from logrisk.agentic.tool_registry import ToolRegistry
from logrisk.database import SQLiteDatabase


FEATURE = {
    "feature_type": "node_memory_pressure",
    "title": "节点内存压力日志",
    "summary": "检测到内存压力异常模板。",
    "importance": "high",
    "template_hashes": ["hash-oom"],
    "components": ["kernel"],
    "tags": ["内存压力"],
    "selection_reason": "模板呈现 OOM 异常。",
}


def _request(key: str = "create-1", *, max_tool_calls: int = 4) -> AgentRunRequest:
    return AgentRunRequest(
        source_job_id="job-1", entity_id="node-a", entity_type="node",
        model_profile_id="profile-1", prompt_id="agent_plan_v1",
        max_steps=4, max_tool_calls=max_tool_calls, timeout_seconds=120,
        allowed_tools=("get_sanitized_evidence", "evaluate_candidate", "register_feature_candidate"),
        idempotency_key=key, actor="alice", roles=("operator",), request_id="req-1",
    )


def _runtime(tmp_path, plan: AgentPlan, *, registered: list[dict] | None = None):
    repository = AgentRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    registry = ToolRegistry()
    registry.register(
        name="get_sanitized_evidence", description="证据", required_arguments=("job_id", "entity_id"),
        handler=lambda arguments, context: {"schema_version": "1.0", "entity": {"id": "node-a"}, "templates": [{"template_hash": "hash-oom"}]},
    )
    registry.register(
        name="evaluate_candidate", description="校验", required_arguments=("feature",),
        handler=lambda arguments, context: {"passed": True, "feature": arguments["feature"]},
    )
    registry.register(
        name="register_feature_candidate", description="登记", required_arguments=("feature",), writes_candidate=True,
        handler=lambda arguments, context: (registered.append(arguments["feature"]) or {"candidate_id": "candidate-1", "status": "pending"}) if registered is not None else {"candidate_id": "candidate-1", "status": "pending"},
    )
    return repository, AgentRuntime(repository, FakeAgentPlanner(plan), registry)


def test_runtime_executes_plan_and_stops_at_human_gate(tmp_path):
    plan = AgentPlan("提取候选", (
        AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),
        AgentStepPlan("evaluate", "evaluate_candidate", {"feature": FEATURE}),
        AgentStepPlan("register", "register_feature_candidate", {"feature": FEATURE}),
    ))
    registered: list[dict] = []
    repository, runtime = _runtime(tmp_path, plan, registered=registered)
    run = repository.create_run(_request(), locked_snapshot={"evidence_summary": {"template_count": 1}})

    result = runtime.execute(run["run_id"])

    assert result["status"] == "awaiting_human"
    assert [step["status"] for step in result["steps"]] == ["completed", "completed", "completed"]
    assert result["used_tool_calls"] == 3
    assert registered == [FEATURE]
    assert {item["artifact_type"] for item in result["artifacts"]} == {"evaluation", "candidate"}


def test_runtime_blocks_candidate_registration_without_passed_evaluation(tmp_path):
    plan = AgentPlan("绕过校验", (
        AgentStepPlan("register", "register_feature_candidate", {"feature": FEATURE}),
    ))
    repository, runtime = _runtime(tmp_path, plan)
    run = repository.create_run(_request(), locked_snapshot={"evidence_summary": {}})

    result = runtime.execute(run["run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "human_gate_bypass"


def test_runtime_rejects_evaluator_result_for_a_different_feature(tmp_path):
    plan = AgentPlan("错误校验", (
        AgentStepPlan("evaluate", "evaluate_candidate", {"feature": FEATURE}),
        AgentStepPlan("register", "register_feature_candidate", {"feature": FEATURE}),
    ))
    repository = AgentRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    registry = ToolRegistry()
    other = {**FEATURE, "feature_type": "different"}
    registry.register(
        name="evaluate_candidate", description="校验", required_arguments=("feature",),
        handler=lambda arguments, context: {"passed": True, "feature": other},
    )
    registry.register(
        name="register_feature_candidate", description="登记", required_arguments=("feature",), writes_candidate=True,
        handler=lambda arguments, context: {"candidate_id": "candidate-1", "status": "pending"},
    )
    request = AgentRunRequest(**{**_request().__dict__, "allowed_tools": ("evaluate_candidate", "register_feature_candidate")})
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {}})

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "human_gate_bypass"


def test_service_pause_resume_cancel_retry_and_replay_are_persisted(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository, runtime = _runtime(tmp_path, plan)
    service = AgentService(repository, runtime)
    created = service.create_run(_request(), locked_snapshot={"evidence_summary": {}})

    paused = service.pause(created["run_id"], idempotency_key="pause-1")
    resumed = service.resume(created["run_id"], idempotency_key="resume-1")
    cancelled = service.cancel(created["run_id"], idempotency_key="cancel-1")
    retried = service.retry(created["run_id"], idempotency_key="retry-1", request_id="req-2")
    replay = service.replay(created["run_id"])

    assert paused["status"] == "paused"
    assert resumed["status"] == "queued"
    assert cancelled["status"] == "cancelled"
    assert retried["parent_run_id"] == created["run_id"]
    assert replay["run_id"] == created["run_id"]
    assert replay["mode"] == "read_only"


def test_resume_keeps_existing_plan_instead_of_replanning(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository, runtime = _runtime(tmp_path, plan)
    service = AgentService(repository, runtime)
    created = service.create_run(_request(), locked_snapshot={"evidence_summary": {}})
    repository.transition(created["run_id"], "planning", allowed_from={"queued"})
    repository.replace_plan(created["run_id"], plan)
    repository.transition(created["run_id"], "paused", allowed_from={"planning"})

    resumed = service.resume(created["run_id"], idempotency_key="resume-1")
    result = service.execute_run(created["run_id"])

    assert resumed["status"] == "running"
    assert result["status"] == "awaiting_human"
    assert [step["step_id"] for step in result["steps"]] == ["read"]


def test_runtime_records_tool_lifecycle_events_and_enforces_elapsed_timeout(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository, runtime = _runtime(tmp_path, plan)
    moments = iter([0.0, 0.1, 0.2, 2.0, 2.1, 2.2])
    runtime.monotonic = lambda: next(moments)
    request = AgentRunRequest(**{**_request().__dict__, "timeout_seconds": 1})
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {}})

    result = runtime.execute(run["run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "agent_timeout"
    event_types = [item["type"] for item in result["events"]]
    assert "tool_call_started" in event_types
    assert "tool_call_failed" in event_types


def test_runtime_recovers_planning_and_running_runs_after_restart(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository, runtime = _runtime(tmp_path, plan)
    planning = repository.create_run(_request("planning"), locked_snapshot={"evidence_summary": {}})
    repository.transition(planning["run_id"], "planning", allowed_from={"queued"})
    running = repository.create_run(_request("running"), locked_snapshot={"evidence_summary": {}})
    repository.transition(running["run_id"], "planning", allowed_from={"queued"})
    repository.replace_plan(running["run_id"], plan)
    repository.transition(running["run_id"], "running", allowed_from={"planning"})

    assert runtime.execute(planning["run_id"])["status"] == "awaiting_human"
    assert runtime.execute(running["run_id"])["status"] == "awaiting_human"


def test_control_idempotency_replays_same_transition(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository, runtime = _runtime(tmp_path, plan)
    service = AgentService(repository, runtime)
    run = service.create_run(_request(), locked_snapshot={"evidence_summary": {}})

    first = service.pause(run["run_id"], idempotency_key="pause-1")
    second = service.pause(run["run_id"], idempotency_key="pause-1")

    assert first["status"] == second["status"] == "paused"
    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    assert len([event for event in second["events"] if event["type"] == "run_paused"]) == 1


def test_runtime_retries_one_locked_tool_call_without_replanning(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository = AgentRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    registry = ToolRegistry()
    attempts = []

    def flaky(arguments, context):
        attempts.append(dict(arguments))
        if len(attempts) == 1:
            raise RuntimeError("temporary")
        return {"schema_version": "1.0", "entity": {"id": "node-a"}, "templates": []}

    registry.register(name="get_sanitized_evidence", description="证据", required_arguments=("job_id", "entity_id"), handler=flaky)
    run = repository.create_run(_request(max_tool_calls=2), locked_snapshot={"evidence_summary": {}})

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "awaiting_human"
    assert attempts == [{"job_id": "job-1", "entity_id": "node-a"}] * 2
    assert result["used_tool_calls"] == 2
    assert [call["status"] for call in result["tool_calls"]] == ["failed", "completed"]


def test_service_recovery_resets_interrupted_step(tmp_path):
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),))
    repository, runtime = _runtime(tmp_path, plan)
    service = AgentService(repository, runtime)
    run = service.create_run(_request(), locked_snapshot={"evidence_summary": {}})
    repository.transition(run["run_id"], "planning", allowed_from={"queued"})
    repository.replace_plan(run["run_id"], plan)
    repository.transition(run["run_id"], "running", allowed_from={"planning"})
    repository.start_step(run["run_id"], "read")

    recovered = service.recover_active_runs()
    result = service.execute_run(run["run_id"])

    assert recovered == [run["run_id"]]
    assert result["status"] == "awaiting_human"
    assert result["steps"][0]["attempt"] == 2


def test_runtime_fails_closed_when_tool_budget_is_exhausted(tmp_path):
    plan = AgentPlan("读取", (
        AgentStepPlan("read-1", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),
        AgentStepPlan("read-2", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),
    ))
    repository, runtime = _runtime(tmp_path, plan)
    run = repository.create_run(_request(max_tool_calls=1), locked_snapshot={"evidence_summary": {}})

    result = runtime.execute(run["run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "agent_budget_exhausted"
    assert result["used_tool_calls"] == 1


def test_retry_budget_exhaustion_marks_started_step_failed(tmp_path):
    plan = AgentPlan("读取", (
        AgentStepPlan("read", "get_sanitized_evidence", {"job_id": "job-1", "entity_id": "node-a"}),
    ))
    repository = AgentRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    registry = ToolRegistry()

    def always_fails(arguments, context):
        raise RuntimeError("temporary")

    registry.register(
        name="get_sanitized_evidence", description="证据",
        required_arguments=("job_id", "entity_id"), handler=always_fails,
    )
    run = repository.create_run(_request(max_tool_calls=1), locked_snapshot={"evidence_summary": {}})

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "agent_budget_exhausted"
    assert result["steps"][0]["status"] == "failed"
    assert result["steps"][0]["error_code"] == "agent_budget_exhausted"
