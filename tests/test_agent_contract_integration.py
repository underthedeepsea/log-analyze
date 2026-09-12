from __future__ import annotations

import hashlib
import json
import copy

import pytest

from logrisk.agentic.artifacts import MAX_ARTIFACT_BYTES, MAX_DEPENDENCY_BYTES, dependency_artifacts, read_tool_artifact
from logrisk.agentic.compiler import WorkflowLimits
from logrisk.agentic.models import AgentPlan, AgentRunRequest, AgentStepPlan
from logrisk.agentic.planner import FakeAgentPlanner, ModelAgentPlanner
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.runtime import AgentRuntime
from logrisk.agentic.roles import build_role_registry
from logrisk.agentic.service import AgentService
from logrisk.agentic.tool_registry import AgentToolContext
from logrisk.agentic.tools import build_agent_tool_registry
from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.agentic.workflow_scheduler import WorkflowScheduler
from logrisk.agentic.workflow_service import WorkflowService
from logrisk.agentic.workflow_worker import WorkflowWorker
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager


FEATURE = {
    "feature_type": "node_memory_pressure", "title": "内存压力异常",
    "summary": "kernel 模板包含 OOM 异常。", "importance": "high",
    "template_hashes": ["hash-oom"], "components": ["kernel"],
    "tags": ["内存", "异常"], "selection_reason": "模板明确记录 OOM 异常。",
}


class Rules:
    def list_rules(self, **kwargs):
        return {"items": [], "pagination": {"total": 0}}


class Packages:
    def list_packages(self):
        return []


def _setup(tmp_path):
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [], auto_start=False)
    job_id = manager.create_job({"risk_entities": [{
        "entity_id": "node-a", "entity_type": "node", "cluster": "prod",
        "risk_score": 82, "risk_level": "high", "samples": ["raw secret"],
        "top_templates": [{"template_hash": "hash-oom", "component": "kernel",
                           "template": "OOM killed process <NUM>", "count": 3,
                           "severity": "ERROR", "raw_sample": "raw secret"}],
    }]}, model="qwen3.5:9b-mlx")
    registry = build_agent_tool_registry(manager, Rules(), Packages())
    repository = AgentRepository(SQLiteDatabase(tmp_path / "agent.sqlite3"))
    request = AgentRunRequest(job_id, "node-a", "node", "profile", "agent_plan_v1",
                              4, 6, 120, tuple(item["name"] for item in registry.describe()),
                              "create", "alice", ("operator",), "req-1")
    return manager, registry, repository, request


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def test_real_evaluation_envelope_passes_guard_and_binds_exact_inputs(tmp_path):
    manager, registry, _, request = _setup(tmp_path)
    context = AgentToolContext("run-1", request.source_job_id, "node-a",
                               frozenset(request.allowed_tools), "alice", "req-1")
    result = registry.execute("evaluate_candidate", {"feature": FEATURE}, context)

    assert result["passed"] is True
    assert result["feature"] == FEATURE
    assert result["fingerprint"] == _hash(FEATURE)
    assert result["evidence_hash"] == _hash(manager.get_agent_evidence(request.source_job_id, "node-a"))
    assert result["evaluator_version"]
    assert all("message" not in row and "detail" in row for row in result["rule_results"])


def test_real_registry_evaluate_then_register_stops_at_pending_candidate(tmp_path):
    manager, registry, repository, request = _setup(tmp_path)
    plan = AgentPlan("提取", (
        AgentStepPlan("evaluate", "evaluate_candidate", {"feature": FEATURE}),
        AgentStepPlan("register", "register_feature_candidate", {"feature": FEATURE}),
    ))
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {"template_count": 1}})

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "awaiting_human"
    assert result["used_tool_calls"] == 2
    candidate = manager.get_job(request.source_job_id)["features"][0]
    assert candidate["status"] == "pending"
    assert candidate.get("rule_id") is None


def test_approved_rule_lookup_matches_only_rule_on_second_page(tmp_path):
    manager, _, _, request = _setup(tmp_path)

    class PagedRules:
        def list_rules(self, *, status, page, page_size):
            assert status == "active"
            rows = [{"rule_id": str(i), "status": "active", "template_signatures": [
                {"template_hash": "hash-oom" if i == 100 else "other", "component": "kernel"},
            ]} for i in range(101)]
            return {"items": rows[(page - 1) * page_size:page * page_size],
                    "pagination": {"total": len(rows)}}

    registry = build_agent_tool_registry(manager, PagedRules(), Packages())
    context = AgentToolContext("run-1", request.source_job_id, "node-a",
                               frozenset(request.allowed_tools), "alice", "req-1")
    result = registry.execute("find_approved_rules", {"template_hashes": ["hash-oom"]}, context)

    assert [item["rule_id"] for item in result["items"]] == ["100"]
    assert result["total"] == 101
    assert result["matched"] == 1
    assert result["truncated"] is False


@pytest.mark.parametrize("invalid", ["missing_feature", "forged_fingerprint", "stale_version", "changed_evidence", "truthy_passed"])
def test_resume_rejects_invalid_evaluation_credentials(tmp_path, monkeypatch, invalid):
    manager, registry, repository, request = _setup(tmp_path)
    plan = AgentPlan("提取", (
        AgentStepPlan("evaluate", "evaluate_candidate", {"feature": FEATURE}),
        AgentStepPlan("register", "register_feature_candidate", {"feature": FEATURE}),
    ))
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {}})
    context = AgentToolContext(run["run_id"], request.source_job_id, "node-a",
                               frozenset(request.allowed_tools), "alice", "req-1")
    envelope = registry.execute("evaluate_candidate", {"feature": FEATURE}, context)
    if invalid == "missing_feature":
        envelope.pop("feature")
    elif invalid == "forged_fingerprint":
        envelope["fingerprint"] = "forged"
    elif invalid == "stale_version":
        envelope["evaluator_version"] = "feature_output_old"
    elif invalid == "truthy_passed":
        envelope["passed"] = "true"
    else:
        changed = manager.get_agent_evidence(request.source_job_id, "node-a")
        changed["templates"][0]["count"] += 1
        monkeypatch.setattr(manager, "get_agent_evidence", lambda *_: copy.deepcopy(changed))
    _checkpoint_evaluation(repository, run["run_id"], plan, envelope)

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "failed"
    assert result["error_code"] == "human_gate_bypass"
    assert manager.get_job(request.source_job_id)["features"] == []


def _checkpoint_evaluation(repository, run_id, plan, envelope):
    repository.transition(run_id, "planning", allowed_from={"queued"})
    repository.replace_plan(run_id, plan)
    repository.transition(run_id, "running", allowed_from={"planning"})
    repository.start_step(run_id, "evaluate")
    repository.add_artifact(run_id, "evaluation", envelope, step_id="evaluate", fingerprint=_hash(FEATURE))
    repository.finish_step(run_id, "evaluate", status="completed", result_summary=envelope)
    repository.transition(run_id, "paused", allowed_from={"running"})


@pytest.mark.parametrize("reevaluate", [False, True])
def test_resume_accepts_current_credentials_and_can_reevaluate_changed_evidence(tmp_path, monkeypatch, reevaluate):
    manager, registry, repository, request = _setup(tmp_path)
    steps = [AgentStepPlan("evaluate", "evaluate_candidate", {"feature": FEATURE})]
    if reevaluate:
        steps.append(AgentStepPlan("reevaluate", "evaluate_candidate", {"feature": FEATURE}))
    steps.append(AgentStepPlan("register", "register_feature_candidate", {"feature": FEATURE}))
    plan = AgentPlan("提取", tuple(steps))
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {}})
    context = AgentToolContext(run["run_id"], request.source_job_id, "node-a",
                               frozenset(request.allowed_tools), "alice", "req-1")
    envelope = registry.execute("evaluate_candidate", {"feature": FEATURE}, context)
    _checkpoint_evaluation(repository, run["run_id"], plan, envelope)
    if reevaluate:
        changed = manager.get_agent_evidence(request.source_job_id, "node-a")
        changed["templates"][0]["count"] += 1
        monkeypatch.setattr(manager, "get_agent_evidence", lambda *_: copy.deepcopy(changed))

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "awaiting_human"
    assert len(manager.get_job(request.source_job_id)["features"]) == 1
    assert result["used_tool_calls"] == (2 if reevaluate else 1)


def test_read_artifact_is_bounded_without_losing_full_tool_result(tmp_path, monkeypatch):
    manager, registry, repository, request = _setup(tmp_path)
    large = manager.get_agent_evidence(request.source_job_id, "node-a")
    large["templates"][0]["template"] = "聚合异常 " * 10000
    monkeypatch.setattr(manager, "get_agent_evidence", lambda *_: copy.deepcopy(large))
    plan = AgentPlan("读取", (AgentStepPlan("read", "get_sanitized_evidence", {
        "job_id": request.source_job_id, "entity_id": request.entity_id,
    }),))
    run = repository.create_run(request, locked_snapshot={"evidence_summary": {}})

    result = AgentRuntime(repository, FakeAgentPlanner(plan), registry).execute(run["run_id"])

    assert result["status"] == "awaiting_human"
    artifact = result["artifacts"][0]
    payload = artifact["payload"]
    assert artifact["artifact_type"] == "evidence_assessment_v1"
    assert len(json.dumps(payload, ensure_ascii=False).encode()) <= MAX_ARTIFACT_BYTES
    assert payload["safe_payload"] == {}
    assert payload["requires_explicit_read"] is True
    assert payload["limitations"]
    source = next(item for item in result["tool_calls"] if item["tool_call_id"] == payload["evidence_refs"][0]["tool_call_id"])
    assert source["result_summary"] == large


def test_dependency_summary_is_bounded_and_preserves_every_artifact_reference():
    nodes = []
    for index in range(3):
        payload = read_tool_artifact(run_id=f"run-{index}", source_job_id="job-1", entity_id="node-a",
                                    tool_name="get_sanitized_evidence", tool_call_id=f"call-{index}",
                                    evidence_hash="hash", output={"templates": [{"template": "x" * 14000}]})
        nodes.append({"node_id": str(index), "result_summary": {
            "child_agent_run_id": f"run-{index}", "artifacts": [
                {"artifact_id": f"artifact-{index}", "artifact_type": "evidence_assessment_v1",
                 "run_id": f"run-{index}", "payload": payload},
            ],
        }})

    summary = dependency_artifacts(nodes, ["0", "1", "2"])

    assert len(json.dumps(summary, ensure_ascii=False).encode()) <= MAX_DEPENDENCY_BYTES
    for index in range(3):
        artifact = summary[str(index)]["artifacts"][0]
        assert artifact["artifact_id"] == f"artifact-{index}"
        assert artifact["run_id"] == f"run-{index}"
        assert artifact["requires_explicit_read"] is True
        assert "payload" not in artifact
        assert summary[str(index)]["limitations"]


def test_real_workflow_passes_read_tool_artifacts_into_downstream_model(tmp_path):
    manager, _, repository, request = _setup(tmp_path)

    class MatchingRules:
        def list_rules(self, **kwargs):
            return {"items": [{"rule_id": "approved-1", "status": "active", "title": "已批准内存异常",
                               "template_signatures": [{"template_hash": "hash-oom", "component": "kernel"}]}],
                    "pagination": {"total": 1}}

    registry = build_agent_tool_registry(manager, MatchingRules(), Packages())
    model_inputs = {}

    class Model:
        def __init__(self, role):
            self.role = role

        def generate_json(self, messages, schema, **kwargs):
            payload = json.loads(messages[1]["content"])
            model_inputs[self.role] = payload
            if self.role == "evidence_specialist":
                steps = [{"step_id": "read", "tool_name": "get_sanitized_evidence",
                          "arguments": {"job_id": request.source_job_id, "entity_id": request.entity_id}}]
            elif self.role == "rule_specialist":
                steps = [{"step_id": "rules", "tool_name": "find_approved_rules", "arguments": {"template_hashes": ["hash-oom"]}}]
            else:
                dependencies = payload["evidence_summary"]["dependency_artifacts"]
                assert dependencies["evidence"]["artifacts"][0]["payload"]["safe_payload"]["templates"][0]["template_hash"] == "hash-oom"
                assert dependencies["rules"]["artifacts"][0]["payload"]["safe_payload"]["items"][0]["rule_id"] == "approved-1"
                steps = [{"step_id": "evaluate", "tool_name": "evaluate_candidate", "arguments": {"feature": FEATURE}},
                         {"step_id": "register", "tool_name": "register_feature_candidate", "arguments": {"feature": FEATURE}}]
            return {"goal": "分析聚合证据", "steps": steps}

    def planner(run):
        return ModelAgentPlanner(Model(run["locked_snapshot"]["workflow_role_id"]), model="test-model",
                                 prompt_content="仅使用脱敏证据", timeout=30)

    child = AgentService(repository, AgentRuntime(repository, planner, registry))
    workflows = WorkflowRepository(repository.database)
    service = WorkflowService(workflows, WorkflowScheduler(workflows, WorkflowWorker(workflows, child)),
                              build_role_registry(), WorkflowLimits())
    workflow = service.create_workflow({
        "schema_version": "1.0", "name": "协作", "description": "固定角色",
        "nodes": [{"node_id": "evidence", "role_id": "evidence_specialist", "depends_on": []},
                  {"node_id": "rules", "role_id": "rule_specialist", "depends_on": []},
                  {"node_id": "feature", "role_id": "feature_specialist", "depends_on": ["evidence", "rules"]}],
        "budget": {"max_nodes": 3, "max_concurrency": 2, "max_tool_calls": 20, "timeout_seconds": 30},
        "retry_policy": {"max_attempts": 1},
    }, actor="alice", idempotency_key="workflow")
    run = service.create_run(workflow["workflow_id"], source_job_id=request.source_job_id,
                             entity_id="node-a", entity_type="node", model_profile_id="profile", prompt_id="agent_plan_v1",
                             actor="alice", roles=("operator",), request_id="req-1", idempotency_key="workflow-run",
                             evidence_summary={"entity": {"id": "node-a"}, "template_count": 1}, runtime_snapshot={
                                 "profile_snapshot": {"profile_id": "profile", "connection_id": "local", "enabled": True},
                                 "connection_snapshot": {"connection_id": "local", "provider": "ollama", "enabled": True},
                                 "prompt_id": "agent_plan_v1", "prompt_sha256": "sha",
                             })

    result = service.execute_run(run["workflow_run_id"])

    assert result["status"] == "awaiting_human"
    assert len(model_inputs) == 3
    assert "raw secret" not in json.dumps(model_inputs)
    assert manager.get_job(request.source_job_id)["features"][0]["status"] == "pending"
