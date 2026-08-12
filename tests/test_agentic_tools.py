from __future__ import annotations

import json

import pytest

from logrisk.agentic.errors import AgenticError
from logrisk.agentic.tool_registry import AgentToolContext, ToolRegistry
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.agentic.tools import build_agent_tool_registry


def _document() -> dict:
    return {
        "summary": {"total_raw_logs": 3},
        "risk_entities": [{
            "window_start": "2026-08-12T10:00:00+08:00",
            "window_end": "2026-08-12T10:05:00+08:00",
            "cluster": "prod-a",
            "entity_type": "node",
            "entity_id": "node-a",
            "risk_score": 82,
            "risk_level": "high",
            "affected_entities": ["pod-a"],
            "samples": ["secret raw log"],
            "raw_sample": "secret raw log",
            "top_templates": [{
                "template_hash": "hash-oom",
                "component": "kernel",
                "severity": "ERROR",
                "template": "Out of memory: Killed process <NUM>",
                "count": 3,
                "samples": ["secret raw log"],
            }],
        }],
    }


def _feature() -> dict:
    return {
        "feature_type": "node_memory_pressure",
        "title": "节点内存压力日志",
        "summary": "检测到 kernel 组件的 OOM 异常模板。",
        "importance": "high",
        "template_hashes": ["hash-oom"],
        "components": ["kernel"],
        "tags": ["内核", "内存压力"],
        "selection_reason": "该模板来自 kernel 组件并呈现 OOM 异常。",
    }


def test_agent_evidence_removes_raw_fields_and_candidate_stays_pending():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [], auto_start=False)
    job_id = manager.create_job(_document(), model="qwen3.5:9b-mlx")

    evidence = manager.get_agent_evidence(job_id, "node-a")
    serialized = json.dumps(evidence, ensure_ascii=False)
    candidate = manager.register_agent_candidate(job_id, "node-a", _feature(), run_id="agent-1")

    assert "secret raw log" not in serialized
    assert "samples" not in serialized
    assert "raw_sample" not in serialized
    assert candidate["status"] == "pending"
    assert candidate["agent_run_id"] == "agent-1"
    assert candidate.get("rule_id") is None
    assert manager.get_job(job_id)["entities"][0]["feature_ids"] == [candidate["candidate_id"]]


def test_agent_candidate_rejects_unknown_template_hash():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [], auto_start=False)
    job_id = manager.create_job(_document(), model="qwen3.5:9b-mlx")
    invalid = _feature()
    invalid["template_hashes"] = ["invented"]

    with pytest.raises(FeatureJobError, match="template_hash"):
        manager.register_agent_candidate(job_id, "node-a", invalid, run_id="agent-1")


def test_agent_candidate_registration_is_idempotent_for_same_run():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [], auto_start=False)
    job_id = manager.create_job(_document(), model="qwen3.5:9b-mlx")

    first = manager.register_agent_candidate(job_id, "node-a", _feature(), run_id="agent-1")
    second = manager.register_agent_candidate(job_id, "node-a", _feature(), run_id="agent-1")
    snapshot = manager.get_job(job_id)

    assert second["candidate_id"] == first["candidate_id"]
    assert len(snapshot["features"]) == 1
    assert len([event for event in manager.list_events(job_id) if event["type"] == "agent_candidate_registered"]) == 1


def test_tool_registry_rejects_unlisted_tool_and_unknown_arguments():
    registry = ToolRegistry()
    registry.register(
        name="get_sanitized_evidence",
        description="读取脱敏证据",
        required_arguments=("job_id", "entity_id"),
        handler=lambda arguments, context: {"entity_id": arguments["entity_id"]},
    )
    context = AgentToolContext(
        run_id="run-1",
        source_job_id="job-1",
        entity_id="node-a",
        allowed_tools=frozenset({"get_sanitized_evidence"}),
        actor="alice",
        request_id="req-1",
    )

    with pytest.raises(AgenticError, match="工具未获授权"):
        registry.execute("delete_rule", {}, context)
    with pytest.raises(AgenticError, match="参数"):
        registry.execute(
            "get_sanitized_evidence",
            {"job_id": "job-1", "entity_id": "node-a", "extra": 1},
            context,
        )


def test_tool_registry_rejects_sensitive_result_fields():
    registry = ToolRegistry()
    registry.register(
        name="unsafe",
        description="测试",
        required_arguments=(),
        handler=lambda arguments, context: {"nested": {"raw_log": "secret"}},
    )
    context = AgentToolContext(
        run_id="run-1",
        source_job_id="job-1",
        entity_id="node-a",
        allowed_tools=frozenset({"unsafe"}),
        actor="alice",
        request_id="req-1",
    )

    with pytest.raises(AgenticError, match="敏感字段"):
        registry.execute("unsafe", {}, context)


def test_tool_registry_rejects_generic_message_field():
    registry = ToolRegistry()
    registry.register(
        name="unsafe",
        description="测试",
        required_arguments=(),
        handler=lambda arguments, context: {"message": "raw"},
    )
    context = AgentToolContext(
        "run-1", "job-1", "node-a", frozenset({"unsafe"}), "alice", "req-1"
    )

    with pytest.raises(AgenticError) as exc:
        registry.execute("unsafe", {}, context)

    assert exc.value.code == "tool_result_sensitive"


def test_tool_registry_rejects_sensitive_argument_before_handler():
    called = []
    registry = ToolRegistry()
    registry.register(
        name="unsafe", description="测试", required_arguments=("payload",),
        handler=lambda arguments, context: called.append(arguments) or {"ok": True},
    )
    context = AgentToolContext("run-1", "job-1", "node-a", frozenset({"unsafe"}), "alice", "req-1")

    with pytest.raises(AgenticError):
        registry.execute("unsafe", {"payload": {"message": "raw"}}, context)

    assert called == []


def test_evidence_tool_cannot_cross_the_run_entity_boundary():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [], auto_start=False)
    job_id = manager.create_job(_document(), model="qwen3.5:9b-mlx")

    class Rules:
        def list_rules(self, **_kwargs): return {"items": []}

    class Packages:
        def list_packages(self): return []

    registry = build_agent_tool_registry(manager, Rules(), Packages())
    context = AgentToolContext("run-1", job_id, "node-a", frozenset({"get_sanitized_evidence"}), "alice", "req-1")
    with pytest.raises(AgenticError, match="边界"):
        registry.execute("get_sanitized_evidence", {"job_id": job_id, "entity_id": "node-b"}, context)


def test_knowledge_tool_returns_only_materialized_assets_from_installed_versions():
    class Jobs: pass

    class Rules:
        def list_rules(self, **_kwargs): return {"items": []}

    class Packages:
        def list_packages(self): return [{"package_id": "linux", "name": "Linux"}]
        def get_package(self, package_id):
            return {"versions": [
                {"version": "1.0.0", "status": "installed", "assets": [
                    {"asset_id": "enabled", "asset_type": "semantic_dictionary", "status": "materialized", "target_domain": "semantic", "target_resource_id": "linux"},
                    {"asset_id": "disabled", "asset_type": "prompt", "status": "disabled"},
                ]},
                {"version": "0.9.0", "status": "retired", "assets": [{"asset_id": "old", "status": "materialized"}]},
            ]}

    registry = build_agent_tool_registry(Jobs(), Rules(), Packages())
    context = AgentToolContext("run-1", "job-1", "node-a", frozenset({"inspect_knowledge_assets"}), "alice", "req-1")

    result = registry.execute("inspect_knowledge_assets", {}, context)

    assert result["items"] == [{
        "package_id": "linux", "package_version": "1.0.0", "asset_id": "enabled",
        "asset_type": "semantic_dictionary", "target_domain": "semantic", "target_resource_id": "linux",
    }]
