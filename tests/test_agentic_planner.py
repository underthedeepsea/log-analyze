from __future__ import annotations

import pytest

from logrisk.agentic.errors import AgenticError
from logrisk.agentic.models import AgentPlan, AgentStepPlan
from logrisk.agentic.planner import FakeAgentPlanner, ModelAgentPlanner, validate_plan
from logrisk.ai_harness.providers.mock import MockModelClient


def test_validate_plan_rejects_unknown_tool_duplicate_step_and_excess_steps():
    plan = AgentPlan(
        goal="检查证据",
        steps=(
            AgentStepPlan("same", "unknown", {}),
            AgentStepPlan("same", "unknown", {}),
        ),
    )
    with pytest.raises(AgenticError, match="计划"):
        validate_plan(plan, allowed_tools={"safe"}, max_steps=1)


def test_model_planner_builds_only_sanitized_structured_request():
    client = MockModelClient({
        "goal": "检查脱敏证据",
        "steps": [{
            "step_id": "read-evidence",
            "tool_name": "get_sanitized_evidence",
            "arguments": {"job_id": "job-1", "entity_id": "node-a"},
        }],
    })
    planner = ModelAgentPlanner(
        client,
        model="qwen3.5:9b-mlx",
        prompt_content="只输出 JSON，不执行 RCA。",
        timeout=30,
        options={"temperature": 0},
    )

    plan = planner.plan(
        goal="检查",
        evidence_summary={"entity": {"id": "node-a"}, "template_count": 2},
        tool_descriptions=[{"name": "get_sanitized_evidence"}],
        max_steps=2,
    )

    assert plan.goal == "检查脱敏证据"
    request = client.requests[0]
    assert request["schema"]["required"] == ["goal", "steps"]
    assert "raw_log" not in str(request["messages"])


def test_fake_planner_returns_validated_copy():
    source = AgentPlan("目标", (AgentStepPlan("step-1", "safe", {}),))
    result = FakeAgentPlanner(source).plan(
        goal="目标", evidence_summary={}, tool_descriptions=[{"name": "safe"}], max_steps=1
    )
    assert result == source


def test_plan_rejects_sensitive_argument_keys_before_persistence():
    plan = AgentPlan("目标", (AgentStepPlan("step-1", "safe", {"nested": {"raw_log": "secret"}}),))
    with pytest.raises(AgenticError, match="敏感"):
        validate_plan(plan, allowed_tools={"safe"}, max_steps=1)
