from __future__ import annotations

import pytest

from logrisk.agentic import AgenticError
from logrisk.agentic.compiler import WorkflowLimits, compile_workflow
from logrisk.agentic.roles import build_role_registry


def _workflow() -> dict:
    return {
        "schema_version": "1.0",
        "name": "节点风险特征协作",
        "description": "固定角色并行收集证据后生成待审批候选",
        "nodes": [
            {"node_id": "evidence", "role_id": "evidence_specialist", "depends_on": []},
            {"node_id": "rules", "role_id": "rule_specialist", "depends_on": []},
            {"node_id": "feature", "role_id": "feature_specialist", "depends_on": ["evidence", "rules"]},
        ],
        "budget": {"max_nodes": 6, "max_concurrency": 2, "max_tool_calls": 18, "timeout_seconds": 300},
        "retry_policy": {"max_attempts": 2},
    }


def test_compiler_builds_stable_parallel_layers_and_locks_role_tools():
    compiled = compile_workflow(_workflow(), build_role_registry(), WorkflowLimits())

    assert compiled.topological_order == ("evidence", "rules", "feature")
    assert compiled.parallel_layers == (("evidence", "rules"), ("feature",))
    assert compiled.nodes[2].allowed_tools == (
        "get_sanitized_evidence", "evaluate_candidate", "register_feature_candidate",
    )
    assert compiled.nodes[2].role_id == "feature_specialist"


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda value: value["nodes"].append({"node_id": "evidence", "role_id": "rule_specialist", "depends_on": []}), "workflow_node_duplicate"),
        (lambda value: value["nodes"][0].update(role_id="unknown"), "workflow_role_unknown"),
        (lambda value: value["nodes"][0].update(depends_on=["missing"]), "workflow_dependency_unknown"),
        (lambda value: value["nodes"][0].update(depends_on=["feature"]), "workflow_cycle"),
        (lambda value: value["budget"].update(max_nodes=2), "workflow_budget_invalid"),
        (lambda value: value["budget"].update(max_concurrency=9), "workflow_budget_invalid"),
    ],
)
def test_compiler_rejects_invalid_graph_or_budget(mutate, code):
    value = _workflow()
    mutate(value)

    with pytest.raises(AgenticError) as raised:
        compile_workflow(value, build_role_registry(), WorkflowLimits(max_concurrency=4))

    assert raised.value.code == code


def test_compiler_rejects_client_tool_override_and_unknown_fields():
    value = _workflow()
    value["nodes"][0]["allowed_tools"] = ["register_feature_candidate"]

    with pytest.raises(AgenticError) as raised:
        compile_workflow(value, build_role_registry(), WorkflowLimits())

    assert raised.value.code == "workflow_schema_invalid"


def test_compiler_rejects_recursive_or_dynamic_role_definition():
    value = _workflow()
    value["nodes"][0]["role_id"] = "workflow_creator"

    with pytest.raises(AgenticError) as raised:
        compile_workflow(value, build_role_registry(), WorkflowLimits())

    assert raised.value.code == "workflow_role_unknown"
