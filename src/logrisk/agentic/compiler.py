from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .errors import AgenticError
from .roles import RoleRegistry
from .workflow_models import CompiledWorkflow, WorkflowBudget, WorkflowNodeDefinition


NODE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


@dataclass(frozen=True)
class WorkflowLimits:
    max_nodes: int = 20
    max_concurrency: int = 4
    max_tool_calls: int = 100
    max_timeout_seconds: float = 3600
    max_attempts: int = 3


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        raise AgenticError("工作流预算无效", code="workflow_budget_invalid")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise AgenticError("工作流预算无效", code="workflow_budget_invalid") from exc
    if parsed <= 0:
        raise AgenticError("工作流预算无效", code="workflow_budget_invalid")
    return parsed


def compile_workflow(value: dict[str, Any], roles: RoleRegistry, limits: WorkflowLimits) -> CompiledWorkflow:
    if not isinstance(value, dict) or set(value) != {"schema_version", "name", "description", "nodes", "budget", "retry_policy"}:
        raise AgenticError("工作流结构无效", code="workflow_schema_invalid")
    raw_nodes, raw_budget, retry = value.get("nodes"), value.get("budget"), value.get("retry_policy")
    if not isinstance(raw_nodes, list) or not raw_nodes or not isinstance(raw_budget, dict) or not isinstance(retry, dict):
        raise AgenticError("工作流结构无效", code="workflow_schema_invalid")
    if set(raw_budget) != {"max_nodes", "max_concurrency", "max_tool_calls", "timeout_seconds"} or set(retry) != {"max_attempts"}:
        raise AgenticError("工作流结构无效", code="workflow_schema_invalid")
    max_nodes, concurrency = _positive_int(raw_budget["max_nodes"]), _positive_int(raw_budget["max_concurrency"])
    max_tool_calls, attempts = _positive_int(raw_budget["max_tool_calls"]), _positive_int(retry["max_attempts"])
    try:
        timeout = float(raw_budget["timeout_seconds"])
    except (TypeError, ValueError) as exc:
        raise AgenticError("工作流预算无效", code="workflow_budget_invalid") from exc
    if (len(raw_nodes) > max_nodes or max_nodes > limits.max_nodes or concurrency > limits.max_concurrency
            or concurrency > len(raw_nodes) or max_tool_calls > limits.max_tool_calls
            or timeout <= 0 or timeout > limits.max_timeout_seconds or attempts > limits.max_attempts):
        raise AgenticError("工作流预算超出平台限制", code="workflow_budget_invalid")

    nodes: list[WorkflowNodeDefinition] = []
    identifiers: set[str] = set()
    for raw in raw_nodes:
        if not isinstance(raw, dict) or set(raw) != {"node_id", "role_id", "depends_on"} or not isinstance(raw["depends_on"], list):
            raise AgenticError("工作流节点结构无效", code="workflow_schema_invalid")
        node_id = str(raw["node_id"])
        if not NODE_ID_RE.fullmatch(node_id):
            raise AgenticError("工作流节点标识无效", code="workflow_schema_invalid")
        if node_id in identifiers:
            raise AgenticError("工作流节点重复", code="workflow_node_duplicate")
        role = roles.get(str(raw["role_id"]))
        if role is None:
            raise AgenticError("工作流角色未注册", code="workflow_role_unknown")
        identifiers.add(node_id)
        nodes.append(WorkflowNodeDefinition(node_id, role.role_id, tuple(map(str, raw["depends_on"])), role.allowed_tools, role.max_steps, role.max_tool_calls, role.timeout_seconds))

    for node in nodes:
        if node.node_id in node.depends_on or len(node.depends_on) != len(set(node.depends_on)):
            raise AgenticError("工作流存在循环依赖", code="workflow_cycle")
        if any(dependency not in identifiers for dependency in node.depends_on):
            raise AgenticError("工作流依赖不存在", code="workflow_dependency_unknown")
    remaining = {node.node_id: set(node.depends_on) for node in nodes}
    order_hint = {node.node_id: index for index, node in enumerate(nodes)}
    layers: list[tuple[str, ...]] = []
    while remaining:
        ready = tuple(sorted((node_id for node_id, dependencies in remaining.items() if not dependencies), key=order_hint.get))
        if not ready:
            raise AgenticError("工作流存在循环依赖", code="workflow_cycle")
        layers.append(ready)
        for node_id in ready:
            remaining.pop(node_id)
        for dependencies in remaining.values():
            dependencies.difference_update(ready)
    order = tuple(node_id for layer in layers for node_id in layer)
    by_id = {node.node_id: node for node in nodes}
    return CompiledWorkflow(str(value["schema_version"]), str(value["name"]), str(value["description"]), tuple(by_id[node_id] for node_id in order), WorkflowBudget(max_nodes, concurrency, max_tool_calls, timeout), attempts, order, tuple(layers))
