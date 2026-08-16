from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WorkflowBudget:
    max_nodes: int
    max_concurrency: int
    max_tool_calls: int
    timeout_seconds: float


@dataclass(frozen=True)
class WorkflowNodeDefinition:
    node_id: str
    role_id: str
    depends_on: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    max_steps: int
    max_tool_calls: int
    timeout_seconds: float


@dataclass(frozen=True)
class CompiledWorkflow:
    schema_version: str
    name: str
    description: str
    nodes: tuple[WorkflowNodeDefinition, ...]
    budget: WorkflowBudget
    max_attempts: int
    topological_order: tuple[str, ...]
    parallel_layers: tuple[tuple[str, ...], ...]
