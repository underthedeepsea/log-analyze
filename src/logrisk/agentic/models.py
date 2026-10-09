from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import AgenticError


RUN_STATUSES = frozenset({
    "queued", "planning", "running", "paused", "awaiting_human", "completed", "failed", "cancelled",
})


def validate_run_scope(source_job_id: Any, entity_id: Any) -> tuple[str, str]:
    """Reject ambiguous run scope before it reaches persistence or a planner."""
    if not isinstance(source_job_id, str) or not source_job_id.strip():
        raise ValueError("source_job_id 必须是非空字符串")
    if not isinstance(entity_id, str) or not entity_id.strip():
        raise ValueError("entity_id 必须是非空字符串")
    return source_job_id, entity_id


def validate_evidence_scope(
    evidence_summary: Any,
    source_job_id: Any,
    entity_id: Any,
    *,
    code: str,
) -> None:
    """Reject snapshot scope claims that disagree with the persisted run."""
    try:
        canonical_job_id, canonical_entity_id = validate_run_scope(source_job_id, entity_id)
    except ValueError as exc:
        raise AgenticError("Agent Run 缺少规范化实体范围", code=code) from exc
    if not isinstance(evidence_summary, dict):
        raise AgenticError("Agent Run Evidence 摘要无效", code=code)
    for key, canonical in (
        ("source_job_id", canonical_job_id),
        ("job_id", canonical_job_id),
        ("entity_id", canonical_entity_id),
    ):
        if key in evidence_summary and (
            not isinstance(evidence_summary[key], str) or evidence_summary[key] != canonical
        ):
            raise AgenticError("Agent Run Evidence 范围与持久化范围不一致", code=code)
    entity = evidence_summary.get("entity")
    if entity is not None:
        if not isinstance(entity, dict) or (
            "id" in entity
            and (not isinstance(entity["id"], str) or entity["id"] != canonical_entity_id)
        ):
            raise AgenticError("Agent Run Evidence 范围与持久化范围不一致", code=code)


@dataclass(frozen=True)
class AgentRunRequest:
    source_job_id: str
    entity_id: str
    entity_type: str
    model_profile_id: str
    prompt_id: str
    max_steps: int
    max_tool_calls: int
    timeout_seconds: float
    allowed_tools: tuple[str, ...]
    idempotency_key: str
    actor: str
    roles: tuple[str, ...]
    request_id: str
    parent_run_id: str | None = None

    def __post_init__(self) -> None:
        validate_run_scope(self.source_job_id, self.entity_id)
        if not 1 <= int(self.max_steps) <= 20:
            raise ValueError("max_steps 必须在 1 到 20 之间")
        if not 1 <= int(self.max_tool_calls) <= 100:
            raise ValueError("max_tool_calls 必须在 1 到 100 之间")
        if not 1 <= float(self.timeout_seconds) <= 3600:
            raise ValueError("timeout_seconds 必须在 1 到 3600 之间")
        if not self.allowed_tools or any(not str(item).strip() for item in self.allowed_tools):
            raise ValueError("allowed_tools 必须是非空工具名数组")
        if not self.idempotency_key.strip() or not self.actor.strip() or not self.request_id.strip():
            raise ValueError("幂等键、操作人和请求标识不能为空")


@dataclass(frozen=True)
class AgentStepPlan:
    step_id: str
    tool_name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class AgentPlan:
    goal: str
    steps: tuple[AgentStepPlan, ...]
