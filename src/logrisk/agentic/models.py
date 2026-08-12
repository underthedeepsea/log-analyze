from __future__ import annotations

from dataclasses import dataclass
from typing import Any


RUN_STATUSES = frozenset({
    "queued", "planning", "running", "paused", "awaiting_human", "completed", "failed", "cancelled",
})


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
