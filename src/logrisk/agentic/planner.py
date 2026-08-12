from __future__ import annotations

import copy
import json
import re
from typing import Any, Protocol

from logrisk.ai_harness.model_client import ModelClient

from .errors import AgenticError
from .models import AgentPlan, AgentStepPlan


_SENSITIVE_KEYS = frozenset({"samples", "raw_sample", "raw_log", "raw_logs", "raw_message", "message", "api_key", "token", "password", "secret", "dsn", "authorization", "cookie"})


def _reject_sensitive(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in _SENSITIVE_KEYS:
                raise AgenticError("Agent 计划包含敏感参数", code="agent_plan_sensitive")
            _reject_sensitive(item)
    elif isinstance(value, list):
        for item in value:
            _reject_sensitive(item)


STEP_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
PLAN_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["goal", "steps"],
    "properties": {
        "goal": {"type": "string", "minLength": 1},
        "steps": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["step_id", "tool_name", "arguments"],
                "properties": {
                    "step_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9-]{0,63}$"},
                    "tool_name": {"type": "string"},
                    "arguments": {"type": "object"},
                },
            },
        },
    },
}


class AgentPlanner(Protocol):
    def plan(
        self,
        *,
        goal: str,
        evidence_summary: dict[str, Any],
        tool_descriptions: list[dict[str, Any]],
        max_steps: int,
    ) -> AgentPlan: ...


def validate_plan(plan: AgentPlan, *, allowed_tools: set[str], max_steps: int) -> AgentPlan:
    if not plan.goal.strip() or not 1 <= len(plan.steps) <= int(max_steps):
        raise AgenticError("Agent 计划步骤数量无效", code="agent_plan_invalid")
    step_ids = [step.step_id for step in plan.steps]
    if len(step_ids) != len(set(step_ids)):
        raise AgenticError("Agent 计划包含重复步骤", code="agent_plan_invalid")
    for step in plan.steps:
        if not STEP_ID_RE.fullmatch(step.step_id) or step.tool_name not in allowed_tools or not isinstance(step.arguments, dict):
            raise AgenticError("Agent 计划包含未授权工具或无效参数", code="agent_plan_invalid")
        _reject_sensitive(step.arguments)
    return plan


def _parse_plan(value: Any) -> AgentPlan:
    if not isinstance(value, dict) or set(value) != {"goal", "steps"}:
        raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
    raw_steps = value.get("steps")
    if not isinstance(raw_steps, list):
        raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
    steps: list[AgentStepPlan] = []
    for item in raw_steps:
        if not isinstance(item, dict) or set(item) != {"step_id", "tool_name", "arguments"}:
            raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
        steps.append(AgentStepPlan(str(item["step_id"]), str(item["tool_name"]), dict(item["arguments"])))
    return AgentPlan(str(value.get("goal") or ""), tuple(steps))


class FakeAgentPlanner:
    def __init__(self, plan: AgentPlan) -> None:
        self.value = plan

    def plan(self, *, goal: str, evidence_summary: dict[str, Any], tool_descriptions: list[dict[str, Any]], max_steps: int) -> AgentPlan:
        allowed = {str(item.get("name")) for item in tool_descriptions}
        return validate_plan(copy.deepcopy(self.value), allowed_tools=allowed, max_steps=max_steps)


class ModelAgentPlanner:
    def __init__(
        self,
        model_client: ModelClient,
        *,
        model: str,
        prompt_content: str,
        timeout: float,
        options: dict[str, Any] | None = None,
    ) -> None:
        self.model_client = model_client
        self.model = model
        self.prompt_content = prompt_content
        self.timeout = float(timeout)
        self.options = dict(options or {})

    def plan(self, *, goal: str, evidence_summary: dict[str, Any], tool_descriptions: list[dict[str, Any]], max_steps: int) -> AgentPlan:
        payload = {
            "goal": str(goal),
            "evidence_summary": evidence_summary,
            "allowed_tools": tool_descriptions,
            "max_steps": int(max_steps),
        }
        try:
            output = self.model_client.generate_json(
                [
                    {"role": "system", "content": self.prompt_content},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
                ],
                PLAN_SCHEMA,
                model=self.model,
                timeout=self.timeout,
                options=self.options,
            )
            plan = _parse_plan(output)
        except AgenticError:
            raise
        except Exception as exc:
            raise AgenticError("模型未返回有效 Agent 计划", code="agent_plan_failed", status_code=502) from exc
        allowed = {str(item.get("name")) for item in tool_descriptions}
        return validate_plan(plan, allowed_tools=allowed, max_steps=max_steps)
