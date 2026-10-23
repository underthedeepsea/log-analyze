from __future__ import annotations

import copy
import json
import re
from typing import Any, Protocol

from logrisk.ai_harness.model_client import ModelClient
from logrisk.ai_harness.usage_accounting import run_model_attempt

from .errors import AgenticError
from .models import AgentPlan, AgentStepPlan
from .tool_registry import validate_argument_schema
from .evidence_context import EXPERT_ROLES, expert_input_summary, assessment_schema, reference_array_schema, validate_assessment


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


def expert_tool_descriptions(tool_descriptions: list[dict[str, Any]], context: dict[str, Any] | None) -> list[dict[str, Any]]:
    if context is None:
        return copy.deepcopy(tool_descriptions)
    if context["role_id"] not in EXPERT_ROLES:
        raise AgenticError("未知固定专家角色", code="agent_plan_invalid")
    if context["role_id"] != "feature_specialist":
        return []  # Counted prefetch already supplied the material for analysis.
    result = []
    hashes = [item["template_hash"] for item in context["evidence"]["templates"]]
    components = [item["component"] for item in context["evidence"]["templates"] if item.get("component")]
    for source in tool_descriptions:
        if source.get("name") not in {"evaluate_candidate", "register_feature_candidate"}:
            continue
        tool = copy.deepcopy(source)
        feature = tool.get("argument_schema", {}).get("properties", {}).get("feature")
        if isinstance(feature, dict):
            properties = feature.get("properties") or {}
            for field, identities in (("template_hashes", hashes), ("components", components)):
                if field in properties:
                    properties[field] = reference_array_schema(identities, minimum=1)
        result.append(tool)
    return result


def build_plan_schema(tool_descriptions: list[dict[str, Any]], *, max_steps: int | None = None, expert_context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Pair every permitted tool name with its own strict argument schema."""
    pairs = []
    tool_descriptions = expert_tool_descriptions(tool_descriptions, expert_context)
    for tool in tool_descriptions:
        name = tool.get("name")
        if not isinstance(name, str) or not name:
            continue
        argument_schema = tool.get("argument_schema")
        if not isinstance(argument_schema, dict):
            argument_schema = {"type": "object"}
        pairs.append({
            "type": "object",
            "additionalProperties": False,
            "required": ["step_id", "tool_name", "arguments"],
            "properties": {
                "step_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9-]{0,63}$"},
                "tool_name": {"const": name},
                "arguments": copy.deepcopy(argument_schema),
            },
        })
    schema = copy.deepcopy(PLAN_SCHEMA)
    steps = schema["properties"]["steps"]
    steps["items"] = {"oneOf": pairs} if pairs else {}
    steps["uniqueItems"] = True
    if max_steps is not None:
        steps["maxItems"] = int(max_steps)
    if expert_context is not None:
        steps["minItems"] = 0
        if expert_context["role_id"] != "feature_specialist":
            steps["maxItems"] = 0
        schema["required"].append("assessment")
        schema["properties"]["assessment"] = assessment_schema(expert_context)
    else:
        steps["minItems"] = 1
    return schema


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
    minimum = 0 if plan.assessment is not None else 1
    if not plan.goal.strip() or not minimum <= len(plan.steps) <= int(max_steps):
        raise AgenticError("Agent 计划步骤数量无效", code="agent_plan_invalid")
    step_ids = [step.step_id for step in plan.steps]
    if len(step_ids) != len(set(step_ids)):
        raise AgenticError("Agent 计划包含重复步骤", code="agent_plan_invalid")
    for step in plan.steps:
        if not STEP_ID_RE.fullmatch(step.step_id) or step.tool_name not in allowed_tools or not isinstance(step.arguments, dict):
            raise AgenticError("Agent 计划包含未授权工具或无效参数", code="agent_plan_invalid")
        _reject_sensitive(step.arguments)
    return plan


def _parse_plan(value: Any, tool_descriptions: list[dict[str, Any]], expert_context: dict[str, Any] | None = None) -> AgentPlan:
    required = {"goal", "steps", "assessment"} if expert_context is not None else {"goal", "steps"}
    if not isinstance(value, dict) or set(value) != required:
        raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
    if not isinstance(value.get("goal"), str):
        raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
    raw_steps = value.get("steps")
    if not isinstance(raw_steps, list):
        raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
    steps: list[AgentStepPlan] = []
    tool_descriptions = expert_tool_descriptions(tool_descriptions, expert_context)
    schemas = {
        item["name"]: item.get("argument_schema") if isinstance(item.get("argument_schema"), dict) else {"type": "object"}
        for item in tool_descriptions
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    for item in raw_steps:
        if not isinstance(item, dict) or set(item) != {"step_id", "tool_name", "arguments"}:
            raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
        if not isinstance(item["step_id"], str) or not isinstance(item["tool_name"], str) or not isinstance(item["arguments"], dict):
            raise AgenticError("模型返回了无效 Agent 计划", code="agent_plan_invalid")
        schema = schemas.get(item["tool_name"])
        if schema is None:
            raise AgenticError("模型返回了未授权 Agent 工具", code="agent_plan_invalid")
        try:
            validate_argument_schema(item["arguments"], schema)
        except AgenticError as exc:
            raise AgenticError("模型返回了不符合工具契约的参数", code="agent_plan_invalid") from exc
        steps.append(AgentStepPlan(item["step_id"], item["tool_name"], item["arguments"]))
    if expert_context is not None:
        if expert_context["role_id"] != "feature_specialist" and steps:
            raise AgenticError("已预取专家只能分析，不能重复读取", code="agent_plan_invalid")
        if expert_context["role_id"] == "feature_specialist":
            if len(steps) % 2:
                raise AgenticError("每个候选必须完整校验再登记", code="agent_plan_invalid")
            for index in range(0, len(steps), 2):
                evaluated, registered = steps[index:index + 2]
                if evaluated.tool_name != "evaluate_candidate" or registered.tool_name != "register_feature_candidate" or evaluated.arguments.get("feature") != registered.arguments.get("feature"):
                    raise AgenticError("候选必须先校验并登记同一特征", code="agent_plan_invalid")
    assessment = validate_assessment(value["assessment"], expert_context, steps) if expert_context is not None else None
    return AgentPlan(value["goal"], tuple(steps), assessment, copy.deepcopy(value) if expert_context is not None else None)


class FakeAgentPlanner:
    def __init__(self, plan: AgentPlan) -> None:
        self.value = plan

    def plan(self, *, goal: str, evidence_summary: dict[str, Any], tool_descriptions: list[dict[str, Any]], max_steps: int) -> AgentPlan:
        allowed = {str(item.get("name")) for item in tool_descriptions}
        return validate_plan(copy.deepcopy(self.value), allowed_tools=allowed, max_steps=max_steps)


class ModelAgentPlanner:
    requires_evidence_context = True

    def __init__(
        self,
        model_client: ModelClient,
        *,
        model: str,
        prompt_content: str,
        timeout: float,
        options: dict[str, Any] | None = None,
        ledger_repository: Any | None = None,
        analysis_run_id: str | None = None,
        environment: str | None = None,
        scope_key: str | None = None,
        caller_id: str | None = None,
        logical_call_id: str | None = None,
    ) -> None:
        self.model_client = model_client
        self.model = model
        self.prompt_content = prompt_content
        self.timeout = float(timeout)
        self.options = dict(options or {})
        self.ledger_repository = ledger_repository
        self.analysis_run_id = analysis_run_id
        self.environment = environment
        self.scope_key = scope_key
        self.caller_id = caller_id
        self.logical_call_id = logical_call_id

    def plan(self, *, goal: str, evidence_summary: dict[str, Any], tool_descriptions: list[dict[str, Any]], max_steps: int) -> AgentPlan:
        context = evidence_summary.get("expert_context")
        planning_tools = expert_tool_descriptions(tool_descriptions, context)
        remaining_steps = 0 if context is not None and context["role_id"] != "feature_specialist" else int(max_steps)
        payload = {
            "goal": str(goal),
            "evidence_summary": expert_input_summary(evidence_summary),
            "allowed_tools": planning_tools,
            "max_steps": remaining_steps,
        }
        schema = build_plan_schema(planning_tools, max_steps=remaining_steps, expert_context=context)
        try:
            output = run_model_attempt(
                self.model_client,
                [
                    {"role": "system", "content": self.prompt_content},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
                ],
                schema,
                model=self.model,
                timeout=self.timeout,
                options=self.options,
                ledger_repository=self.ledger_repository,
                analysis_run_id=self.analysis_run_id,
                environment=self.environment,
                scope_key=self.scope_key,
                provider=getattr(self.model_client, "provider", None),
                caller_kind="agent_planner",
                caller_id=self.caller_id,
                logical_call_id=self.logical_call_id,
            )
            plan = _parse_plan(output, planning_tools, context)
        except AgenticError:
            raise
        except Exception as exc:
            raise AgenticError("模型未返回有效 Agent 计划", code="agent_plan_failed", status_code=502) from exc
        allowed = {str(item.get("name")) for item in planning_tools}
        return validate_plan(plan, allowed_tools=allowed, max_steps=remaining_steps)
