from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable

from .errors import AgenticError


FORBIDDEN_KEYS = frozenset({
    "raw", "raw_record", "raw_records", "raw_samples", "log_stream", "raw_stream",
    "samples", "raw_sample", "raw_log", "raw_logs", "raw_message", "message", "api_key", "token",
    "password", "secret", "dsn", "authorization", "cookie",
})


@dataclass(frozen=True)
class AgentToolContext:
    run_id: str
    source_job_id: str
    entity_id: str
    allowed_tools: frozenset[str]
    actor: str
    request_id: str
    evidence_hash: str | None = None


@dataclass(frozen=True)
class AgentTool:
    name: str
    description: str
    required_arguments: tuple[str, ...]
    optional_arguments: tuple[str, ...]
    argument_schema: dict[str, Any]
    cost_units: int
    writes_candidate: bool
    handler: Callable[[dict[str, Any], AgentToolContext], dict[str, Any]]


class ToolRegistry:
    def __init__(self, *, evidence_loader: Callable[[AgentToolContext], dict[str, Any]] | None = None) -> None:
        self._tools: dict[str, AgentTool] = {}
        self._evidence_loader = evidence_loader

    def current_evidence_hash(self, context: AgentToolContext) -> str | None:
        if self._evidence_loader is None:
            return context.evidence_hash
        from .artifacts import canonical_fingerprint
        evidence = self._evidence_loader(context)
        _reject_sensitive(evidence)
        return canonical_fingerprint(evidence)

    def register(
        self,
        *,
        name: str,
        description: str,
        required_arguments: tuple[str, ...],
        handler: Callable[[dict[str, Any], AgentToolContext], dict[str, Any]],
        optional_arguments: tuple[str, ...] = (),
        argument_schema: dict[str, Any] | None = None,
        cost_units: int = 1,
        writes_candidate: bool = False,
    ) -> None:
        if not name or name in self._tools:
            raise AgenticError("工具名称无效或重复", code="tool_registration_invalid")
        schema = copy.deepcopy(argument_schema) if argument_schema is not None else {
            "type": "object",
            "required": list(required_arguments),
            "properties": {key: {} for key in (*required_arguments, *optional_arguments)},
            "additionalProperties": False,
        }
        if not isinstance(schema, dict) or schema.get("type") != "object":
            raise AgenticError("工具参数 Schema 无效", code="tool_registration_invalid")
        self._tools[name] = AgentTool(
            name=name,
            description=description,
            required_arguments=required_arguments,
            optional_arguments=optional_arguments,
            argument_schema=schema,
            cost_units=max(1, int(cost_units)),
            writes_candidate=bool(writes_candidate),
            handler=handler,
        )

    def describe(self, allowed_tools: frozenset[str] | None = None) -> list[dict[str, Any]]:
        names = sorted(self._tools)
        if allowed_tools is not None:
            names = [name for name in names if name in allowed_tools]
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "required_arguments": list(tool.required_arguments),
                "optional_arguments": list(tool.optional_arguments),
                "argument_schema": copy.deepcopy(tool.argument_schema),
                "cost_units": tool.cost_units,
                "writes_candidate": tool.writes_candidate,
            }
            for name in names
            for tool in (self._tools[name],)
        ]

    def get(self, name: str) -> AgentTool:
        tool = self._tools.get(str(name))
        if not tool:
            raise AgenticError("工具未获授权", code="tool_not_allowed", status_code=403)
        return tool

    def execute(self, name: str, arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        if name not in context.allowed_tools:
            raise AgenticError("工具未获授权", code="tool_not_allowed", status_code=403)
        tool = self.get(name)
        if not isinstance(arguments, dict):
            raise AgenticError("工具参数必须是 object", code="tool_arguments_invalid")
        validate_argument_schema(arguments, tool.argument_schema)
        _reject_sensitive(arguments)
        output = tool.handler(dict(arguments), context)
        if not isinstance(output, dict):
            raise AgenticError("工具结果必须是 object", code="tool_result_invalid")
        _reject_sensitive(output)
        return output


def validate_argument_schema(value: Any, schema: dict[str, Any], *, path: str = "arguments") -> None:
    """Validate the small, strict JSON-Schema subset used by Agent tools.

    This deliberately validates without coercing values, so the object that is
    evaluated is the exact object that reaches candidate registration.
    """
    if not isinstance(schema, dict):
        raise AgenticError("工具参数 Schema 无效", code="tool_arguments_invalid")
    expected = schema.get("type")
    valid_types = {
        "null": value is None,
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
    }
    expected_types = expected if isinstance(expected, list) else [expected]
    if expected is not None and not any(valid_types.get(str(item), False) for item in expected_types):
        raise AgenticError(f"工具参数 {path} 类型无效", code="tool_arguments_invalid")
    if "enum" in schema and value not in schema["enum"]:
        raise AgenticError(f"工具参数 {path} 不在允许范围", code="tool_arguments_invalid")
    if isinstance(value, str) and "minLength" in schema and len(value) < int(schema["minLength"]):
        raise AgenticError(f"工具参数 {path} 不能为空", code="tool_arguments_invalid")
    if isinstance(value, list):
        if "minItems" in schema and len(value) < int(schema["minItems"]):
            raise AgenticError(f"工具参数 {path} 不能为空", code="tool_arguments_invalid")
        item_schema = schema.get("items")
        if item_schema is not None:
            for index, item in enumerate(value):
                validate_argument_schema(item, item_schema, path=f"{path}[{index}]")
    if isinstance(value, dict):
        properties = schema.get("properties") or {}
        required = schema.get("required") or []
        if not isinstance(properties, dict) or not isinstance(required, list):
            raise AgenticError("工具参数 Schema 无效", code="tool_arguments_invalid")
        missing = [key for key in required if key not in value]
        unknown = set(value) - set(properties)
        if missing or (schema.get("additionalProperties") is False and unknown):
            raise AgenticError("工具参数不符合注册契约", code="tool_arguments_invalid")
        for key, item in value.items():
            item_schema = properties.get(key)
            if item_schema is not None:
                validate_argument_schema(item, item_schema, path=f"{path}.{key}")


def _reject_sensitive(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in FORBIDDEN_KEYS:
                raise AgenticError("工具结果包含敏感字段", code="tool_result_sensitive")
            _reject_sensitive(item)
    elif isinstance(value, list):
        for item in value:
            _reject_sensitive(item)
