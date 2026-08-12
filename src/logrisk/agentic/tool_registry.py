from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .errors import AgenticError


FORBIDDEN_KEYS = frozenset({
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


@dataclass(frozen=True)
class AgentTool:
    name: str
    description: str
    required_arguments: tuple[str, ...]
    optional_arguments: tuple[str, ...]
    cost_units: int
    writes_candidate: bool
    handler: Callable[[dict[str, Any], AgentToolContext], dict[str, Any]]


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, AgentTool] = {}

    def register(
        self,
        *,
        name: str,
        description: str,
        required_arguments: tuple[str, ...],
        handler: Callable[[dict[str, Any], AgentToolContext], dict[str, Any]],
        optional_arguments: tuple[str, ...] = (),
        cost_units: int = 1,
        writes_candidate: bool = False,
    ) -> None:
        if not name or name in self._tools:
            raise AgenticError("工具名称无效或重复", code="tool_registration_invalid")
        self._tools[name] = AgentTool(
            name=name,
            description=description,
            required_arguments=required_arguments,
            optional_arguments=optional_arguments,
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
        keys = set(arguments)
        required = set(tool.required_arguments)
        allowed = required | set(tool.optional_arguments)
        if required - keys or keys - allowed:
            raise AgenticError("工具参数不符合注册契约", code="tool_arguments_invalid")
        _reject_sensitive(arguments)
        output = tool.handler(dict(arguments), context)
        if not isinstance(output, dict):
            raise AgenticError("工具结果必须是 object", code="tool_result_invalid")
        _reject_sensitive(output)
        return output


def _reject_sensitive(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in FORBIDDEN_KEYS:
                raise AgenticError("工具结果包含敏感字段", code="tool_result_sensitive")
            _reject_sensitive(item)
    elif isinstance(value, list):
        for item in value:
            _reject_sensitive(item)
