from __future__ import annotations

import json
from typing import Any, Protocol


class ModelClientError(RuntimeError):
    def __init__(self, message: str, *, raw_output: str = "", status: str = "model_failed") -> None:
        super().__init__(message)
        self.raw_output = raw_output
        self.status = status


def parse_content_json(content: str) -> dict[str, Any]:
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        text = content.strip()
        if not text.startswith("```"):
            raise
        lines = text.splitlines()
        if not lines or lines[0].strip() not in {"```", "```json", "```JSON"} or lines[-1].strip() != "```":
            raise
        parsed = json.loads("\n".join(lines[1:-1]).strip())
    if not isinstance(parsed, dict):
        raise TypeError("模型 content JSON 必须是 object")
    return parsed


class ModelClient(Protocol):
    def generate_json(
        self,
        messages: list[dict[str, Any]],
        schema: dict[str, Any],
        *,
        model: str,
        timeout: float,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...
