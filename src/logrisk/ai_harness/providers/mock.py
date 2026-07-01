from __future__ import annotations

import copy
from typing import Any


class MockModelClient:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.requests: list[dict[str, Any]] = []

    def generate_json(
        self,
        messages: list[dict[str, Any]],
        schema: dict[str, Any],
        *,
        model: str,
        timeout: float,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.requests.append({
            "messages": copy.deepcopy(messages),
            "schema": copy.deepcopy(schema),
            "model": model,
            "timeout": timeout,
            "options": copy.deepcopy(options or {}),
        })
        return copy.deepcopy(self.response)
