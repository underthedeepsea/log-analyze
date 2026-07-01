from __future__ import annotations

from typing import Any, Protocol


class ModelClientError(RuntimeError):
    def __init__(self, message: str, *, raw_output: str = "", status: str = "model_failed") -> None:
        super().__init__(message)
        self.raw_output = raw_output
        self.status = status


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
