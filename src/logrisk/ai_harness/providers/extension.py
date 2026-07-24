from __future__ import annotations

import os
from typing import Any, Mapping

from logrisk.ai_harness.model_client import ModelClientError, parse_content_json
from logrisk.ai_harness.providers.extensions.base import ExtensionRequest
from logrisk.ai_harness.providers.extensions.registry import get_extension_adapter


def redact_model_error(text: str, credential_values: list[str]) -> str:
    """Prevent configured credential values from escaping to jobs or traces."""

    redacted = str(text)
    for value in sorted((item for item in credential_values if item), key=len, reverse=True):
        redacted = redacted.replace(value, "***")
    return redacted


class ExtensionModelClient:
    """Common model contract around one explicitly registered private adapter."""

    def __init__(self, connection: Mapping[str, Any]) -> None:
        self.connection = dict(connection)
        self.adapter = get_extension_adapter(str(self.connection.get("adapter_id") or ""))

    def _credential_values(self) -> list[str]:
        mapping = self.connection.get("credential_envs")
        if not isinstance(mapping, Mapping):
            return []
        return [os.environ.get(str(env_name), "") for env_name in mapping.values()]

    def _error(self, exc: Exception, *, status: str = "model_failed", raw_output: str = "") -> ModelClientError:
        values = self._credential_values()
        source_status = exc.status if isinstance(exc, ModelClientError) else status
        source_raw = exc.raw_output if isinstance(exc, ModelClientError) else raw_output
        return ModelClientError(
            redact_model_error(str(exc), values),
            raw_output=redact_model_error(source_raw, values),
            status=source_status,
        )

    def generate_json(
        self,
        messages: list[dict[str, Any]],
        schema: dict[str, Any],
        *,
        model: str,
        timeout: float,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request_options = dict(options or {})
        mode = str(request_options.get("structured_output_mode", "json_schema"))
        if mode not in self.adapter.descriptor.supported_output_modes:
            raise ModelClientError(f"扩展适配器不支持 structured_output_mode: {mode}")
        request = ExtensionRequest(
            connection=self.connection,
            messages=messages,
            schema=schema,
            model=model,
            timeout=timeout,
            options=request_options,
        )
        try:
            self.adapter.validate_connection(self.connection)
            content = self.adapter.generate_content(request)
        except Exception as exc:
            raise self._error(exc) from exc
        try:
            return parse_content_json(content)
        except (TypeError, ValueError) as exc:
            raise self._error(
                ModelClientError("扩展模型返回了无效的结构化响应", raw_output=str(content), status="parse_failed")
            ) from exc
