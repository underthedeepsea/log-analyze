from __future__ import annotations
from __future__ import annotations

from typing import Any

from logrisk.ai_harness.model_client import ModelClient, ModelClientError
from logrisk.ai_harness.providers.extension import ExtensionModelClient
from logrisk.ai_harness.providers.ollama import OllamaModelClient
from logrisk.ai_harness.providers.openai_compatible import OpenAICompatibleModelClient


def create_model_client(connection: dict[str, Any]) -> ModelClient:
    provider = str(connection.get("provider") or "")
    base_url = str(connection.get("base_url") or "")
    if provider == "ollama":
        return OllamaModelClient(base_url)
    if provider == "openai_compatible":
        return OpenAICompatibleModelClient(base_url, api_key_env=str(connection.get("api_key_env") or ""))
    if provider == "extension":
        return ExtensionModelClient(connection)
    raise ModelClientError(f"不支持的模型 Provider: {provider}")
