from __future__ import annotations

from typing import Any

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.extensions.base import ExtensionAdapter
from logrisk.ai_harness.providers.extensions.token_auth_template import TokenAuthTemplateAdapter


# Keep this literal allow-list. Never import a Python module from API/database input.
ADAPTERS: dict[str, ExtensionAdapter] = {
    "token_auth_template": TokenAuthTemplateAdapter(),
}


def get_extension_adapter(adapter_id: str) -> ExtensionAdapter:
    adapter = ADAPTERS.get(adapter_id)
    if adapter is None:
        raise ModelClientError(f"未注册的扩展适配器: {adapter_id}")
    return adapter


def list_extension_descriptors() -> list[dict[str, Any]]:
    return [adapter.descriptor.public_dict() for _, adapter in sorted(ADAPTERS.items())]
