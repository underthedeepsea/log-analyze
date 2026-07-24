from __future__ import annotations

import pytest

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.extensions.base import ExtensionRequest
from logrisk.ai_harness.providers.extensions.registry import (
    get_extension_adapter,
    list_extension_descriptors,
)


def test_token_auth_template_is_discoverable_but_refuses_unimplemented_calls():
    adapter = get_extension_adapter("token_auth_template")

    with pytest.raises(ModelClientError, match="需在内部环境补全"):
        adapter.generate_content(
            ExtensionRequest(
                connection={
                    "adapter_id": "token_auth_template",
                    "credential_envs": {"access_token": "INTERNAL_ACCESS_TOKEN"},
                },
                messages=[],
                schema={"type": "object"},
                model="internal-model",
                timeout=5,
                options={},
            )
        )


def test_unknown_extension_adapter_is_rejected_without_dynamic_import():
    with pytest.raises(ModelClientError, match="未注册的扩展适配器"):
        get_extension_adapter("../../unsafe")


def test_extension_descriptor_declares_non_secret_configuration_fields():
    descriptor = list_extension_descriptors()[0]

    assert descriptor["adapter_id"] == "token_auth_template"
    assert descriptor["credential_fields"] == {"access_token": "访问 Token 环境变量"}
    assert "Token 实际值" in descriptor["config_help"]
