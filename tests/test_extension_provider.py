from __future__ import annotations

import pytest

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.extension import ExtensionModelClient
from logrisk.ai_harness.providers.extensions.base import (
    ExtensionDescriptor,
    ExtensionRequest,
)
from logrisk.ai_harness.providers.extensions.registry import (
    ADAPTERS,
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


class FakeExtensionAdapter:
    descriptor = ExtensionDescriptor(
        adapter_id="fake_extension",
        display_name="Fake",
        supported_output_modes=("json_schema",),
        credential_fields={"access_token": "Token"},
        config_help="test",
    )

    def __init__(self, content: str | None = None, error: ModelClientError | None = None):
        self.content = content
        self.error = error

    def validate_connection(self, connection):
        return None

    def check_connection(self, connection):
        return {"online": True, "models": ["fake"]}

    def generate_content(self, request):
        if self.error:
            raise self.error
        return self.content


def test_extension_client_accepts_fenced_json_from_registered_adapter(monkeypatch):
    monkeypatch.setitem(ADAPTERS, "fake_extension", FakeExtensionAdapter("```json\n{\"features\": []}\n```"))

    result = ExtensionModelClient({
        "provider": "extension",
        "adapter_id": "fake_extension",
        "credential_envs": {},
    }).generate_json([], {"type": "object"}, model="fake", timeout=5)

    assert result == {"features": []}


def test_extension_client_redacts_configured_credential_from_error_and_raw_output(monkeypatch):
    monkeypatch.setenv("INTERNAL_ACCESS_TOKEN", "secret-token")
    monkeypatch.setitem(
        ADAPTERS,
        "fake_extension",
        FakeExtensionAdapter(error=ModelClientError("failed secret-token", raw_output="response secret-token")),
    )
    client = ExtensionModelClient({
        "provider": "extension",
        "adapter_id": "fake_extension",
        "credential_envs": {"access_token": "INTERNAL_ACCESS_TOKEN"},
    })

    with pytest.raises(ModelClientError, match=r"failed \*\*\*") as captured:
        client.generate_json([], {"type": "object"}, model="fake", timeout=5)

    assert "secret-token" not in captured.value.raw_output
