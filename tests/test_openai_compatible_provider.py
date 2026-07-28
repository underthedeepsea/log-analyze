from __future__ import annotations

import json

import pytest

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers import create_model_client
from logrisk.ai_harness.providers.openai_compatible import OpenAICompatibleModelClient
from logrisk.ai_harness.providers.ollama import OllamaModelClient


class Response:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self):
        return self.payload


def test_openai_compatible_client_sends_auth_and_json_schema(monkeypatch):
    captured = {}

    def opener(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["body"] = json.loads(request.data)
        captured["timeout"] = timeout
        return Response({
            "choices": [{"message": {"content": "```json\n{\"features\": []}\n```"}}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60},
        })

    monkeypatch.setenv("REMOTE_KEY", "secret-token")
    client = OpenAICompatibleModelClient(
        "https://gateway.example/v1/",
        api_key_env="REMOTE_KEY",
        opener=opener,
    )
    result = client.generate_json(
        [{"role": "user", "content": "evidence"}],
        {"type": "object", "properties": {"features": {"type": "array"}}},
        model="remote-model",
        timeout=30,
        options={"temperature": 0, "max_output_tokens": 900, "structured_output_mode": "json_schema"},
    )

    assert result == {"features": []}
    assert captured["url"] == "https://gateway.example/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer secret-token"
    assert captured["body"]["max_tokens"] == 900
    assert captured["body"]["response_format"]["type"] == "json_schema"
    assert client.last_metadata["usage"]["total_tokens"] == 60


def test_openai_compatible_client_requires_configured_api_key(monkeypatch):
    monkeypatch.delenv("MISSING_REMOTE_KEY", raising=False)
    client = OpenAICompatibleModelClient("https://gateway.example/v1", api_key_env="MISSING_REMOTE_KEY")

    with pytest.raises(ModelClientError, match="MISSING_REMOTE_KEY"):
        client.generate_json([], {"type": "object"}, model="m", timeout=10)


def test_provider_factory_selects_connection_type():
    assert isinstance(create_model_client({"provider": "ollama", "base_url": "http://127.0.0.1:11434"}), OllamaModelClient)
    assert isinstance(create_model_client({
        "provider": "openai_compatible",
        "base_url": "https://gateway.example/v1",
        "api_key_env": "REMOTE_KEY",
    }), OpenAICompatibleModelClient)
