from __future__ import annotations

import io
import json

import pytest

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.openai_compatible import OpenAICompatibleModelClient
from logrisk.ai_harness.usage_accounting import public_usage, reset_client_metadata, usage_metadata


class Response:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode()


def test_http_200_parse_failure_retains_supplier_usage(monkeypatch):
    monkeypatch.setenv("TEST_MODEL_KEY", "secret")
    client = OpenAICompatibleModelClient(
        "https://model.invalid", api_key_env="TEST_MODEL_KEY",
        opener=lambda *args, **kwargs: Response({"usage": {"prompt_tokens": 125, "completion_tokens": 0, "total_tokens": 125}, "choices": [{"message": {"content": "not json"}}]}),
    )
    with pytest.raises(ModelClientError):
        client.generate_json([], {}, model="m", timeout=1)
    assert public_usage(client) == {"input_tokens": 125, "output_tokens": 0, "total_tokens": 125}


def test_missing_usage_is_unknown_zero_is_known_and_cache_reset_is_empty():
    assert usage_metadata(None)["usage"] == {}
    assert usage_metadata(None)["usage_quality"] == "unknown"
    assert usage_metadata({"input_tokens": 0, "output_tokens": 0})["usage"] == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    client = type("Client", (), {"last_metadata": {"usage": {"total_tokens": 99}}})()
    reset_client_metadata(client)
    assert public_usage(client) == {}


def test_shared_client_concurrent_attempts_keep_request_local_usage():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    import time
    from logrisk.ai_harness.usage_accounting import run_model_attempt

    class Client:
        def generate_json(self, messages, schema, **kwargs):
            self.last_metadata = usage_metadata({"total_tokens": messages[0]["tokens"]})
            time.sleep(0.01)
            return {}

    client = Client()
    finished = Barrier(2)
    def call(tokens):
        run_model_attempt(client,[{"tokens":tokens}],{},model="fake",timeout=1)
        finished.wait(timeout=2)
        return public_usage(client)["total_tokens"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(call,[125,100])) == [125,100]
