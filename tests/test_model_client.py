import json
import socket
from urllib.error import HTTPError, URLError

import pytest

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.mock import MockModelClient
from logrisk.ai_harness.providers.ollama import OllamaModelClient


SCHEMA = {"type": "object", "properties": {"features": {"type": "array"}}}


def test_mock_model_client_returns_response_and_records_request():
    client = MockModelClient(response={"features": []})

    result = client.generate_json(
        [{"role": "user", "content": "{}"}],
        SCHEMA,
        model="qwen3:1.7b",
        timeout=3,
    )

    assert result == {"features": []}
    assert client.requests[0]["model"] == "qwen3:1.7b"
    assert client.requests[0]["schema"] == SCHEMA


def test_ollama_model_client_posts_non_streaming_json_schema_request():
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data)

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({
                    "message": {"content": json.dumps({"features": []})},
                }).encode()

        return Response()

    client = OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen)
    result = client.generate_json(
        [{"role": "user", "content": "{}"}],
        SCHEMA,
        model="qwen3:1.7b",
        timeout=9,
    )

    assert result == {"features": []}
    assert captured["url"] == "http://127.0.0.1:11434/api/chat"
    assert captured["timeout"] == 9
    assert captured["body"]["stream"] is False
    assert captured["body"]["format"] == SCHEMA
    assert captured["body"]["options"] == {"temperature": 0}


def test_ollama_model_client_merges_options_with_temperature_default():
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({"message": {"content": json.dumps({"features": []})}}).encode()

        return Response()

    OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen).generate_json(
        [],
        SCHEMA,
        model="qwen3:1.7b",
        timeout=9,
        options={"think": False, "num_predict": 1200},
    )

    assert captured["body"]["think"] is False
    assert captured["body"]["options"] == {"temperature": 0, "num_predict": 1200}


def test_ollama_model_client_reports_when_thinking_consumes_output_budget():
    def fake_urlopen(request, timeout):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({
                    "message": {"content": "", "thinking": "still reasoning"},
                    "done_reason": "length",
                    "eval_count": 900,
                }).encode()

        return Response()

    with pytest.raises(ModelClientError, match="Thinking 耗尽了输出预算") as error:
        OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen).generate_json(
            [],
            SCHEMA,
            model="qwen3.5:4b-mlx",
            timeout=9,
            options={"think": False, "num_predict": 900},
        )

    assert error.value.status == "parse_failed"


def test_ollama_model_client_accepts_markdown_fenced_json_response():
    def fake_urlopen(request, timeout):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({
                    "message": {"content": "```json\n{\"features\": []}\n```"},
                }).encode()

        return Response()

    result = OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen).generate_json(
        [],
        SCHEMA,
        model="qwen3.5:4b-mlx",
        timeout=9,
    )

    assert result == {"features": []}


def test_ollama_model_client_still_rejects_explanatory_text_around_json():
    def fake_urlopen(request, timeout):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({
                    "message": {"content": "Here is JSON:\n{\"features\": []}"},
                }).encode()

        return Response()

    with pytest.raises(ModelClientError, match="无效的结构化响应"):
        OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen).generate_json(
            [],
            SCHEMA,
            model="qwen3.5:4b-mlx",
            timeout=9,
        )


def test_ollama_model_client_rejects_bad_base_url():
    with pytest.raises(ModelClientError, match="有效的 http 或 https 地址"):
        OllamaModelClient("file:///tmp/ollama.sock")


@pytest.mark.parametrize("exc", [URLError("down"), socket.timeout("slow")])
def test_ollama_model_client_wraps_network_errors(exc):
    def fake_urlopen(*args, **kwargs):
        raise exc

    client = OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen)

    with pytest.raises(ModelClientError, match="无法连接 Ollama"):
        client.generate_json([], SCHEMA, model="qwen3:1.7b", timeout=1)


def test_ollama_model_client_wraps_http_errors():
    def fake_urlopen(*args, **kwargs):
        raise HTTPError("http://x", 500, "bad", {}, None)

    client = OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen)

    with pytest.raises(ModelClientError, match="Ollama HTTP 500"):
        client.generate_json([], SCHEMA, model="qwen3:1.7b", timeout=1)


def test_ollama_model_client_rejects_invalid_json_response():
    def fake_urlopen(*args, **kwargs):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"message":{"content":"not-json"}}'

        return Response()

    client = OllamaModelClient("http://127.0.0.1:11434", opener=fake_urlopen)

    with pytest.raises(ModelClientError, match="无效的结构化响应"):
        client.generate_json([], SCHEMA, model="qwen3:1.7b", timeout=1)
