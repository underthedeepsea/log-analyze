from __future__ import annotations

import json
import os
import socket
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from logrisk.ai_harness.model_client import ModelClientError, parse_content_json


class OpenAICompatibleModelClient:
    def __init__(self, base_url: str, *, api_key_env: str, opener: Callable[..., Any] | None = None) -> None:
        self.base_url = self._validate_base_url(base_url)
        self.api_key_env = api_key_env
        self.opener = opener or urlopen

    @staticmethod
    def _validate_base_url(base_url: str) -> str:
        normalized = base_url.rstrip("/")
        parsed = urlparse(normalized)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ModelClientError("远端模型 URL 必须是有效的 http 或 https 地址")
        return normalized

    def generate_json(
        self,
        messages: list[dict[str, Any]],
        schema: dict[str, Any],
        *,
        model: str,
        timeout: float,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        api_key = os.environ.get(self.api_key_env)
        if not api_key:
            raise ModelClientError(f"未配置 API Key 环境变量: {self.api_key_env}")
        request_options = dict(options or {})
        mode = str(request_options.pop("structured_output_mode", "json_schema"))
        max_tokens = request_options.pop("max_output_tokens", request_options.pop("num_predict", None))
        request_options.pop("think", None)
        body: dict[str, Any] = {"model": model, "messages": messages, **request_options}
        if max_tokens is not None:
            body["max_tokens"] = int(max_tokens)
        if mode == "json_schema":
            body["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "log_feature_extraction", "strict": True, "schema": schema},
            }
        elif mode == "json_object":
            body["response_format"] = {"type": "json_object"}
        elif mode != "prompt_only":
            raise ModelClientError("structured_output_mode 无效")
        request = Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        try:
            with self.opener(request, timeout=timeout) as response:
                raw_response = response.read()
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise ModelClientError(f"远端模型 HTTP {exc.code}: {details}", raw_output=details) from exc
        except (URLError, TimeoutError, socket.timeout) as exc:
            raise ModelClientError(f"无法连接远端模型: {exc}", raw_output=str(exc)) from exc
        try:
            payload = json.loads(raw_response)
            return parse_content_json(payload["choices"][0]["message"]["content"])
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError, IndexError, TypeError) as exc:
            raw = raw_response.decode("utf-8", errors="replace")
            raise ModelClientError("远端模型返回了无效的结构化响应", raw_output=raw, status="parse_failed") from exc
