from __future__ import annotations

import json
import socket
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from logrisk.ai_harness.model_client import ModelClientError, parse_content_json


class OllamaModelClient:
    def __init__(
        self,
        base_url: str,
        *,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        self.base_url = self._validate_base_url(base_url)
        self.opener = opener or urlopen

    @staticmethod
    def _validate_base_url(base_url: str) -> str:
        normalized = base_url.rstrip("/")
        parsed = urlparse(normalized)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ModelClientError("Ollama URL 必须是有效的 http 或 https 地址")
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
        request_options = {"temperature": 0, **(options or {})}
        request_options.pop("structured_output_mode", None)
        if "max_output_tokens" in request_options:
            request_options["num_predict"] = request_options.pop("max_output_tokens")
        think = request_options.pop("think", None)
        body = {
            "model": model,
            "stream": False,
            "format": schema,
            "options": request_options,
            "messages": messages,
        }
        if think is not None:
            body["think"] = think
        request = Request(
            f"{self.base_url}/api/chat",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with self.opener(request, timeout=timeout) as response:
                raw_response = response.read()
        except HTTPError as exc:
            details = ""
            try:
                details = exc.read().decode("utf-8", errors="replace")
            except Exception:
                details = str(exc)
            raise ModelClientError(f"Ollama HTTP {exc.code}: {details}", raw_output=details) from exc
        except (URLError, TimeoutError, socket.timeout) as exc:
            raise ModelClientError(f"无法连接 Ollama: {exc}", raw_output=str(exc)) from exc

        try:
            payload = json.loads(raw_response)
            message = payload["message"]
            content = message["content"]
            if not str(content).strip() and message.get("thinking") and payload.get("done_reason") == "length":
                raise ModelClientError(
                    "Ollama Thinking 耗尽了输出预算，未返回结构化内容",
                    raw_output=raw_response.decode("utf-8", errors="replace"),
                    status="parse_failed",
                )
            parsed = parse_content_json(content)
        except ModelClientError:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError) as exc:
            raw = raw_response.decode("utf-8", errors="replace")
            raise ModelClientError(
                "Ollama 返回了无效的结构化响应",
                raw_output=raw,
                status="parse_failed",
            ) from exc
        return parsed
