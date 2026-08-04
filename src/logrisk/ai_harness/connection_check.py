from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from .model_client import ModelClientError
from .providers.extension import redact_model_error
from .providers.extensions.registry import get_extension_adapter


def check_ollama(base_url: str, timeout: float = 2) -> dict[str, Any]:
    """Return a safe Ollama status without retaining response bodies or credentials."""
    try:
        with urlopen(f"{base_url.rstrip('/')}/api/tags", timeout=timeout) as response:
            payload = json.load(response)
        models = [item.get("name") for item in payload.get("models", []) if isinstance(item, dict) and item.get("name")]
        return {"online": True, "models": models}
    except (OSError, URLError, ValueError, json.JSONDecodeError):
        return {"online": False, "models": [], "error": "无法连接 Ollama"}


def check_model_connection(connection: dict[str, Any]) -> dict[str, Any]:
    """Check a configured connection while keeping secrets and transport details out of the result."""
    if not connection.get("enabled"):
        return {"online": False, "models": [], "error": "连接已停用"}
    provider = str(connection.get("provider") or "")
    if provider == "ollama":
        return check_ollama(str(connection.get("base_url") or ""), float(connection.get("timeout_seconds") or 2))
    if provider == "extension":
        values = [
            os.environ.get(str(env_name), "")
            for env_name in dict(connection.get("credential_envs") or {}).values()
        ]
        try:
            result = get_extension_adapter(str(connection.get("adapter_id") or "")).check_connection(connection)
            if not isinstance(result, dict):
                raise ModelClientError("扩展适配器连接检查必须返回 JSON object")
            return {
                "online": bool(result.get("online")),
                "models": list(result.get("models") or []),
                **({"error": redact_model_error(str(result["error"]), values)} if result.get("error") else {}),
            }
        except Exception as exc:
            return {"online": False, "models": [], "error": redact_model_error(str(exc), values)}
    api_key_env = str(connection.get("api_key_env") or "")
    api_key = os.environ.get(api_key_env)
    if not api_key:
        return {"online": False, "models": [], "error": "未配置远端模型 API Key 环境变量"}
    request = Request(
        f"{str(connection.get('base_url') or '').rstrip('/')}/models",
        headers={"Accept": "application/json", "Authorization": f"Bearer {api_key}"},
    )
    try:
        with urlopen(request, timeout=float(connection.get("timeout_seconds") or 10)) as response:
            payload = json.load(response)
        models = [
            item.get("id")
            for item in payload.get("data", [])
            if isinstance(item, dict) and item.get("id")
        ]
        return {"online": True, "models": models}
    except (OSError, URLError, ValueError, json.JSONDecodeError):
        return {"online": False, "models": [], "error": "远端模型连接检查失败"}
