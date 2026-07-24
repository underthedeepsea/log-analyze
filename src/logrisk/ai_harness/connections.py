from __future__ import annotations

import json
import os
import re
from typing import Any, Mapping
from urllib.parse import urlparse

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.extensions.registry import get_extension_adapter
from logrisk.database import Database, utc_now


PROVIDERS = {"ollama", "openai_compatible", "extension"}
ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ID = re.compile(r"^[A-Za-z0-9_-]+$")
SENSITIVE_CONFIG_KEYS = {"token", "secret", "password", "api_key", "authorization", "access_token", "refresh_token"}


class ConnectionStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    @staticmethod
    def _public(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["enabled"] = bool(item["enabled"])
        item["is_default"] = bool(item["is_default"])
        item["api_key_configured"] = bool(item.get("api_key_env") and os.environ.get(item["api_key_env"]))
        item["credential_envs"] = ConnectionStore._json_object(item.pop("credential_envs_json", {}), "credential_envs_json")
        item["extension_config"] = ConnectionStore._json_object(item.pop("extension_config_json", {}), "extension_config_json")
        item["credential_envs_configured"] = {
            str(name): bool(os.environ.get(str(env_name)))
            for name, env_name in item["credential_envs"].items()
        }
        return item

    @staticmethod
    def _json_object(value: Any, field: str) -> dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        if not isinstance(value, str):
            raise ValueError(f"{field} 必须是 JSON object")
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field} 必须是 JSON object") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{field} 必须是 JSON object")
        return parsed

    @staticmethod
    def _require_non_sensitive_config(value: Any, *, field: str = "extension_config") -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError(f"{field} 必须是 JSON object")
        result = dict(value)

        def inspect(item: Any) -> None:
            if isinstance(item, Mapping):
                for key, child in item.items():
                    if str(key).strip().lower().replace("-", "_") in SENSITIVE_CONFIG_KEYS:
                        raise ValueError("extension_config 不得保存敏感 Token、密钥或密码")
                    inspect(child)
            elif isinstance(item, list):
                for child in item:
                    inspect(child)

        inspect(result)
        try:
            json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} 必须可序列化为 JSON") from exc
        return result

    @staticmethod
    def _credential_mapping(value: Any, *, allowed: set[str]) -> dict[str, str]:
        if not isinstance(value, Mapping):
            raise ValueError("credential_envs 必须是逻辑凭据名到环境变量名的 JSON object")
        mapping = {str(name): str(env_name) for name, env_name in value.items()}
        if set(mapping) != allowed:
            raise ValueError("credential_envs 必须与扩展适配器声明的凭据字段完全一致")
        if not all(ID.fullmatch(name) and ENV_NAME.fullmatch(env_name) for name, env_name in mapping.items()):
            raise ValueError("credential_envs 包含无效的逻辑名或环境变量名")
        return mapping

    def save(self, raw: dict[str, Any]) -> dict[str, Any]:
        connection_id = str(raw.get("connection_id") or "").strip()
        provider = str(raw.get("provider") or "").strip()
        base_url = str(raw.get("base_url") or "").strip().rstrip("/")
        api_key_env = str(raw.get("api_key_env") or "").strip() or None
        if not ID.fullmatch(connection_id):
            raise ValueError("connection_id 只能包含字母、数字、下划线和短横线")
        if provider not in PROVIDERS:
            raise ValueError("provider 必须是 ollama、openai_compatible 或 extension")
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url 必须是有效的 HTTP(S) 地址")
        if api_key_env and not ENV_NAME.fullmatch(api_key_env):
            raise ValueError("API Key 环境变量名无效")
        if provider == "openai_compatible" and not api_key_env:
            raise ValueError("OpenAI-compatible 连接必须配置 API Key 环境变量名")
        adapter_id: str | None = None
        credential_envs: dict[str, str] = {}
        extension_config: dict[str, Any] = {}
        if provider == "extension":
            adapter_id = str(raw.get("adapter_id") or "").strip()
            try:
                adapter = get_extension_adapter(adapter_id)
            except ModelClientError as exc:
                raise ValueError(str(exc)) from exc
            credential_envs = self._credential_mapping(
                raw.get("credential_envs") or {},
                allowed=set(adapter.descriptor.credential_fields),
            )
            extension_config = self._require_non_sensitive_config(raw.get("extension_config") or {})
            try:
                adapter.validate_connection({
                    "adapter_id": adapter_id,
                    "credential_envs": credential_envs,
                    "extension_config": extension_config,
                })
            except ModelClientError as exc:
                raise ValueError(str(exc)) from exc
            api_key_env = None
        elif raw.get("adapter_id") or raw.get("credential_envs") or raw.get("extension_config"):
            raise ValueError("仅 extension 连接可以配置 adapter_id、credential_envs 或 extension_config")
        timeout = float(raw.get("timeout_seconds", 120))
        if timeout <= 0:
            raise ValueError("timeout_seconds 必须大于 0")
        now = utc_now()
        is_default = bool(raw.get("is_default", False))
        with self.database.transaction() as connection:
            if is_default:
                connection.execute("UPDATE provider_connections SET is_default = FALSE")
            connection.execute(
                "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, api_key_env, "
                "adapter_id, credential_envs_json, extension_config_json, timeout_seconds, enabled, is_default, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(connection_id) DO UPDATE SET display_name=excluded.display_name, provider=excluded.provider, "
                "base_url=excluded.base_url, api_key_env=excluded.api_key_env, timeout_seconds=excluded.timeout_seconds, "
                "adapter_id=excluded.adapter_id, credential_envs_json=excluded.credential_envs_json, "
                "extension_config_json=excluded.extension_config_json, enabled=excluded.enabled, "
                "is_default=excluded.is_default, updated_at=excluded.updated_at",
                (
                    connection_id,
                    str(raw.get("display_name") or connection_id).strip(),
                    provider,
                    base_url,
                    api_key_env,
                    adapter_id,
                    json.dumps(credential_envs, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(extension_config, ensure_ascii=False, separators=(",", ":")),
                    timeout,
                    bool(raw.get("enabled", True)),
                    is_default,
                    now,
                    now,
                ),
            )
        return self.get(connection_id)

    def get(self, connection_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM provider_connections WHERE connection_id = ?", (connection_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"连接不存在: {connection_id}")
        return self._public(row)

    def list(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM provider_connections ORDER BY is_default DESC, display_name, connection_id"
            ).fetchall()
        return [self._public(row) for row in rows]

    def seed_defaults(self, ollama_url: str) -> None:
        with self.database.connect() as connection:
            exists = connection.execute("SELECT 1 FROM provider_connections LIMIT 1").fetchone()
            local = connection.execute("SELECT 1 FROM provider_connections WHERE connection_id='ollama-local'").fetchone()
        if not local:
            self.save({
                "connection_id": "ollama-local",
                "display_name": "本机 Ollama",
                "provider": "ollama",
                "base_url": ollama_url,
                "enabled": True,
                "is_default": not bool(exists),
            })
