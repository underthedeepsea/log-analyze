from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlparse

from logrisk.database import SQLiteDatabase, utc_now


PROVIDERS = {"ollama", "openai_compatible"}
ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ID = re.compile(r"^[A-Za-z0-9_-]+$")


class ConnectionStore:
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    @staticmethod
    def _public(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["enabled"] = bool(item["enabled"])
        item["is_default"] = bool(item["is_default"])
        item["api_key_configured"] = bool(item.get("api_key_env") and os.environ.get(item["api_key_env"]))
        return item

    def save(self, raw: dict[str, Any]) -> dict[str, Any]:
        connection_id = str(raw.get("connection_id") or "").strip()
        provider = str(raw.get("provider") or "").strip()
        base_url = str(raw.get("base_url") or "").strip().rstrip("/")
        api_key_env = str(raw.get("api_key_env") or "").strip() or None
        if not ID.fullmatch(connection_id):
            raise ValueError("connection_id 只能包含字母、数字、下划线和短横线")
        if provider not in PROVIDERS:
            raise ValueError("provider 必须是 ollama 或 openai_compatible")
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url 必须是有效的 HTTP(S) 地址")
        if api_key_env and not ENV_NAME.fullmatch(api_key_env):
            raise ValueError("API Key 环境变量名无效")
        if provider == "openai_compatible" and not api_key_env:
            raise ValueError("OpenAI-compatible 连接必须配置 API Key 环境变量名")
        timeout = float(raw.get("timeout_seconds", 120))
        if timeout <= 0:
            raise ValueError("timeout_seconds 必须大于 0")
        now = utc_now()
        is_default = bool(raw.get("is_default", False))
        with self.database.transaction() as connection:
            if is_default:
                connection.execute("UPDATE provider_connections SET is_default = 0")
            connection.execute(
                "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, api_key_env, "
                "timeout_seconds, enabled, is_default, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(connection_id) DO UPDATE SET display_name=excluded.display_name, provider=excluded.provider, "
                "base_url=excluded.base_url, api_key_env=excluded.api_key_env, timeout_seconds=excluded.timeout_seconds, "
                "enabled=excluded.enabled, is_default=excluded.is_default, updated_at=excluded.updated_at",
                (
                    connection_id,
                    str(raw.get("display_name") or connection_id).strip(),
                    provider,
                    base_url,
                    api_key_env,
                    timeout,
                    int(bool(raw.get("enabled", True))),
                    int(is_default),
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
