from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote


_PROVIDERS = {"sqlite", "postgres"}
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SSL_MODES = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}


@dataclass(frozen=True)
class DatabaseRuntime:
    provider: str
    sqlite_path: Path
    database_url: str | None
    source: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "source": self.source,
            "database_configured": bool(self.database_url) if self.provider == "postgres" else True,
            "restart_required": False,
        }


class DatabaseConnectionSettings:
    """A no-secret candidate configuration used only after the next restart."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, Any] | None:
        if not self.path.is_file():
            return None
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("数据库候选配置文件无效") from exc
        return self.validate(raw)

    def save(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        candidate = self.validate(raw)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(candidate, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return self.public_dict(candidate)

    @staticmethod
    def validate(raw: Mapping[str, Any]) -> dict[str, Any]:
        provider = str(raw.get("provider") or "postgres").strip().lower()
        if provider not in _PROVIDERS:
            raise ValueError("数据库 Provider 必须是 sqlite 或 postgres")
        if provider == "sqlite":
            return {"provider": "sqlite"}
        host = str(raw.get("host") or "").strip()
        database = str(raw.get("database") or "").strip()
        user = str(raw.get("user") or "").strip()
        password_env = str(raw.get("password_env") or "").strip()
        if not host or not database or not user:
            raise ValueError("PostgreSQL 主机、数据库和用户不能为空")
        if not _ENV_NAME.fullmatch(password_env):
            raise ValueError("密码环境变量名无效")
        try:
            port = int(raw.get("port") or 5432)
        except (TypeError, ValueError) as exc:
            raise ValueError("PostgreSQL 端口无效") from exc
        if not 1 <= port <= 65535:
            raise ValueError("PostgreSQL 端口必须在 1 到 65535 之间")
        sslmode = str(raw.get("sslmode") or "prefer").strip().lower()
        if sslmode not in _SSL_MODES:
            raise ValueError("SSL 模式无效")
        return {
            "provider": "postgres",
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "sslmode": sslmode,
            "password_env": password_env,
        }

    @staticmethod
    def public_dict(candidate: Mapping[str, Any], environ: Mapping[str, str] | None = None) -> dict[str, Any]:
        environ = os.environ if environ is None else environ
        result = dict(candidate)
        password_env = str(result.get("password_env") or "")
        result["password_configured"] = bool(password_env and environ.get(password_env))
        result["restart_required"] = True
        return result


def database_url_from_candidate(candidate: Mapping[str, Any], environ: Mapping[str, str]) -> str:
    password_env = str(candidate["password_env"])
    password = environ.get(password_env)
    if not password:
        raise ValueError(f"未配置 PostgreSQL 密码环境变量: {password_env}")
    user = quote(str(candidate["user"]), safe="")
    encoded_password = quote(password, safe="")
    database = quote(str(candidate["database"]), safe="")
    host = str(candidate["host"])
    return f"postgresql://{user}:{encoded_password}@{host}:{int(candidate['port'])}/{database}?sslmode={candidate['sslmode']}"


def resolve_database_runtime(
    *,
    provider: str | None = None,
    database_url: str | None = None,
    database_path: str | Path | None = None,
    settings: DatabaseConnectionSettings | None = None,
    environ: Mapping[str, str] | None = None,
) -> DatabaseRuntime:
    environ = os.environ if environ is None else environ
    candidate = settings.load() if settings else None
    explicit_provider = str(provider or "").strip().lower() or None
    env_provider = str(environ.get("LOGRISK_DATABASE_PROVIDER") or "").strip().lower() or None
    selected = explicit_provider or ("postgres" if database_url else None) or env_provider or str((candidate or {}).get("provider") or "sqlite")
    if selected not in _PROVIDERS:
        raise ValueError("LOGRISK_DATABASE_PROVIDER 必须是 sqlite 或 postgres")
    sqlite_path = Path(database_path or environ.get("LOGRISK_DB_PATH") or "state/logrisk.sqlite3")
    if selected == "sqlite":
        return DatabaseRuntime("sqlite", sqlite_path, None, "cli" if explicit_provider else "default")
    explicit_url = str(database_url or "").strip() or None
    env_url = str(environ.get("LOGRISK_DATABASE_URL") or "").strip() or None
    if explicit_url:
        return DatabaseRuntime("postgres", sqlite_path, explicit_url, "cli")
    if env_url:
        return DatabaseRuntime("postgres", sqlite_path, env_url, "environment")
    if candidate and candidate.get("provider") == "postgres":
        return DatabaseRuntime("postgres", sqlite_path, database_url_from_candidate(candidate, environ), "saved_candidate")
    raise ValueError("PostgreSQL 模式需要 LOGRISK_DATABASE_URL 或已保存的无密连接配置")
