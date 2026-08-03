from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from logrisk.application import ApplicationConfig


class LogriskSettingsError(ValueError):
    """Safe configuration error; it never includes configured secret values."""


_ALLOWED = {
    "project_root",
    "state_root",
    "output_root",
    "database_provider",
    "database_url_env",
    "database_path",
    "shared_root",
    "airflow_base_url",
    "airflow_dag_id",
    "airflow_timeout_seconds",
    "airflow_authorization_env",
    "identity_resolver",
    "write_roles",
    "runtime_config_path",
}


@dataclass(frozen=True)
class LogriskConfig:
    project_root: Path
    state_root: Path
    output_root: Path
    database_provider: str
    database_url_env: str
    database_path: Path | None
    shared_root: Path
    airflow_base_url: str
    airflow_dag_id: str
    airflow_timeout_seconds: float
    airflow_authorization_env: str | None
    identity_resolver: str
    write_roles: tuple[str, ...]
    runtime_config_path: Path | None = None

    @classmethod
    def from_django_settings(cls, django_settings: Any) -> "LogriskConfig":
        raw = getattr(django_settings, "LOGRISK", {})
        if not isinstance(raw, Mapping):
            raise LogriskSettingsError("LOGRISK 必须是对象")
        unknown = set(raw) - _ALLOWED
        if unknown:
            raise LogriskSettingsError("LOGRISK 包含未知配置项: " + ", ".join(sorted(unknown)))
        project_root = _path(raw.get("project_root") or Path.cwd(), "project_root")
        state_root = _path(raw.get("state_root") or project_root / "state", "state_root")
        output_root = _path(raw.get("output_root") or project_root / "output", "output_root")
        provider = str(raw.get("database_provider") or "postgres").strip().lower()
        if provider not in {"sqlite", "postgres"}:
            raise LogriskSettingsError("database_provider 必须是 sqlite 或 postgres")
        database_url_env = _environment_name(raw.get("database_url_env") or "LOGRISK_DATABASE_URL", "database_url_env")
        database_path = _path(raw["database_path"], "database_path") if raw.get("database_path") else None
        if provider == "sqlite" and database_path is None:
            database_path = state_root / "logrisk.sqlite3"
        shared_root = _path(raw.get("shared_root") or state_root, "shared_root")
        airflow_base_url = str(raw.get("airflow_base_url") or "").strip().rstrip("/")
        parsed = urlparse(airflow_base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise LogriskSettingsError("airflow_base_url 必须是有效的 HTTP(S) 地址")
        airflow_dag_id = _identifier(raw.get("airflow_dag_id") or "logrisk_analysis", "airflow_dag_id")
        timeout = raw.get("airflow_timeout_seconds", 10)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < float(timeout) <= 300:
            raise LogriskSettingsError("airflow_timeout_seconds 必须是 0 到 300 的数字")
        authorization_env = raw.get("airflow_authorization_env")
        if authorization_env is not None:
            authorization_env = _environment_name(authorization_env, "airflow_authorization_env")
        identity_resolver = str(raw.get("identity_resolver") or "logrisk_django.identity.DjangoUserIdentityResolver").strip()
        if "." not in identity_resolver:
            raise LogriskSettingsError("identity_resolver 必须是 dotted import path")
        write_roles = _string_tuple(raw.get("write_roles") or [], "write_roles")
        runtime_config_path = _path(raw["runtime_config_path"], "runtime_config_path") if raw.get("runtime_config_path") else None
        return cls(
            project_root=project_root,
            state_root=state_root,
            output_root=output_root,
            database_provider=provider,
            database_url_env=database_url_env,
            database_path=database_path,
            shared_root=shared_root,
            airflow_base_url=airflow_base_url,
            airflow_dag_id=airflow_dag_id,
            airflow_timeout_seconds=float(timeout),
            airflow_authorization_env=authorization_env,
            identity_resolver=identity_resolver,
            write_roles=write_roles,
            runtime_config_path=runtime_config_path,
        )

    def application_config(self) -> ApplicationConfig:
        database_url = os.environ.get(self.database_url_env) if self.database_provider == "postgres" else None
        if self.database_provider == "postgres" and not database_url:
            raise LogriskSettingsError("未配置 LOGRISK PostgreSQL 连接环境变量")
        return ApplicationConfig(
            project_root=self.project_root,
            state_root=self.state_root,
            output_root=self.output_root,
            database_provider=self.database_provider,
            database_url=database_url,
            database_path=self.database_path,
            shared_root=self.shared_root,
            runtime_config_path=self.runtime_config_path,
            import_legacy_state=False,
            interrupt_streaming_tasks=False,
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "database_provider": self.database_provider,
            "database_url_env": self.database_url_env,
            "shared_root": str(self.shared_root),
            "airflow_base_url": self.airflow_base_url,
            "airflow_dag_id": self.airflow_dag_id,
            "airflow_timeout_seconds": self.airflow_timeout_seconds,
            "airflow_authorization_env": self.airflow_authorization_env,
            "identity_resolver": self.identity_resolver,
            "write_roles": list(self.write_roles),
        }


def _path(value: Any, field: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise LogriskSettingsError(f"{field} 必须是非空路径")
    return Path(value).expanduser().resolve()


def _environment_name(value: Any, field: str) -> str:
    name = str(value or "").strip()
    if not name or not name.replace("_", "a").isalnum() or name[0].isdigit():
        raise LogriskSettingsError(f"{field} 必须是环境变量名")
    return name


def _identifier(value: Any, field: str) -> str:
    identifier = str(value or "").strip()
    if not identifier or not all(character.isalnum() or character in "_.-" for character in identifier):
        raise LogriskSettingsError(f"{field} 包含无效字符")
    return identifier


def _string_tuple(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise LogriskSettingsError(f"{field} 必须是非空字符串数组")
    items = tuple(item.strip() for item in value)
    if len(items) != len(set(items)):
        raise LogriskSettingsError(f"{field} 不能重复")
    return items
