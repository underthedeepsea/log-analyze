from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from typing import Any, Mapping


_HEADER_NAME = re.compile(r"^[A-Za-z0-9-]{1,100}$")
_IDENTITY_KEYS = {
    "enabled",
    "allow_loopback_bypass",
    "trusted_proxy_cidrs",
    "actor_header",
    "roles_header",
    "request_id_header",
    "write_roles",
}
_RUNTIME_KEYS = {"identity", "retention", "quota"}


class RuntimeConfigError(ValueError):
    """A runtime configuration error that is safe to present to an operator."""


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise RuntimeConfigError(f"{field} 必须是对象")
    return value


def _boolean(value: Any, field: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise RuntimeConfigError(f"{field} 必须是布尔值")
    return value


def _header(value: Any, field: str, default: str) -> str:
    name = str(default if value is None else value).strip()
    if not _HEADER_NAME.fullmatch(name):
        raise RuntimeConfigError(f"{field} 必须是有效 HTTP Header 名")
    return name


def _strings(value: Any, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise RuntimeConfigError(f"{field} 必须是字符串数组")
    result = tuple(item.strip() for item in value if item.strip())
    if len(set(result)) != len(result):
        raise RuntimeConfigError(f"{field} 不能包含重复值")
    return result


@dataclass(frozen=True)
class IdentityConfig:
    enabled: bool = False
    allow_loopback_bypass: bool = True
    trusted_proxy_networks: tuple[ipaddress._BaseNetwork, ...] = ()
    actor_header: str = "X-LOGRISK-Actor"
    roles_header: str = "X-LOGRISK-Roles"
    request_id_header: str = "X-Request-ID"
    write_roles: tuple[str, ...] = ()

    @property
    def trusted_proxy_cidrs(self) -> tuple[str, ...]:
        return tuple(str(network) for network in self.trusted_proxy_networks)


@dataclass(frozen=True)
class RetentionConfig:
    enabled: bool = False
    completed_days: int = 30
    trace_days: int = 30
    cache_days: int = 14


@dataclass(frozen=True)
class QuotaConfig:
    soft_limit_bytes: int = 5 * 1024 * 1024 * 1024
    hard_limit_bytes: int = 10 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class RuntimeConfig:
    identity: IdentityConfig = IdentityConfig()
    retention: RetentionConfig = RetentionConfig()
    quota: QuotaConfig = QuotaConfig()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "RuntimeConfig":
        source = _mapping(value, "runtime")
        if "runtime" in source:
            if len(source) != 1:
                raise RuntimeConfigError("runtime 配置不能与其他顶层键混用")
            source = _mapping(source["runtime"], "runtime")
        unknown = set(source) - _RUNTIME_KEYS
        if unknown:
            raise RuntimeConfigError(f"runtime 包含未知配置项: {', '.join(sorted(unknown))}")
        return cls(
            identity=cls._identity(_mapping(source.get("identity"), "runtime.identity")),
            retention=cls._retention(_mapping(source.get("retention"), "runtime.retention")),
            quota=cls._quota(_mapping(source.get("quota"), "runtime.quota")),
        )

    @staticmethod
    def _identity(source: Mapping[str, Any]) -> IdentityConfig:
        unknown = set(source) - _IDENTITY_KEYS
        if unknown:
            raise RuntimeConfigError(f"runtime.identity 包含未知配置项: {', '.join(sorted(unknown))}")
        cidrs = _strings(source.get("trusted_proxy_cidrs"), "runtime.identity.trusted_proxy_cidrs")
        networks: list[ipaddress._BaseNetwork] = []
        for cidr in cidrs:
            try:
                networks.append(ipaddress.ip_network(cidr, strict=False))
            except ValueError as exc:
                raise RuntimeConfigError("runtime.identity.trusted_proxy_cidrs 包含无效 CIDR") from exc
        return IdentityConfig(
            enabled=_boolean(source.get("enabled"), "runtime.identity.enabled", False),
            allow_loopback_bypass=_boolean(
                source.get("allow_loopback_bypass"),
                "runtime.identity.allow_loopback_bypass",
                True,
            ),
            trusted_proxy_networks=tuple(networks),
            actor_header=_header(source.get("actor_header"), "runtime.identity.actor_header", "X-LOGRISK-Actor"),
            roles_header=_header(source.get("roles_header"), "runtime.identity.roles_header", "X-LOGRISK-Roles"),
            request_id_header=_header(source.get("request_id_header"), "runtime.identity.request_id_header", "X-Request-ID"),
            write_roles=_strings(source.get("write_roles"), "runtime.identity.write_roles"),
        )

    @staticmethod
    def _retention(source: Mapping[str, Any]) -> RetentionConfig:
        allowed = {"enabled", "completed_days", "trace_days", "cache_days"}
        unknown = set(source) - allowed
        if unknown:
            raise RuntimeConfigError(f"runtime.retention 包含未知配置项: {', '.join(sorted(unknown))}")

        def days(name: str, default: int) -> int:
            value = source.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 3650:
                raise RuntimeConfigError(f"runtime.retention.{name} 必须是 1 到 3650 的整数")
            return value

        return RetentionConfig(
            enabled=_boolean(source.get("enabled"), "runtime.retention.enabled", False),
            completed_days=days("completed_days", 30),
            trace_days=days("trace_days", 30),
            cache_days=days("cache_days", 14),
        )

    @staticmethod
    def _quota(source: Mapping[str, Any]) -> QuotaConfig:
        allowed = {"soft_limit_bytes", "hard_limit_bytes"}
        unknown = set(source) - allowed
        if unknown:
            raise RuntimeConfigError(f"runtime.quota 包含未知配置项: {', '.join(sorted(unknown))}")

        def size(name: str, default: int) -> int:
            value = source.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise RuntimeConfigError(f"runtime.quota.{name} 必须是正整数")
            return value

        soft = size("soft_limit_bytes", 5 * 1024 * 1024 * 1024)
        hard = size("hard_limit_bytes", 10 * 1024 * 1024 * 1024)
        if soft > hard:
            raise RuntimeConfigError("runtime.quota.soft_limit_bytes 不能大于 hard_limit_bytes")
        return QuotaConfig(soft_limit_bytes=soft, hard_limit_bytes=hard)
