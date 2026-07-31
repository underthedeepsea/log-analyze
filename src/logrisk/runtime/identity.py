from __future__ import annotations

import ipaddress
import uuid
from dataclasses import dataclass
from typing import Mapping

from logrisk.runtime.config import RuntimeConfig


class RuntimeAccessError(PermissionError):
    """A stable runtime access boundary error."""

    code = "runtime_identity_required"


def _header_value(headers: Mapping[str, str], name: str) -> str:
    target = name.lower()
    for key, value in headers.items():
        if str(key).lower() == target:
            return str(value).strip()
    return ""


@dataclass(frozen=True)
class RequestIdentity:
    actor: str | None
    roles: tuple[str, ...]
    request_id: str
    authenticated: bool
    source: str
    client_host: str

    @classmethod
    def from_request(
        cls,
        client_host: str,
        headers: Mapping[str, str],
        config: RuntimeConfig,
    ) -> "RequestIdentity":
        try:
            address = ipaddress.ip_address(client_host)
        except ValueError:
            address = None
        request_id = _header_value(headers, config.identity.request_id_header) or f"request-{uuid.uuid4().hex}"
        if address and address.is_loopback and config.identity.allow_loopback_bypass:
            return cls("local-development", (), request_id, True, "loopback", client_host)
        if not config.identity.enabled:
            return cls(None, (), request_id, False, "identity_disabled", client_host)
        trusted = bool(address and any(address in network for network in config.identity.trusted_proxy_networks))
        if not trusted:
            return cls(None, (), request_id, False, "untrusted_proxy", client_host)
        actor = _header_value(headers, config.identity.actor_header)
        role_values = tuple(
            role.strip()
            for role in _header_value(headers, config.identity.roles_header).split(",")
            if role.strip()
        )
        if len(set(role_values)) != len(role_values):
            return cls(actor or None, (), request_id, False, "trusted_proxy", client_host)
        return cls(actor or None, role_values, request_id, bool(actor), "trusted_proxy", client_host)

    def public_dict(self) -> dict[str, object]:
        return {
            "actor": self.actor,
            "roles": list(self.roles),
            "request_id": self.request_id,
            "authenticated": self.authenticated,
            "source": self.source,
        }


def require_write_access(identity: RequestIdentity, config: RuntimeConfig) -> None:
    if not config.identity.enabled or identity.source == "loopback":
        return
    if identity.source == "untrusted_proxy":
        raise RuntimeAccessError("写操作必须经由可信代理")
    if not identity.authenticated:
        raise RuntimeAccessError("写操作缺少可信身份")
    required = set(config.identity.write_roles)
    if required and not required.intersection(identity.roles):
        raise RuntimeAccessError("当前身份缺少写操作角色")
