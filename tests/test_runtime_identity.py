from __future__ import annotations

import pytest

from logrisk.runtime.config import RuntimeConfig
from logrisk.runtime.identity import (
    RequestIdentity,
    RuntimeAccessError,
    require_write_access,
)


def test_external_write_without_trusted_proxy_is_rejected() -> None:
    config = RuntimeConfig.from_mapping(
        {"identity": {"enabled": True, "trusted_proxy_cidrs": ["10.0.0.0/8"]}}
    )
    identity = RequestIdentity.from_request(
        "203.0.113.7", {"X-LOGRISK-Actor": "alice"}, config
    )

    with pytest.raises(RuntimeAccessError, match="可信代理"):
        require_write_access(identity, config)


def test_loopback_development_mode_returns_local_operator() -> None:
    config = RuntimeConfig.from_mapping(
        {"identity": {"enabled": True, "allow_loopback_bypass": True}}
    )
    identity = RequestIdentity.from_request("127.0.0.1", {}, config)

    assert identity.public_dict()["actor"] == "local-development"


def test_trusted_proxy_identity_needs_allowed_role_when_roles_are_configured() -> None:
    config = RuntimeConfig.from_mapping(
        {
            "identity": {
                "enabled": True,
                "trusted_proxy_cidrs": ["10.0.0.0/8"],
                "write_roles": ["logrisk:operator"],
            }
        }
    )
    identity = RequestIdentity.from_request(
        "10.10.1.5",
        {"X-LOGRISK-Actor": "alice", "X-LOGRISK-Roles": "viewer"},
        config,
    )

    with pytest.raises(RuntimeAccessError, match="角色"):
        require_write_access(identity, config)
