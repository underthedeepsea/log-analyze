from __future__ import annotations

import uuid
from typing import Protocol

from django.http import HttpRequest

from logrisk.runtime.identity import RequestIdentity


class IdentityResolver(Protocol):
    def resolve(self, request: HttpRequest) -> RequestIdentity:
        """Resolve the PACAS/RBAC identity already attached by the host Django stack."""


class DjangoUserIdentityResolver:
    """Adapter for the host's authenticated Django/PACAS user; it creates no identities."""

    def resolve(self, request: HttpRequest) -> RequestIdentity:
        request_id = str(request.headers.get("X-Request-ID") or f"request-{uuid.uuid4().hex}")[:256]
        client_host = str(request.META.get("REMOTE_ADDR") or "")
        user = getattr(request, "user", None)
        if not user or not bool(getattr(user, "is_authenticated", False)):
            return RequestIdentity(None, (), request_id, False, "django", client_host)
        groups = getattr(user, "groups", None)
        roles = tuple(str(name) for name in groups.values_list("name", flat=True) if str(name)) if groups else ()
        return RequestIdentity(
            actor=str(user.get_username() or "") or None,
            roles=roles,
            request_id=request_id,
            authenticated=True,
            source="django",
            client_host=client_host,
        )
