from __future__ import annotations

from logrisk.runtime.identity import RequestIdentity


class OperatorIdentityResolver:
    def resolve(self, _request: object) -> RequestIdentity:
        return RequestIdentity(
            actor="pacas-alice",
            roles=("logrisk:operator",),
            request_id="request-django-test",
            authenticated=True,
            source="django",
            client_host="127.0.0.1",
        )


class AnonymousIdentityResolver:
    def resolve(self, _request: object) -> RequestIdentity:
        return RequestIdentity(None, (), "request-anonymous", False, "django", "127.0.0.1")
