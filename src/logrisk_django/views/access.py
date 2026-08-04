from __future__ import annotations

from django.http import HttpRequest, JsonResponse

from logrisk.runtime.identity import RequestIdentity
from logrisk_django.service_factory import get_config, get_container, get_identity_resolver


def require_django_write_access(request: HttpRequest) -> RequestIdentity | JsonResponse:
    """Enforce the host PACAS/RBAC identity without creating a second identity system."""
    identity = get_identity_resolver().resolve(request)
    config = get_config()
    if identity.authenticated and (not config.write_roles or set(config.write_roles).intersection(identity.roles)):
        return identity
    reason = "identity_required" if not identity.authenticated else "write_role_required"
    _record_denial(identity, reason)
    message = "写操作需要 PACAS/RBAC 认证身份" if reason == "identity_required" else "当前身份缺少 LOGRISK 写操作角色"
    return JsonResponse(
        {"code": "runtime_identity_required", "error": message, "request_id": identity.request_id},
        status=403,
        json_dumps_params={"ensure_ascii": False},
    )


def _record_denial(identity: RequestIdentity, reason: str) -> None:
    try:
        get_container().runtime_repository.append_audit(
            "access.denied",
            "write_api",
            identity.actor,
            identity.request_id,
            {"reason": reason},
            roles=identity.roles,
            outcome="denied",
        )
    except Exception:
        # The response must still fail closed if the audit sink is unavailable.
        return
