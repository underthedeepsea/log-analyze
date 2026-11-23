from __future__ import annotations

from django.http import Http404, HttpRequest, JsonResponse
from django.views.decorators.http import require_GET

from logrisk_django.service_factory import get_facade, get_identity_resolver


@require_GET
def core_read(request: HttpRequest, endpoint: str) -> JsonResponse:
    overview = endpoint == "stream-overview" or endpoint.startswith("stream-overview/")
    identity = (
        get_identity_resolver().resolve(request)
        if overview or endpoint.startswith("streaming/observability/")
        or endpoint in {"streaming/tasks", "streaming/unknown-templates"}
        else None
    )
    if overview and (
        not identity or not identity.authenticated or not identity.actor
        or identity.source not in {"django", "trusted_proxy"}
    ):
        return JsonResponse(
            {"code": "forbidden", "error": "流运行总览需要可信 PACAS/RBAC 身份"},
            status=403, headers={"Cache-Control": "no-store"},
        )
    result = get_facade().dispatch_read("/api/" + endpoint, request.GET, identity)
    if result is None:
        raise Http404("资源不存在")
    response = JsonResponse(result.body, status=result.status, json_dumps_params={"ensure_ascii": False})
    for name, value in result.headers.items():
        response[name] = value
    return response
