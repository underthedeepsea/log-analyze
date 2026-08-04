from __future__ import annotations

from django.http import Http404, HttpRequest, JsonResponse
from django.views.decorators.http import require_GET

from logrisk_django.service_factory import get_facade


@require_GET
def core_read(request: HttpRequest, endpoint: str) -> JsonResponse:
    result = get_facade().dispatch_read("/api/" + endpoint, request.GET)
    if result is None:
        raise Http404("资源不存在")
    response = JsonResponse(result.body, status=result.status, json_dumps_params={"ensure_ascii": False})
    for name, value in result.headers.items():
        response[name] = value
    return response
