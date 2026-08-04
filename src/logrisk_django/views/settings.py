from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_http_methods

from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_http_methods(["POST"])
def save_retention_policy(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().save_retention_policy(payload, identity))


@require_http_methods(["POST"])
def retention_maintenance(request: HttpRequest, mode: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    if mode not in {"preview", "execute"}:
        return _error(404, "maintenance_not_found", "维护操作不存在")
    try:
        result = get_facade().run_retention(execute=mode == "execute", identity=identity)
    except (TypeError, ValueError) as exc:
        return _error(422, "retention_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["POST"])
def save_database_candidate(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().save_database_candidate(payload, identity))


@require_http_methods(["POST"])
def test_database_candidate(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().test_database_candidate(payload, identity))


def _write(request: HttpRequest, operation: Any) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = operation(identity, payload)
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "settings_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


def _payload(request: HttpRequest) -> dict[str, Any] | JsonResponse:
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json", "请求体必须是 JSON object")
    if not isinstance(payload, dict):
        return _error(400, "invalid_payload", "请求体必须是 JSON object")
    return payload


def _response(body: object, status: int, headers: object) -> JsonResponse:
    response = JsonResponse(body, status=status, json_dumps_params={"ensure_ascii": False})
    for name, value in dict(headers).items():
        response[str(name)] = str(value)
    return response


def _error(status: int, code: str, message: str) -> JsonResponse:
    return JsonResponse({"code": code, "error": message}, status=status, json_dumps_params={"ensure_ascii": False})


def _safe_message(exc: Exception) -> str:
    return str(exc).replace("\n", " ")[:300]
