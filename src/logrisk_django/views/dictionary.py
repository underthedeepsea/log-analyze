from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from logrisk.semantic.schema import SemanticValidationError
from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_GET
def dictionary_collection(request: HttpRequest) -> JsonResponse:
    result = get_facade().dispatch_read("/api/semantic/dictionaries", request.GET)
    return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]


@require_GET
def dictionary_detail(request: HttpRequest, dictionary_id: str) -> JsonResponse:
    try:
        result = get_facade().dispatch_read(f"/api/semantic/dictionaries/{dictionary_id}", request.GET)
    except (KeyError, SemanticValidationError, StopIteration) as exc:
        return _error(404, "dictionary_not_found", _safe_message(exc))
    return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]


@require_POST
def dictionary_test(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().test_semantic_dictionary(payload, identity))


@require_POST
def dictionary_candidate(request: HttpRequest, dictionary_id: str) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().create_semantic_dictionary_candidate(dictionary_id, payload, identity))


@require_POST
def dictionary_action(request: HttpRequest, dictionary_id: str, action: str) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().semantic_dictionary_action(dictionary_id, action, payload, identity))


def _write(request: HttpRequest, operation: Any) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = operation(identity, payload)
    except SemanticValidationError as exc:
        return _error(422, "dictionary_invalid", _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "dictionary_invalid", _safe_message(exc))
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
