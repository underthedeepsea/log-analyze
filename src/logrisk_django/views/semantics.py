from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_GET, require_http_methods

from logrisk.risk_semantics import RiskSemanticError
from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_http_methods(["GET", "POST"])
def semantic_collection(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/semantics", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return create_semantic(request)


@require_http_methods(["POST"])
def create_semantic(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().create_semantic_rule(payload, identity))


@require_http_methods(["POST"])
def validate_semantic(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().validate_semantic(payload, identity))


@require_http_methods(["POST"])
def test_semantic(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().test_semantic(payload, identity))


@require_http_methods(["POST"])
def import_semantic(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().import_semantic_bundle(payload, identity))


@require_http_methods(["GET", "PATCH"])
def semantic_detail(request: HttpRequest, rule_id: str) -> JsonResponse:
    if request.method == "PATCH":
        return update_semantic(request, rule_id)
    try:
        result = get_facade().semantic_detail(rule_id)
    except RiskSemanticError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_GET
def semantic_versions(_request: HttpRequest, rule_id: str) -> JsonResponse:
    try:
        result = get_facade().semantic_versions(rule_id)
    except RiskSemanticError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["PATCH"])
def update_semantic(request: HttpRequest, rule_id: str) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().update_semantic_rule(rule_id, payload, identity))


@require_http_methods(["POST"])
def semantic_action(request: HttpRequest, rule_id: str, action: str) -> JsonResponse:
    if action == "override":
        return _write(request, lambda identity, payload: get_facade().create_semantic_override(rule_id, payload, identity))
    if action == "rollback":
        return _write(request, lambda identity, payload: get_facade().rollback_semantic(rule_id, payload, identity))
    return _write(request, lambda identity, payload: get_facade().semantic_action(rule_id, action, payload, identity))


def _write(request: HttpRequest, operation: Any) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = operation(identity, payload)
    except RiskSemanticError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "semantic_invalid", _safe_message(exc))
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
