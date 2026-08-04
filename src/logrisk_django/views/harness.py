from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_http_methods

from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_http_methods(["GET", "PATCH"])
def prompt_detail(request: HttpRequest, prompt_id: str) -> JsonResponse:
    if request.method == "PATCH":
        return update_prompt(request, prompt_id)
    try:
        result = get_facade().prompt_detail(prompt_id)
    except (FileNotFoundError, KeyError):
        return _error(404, "prompt_not_found", "Prompt 不存在")
    return _response(result.body, result.status, result.headers)


@require_http_methods(["GET", "POST"])
def connections(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().model_connections()
        return _response(result.body, result.status, result.headers)
    return save_connection(request)


@require_http_methods(["GET", "POST"])
def model_profiles(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().model_profiles()
        return _response(result.body, result.status, result.headers)
    return save_model_profile(request)


@require_http_methods(["POST"])
def save_connection(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = get_facade().save_connection(payload, identity)
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "connection_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["POST"])
def save_model_profile(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = get_facade().save_model_profile(payload, identity)
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "model_profile_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["POST"])
def test_connection(request: HttpRequest, connection_id: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        result = get_facade().test_connection(connection_id, identity)
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "connection_test_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["PATCH"])
def update_connection(request: HttpRequest, connection_id: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = get_facade().update_connection(connection_id, payload, identity)
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "connection_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["PATCH"])
def update_prompt(request: HttpRequest, prompt_id: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = get_facade().update_prompt(prompt_id, payload, identity)
    except (FileNotFoundError, KeyError):
        return _error(404, "prompt_not_found", "Prompt 不存在")
    except (TypeError, ValueError) as exc:
        return _error(422, "prompt_invalid", _safe_message(exc))
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
