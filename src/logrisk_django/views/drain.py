from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_http_methods, require_POST

from logrisk.drain_eval.schema import DrainQualityError
from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_http_methods(["GET", "POST"])
def datasets(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/drain-quality/datasets", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return _write(request, lambda identity, payload: get_facade().create_drain_dataset(payload, identity))


@require_http_methods(["GET", "POST"])
def annotations(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/drain-quality/annotations", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return _write(request, lambda identity, payload: get_facade().append_drain_annotation(payload, identity))


@require_POST
def annotation_review(request: HttpRequest, annotation_id: str) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().review_drain_annotation(annotation_id, payload, identity))


@require_http_methods(["GET", "POST"])
def eval_runs(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/drain-quality/eval-runs", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return _write(request, lambda identity, payload: get_facade().create_drain_eval_run(payload, identity))


@require_http_methods(["GET", "POST"])
def configs(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/drain-quality/configs", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return _write(request, lambda identity, payload: get_facade().create_drain_config(payload, identity))


@require_POST
def tune_runs(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().create_drain_tune_run(payload, identity))


def _write(request: HttpRequest, operation: Any) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = operation(identity, payload)
    except DrainQualityError as exc:
        return _error(422, "drain_invalid", _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "drain_invalid", _safe_message(exc))
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
