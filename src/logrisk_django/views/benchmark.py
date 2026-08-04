from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_http_methods, require_POST

from logrisk.benchmark_center.repository import BenchmarkError
from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_http_methods(["GET", "POST"])
def suites(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/benchmark-center/suites", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return _write(request, lambda identity, payload: get_facade().create_benchmark_suite(payload, identity))


@require_http_methods(["GET", "POST"])
def runs(request: HttpRequest) -> JsonResponse:
    if request.method == "GET":
        result = get_facade().dispatch_read("/api/benchmark-center/runs", request.GET)
        return _response(result.body, result.status, result.headers)  # type: ignore[union-attr]
    return _write(request, lambda identity, payload: get_facade().create_benchmark_run(payload, identity))


@require_POST
def cancel_run(request: HttpRequest, run_id: str) -> JsonResponse:
    return _write(request, lambda identity, _payload: get_facade().cancel_benchmark_run(run_id, identity))


@require_POST
def compare(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().compare_benchmark(payload, identity))


@require_POST
def evaluate_gate(request: HttpRequest) -> JsonResponse:
    return _write(request, lambda identity, payload: get_facade().evaluate_benchmark_gate(payload, identity))


def _write(request: HttpRequest, operation: Any) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = operation(identity, payload)
    except BenchmarkError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "benchmark_invalid", _safe_message(exc))
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
