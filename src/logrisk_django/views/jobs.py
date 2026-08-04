from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET, require_POST

from logrisk.orchestration import AirflowOrchestratorError, InputOrchestrationConflict, OrchestrationConflict
from logrisk_django.service_factory import (
    get_airflow_orchestrator,
    get_container,
    get_facade,
)
from logrisk_django.views.access import require_django_write_access


@require_POST
def create_job(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json", "请求体必须是 JSON object")
    if not isinstance(payload, dict):
        return _error(400, "invalid_payload", "请求体必须是 JSON object")
    try:
        job_id = get_facade().create_feature_job(payload)
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "invalid_job", _safe_message(exc))
    except Exception as exc:
        return _error(422, getattr(exc, "code", "feature_job_failed"), _safe_message(exc))

    orchestration = get_container().orchestration
    run = orchestration.create_pending(job_id, identity.request_id, identity.actor or "unknown", identity.roles)
    try:
        airflow = get_airflow_orchestrator()
        triggered = airflow.trigger(job_id, run["orchestration_run_id"], identity.request_id)
    except AirflowOrchestratorError as exc:
        failed = orchestration.mark_dispatch_failed(
            run["orchestration_run_id"],
            expected_version=run["state_version"],
            error_code=exc.code,
            error_summary=str(exc),
        )
        return JsonResponse({
            "job_id": job_id,
            "orchestration_run_id": failed["orchestration_run_id"],
            "status": failed["status"],
            "code": exc.code,
            "error": str(exc),
        }, status=exc.status_code, json_dumps_params={"ensure_ascii": False})
    dispatched = orchestration.mark_dispatched(
        run["orchestration_run_id"], airflow.dag_id, triggered.external_run_id, expected_version=run["state_version"]
    )
    return JsonResponse({
        "job_id": job_id,
        "orchestration_run_id": dispatched["orchestration_run_id"],
        "status": dispatched["status"],
        "external_dag_id": dispatched["external_dag_id"],
        "external_run_id": dispatched["external_run_id"],
    }, status=202, json_dumps_params={"ensure_ascii": False})


@require_GET
def job_detail(_request: HttpRequest, job_id: str) -> JsonResponse:
    try:
        result = get_facade().feature_job(job_id)
    except (KeyError, ValueError) as exc:
        return _error(404, "feature_job_not_found", _safe_message(exc))
    return JsonResponse(dict(result.body), status=result.status, json_dumps_params={"ensure_ascii": False})


@require_GET
def job_events(request: HttpRequest, job_id: str) -> StreamingHttpResponse | JsonResponse:
    try:
        cursor = _event_cursor(request)
        events, _next_cursor = get_facade().feature_job_events(job_id, cursor)
    except (KeyError, ValueError) as exc:
        return _error(404, "feature_job_not_found", _safe_message(exc))
    return StreamingHttpResponse(_sse(events, cursor), content_type="text/event-stream; charset=utf-8")


@require_POST
def orchestration_action(request: HttpRequest, orchestration_run_id: str, action: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json", "请求体必须是 JSON object")
    if not isinstance(payload, dict):
        return _error(400, "invalid_payload", "请求体必须是 JSON object")
    try:
        facade = get_facade()
        if action == "cancel":
            result = facade.cancel_orchestration(orchestration_run_id, payload, identity)
        elif action == "retry-dispatch":
            result = facade.retry_orchestration(orchestration_run_id, payload, identity)
        elif action == "sync":
            result = facade.sync_orchestration(orchestration_run_id, payload, identity)
        else:
            return _error(404, "orchestration_action_not_found", "不支持的编排操作")
    except OrchestrationConflict as exc:
        return _error(409, getattr(exc, "code", "orchestration_state_conflict"), _safe_message(exc))
    except AirflowOrchestratorError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "orchestration_invalid", _safe_message(exc))
    return JsonResponse(dict(result.body), status=result.status, json_dumps_params={"ensure_ascii": False})


@require_GET
def input_orchestration_detail(_request: HttpRequest, input_orchestration_run_id: str) -> JsonResponse:
    try:
        result = get_facade().input_orchestration_detail(input_orchestration_run_id)
    except (KeyError, ValueError) as exc:
        return _error(404, "input_orchestration_not_found", _safe_message(exc))
    return JsonResponse(dict(result.body), status=result.status, json_dumps_params={"ensure_ascii": False})


@require_POST
def input_orchestration_action(request: HttpRequest, input_orchestration_run_id: str, action: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json", "请求体必须是 JSON object")
    if not isinstance(payload, dict):
        return _error(400, "invalid_payload", "请求体必须是 JSON object")
    try:
        facade = get_facade()
        if action == "sync":
            result = facade.sync_input_orchestration(input_orchestration_run_id, payload, identity)
        else:
            return _error(404, "input_orchestration_action_not_found", "不支持的输入编排操作")
    except InputOrchestrationConflict as exc:
        return _error(409, getattr(exc, "code", "input_orchestration_state_conflict"), _safe_message(exc))
    except AirflowOrchestratorError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "input_orchestration_invalid", _safe_message(exc))
    return JsonResponse(dict(result.body), status=result.status, json_dumps_params={"ensure_ascii": False})


def _error(status: int, code: str, message: str) -> JsonResponse:
    return JsonResponse({"code": code, "error": message}, status=status, json_dumps_params={"ensure_ascii": False})


def _safe_message(exc: Exception) -> str:
    code = getattr(exc, "code", None)
    if code:
        return "LOGRISK 任务创建失败"
    return str(exc)[:300]


def _event_cursor(request: HttpRequest) -> int:
    value = request.headers.get("Last-Event-ID") or request.GET.get("cursor") or "0"
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _sse(events: list[dict[str, object]], cursor: int):
    if not events:
        yield b": heartbeat\n\n"
        return
    for offset, event in enumerate(events, start=1):
        event_type = str(event.get("type") or "message").replace("\n", "")[:100]
        data = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        yield f"id: {cursor + offset}\nevent: {event_type}\ndata: {data}\n\n".encode("utf-8")
