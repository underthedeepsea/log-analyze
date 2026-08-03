from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_POST

from logrisk.orchestration import AirflowOrchestratorError
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


def _error(status: int, code: str, message: str) -> JsonResponse:
    return JsonResponse({"code": code, "error": message}, status=status, json_dumps_params={"ensure_ascii": False})


def _safe_message(exc: Exception) -> str:
    code = getattr(exc, "code", None)
    if code:
        return "LOGRISK 任务创建失败"
    return str(exc)[:300]
