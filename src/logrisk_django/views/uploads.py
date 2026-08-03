from __future__ import annotations

import json
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from logrisk.orchestration import AirflowOrchestratorError
from logrisk.runtime.service import RuntimeQuotaError
from logrisk_django.service_factory import (
    get_container,
    get_input_airflow_orchestrator,
)
from logrisk_django.views.access import require_django_write_access


def _json_payload(request: HttpRequest) -> dict[str, Any] | JsonResponse:
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json", "请求体必须是 JSON object")
    if not isinstance(payload, dict):
        return _error(400, "invalid_payload", "请求体必须是 JSON object")
    return payload


@require_POST
def create_upload(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _json_payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        services = get_container()
        services.runtime_service.require_capacity("上传", additional_bytes=max(0, int(payload.get("size_bytes") or 0)))
        manifest = services.upload_store.create(
            filename=str(payload.get("filename") or "upload.log"),
            size_bytes=int(payload.get("size_bytes") or 0),
            chunk_size_bytes=int(payload.get("chunk_size_bytes") or 0) or None,
        )
    except RuntimeQuotaError as exc:
        return _error(507, exc.code, "存储空间不足，无法创建上传")
    except (TypeError, ValueError) as exc:
        return _error(422, "upload_invalid", _safe_message(exc))
    return JsonResponse({
        "upload_id": manifest["upload_id"],
        "chunk_size_bytes": manifest["chunk_size_bytes"],
        "total_chunks": manifest["total_chunks"],
        "max_upload_bytes": services.upload_store.config.max_upload_bytes,
        "status": manifest["status"],
    }, status=201, json_dumps_params={"ensure_ascii": False})


@require_http_methods(["PUT"])
def append_upload_chunk(request: HttpRequest, upload_id: str, index: int) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        services = get_container()
        data = request.body
        services.runtime_service.require_capacity("上传分片", additional_bytes=len(data))
        manifest = services.upload_store.append_chunk(
            upload_id=upload_id,
            index=int(index),
            data=data,
            chunk_sha256=request.headers.get("X-Chunk-SHA256"),
        )
    except RuntimeQuotaError as exc:
        return _error(507, exc.code, "存储空间不足，无法写入上传分片")
    except KeyError:
        return _error(404, "upload_not_found", "上传会话不存在")
    except (TypeError, ValueError) as exc:
        return _error(422, "upload_chunk_invalid", _safe_message(exc))
    received = len(manifest.get("received_chunks") or [])
    total = int(manifest["total_chunks"])
    return JsonResponse({
        "upload_id": manifest["upload_id"],
        "chunk_index": int(index),
        "received": True,
        "received_chunks": received,
        "total_chunks": total,
        "progress": received / total if total else 1.0,
    }, json_dumps_params={"ensure_ascii": False})


@require_POST
def complete_upload(request: HttpRequest, upload_id: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _json_payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        services = get_container()
        manifest = services.upload_store.complete(upload_id=upload_id, final_sha256=payload.get("sha256"))
    except KeyError:
        return _error(404, "upload_not_found", "上传会话不存在")
    except (TypeError, ValueError) as exc:
        return _error(422, "upload_complete_invalid", _safe_message(exc))
    return JsonResponse({
        "upload_id": manifest["upload_id"],
        "status": manifest["status"],
        "path": services.upload_store.source_reference(upload_id),
    }, json_dumps_params={"ensure_ascii": False})


@require_POST
def analyze_upload(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _json_payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    services = get_container()
    try:
        services.runtime_service.require_capacity("大文件分析")
        upload_id = str(payload.get("upload_id") or "")
        manifest = services.upload_store.get(upload_id)
        if manifest.get("status") != "completed":
            return _error(422, "upload_incomplete", "上传尚未完成")
        job = services.input_jobs.create(
            upload_id=upload_id,
            filename=str(payload.get("filename") or manifest.get("safe_filename") or "upload.log"),
            source_path=str(services.upload_store.source_path(upload_id)),
            drain_config=services.drain_quality.configs.active_snapshot(),
            semantic_snapshot=services.semantic_dictionaries.active_snapshot(),
        )
        run = services.input_orchestration.create_pending(
            job["input_job_id"], identity.request_id, identity.actor or "unknown", identity.roles
        )
    except RuntimeQuotaError as exc:
        return _error(507, exc.code, "存储空间不足，无法启动日志分析")
    except KeyError:
        return _error(404, "upload_not_found", "上传会话不存在")
    except (TypeError, ValueError) as exc:
        return _error(422, "input_job_invalid", _safe_message(exc))
    try:
        airflow = get_input_airflow_orchestrator()
        triggered = airflow.trigger_input(job["input_job_id"], run["input_orchestration_run_id"], identity.request_id)
    except AirflowOrchestratorError as exc:
        failed = services.input_orchestration.mark_dispatch_failed(
            run["input_orchestration_run_id"],
            expected_version=run["state_version"],
            error_code=exc.code,
            error_summary=str(exc),
        )
        return JsonResponse({
            "input_job_id": job["input_job_id"],
            "input_orchestration_run_id": failed["input_orchestration_run_id"],
            "status": failed["status"],
            "code": exc.code,
            "error": str(exc),
        }, status=exc.status_code, json_dumps_params={"ensure_ascii": False})
    dispatched = services.input_orchestration.mark_dispatched(
        run["input_orchestration_run_id"],
        airflow.dag_id,
        triggered.external_run_id,
        expected_version=run["state_version"],
    )
    return JsonResponse({
        "input_job_id": job["input_job_id"],
        "input_orchestration_run_id": dispatched["input_orchestration_run_id"],
        "status": dispatched["status"],
        "external_dag_id": dispatched["external_dag_id"],
        "external_run_id": dispatched["external_run_id"],
    }, status=202, json_dumps_params={"ensure_ascii": False})


@require_GET
def input_job_progress(_request: HttpRequest, input_job_id: str) -> JsonResponse:
    try:
        return JsonResponse(get_container().input_jobs.get_progress(input_job_id), json_dumps_params={"ensure_ascii": False})
    except KeyError:
        return _error(404, "input_job_not_found", "输入任务不存在")


@require_GET
def input_job_result(_request: HttpRequest, input_job_id: str) -> JsonResponse:
    try:
        return JsonResponse({"result": get_container().input_jobs.get_result(input_job_id)}, json_dumps_params={"ensure_ascii": False})
    except KeyError:
        return _error(404, "input_job_result_not_found", "输入任务结果尚不可用")


def _error(status: int, code: str, message: str) -> JsonResponse:
    return JsonResponse({"code": code, "error": message}, status=status, json_dumps_params={"ensure_ascii": False})


def _safe_message(exc: Exception) -> str:
    return str(exc).replace("\n", " ")[:300]
