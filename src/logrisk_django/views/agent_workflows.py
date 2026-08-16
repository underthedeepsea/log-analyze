from __future__ import annotations

import json

from django.http import HttpRequest, JsonResponse

from logrisk.agentic import AgenticError
from logrisk.orchestration import AirflowOrchestratorError
from logrisk_django.service_factory import get_agent_workflow_airflow_orchestrator, get_config, get_container
from logrisk_django.views.access import require_django_write_access


def _service():
    service = get_container().agent_workflows
    if not get_config().agent_workflows_enabled or service is None:
        raise AgenticError("Agent 工作流功能未启用", code="agent_workflows_disabled", status_code=404)
    return service


def workflows(request: HttpRequest) -> JsonResponse:
    try:
        if request.method == "GET": return JsonResponse({"items": _service().list_workflows()}, json_dumps_params={"ensure_ascii": False})
        if request.method != "POST": return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
        identity = require_django_write_access(request)
        if isinstance(identity, JsonResponse): return identity
        payload = json.loads(request.body.decode() or "{}")
        key = str(request.headers.get("Idempotency-Key") or payload.pop("idempotency_key", "") or "")
        result = _service().create_workflow(payload, actor=identity.actor or "unknown", idempotency_key=key)
        return JsonResponse(result, status=201, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc: return _error(exc)
    except (json.JSONDecodeError, TypeError, ValueError) as exc: return JsonResponse({"code": "workflow_invalid", "error": str(exc)[:300]}, status=422)


def workflow_detail(request: HttpRequest, workflow_id: str, view: str | None = None) -> JsonResponse:
    try:
        if request.method == "GET":
            service = _service()
            if view == "runs": return JsonResponse({"items": service.list_runs(workflow_id=workflow_id)}, json_dumps_params={"ensure_ascii": False})
            return JsonResponse(service.get_workflow(workflow_id), json_dumps_params={"ensure_ascii": False})
        if view != "runs": return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
        identity = require_django_write_access(request)
        if isinstance(identity, JsonResponse): return identity
        payload = json.loads(request.body.decode() or "{}")
        container = get_container()
        evidence = container.feature_jobs.get_agent_evidence(str(payload.get("source_job_id") or ""), str(payload.get("entity_id") or ""))
        profile = container.model_profiles.get(payload.get("model_profile_id")); connection = container.connections.get(profile.connection_id); prompt = container.prompt_registry.load(str(payload.get("prompt_id") or "agent_plan_v1"))
        run = _service().create_run(workflow_id, source_job_id=str(payload.get("source_job_id") or ""), entity_id=str(payload.get("entity_id") or ""), entity_type=str(evidence["entity"].get("type") or ""), model_profile_id=profile.profile_id, prompt_id=prompt.prompt_id, actor=identity.actor or "unknown", roles=tuple(identity.roles), request_id=identity.request_id, idempotency_key=str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or ""), evidence_summary={"entity": evidence["entity"], "risk_score": evidence["risk_score"], "template_count": len(evidence["templates"])}, runtime_snapshot={"profile_snapshot": profile.public_dict(), "connection_snapshot": connection, "prompt_id": prompt.prompt_id, "prompt_sha256": prompt.sha256})
        if run.get("idempotent_replay"): return JsonResponse(run, status=202)
        airflow = get_agent_workflow_airflow_orchestrator()
        try: triggered = airflow.trigger_agent_workflow(run["workflow_run_id"], run["request_id"])
        except AirflowOrchestratorError as exc:
            failed = _service().repository.transition_run(run["workflow_run_id"], "failed", allowed_from={"queued", "running"}, error_code=exc.code, error_summary="Agent 工作流 Airflow 分派失败")
            return JsonResponse({"workflow_run_id": failed["workflow_run_id"], "status": failed["status"], "code": exc.code, "error": str(exc)}, status=exc.status_code)
        return JsonResponse({**run, "external_dag_id": airflow.dag_id, "external_run_id": triggered.external_run_id}, status=202, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc: return _error(exc)
    except (json.JSONDecodeError, TypeError, ValueError) as exc: return JsonResponse({"code": "workflow_run_invalid", "error": str(exc)[:300]}, status=422)


def workflow_runs(request: HttpRequest) -> JsonResponse:
    try:
        if request.method != "GET": return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
        return JsonResponse({"items": _service().list_runs()}, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc: return _error(exc)


def workflow_run_detail(request: HttpRequest, run_id: str, view: str | None = None) -> JsonResponse:
    try:
        if request.method == "POST" and view in {"pause", "resume", "cancel", "retry", "replay"}: return workflow_run_action(request, run_id, view)
        if request.method != "GET": return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
        run = _service().replay(run_id) if view == "replay" else _service().get_run(run_id)
        if view in {"events", "artifacts"}: return JsonResponse({"items": run[view]}, json_dumps_params={"ensure_ascii": False})
        return JsonResponse(run, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc: return _error(exc)


def workflow_run_action(request: HttpRequest, run_id: str, action: str) -> JsonResponse:
    if request.method != "POST": return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
    if action == "replay": return JsonResponse(_service().replay(run_id), json_dumps_params={"ensure_ascii": False})
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse): return identity
    try:
        payload = json.loads(request.body.decode() or "{}"); key = str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or ""); service = _service()
        result = service.retry(run_id, idempotency_key=key, request_id=identity.request_id) if action == "retry" else getattr(service, action)(run_id, idempotency_key=key)
        if action in {"resume", "retry"} and not result.get("idempotent_replay"):
            try:
                triggered = get_agent_workflow_airflow_orchestrator().trigger_agent_workflow(result["workflow_run_id"], result["request_id"]); result = {**result, "external_run_id": triggered.external_run_id}
            except AirflowOrchestratorError as exc:
                service.repository.transition_run(result["workflow_run_id"], "failed", allowed_from={"queued", "running"}, error_code=exc.code, error_summary="Agent 工作流 Airflow 分派失败")
                raise
        return JsonResponse(result, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc: return _error(exc)
    except AirflowOrchestratorError as exc: return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code)


def workflow_node_retry(request: HttpRequest, run_id: str, node_id: str) -> JsonResponse:
    if request.method != "POST": return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse): return identity
    try:
        payload = json.loads(request.body.decode() or "{}"); service = _service(); result = service.retry_node(run_id, node_id, idempotency_key=str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or ""))
        if result.get("idempotent_replay"):
            return JsonResponse(result, json_dumps_params={"ensure_ascii": False})
        try:
            triggered = get_agent_workflow_airflow_orchestrator().trigger_agent_workflow(run_id, result["request_id"])
        except AirflowOrchestratorError as exc:
            service.repository.transition_run(run_id, "failed", allowed_from={"queued", "running"}, error_code=exc.code, error_summary="Agent 工作流 Airflow 分派失败")
            raise
        return JsonResponse({**result, "external_run_id": triggered.external_run_id}, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc: return _error(exc)
    except AirflowOrchestratorError as exc: return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code)


def _error(exc: AgenticError) -> JsonResponse:
    return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code, json_dumps_params={"ensure_ascii": False})
