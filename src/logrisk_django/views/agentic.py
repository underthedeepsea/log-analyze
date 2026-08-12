from __future__ import annotations

import json

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_GET, require_POST

from logrisk.agentic import AgentRunRequest, AgenticError
from logrisk.orchestration import AirflowOrchestratorError
from logrisk_django.service_factory import get_agent_airflow_orchestrator, get_config, get_container
from logrisk_django.views.access import require_django_write_access


def _service():
    service = get_container().agent_runs
    if not get_config().agentic_enabled or service is None:
        raise AgenticError("Agent 功能未启用", code="agentic_disabled", status_code=404)
    return service


def agent_runs(request: HttpRequest) -> JsonResponse:
    if request.method == "POST":
        return create_agent_run(request)
    if request.method != "GET":
        return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
    try:
        return JsonResponse({"items": _service().list_runs()}, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc:
        return _error(exc)


def agent_run_detail(request: HttpRequest, run_id: str, view: str | None = None) -> JsonResponse:
    if request.method == "POST" and view in {"pause", "resume", "cancel", "retry", "replay"}:
        return agent_run_action(request, run_id, view)
    if request.method != "GET":
        return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
    try:
        run = _service().replay(run_id) if view == "replay" else _service().get_run(run_id)
        if view in {"events", "artifacts"}:
            return JsonResponse({"items": run[view]}, json_dumps_params={"ensure_ascii": False})
        return JsonResponse(run, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc:
        return _error(exc)


@require_POST
def create_agent_run(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        payload = json.loads(request.body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise AgenticError("请求体必须是 JSON object")
        container = get_container()
        evidence = container.feature_jobs.get_agent_evidence(str(payload.get("source_job_id") or ""), str(payload.get("entity_id") or ""))
        profile = container.model_profiles.get(payload.get("model_profile_id"))
        connection = container.connections.get(profile.connection_id)
        prompt = container.prompt_registry.load(str(payload.get("prompt_id") or "agent_plan_v1"))
        key = str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or "")
        if not key:
            raise AgenticError("缺少幂等键", code="idempotency_required")
        run = _service().create_run(AgentRunRequest(
            source_job_id=str(payload.get("source_job_id")), entity_id=str(payload.get("entity_id")),
            entity_type=str(evidence["entity"].get("type") or ""), model_profile_id=profile.profile_id,
            prompt_id=prompt.prompt_id, max_steps=int(payload.get("max_steps") or 6),
            max_tool_calls=int(payload.get("max_tool_calls") or 10), timeout_seconds=float(payload.get("timeout_seconds") or 120),
            allowed_tools=tuple(payload.get("allowed_tools") or ["get_sanitized_evidence", "find_approved_rules", "inspect_knowledge_assets", "evaluate_candidate", "register_feature_candidate"]),
            idempotency_key=key, actor=identity.actor or "unknown", roles=tuple(identity.roles), request_id=identity.request_id,
        ), locked_snapshot={
            "schema_version": "1.0", "goal": str(payload.get("goal") or "提取可审批日志特征"),
            "evidence_summary": {"entity": evidence["entity"], "risk_score": evidence["risk_score"], "template_count": len(evidence["templates"])},
            "profile_snapshot": profile.public_dict(), "connection_snapshot": connection,
            "prompt_id": prompt.prompt_id, "prompt_sha256": prompt.sha256,
        })
        if run.get("idempotent_replay"):
            return JsonResponse({"run_id": run["run_id"], "status": run["status"], "idempotent_replay": True}, status=202)
        airflow = get_agent_airflow_orchestrator()
        try:
            triggered = airflow.trigger_agent(run["run_id"], run["request_id"])
        except AirflowOrchestratorError as exc:
            failed = _service().mark_dispatch_failed(run["run_id"], error_code=exc.code)
            return JsonResponse({"run_id": failed["run_id"], "status": failed["status"], "code": exc.code, "error": str(exc)}, status=exc.status_code, json_dumps_params={"ensure_ascii": False})
        return JsonResponse({
            "run_id": run["run_id"], "status": run["status"], "external_dag_id": airflow.dag_id,
            "external_run_id": triggered.external_run_id,
        }, status=202, json_dumps_params={"ensure_ascii": False})
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"code": "invalid_json", "error": "请求体必须是 JSON object"}, status=400)
    except AgenticError as exc:
        return _error(exc)
    except AirflowOrchestratorError as exc:
        return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code, json_dumps_params={"ensure_ascii": False})
    except (KeyError, TypeError, ValueError) as exc:
        return JsonResponse({"code": "agent_run_invalid", "error": str(exc)[:300]}, status=422, json_dumps_params={"ensure_ascii": False})


@require_POST
def agent_run_action(request: HttpRequest, run_id: str, action: str) -> JsonResponse:
    if action == "replay":
        try:
            return JsonResponse(_service().replay(run_id), json_dumps_params={"ensure_ascii": False})
        except AgenticError as exc:
            return _error(exc)
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
        service = _service()
        key = str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or "")
        if action == "retry":
            result = service.retry(run_id, idempotency_key=key, request_id=identity.request_id)
            if not result.get("idempotent_replay"):
                try:
                    triggered = get_agent_airflow_orchestrator().trigger_agent(result["run_id"], result["request_id"])
                except AirflowOrchestratorError as exc:
                    service.mark_dispatch_failed(result["run_id"], error_code=exc.code)
                    raise
                result = {**result, "external_run_id": triggered.external_run_id}
        elif action in {"pause", "resume", "cancel"}:
            result = getattr(service, action)(run_id, idempotency_key=key)
            if action == "resume" and not result.get("idempotent_replay"):
                try:
                    triggered = get_agent_airflow_orchestrator().trigger_agent(run_id, result["request_id"])
                except AirflowOrchestratorError as exc:
                    service.mark_dispatch_failed(run_id, error_code=exc.code)
                    raise
                result = {**result, "external_run_id": triggered.external_run_id}
        else:
            return JsonResponse({"code": "agent_action_not_found", "error": "不支持的 Agent 操作"}, status=404)
        return JsonResponse(result, json_dumps_params={"ensure_ascii": False})
    except AgenticError as exc:
        return _error(exc)
    except AirflowOrchestratorError as exc:
        return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code, json_dumps_params={"ensure_ascii": False})


def _error(exc: AgenticError) -> JsonResponse:
    return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code, json_dumps_params={"ensure_ascii": False})
