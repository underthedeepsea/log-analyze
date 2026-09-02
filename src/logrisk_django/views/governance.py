from __future__ import annotations

import json

from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_http_methods

from logrisk.feature_jobs import FeatureJobError
from logrisk.rule_governance import RuleGovernanceError
from logrisk_django.service_factory import get_facade
from logrisk_django.views.access import require_django_write_access


@require_http_methods(["PATCH"])
def update_feature(request: HttpRequest, job_id: str, candidate_id: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        result = get_facade().update_feature(job_id, candidate_id, payload, identity)
    except FeatureJobError as exc:
        return _error(
            getattr(exc, "status_code", 422),
            getattr(exc, "code", "invalid_feature_update"),
            _safe_message(exc),
        )
    except (KeyError, ValueError) as exc:
        return _error(422, "invalid_feature_update", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["POST"])
def export_approved(request: HttpRequest, job_id: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    try:
        result = get_facade().export_approved(job_id, identity)
    except (FeatureJobError, KeyError, ValueError) as exc:
        return _error(422, getattr(exc, "code", "export_failed"), _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["POST"])
def validate_release(request: HttpRequest) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    key = str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or "").strip()
    try:
        result = get_facade().validate_release(payload, identity, idempotency_key=key)
    except (KeyError, ValueError) as exc:
        return _error(422, getattr(exc, "code", "release_validation_failed"), _safe_message(exc))
    return _response(result.body, result.status, result.headers)


def rule_review_queue(request: HttpRequest) -> JsonResponse:
    result = get_facade().rule_review_queue()
    return _response(result.body, result.status, result.headers)


def rule_detail(request: HttpRequest, rule_id: str) -> JsonResponse:
    try:
        result = get_facade().rule_governance_detail(rule_id)
    except RuleGovernanceError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    return _response(result.body, result.status, result.headers)


@require_http_methods(["POST"])
def rule_action(request: HttpRequest, rule_id: str, action: str) -> JsonResponse:
    identity = require_django_write_access(request)
    if isinstance(identity, JsonResponse):
        return identity
    payload = _payload(request)
    if isinstance(payload, JsonResponse):
        return payload
    try:
        facade = get_facade()
        if action == "status":
            result = facade.change_rule_status(rule_id, payload, identity)
        elif action == "feedback":
            result = facade.record_rule_feedback(rule_id, payload, identity)
        else:
            result = facade.rollback_rule(rule_id, payload, identity)
    except RuleGovernanceError as exc:
        return _error(exc.status_code, exc.code, _safe_message(exc))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(422, "rule_invalid", _safe_message(exc))
    return _response(result.body, result.status, result.headers)


def _payload(request: HttpRequest) -> dict[str, object] | JsonResponse:
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _error(400, "invalid_json", "请求体必须是 JSON object")
    if not isinstance(body, dict):
        return _error(400, "invalid_payload", "请求体必须是 JSON object")
    return body


def _response(body: object, status: int, headers: object) -> JsonResponse:
    response = JsonResponse(body, status=status, json_dumps_params={"ensure_ascii": False})
    for name, value in dict(headers).items():
        response[str(name)] = str(value)
    return response


def _error(status: int, code: str, message: str) -> JsonResponse:
    return JsonResponse({"code": code, "error": message}, status=status, json_dumps_params={"ensure_ascii": False})


def _safe_message(exc: Exception) -> str:
    return str(exc)[:300] if not getattr(exc, "code", None) else "LOGRISK 治理操作失败"
