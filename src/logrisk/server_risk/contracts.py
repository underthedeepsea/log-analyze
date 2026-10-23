from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

LING_MODEL = "maternion/ling-3.0-tiny:8b-Q4_K_M"
LING_PROFILE = "ling_3_0_tiny_8b_q4km"
PROMPT_ID = "node_risk_report_v1"
CLAIMS = ("risk_summary", "repeated_occurrence", "multiple_objects", "historical_only", "coverage_gap", "no_recovery_evidence", "observed_recovery", "hardware_unresolved")
TYPED_FIELDS = frozenset(("unit", "device", "gpu_uuid", "pod_uid", "mount", "filesystem", "boot_id", "ready", "expected_state", "verified", "activity", "success", "severity", "object_id", "object_kind"))


class ServerRiskError(ValueError):
    def __init__(self, code: str, message: str, status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class DeadlineHTTPResponse:
    """Compatibility facade for the report's owned cancellable transport."""

    def __new__(cls, request: Any, **kwargs: Any) -> Any:
        from logrisk.server_risk.transport import CancellableHTTPResponse
        return CancellableHTTPResponse(request, **kwargs)


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(json_text(value).encode()).hexdigest()


def decode(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError, AttributeError, TypeError) as exc:
        raise ServerRiskError("invalid_event_time", "时间必须为带时区 ISO-8601") from exc


def strict_object(value: Any, allowed: set[str], required: set[str] = frozenset()) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) - allowed or required - set(value):
        raise ServerRiskError("invalid_schema", "字段或对象结构不符合合同")
    return value


def identifier(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 256 or any(ord(c) < 32 for c in value):
        raise ServerRiskError("invalid_identifier", "标识必须为非空且不超过256字符")
    return value


def integer(value: Any, lower: int, upper: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
        raise ServerRiskError("invalid_limit", "数量超出允许范围")
    return value


def day_bounds(day: str, zone: str) -> tuple[datetime, datetime]:
    try:
        if not isinstance(day, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise ValueError()
        local_date = date.fromisoformat(day)
        tz = ZoneInfo(zone)
        return (datetime.combine(local_date, time.min, tz).astimezone(timezone.utc), datetime.combine(local_date + timedelta(days=1), time.min, tz).astimezone(timezone.utc))
    except (ValueError, OverflowError, TypeError, ZoneInfoNotFoundError) as exc:
        raise ServerRiskError("invalid_date_or_timezone", "日期或 IANA 时区无效") from exc


def query_contract(cluster: Any, node_id: Any, day: Any, zone: Any) -> dict[str, str]:
    cluster, node_id = identifier(cluster), identifier(node_id)
    day_bounds(day, zone)
    return {"cluster": cluster, "node_id": node_id, "date": day, "timezone": zone}


def typed_fields(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {key: val for key, val in value.items() if key in TYPED_FIELDS and isinstance(val, (str, int, float, bool)) and (not isinstance(val, float) or math.isfinite(val)) and (not isinstance(val, str) or len(val) <= 512)}


def cursor_encode(scope: Any, offset: int, revision: str) -> str:
    return base64.urlsafe_b64encode(json_text({"scope": digest(scope), "offset": offset, "revision": revision}).encode()).decode().rstrip("=")


def cursor_offset(value: str | None, scope: Any, revision: str) -> int:
    if value is None:
        return 0
    try:
        if len(value) > 4096:
            raise ValueError()
        data = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
        if set(data) != {"scope", "offset", "revision"} or data["scope"] != digest(scope) or data["revision"] != revision:
            raise ValueError()
        return integer(data["offset"], 0, 1000000)
    except (ValueError, TypeError, KeyError) as exc:
        raise ServerRiskError("invalid_cursor", "分页游标不属于当前查询或数据版本") from exc

ID_SCHEMA = {"type": "string", "minLength": 1, "maxLength": 256}
DATE_SCHEMA = {"type": "string", "format": "date", "pattern": r"^\d{4}-\d{2}-\d{2}$"}
ZONE_SCHEMA = {"type": "string", "minLength": 1, "maxLength": 256}
PAGE_PROPERTIES = {"cursor": {"type": "string", "maxLength": 4096}, "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50}}
SUBJECT_PROPERTIES = {"cluster": ID_SCHEMA, "node_id": ID_SCHEMA}
DAY_PROPERTIES = {**SUBJECT_PROPERTIES, "date": DATE_SCHEMA, "timezone": ZONE_SCHEMA}
TOOL_CONTRACTS = {
    "logrisk_find_nodes": ({"q": {"type": "string", "maxLength": 256}, "cluster": ID_SCHEMA, **PAGE_PROPERTIES}, [], ["schema_version", "items", "next_cursor"]),
    "logrisk_daily_series": ({**SUBJECT_PROPERTIES, "from": DATE_SCHEMA, "to": DATE_SCHEMA, "timezone": ZONE_SCHEMA}, ["cluster", "node_id", "from", "to", "timezone"], ["schema_version", "query", "items", "revision", "projection_complete"]),
    "logrisk_day_evidence": ({**DAY_PROPERTIES, **PAGE_PROPERTIES}, ["cluster", "node_id", "date", "timezone"], ["schema_version", "query", "evidence_revision", "summary", "items", "next_cursor", "total", "quality"]),
    "logrisk_current_assessment": (SUBJECT_PROPERTIES, ["cluster", "node_id"], ["schema_version", "subject", "status", "severity", "assessed_at", "evidence_at", "valid_until", "last_known_state", "reasons", "coverage", "revision"]),
    "logrisk_generate_day_report": ({**DAY_PROPERTIES, "model_profile_id": ID_SCHEMA, "idempotency_key": ID_SCHEMA}, ["cluster", "node_id", "date", "timezone", "model_profile_id", "idempotency_key"], ["schema_version", "report_id", "status", "query", "profile", "artifact", "error"]),
    "logrisk_get_report": ({"report_id": ID_SCHEMA}, ["report_id"], ["schema_version", "report_id", "status", "query", "profile", "artifact", "error"]),
    "logrisk_cancel_report": ({"report_id": ID_SCHEMA, "idempotency_key": ID_SCHEMA}, ["report_id", "idempotency_key"], ["schema_version", "report_id", "status", "query", "profile", "artifact", "error"]),
}


def tool_arguments(name: str, arguments: Any) -> dict[str, Any]:
    if name not in TOOL_CONTRACTS:
        raise ServerRiskError("mcp_unknown_tool", "工具未登记")
    props, required, _ = TOOL_CONTRACTS[name]
    strict_object(arguments, set(props), set(required))
    for key, value in arguments.items():
        schema = props[key]
        if schema["type"] == "integer":
            integer(value, schema["minimum"], schema["maximum"])
        elif not isinstance(value, str) or len(value) < schema.get("minLength", 0) or len(value) > schema.get("maxLength", 4096):
            raise ServerRiskError("invalid_schema", "工具字段类型或长度无效")
    if "date" in arguments:
        day_bounds(arguments["date"], arguments["timezone"])
    if "from" in arguments:
        day_bounds(arguments["from"], arguments["timezone"])
        day_bounds(arguments["to"], arguments["timezone"])
        if not 0 <= (date.fromisoformat(arguments["to"]) - date.fromisoformat(arguments["from"])).days < 90:
            raise ServerRiskError("invalid_date_range", "日期范围必须为1至90日")
    return arguments
