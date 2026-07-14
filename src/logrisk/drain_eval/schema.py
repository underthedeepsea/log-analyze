from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class DrainQualityError(ValueError):
    pass


GOLD_REQUIRED = (
    "record_id",
    "source_type",
    "component",
    "message_core",
    "gold_group_id",
    "gold_template",
    "semantic_fields",
    "protected_tokens",
    "expected_risk_type",
    "annotation_status",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_gold_record(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise DrainQualityError("Gold record 必须是 JSON object")
    if record.get("schema_version") != "drain_gold_v1":
        raise DrainQualityError("schema_version 必须是 drain_gold_v1")
    for field in GOLD_REQUIRED:
        if field not in record:
            raise DrainQualityError(f"Gold record 缺少字段: {field}")
    for field in ("record_id", "source_type", "component", "message_core", "gold_group_id", "gold_template"):
        if not isinstance(record[field], str) or not record[field].strip():
            raise DrainQualityError(f"{field} 必须是非空字符串")
    if not isinstance(record["semantic_fields"], dict):
        raise DrainQualityError("semantic_fields 必须是 object")
    if not isinstance(record["protected_tokens"], list) or not all(isinstance(item, str) for item in record["protected_tokens"]):
        raise DrainQualityError("protected_tokens 必须是字符串数组")
    if record["annotation_status"] not in {"draft", "review", "approved", "ignored"}:
        raise DrainQualityError("annotation_status 无效")
    return dict(record)


def require_object(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise DrainQualityError("请求体必须是 JSON object")
    return payload
