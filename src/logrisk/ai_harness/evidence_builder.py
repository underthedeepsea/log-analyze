from __future__ import annotations

import hashlib
import json
from typing import Any, Dict


TEMPLATE_FIELDS = (
    "template_hash",
    "component",
    "severity",
    "template",
    "category",
    "count",
    "first_seen",
    "last_seen",
    "feature_hint",
)


def sanitized_templates(entity: Dict[str, Any]) -> list[Dict[str, Any]]:
    return [
        {key: template.get(key) for key in TEMPLATE_FIELDS}
        for template in (entity.get("top_templates") or [])
        if isinstance(template, dict)
    ]


def build_feature_evidence(entity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "window_start": entity.get("window_start"),
        "window_end": entity.get("window_end"),
        "cluster": entity.get("cluster"),
        "entity": {"type": entity.get("entity_type"), "id": entity.get("entity_id")},
        "risk_score": entity.get("risk_score"),
        "risk_level": entity.get("risk_level"),
        "affected_entities": entity.get("affected_entities") or [],
        "templates": sanitized_templates(entity),
    }


def evidence_hash(evidence: Dict[str, Any]) -> str:
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
