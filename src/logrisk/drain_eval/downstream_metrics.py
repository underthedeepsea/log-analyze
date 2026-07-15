from __future__ import annotations

from typing import Any


def evaluate_downstream(expected: dict[str, Any] | None, actual: dict[str, Any] | None) -> dict[str, float]:
    expected = expected or {}
    actual = actual or {}

    def consistency(key: str) -> float:
        left = set(expected.get(key) or [])
        right = set(actual.get(key) or [])
        return round(len(left & right) / len(left | right), 6) if left or right else 1.0

    critical = set(expected.get("critical_risks") or [])
    actual_critical = set(actual.get("critical_risks") or [])
    normal = set(expected.get("normal_logs") or [])
    flagged = set(actual.get("flagged_logs") or [])
    return {
        "critical_risk_recall": round(len(critical & actual_critical) / len(critical), 6) if critical else 1.0,
        "normal_log_false_positive_rate": round(len(normal & flagged) / len(normal), 6) if normal else 0.0,
        "risk_entity_consistency": consistency("risk_entities"),
        "evidence_consistency": consistency("evidence_ids"),
        "approved_rule_hit_consistency": consistency("approved_rule_hits"),
    }
