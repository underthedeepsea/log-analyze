from __future__ import annotations

from typing import Any


def comparison_report(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_metrics = left.get("metrics") or {}
    right_metrics = right.get("metrics") or {}
    sections = sorted(set(left_metrics) | set(right_metrics))
    return {
        "schema_version": "drain_comparison_report_v1",
        "left_run_id": left.get("run_id"),
        "right_run_id": right.get("run_id"),
        "sections": {
            section: {
                key: {
                    "left": (left_metrics.get(section) or {}).get(key),
                    "right": (right_metrics.get(section) or {}).get(key),
                }
                for key in sorted(set(left_metrics.get(section) or {}) | set(right_metrics.get(section) or {}))
            }
            for section in sections
        },
    }
