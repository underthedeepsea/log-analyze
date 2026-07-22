from __future__ import annotations

import math
from typing import Any, Iterable


def _rate(value: int | float, total: int | float) -> float:
    return round(float(value) / float(total), 4) if total else 0.0


def _p95(values: Iterable[float]) -> float:
    ordered = sorted(max(0.0, float(value)) for value in values)
    if not ordered:
        return 0.0
    return round(ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)], 2)


def normalize_case_results(
    case_results: list[dict[str, Any]],
    *,
    durations_ms: list[float] | None = None,
    counters: dict[str, int | float] | None = None,
) -> dict[str, Any]:
    counters = counters or {}
    durations = durations_ms if durations_ms is not None else [float(item.get("duration_ms") or 0) for item in case_results]
    total = len(case_results)
    calls = int(counters["model_calls"]) if "model_calls" in counters else total
    return {
        "case_count": total,
        "passed_cases": sum(bool(item.get("passed")) for item in case_results),
        "pass_rate": _rate(sum(bool(item.get("passed")) for item in case_results), total),
        "json_valid_rate": _rate(sum(bool(item.get("json_valid")) for item in case_results), total),
        "schema_valid_rate": _rate(sum(bool(item.get("schema_valid")) for item in case_results), total),
        "template_reference_accuracy": _rate(sum(bool(item.get("template_reference_ok")) for item in case_results), total),
        "latency_avg_ms": round(sum(durations) / len(durations), 2) if durations else 0.0,
        "latency_p95_ms": _p95(durations),
        "cache_hit_rate": _rate(int(counters.get("cache_hits") or 0), calls),
        "rule_skip_rate": _rate(int(counters.get("rule_skips") or 0), total),
        "model_calls": calls,
        "input_units": int(counters.get("input_units") or 0),
        "output_units": int(counters.get("output_units") or 0),
    }
