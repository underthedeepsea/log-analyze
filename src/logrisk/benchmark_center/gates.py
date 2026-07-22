from __future__ import annotations

from typing import Any


def evaluate_regression_gate(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    thresholds = thresholds or {}
    quality_keys = ("pass_rate", "schema_valid_rate", "template_reference_accuracy")
    deltas = {key: round(float(candidate.get(key) or 0) - float(baseline.get(key) or 0), 4) for key in quality_keys}
    deltas["latency_p95_ms"] = round(float(candidate.get("latency_p95_ms") or 0) - float(baseline.get("latency_p95_ms") or 0), 2)
    reasons: list[str] = []
    blocked = False
    labels = {"pass_rate": "通过率", "schema_valid_rate": "Schema 有效率", "template_reference_accuracy": "模板引用准确率"}
    minimum_keys = {
        "pass_rate": "min_pass_rate",
        "schema_valid_rate": "min_schema_valid_rate",
        "template_reference_accuracy": "min_template_reference_accuracy",
    }
    for key in quality_keys:
        minimum = thresholds.get(minimum_keys[key])
        if minimum is not None and float(candidate.get(key) or 0) < float(minimum):
            blocked = True
            reasons.append(f"{labels[key]}低于门槛 {float(minimum):.2f}")
    max_drop = float(thresholds.get("max_pass_rate_drop", 0.05))
    if -deltas["pass_rate"] > max_drop:
        blocked = True
        reasons.append(f"通过率下降 {abs(deltas['pass_rate']):.4f}，超过允许值 {max_drop:.4f}")
    baseline_latency = float(baseline.get("latency_p95_ms") or 0)
    latency_increase = (deltas["latency_p95_ms"] / baseline_latency * 100) if baseline_latency else 0.0
    max_latency_value = thresholds.get("max_latency_increase_percent")
    max_latency = float(max_latency_value) if max_latency_value is not None else None
    if not blocked and max_latency is not None and latency_increase > max_latency:
        reasons.append(f"P95 延迟增加 {latency_increase:.2f}%，需要人工复核")
    decision = "blocked" if blocked else ("manual_review" if reasons else "passed")
    return {
        "decision": decision,
        "deltas": deltas,
        "reasons": reasons or ["候选版本满足全部质量与性能门槛"],
        "thresholds": thresholds,
        "schema_version": "benchmark_gate_evaluation_v1",
    }
