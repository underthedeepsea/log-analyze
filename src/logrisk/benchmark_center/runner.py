from __future__ import annotations

import time
from typing import Any, Callable

from logrisk.ai_eval.evaluators import evaluate_case
from logrisk.benchmark_center.metrics import normalize_case_results


Extractor = Callable[..., list[dict[str, Any]]]
ResultCallback = Callable[[dict[str, Any], int], None]


def classify_failure(result: dict[str, Any]) -> str | None:
    if result.get("passed"):
        return None
    text = " ".join(str(item) for item in result.get("errors") or []).lower()
    if "timeout" in text or "超时" in text:
        return "timeout"
    if "schema" in text:
        return "schema_invalid"
    if "template" in text:
        return "template_reference"
    if "forbidden" in text or "越界" in text:
        return "forbidden_claim"
    return "model_or_expectation"


def execute_cases(
    cases: list[dict[str, Any]],
    *,
    extractor: Extractor,
    extractor_options: dict[str, Any] | None = None,
    on_result: ResultCallback | None = None,
    should_cancel: Callable[[], bool] | None = None,
    retry_count: int = 0,
    max_calls: int | None = None,
    count_model_calls: bool = False,
) -> dict[str, Any]:
    case_results: list[dict[str, Any]] = []
    durations: list[float] = []
    model_calls = 0
    for index, case in enumerate(cases, 1):
        if should_cancel and should_cancel():
            return {"cancelled": True, "case_results": case_results, "metrics": normalize_case_results(case_results, durations_ms=durations, counters={"model_calls": model_calls})}
        started = time.perf_counter()
        error = None
        features: list[dict[str, Any]] = []
        for _attempt in range(max(0, int(retry_count)) + 1):
            if count_model_calls and max_calls is not None and model_calls >= max_calls:
                error = "benchmark call budget exhausted"
                break
            if count_model_calls:
                model_calls += 1
            try:
                features = extractor(case.get("input_entity") or {}, case=case, **(extractor_options or {}))
                error = None
                break
            except Exception as exc:  # Model/provider errors are evaluation results, not worker crashes.
                error = str(exc)
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        evaluated = evaluate_case(case, features, error)
        evaluated.update({
            "case_id": str(case.get("name") or f"case-{index}"),
            "duration_ms": duration_ms,
        })
        evaluated["error_type"] = classify_failure(evaluated)
        case_results.append(evaluated)
        durations.append(duration_ms)
        if on_result:
            on_result(evaluated, index)
    return {
        "cancelled": False,
        "case_results": case_results,
        "metrics": normalize_case_results(case_results, durations_ms=durations, counters={"model_calls": model_calls}),
    }
