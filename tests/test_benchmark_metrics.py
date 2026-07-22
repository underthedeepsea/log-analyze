from __future__ import annotations


def test_metric_normalizer_unifies_quality_latency_and_efficiency():
    from logrisk.benchmark_center.metrics import normalize_case_results

    metrics = normalize_case_results(
        [
            {"passed": True, "json_valid": True, "schema_valid": True, "template_reference_ok": True},
            {"passed": False, "json_valid": True, "schema_valid": False, "template_reference_ok": False},
        ],
        durations_ms=[10, 30],
        counters={"cache_hits": 1, "rule_skips": 1, "model_calls": 2, "input_units": 120, "output_units": 40},
    )

    assert metrics == {
        "case_count": 2,
        "passed_cases": 1,
        "pass_rate": 0.5,
        "json_valid_rate": 1.0,
        "schema_valid_rate": 0.5,
        "template_reference_accuracy": 0.5,
        "latency_avg_ms": 20.0,
        "latency_p95_ms": 30.0,
        "cache_hit_rate": 0.5,
        "rule_skip_rate": 0.5,
        "model_calls": 2,
        "input_units": 120,
        "output_units": 40,
    }


def test_metric_normalizer_handles_empty_suite_without_division_error():
    from logrisk.benchmark_center.metrics import normalize_case_results

    metrics = normalize_case_results([])

    assert metrics["case_count"] == 0
    assert metrics["pass_rate"] == 0.0
    assert metrics["latency_p95_ms"] == 0.0


def test_metric_normalizer_preserves_explicit_zero_model_calls():
    from logrisk.benchmark_center.metrics import normalize_case_results

    metrics = normalize_case_results(
        [{"passed": True, "json_valid": True, "schema_valid": True, "template_reference_ok": True}],
        counters={"model_calls": 0},
    )

    assert metrics["model_calls"] == 0


def test_regression_gate_explains_block_and_manual_review():
    from logrisk.benchmark_center.gates import evaluate_regression_gate

    blocked = evaluate_regression_gate(
        {"pass_rate": 0.95, "schema_valid_rate": 1.0, "template_reference_accuracy": 1.0, "latency_p95_ms": 100},
        {"pass_rate": 0.80, "schema_valid_rate": 0.90, "template_reference_accuracy": 1.0, "latency_p95_ms": 110},
        {"min_pass_rate": 0.85, "min_schema_valid_rate": 0.95, "max_pass_rate_drop": 0.05, "max_latency_increase_percent": 20},
    )
    review = evaluate_regression_gate(
        {"pass_rate": 0.95, "schema_valid_rate": 1.0, "template_reference_accuracy": 1.0, "latency_p95_ms": 100},
        {"pass_rate": 0.95, "schema_valid_rate": 1.0, "template_reference_accuracy": 1.0, "latency_p95_ms": 125},
        {"min_pass_rate": 0.85, "min_schema_valid_rate": 0.95, "max_pass_rate_drop": 0.05, "max_latency_increase_percent": 20},
    )

    assert blocked["decision"] == "blocked"
    assert any("通过率" in reason for reason in blocked["reasons"])
    assert review["decision"] == "manual_review"
    assert review["deltas"]["latency_p95_ms"] == 25.0


def test_regression_gate_enforces_absolute_quality_minimums():
    from logrisk.benchmark_center.gates import evaluate_regression_gate

    metrics = {
        "pass_rate": 0.7,
        "schema_valid_rate": 0.9,
        "template_reference_accuracy": 0.94,
        "latency_p95_ms": 100,
    }
    result = evaluate_regression_gate(
        metrics,
        metrics,
        {
            "min_pass_rate": 0.8,
            "min_schema_valid_rate": 0.95,
            "min_template_reference_accuracy": 0.95,
        },
    )

    assert result["decision"] == "blocked"
    assert len(result["reasons"]) == 3
