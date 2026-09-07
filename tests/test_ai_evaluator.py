from logrisk.ai_harness.evaluator import evaluate_feature_output
import pytest


def evidence():
    return {
        "entity": {"type": "node", "id": "node-a"},
        "affected_entities": ["pod/pay-api"],
        "templates": [
            {
                "template_hash": "oom-hash",
                "component": "kernel",
                "template": "Memory cgroup out of memory Killed process <*>",
            }
        ],
    }


def feature(**overrides):
    value = {
        "feature_type": "node_memory_pressure",
        "title": "节点内存压力",
        "summary": "OOM 模板在窗口内重复出现",
        "importance": "critical",
        "template_hashes": ["oom-hash"],
        "components": ["kernel"],
    }
    value.update(overrides)
    return value


def test_valid_feature_passes_quality_gate():
    result = evaluate_feature_output(feature=feature(), entity={"entity_id": "node-a"}, evidence=evidence())

    assert result["passed"] is True
    assert result["errors"] == []
    assert result["score"] == 1.0
    assert any(item["rule_id"] == "template_hash_reference" for item in result["rule_results"])


def test_unknown_references_and_rca_claims_are_blocked():
    result = evaluate_feature_output(
        feature=feature(
            summary="根因是内存不足，建议重启并扩容",
            template_hashes=["fake-hash"],
            components=["etcd"],
        ),
        entity={"entity_id": "node-a"},
        evidence=evidence(),
    )

    assert result["passed"] is False
    assert result["score"] == 0.0
    assert any("fake-hash" in error for error in result["errors"])
    assert any("etcd" in error for error in result["errors"])
    assert any("建议重启" in error for error in result["errors"])


@pytest.mark.parametrize("field", ("title", "summary", "selection_reason"))
def test_forbidden_claims_are_blocked_in_all_model_text_fields(field):
    result = evaluate_feature_output(
        feature=feature(**{field: "根因是内存不足"}),
        entity={"entity_id": "node-a"},
        evidence=evidence(),
    )

    assert result["passed"] is False
    assert any("根因是" in error for error in result["errors"])


def test_final_candidate_reports_incomplete_semantic_coverage_for_review():
    result = evaluate_feature_output(
        feature=feature(
            feature_type="mixed_runtime_failure",
            template_hashes=["oom-hash", "opaque-hash"],
            components=["kernel"],
            source_templates=[
                evidence()["templates"][0],
                {
                    "template_hash": "opaque-hash",
                    "component": "kernel",
                    "template": "opaque vendor cleanup failure",
                },
            ],
        ),
        entity={"entity_id": "node-a"},
        evidence={**evidence(), "templates": [
            evidence()["templates"][0],
            {"template_hash": "opaque-hash", "component": "kernel", "template": "opaque vendor cleanup failure"},
        ]},
        final_candidate=True,
    )

    assert result["passed"] is True
    assert any(item["rule_id"] == "final_semantic_evidence" and item["status"] == "passed" for item in result["rule_results"])
    assert any("待人工复核" in warning for warning in result["warnings"])


def test_final_candidate_cannot_claim_semantic_safety_without_complete_evidence():
    result = evaluate_feature_output(
        feature=feature(
            feature_type="mixed_runtime_failure",
            template_hashes=["oom-hash", "opaque-hash"],
            components=["kernel"],
            source_templates=[
                evidence()["templates"][0],
                {"template_hash": "opaque-hash", "component": "kernel", "template": "opaque vendor cleanup failure"},
            ],
            problem_resolution={"semantic_safe": True},
            match_mode="semantic",
        ),
        entity={"entity_id": "node-a"},
        evidence={**evidence(), "templates": [
            evidence()["templates"][0],
            {"template_hash": "opaque-hash", "component": "kernel", "template": "opaque vendor cleanup failure"},
        ]},
        final_candidate=True,
    )

    assert result["passed"] is False
    assert any(item["rule_id"] == "final_semantic_evidence" and item["status"] == "failed" for item in result["rule_results"])
