from logrisk.ai_harness.evaluator import evaluate_feature_output


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
