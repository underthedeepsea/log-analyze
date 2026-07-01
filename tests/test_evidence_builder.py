from logrisk.ai_harness.evidence_builder import (
    build_feature_evidence,
    evidence_hash,
    sanitized_templates,
)


def entity():
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "risk_score": 96,
        "risk_level": "critical",
        "affected_entities": ["pay-api-1"],
        "top_templates": [
            {
                "template_hash": "oom-hash",
                "component": "kernel",
                "severity": "ERROR",
                "template": "Memory cgroup out of memory Killed process <*>",
                "category": "node_memory_pressure",
                "count": 3,
                "first_seen": "2026-06-22T10:01:02+08:00",
                "last_seen": "2026-06-22T10:02:02+08:00",
                "feature_hint": "检查内存水位",
                "samples": ["SECRET RAW LOG"],
                "raw_sample": "SECRET RAW SAMPLE",
            },
            "bad-template",
        ],
    }


def test_sanitized_templates_keep_only_allowed_fields():
    templates = sanitized_templates(entity())

    assert templates == [{
        "template_hash": "oom-hash",
        "component": "kernel",
        "severity": "ERROR",
        "template": "Memory cgroup out of memory Killed process <*>",
        "category": "node_memory_pressure",
        "count": 3,
        "first_seen": "2026-06-22T10:01:02+08:00",
        "last_seen": "2026-06-22T10:02:02+08:00",
        "feature_hint": "检查内存水位",
    }]


def test_build_feature_evidence_excludes_raw_log_fields():
    evidence = build_feature_evidence(entity())

    assert set(evidence) == {
        "window_start",
        "window_end",
        "cluster",
        "entity",
        "risk_score",
        "risk_level",
        "affected_entities",
        "templates",
    }
    assert evidence["entity"] == {"type": "node", "id": "node-a"}
    assert "SECRET RAW LOG" not in str(evidence)
    assert "SECRET RAW SAMPLE" not in str(evidence)


def test_evidence_hash_is_stable_and_ordered_by_keys():
    left = {"b": 2, "a": [{"y": 2, "x": 1}]}
    right = {"a": [{"x": 1, "y": 2}], "b": 2}

    assert evidence_hash(left) == evidence_hash(right)
    assert len(evidence_hash(left)) == 64
