from logrisk.ai_harness.context_budget import EvidenceBudget
from logrisk.ai_harness.evidence_builder import build_feature_evidence


def entity():
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "risk_score": 96,
        "risk_level": "critical",
        "affected_entities": ["svc-a", "svc-b", "svc-c"],
        "top_templates": [
            {"template_hash": "h1", "component": "kernel", "template": "A" * 20, "count": 9},
            {"template_hash": "h2", "component": "app", "template": "B" * 20, "count": 3},
            {"template_hash": "h3", "component": "db", "template": "C" * 20, "count": 1},
        ],
    }


def test_build_feature_evidence_applies_budget_and_returns_meta():
    budget = EvidenceBudget(
        max_templates=2,
        max_template_chars=5,
        max_affected_entities=1,
        max_evidence_chars=2000,
    )

    evidence, meta = build_feature_evidence(
        entity(),
        budget=budget,
        model_profile_id="qwen3_1_7b_fast",
        return_meta=True,
    )

    assert [item["template_hash"] for item in evidence["templates"]] == ["h1", "h2"]
    assert evidence["templates"][0]["template"] == "AAAAA"
    assert evidence["affected_entities"] == ["svc-a"]
    assert meta.model_profile_id == "qwen3_1_7b_fast"
    assert meta.original_template_count == 3
    assert meta.kept_template_count == 2
    assert meta.original_affected_entity_count == 3
    assert meta.kept_affected_entity_count == 1
    assert meta.truncated is True
    assert "template_count_budget" in (meta.truncation_reason or "")


def test_build_feature_evidence_old_call_stays_compatible():
    evidence = build_feature_evidence(entity())

    assert isinstance(evidence, dict)
    assert len(evidence["templates"]) == 3
