import pytest

from logrisk.ai_harness.context_budget import EvidenceBudget
from logrisk.ai_harness.evidence_builder import _json_chars, build_feature_evidence


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


def test_build_feature_evidence_keeps_tail_error_with_explicit_omission_metadata():
    payload = entity()
    payload["top_templates"] = [{
        "template_hash": "tail-error",
        "component": "runtime",
        "template": "normal context " * 20 + "FATAL unauthorized image pull",
        "count": 1,
    }]
    evidence, meta = build_feature_evidence(
        payload,
        budget=EvidenceBudget(max_template_chars=80, max_evidence_chars=2000),
        return_meta=True,
    )

    model_visible = evidence["templates"][0]
    assert "normal context" in model_visible["template"]
    assert "FATAL unauthorized image pull" in model_visible["template"]
    assert "[...omitted... ]" in model_visible["template"]
    assert model_visible["template_hash"] == "tail-error"
    assert model_visible["truncation"] == {"strategy": "head_tail", "original_chars": 329, "omitted_chars": 265}
    assert "template_char_budget" in (meta.truncation_reason or "")


def test_evidence_char_budget_preserves_original_truncation_diagnostics():
    payload = entity()
    payload["top_templates"] = [{
        "template_hash": "tail-error",
        "component": "runtime",
        "template": "normal context " * 20 + "FATAL unauthorized image pull",
        "count": 1,
    }]
    evidence, meta = build_feature_evidence(
        payload,
        budget=EvidenceBudget(max_template_chars=80, max_evidence_chars=450),
        return_meta=True,
    )

    assert meta.evidence_chars <= 450
    assert evidence["templates"][0]["truncation"]["original_chars"] == 329
    assert evidence["templates"][0]["truncation"]["omitted_chars"] > 0


def test_evidence_char_budget_bounds_long_metadata_and_affected_entities():
    payload = entity()
    payload["affected_entities"] = [f"service-{index}-" + "x" * 400 for index in range(4)]
    payload["top_templates"] = [{
        "template_hash": "metadata-heavy-hash",
        "component": "runtime",
        "template": "normal context " * 30 + "FATAL image pull failure",
        "count": 1,
        "semantic_fields": {"explanation": "y" * 400},
        "semantic_tags": ["z" * 200],
        "typed_parameters": [{"name": "argument", "description": "w" * 400}],
        "semantic_dictionary_versions": ["dictionary-" + "v" * 200],
    }]
    budget = EvidenceBudget(
        max_templates=1,
        max_template_chars=120,
        max_affected_entities=4,
        max_evidence_chars=520,
    )

    evidence, meta = build_feature_evidence(payload, budget=budget, return_meta=True)

    assert _json_chars(evidence) <= budget.max_evidence_chars
    assert meta.evidence_chars == _json_chars(evidence)
    assert evidence["templates"][0]["template_hash"] == "metadata-heavy-hash"
    assert "evidence_char_budget" in (meta.truncation_reason or "")


def test_evidence_char_budget_rejects_an_unfit_required_envelope():
    payload = entity()
    payload["entity_id"] = "node-" + "x" * 500

    with pytest.raises(ValueError, match="max_evidence_chars"):
        build_feature_evidence(payload, budget=EvidenceBudget(max_evidence_chars=80))
