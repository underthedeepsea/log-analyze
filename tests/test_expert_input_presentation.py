from __future__ import annotations

import copy
import pytest
from logrisk.agentic.evidence_context import evidence_facts, expert_goal, expert_input_summary
from logrisk.agentic.errors import AgenticError


def summary():
    evidence = {"entity": {"id": "host-a", "cluster": "c"}, "window": {"start": "2026-08-31T07:00:00Z"}, "templates": [
        {"template_hash": "orphan", "template": "Orphaned pod <UUID>", "count": 1800},
        {"template_hash": "backoff", "template": "ImagePullBackOff registry.example", "count": 265, "risk_semantic": {"risk_type": "backoff"}},
        {"template_hash": "pull", "template": "ErrImagePull EOF", "count": 1, "risk_semantic": {"risk_type": "pull"}},
        {"template_hash": "pull-reset", "template": "ErrImagePull reset", "count": 11, "risk_semantic": {"risk_type": "pull"}},
    ]}
    return {"expert_context": {"schema_version": "expert_context_v3", "role_id": "evidence_specialist", "scope": {"source_job_id": "j", "entity_id": "host-a", "cluster": "c"}, "context_hash": "locked-original", "evidence": evidence, "facts": evidence_facts(evidence)}}


def test_lossless_groups_and_one_count_source():
    original = summary(); before = copy.deepcopy(original)
    result = expert_input_summary(original); ctx = result["expert_context"]
    assert original == before
    assert ctx["evidence"] == original["expert_context"]["evidence"]
    assert ctx["context_hash"] == "locked-original" and "facts" not in ctx
    groups = ctx["evidence_groups"]
    refs = [ref for group in groups for ref in group["template_refs"]]
    assert len(refs) == len(set(refs)) == 4
    assert set(refs) == set(ctx["object_evidence"])
    assert next(g["template_refs"] for g in groups if g["anomaly_type"] == "pull") == ["pull", "pull-reset"]
    assert all(x["proven_object_count"] == "unknown" and x["object_ids"] == [] for x in ctx["object_evidence"].values())
    assert "实际根因、影响及恢复未知" in ctx["presentation_boundary"]


@pytest.mark.parametrize("field,value", [("node", "host-b"), ("cluster", "other"), ("source_job_id", "other")])
def test_cross_scope_material_rejected(field, value):
    value_in = summary(); value_in["expert_context"]["evidence"]["templates"][0][field] = value
    with pytest.raises(AgenticError): expert_input_summary(value_in)


def test_changed_count_is_not_silently_repaired():
    value = summary(); value["expert_context"]["facts"][0]["log_occurrence_count"] = 11
    with pytest.raises(AgenticError): expert_input_summary(value)


def test_typed_object_proof_preserved():
    value = summary(); item = value["expert_context"]["evidence"]["templates"][0]
    uid = "12345678-1234-1234-1234-123456789abc"
    item.update(semantic_fields={"pod_uid": uid}, semantic_dictionary_versions={"k": {"version": 1}}, typed_parameters=[{"field": "pod_uid", "dictionary_id": "k", "rule_id": "typed", "value": uid}])
    value["expert_context"]["facts"] = evidence_facts(value["expert_context"]["evidence"])
    assert expert_input_summary(value)["expert_context"]["object_evidence"]["orphan"] == {"proven_object_count": 1, "object_ids": [uid]}


def test_role_goals_and_generic_compatibility():
    assert "人工核查" in expert_goal("evidence_specialist", "提取可审批日志特征")
    assert "批准规则" in expert_goal("rule_specialist")
    assert "补充目标：用户指定范围" in expert_goal("evidence_specialist", "用户指定范围")
    assert expert_goal("feature_specialist") == expert_goal("feature_specialist", expert_goal("feature_specialist"))
    value = {"entity": {"id": "host-a"}}
    assert expert_input_summary(value) == value
