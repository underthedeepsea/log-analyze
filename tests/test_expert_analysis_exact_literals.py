from __future__ import annotations

import copy

import pytest

from logrisk.agentic.errors import AgenticError
from logrisk.agentic.evidence_context import validate_assessment


def context() -> dict:
    return {
        "schema_version": "expert_context_v3", "role_id": "evidence_specialist",
        "context_hash": "locked", "approved_rules": {"items": []}, "upstream_assessments": [],
        "evidence": {
            "entity": {"id": "NODE-AB12", "cluster": "test"},
            "window": {"start": "2026-08-31T07:00:01.123000+00:00"},
            "templates": [
                {"template_hash": "a", "template": "registry.example.invalid returned reset from 110.20.30.40", "count": 11},
                {"template_hash": "b", "template": "Orphaned pod <UUID> volume subpaths remain", "count": 1800},
            ],
        },
    }


def assessment(text: str) -> dict:
    return {
        "schema_version": "expert_analysis_v3", "role_id": "evidence_specialist",
        "context_hash": "locked", "template_refs": ["a"], "rule_refs": [], "upstream_refs": [],
        "analyses": [{"kind": "manual_guidance", "status": "guidance", "fact_refs": ["a"],
                      "text": text, "missing_evidence": [], "manual_checks": ["人工核对对应对象事件；恢复与根因未知。"]}],
        "limitations": ["仅为人工核查建议，实际影响未知。"],
    }


@pytest.mark.parametrize("literal", ["registry.example.invalid", "NODE-AB12", "110.20.30.40", "2026-08-31T07:00:01.123000+00:00", "人工核对日志，恢复未知。"])
def test_exact_literal_or_plain_guidance_is_preserved(literal: str) -> None:
    assert validate_assessment(assessment(literal), context())["analyses"][0]["text"] == literal


@pytest.mark.parametrize("literal", ["example.invalid", "NODE-AB1", "10.20.30.40", "2026-08-31T07:00:01"])
def test_different_literal_is_not_accepted_as_source_substring(literal: str) -> None:
    with pytest.raises(AgenticError, match="事实标识"):
        validate_assessment(assessment(literal), context())


def test_foreign_literal_is_still_rejected() -> None:
    with pytest.raises(AgenticError, match="事实标识"):
        validate_assessment(assessment("outside.invalid"), context())


def test_duplicate_limitations_are_still_rejected() -> None:
    value = assessment("人工核对")
    value["limitations"] *= 2
    with pytest.raises(AgenticError, match="重复"):
        validate_assessment(value, context())


def test_reference_enum_still_rejects_foreign_template() -> None:
    value = assessment("人工核对")
    value["analyses"][0]["fact_refs"] = ["foreign"]
    with pytest.raises(AgenticError):
        validate_assessment(value, context())


def test_multiple_citations_and_source_inputs_are_preserved() -> None:
    ctx = context()
    value = assessment("逐模板核对不同异常；关联与根因未知。")
    value["template_refs"] = ["a", "b"]
    value["analyses"][0]["fact_refs"] = ["a", "b"]
    original = copy.deepcopy((value, ctx))
    validate_assessment(value, ctx)
    assert (value, ctx) == original
