from __future__ import annotations

import json
from pathlib import Path

from logrisk.approval_dedup import approval_identity
from logrisk.problem_resolver import resolve_problem


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "approval_semantic_regression" / "cases.json"


def test_sanitized_approval_regression_corpus_preserves_expected_semantics():
    corpus = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    assert corpus["schema_version"] == "approval_semantic_regression_v1"
    assert len(corpus["cases"]) >= 16

    for case in corpus["cases"]:
        feature = case["feature"]
        expected = case["expected"]
        resolution = resolve_problem(feature)
        identity = approval_identity(feature)

        assert resolution.problem_code == expected["problem_code"]
        assert resolution.semantic_safe is expected["semantic_safe"]
        assert resolution.ambiguity is expected["ambiguity"]
        assert resolution.subtype == expected["subtype"], case["case_id"]
        assert identity["subtype"] == expected["subtype"], case["case_id"]
        assert identity["match_mode"] == ("semantic" if expected["semantic_safe"] else "template_set")
        if expected["problem_code"] is None:
            assert identity["problem_code"].startswith("logrisk.")


def test_regression_corpus_contains_no_raw_log_fields():
    corpus = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    forbidden = {"raw", "raw_log", "raw_logs", "samples", "raw_sample", "raw_samples"}

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                assert key.lower() not in forbidden
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)

    list(walk(corpus))
