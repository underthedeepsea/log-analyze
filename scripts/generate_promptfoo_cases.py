from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CASE_DIR = ROOT / "eval_cases" / "canonical"
OUTPUT = ROOT / "eval_cases" / "promptfoo" / "generated_cases.json"


def normalized(case: dict) -> dict:
    entity = case["input_entity"]
    evidence = case.get("evidence") or {"templates": entity.get("top_templates") or []}
    expected = case.get("expected") or {}
    feature_types = expected.get("expected_feature_types", expected.get("must_include_feature_type", []))
    forbidden = expected.get("forbidden_claims", expected.get("must_not_claim", []))
    hashes = expected.get("must_reference_hashes") or [
        item["template_hash"] for item in evidence["templates"] if item.get("template_hash")
    ]
    components = expected.get("allowed_components") or sorted({
        str(item["component"]) for item in evidence["templates"] if item.get("component")
    })
    canonical_expected = {
        "expected_feature_types": feature_types,
        "expect_empty_features": expected.get("expect_empty_features", not feature_types),
        "allowed_importance": expected.get("allowed_importance", ["low", "medium", "high", "critical"]),
        "allowed_components": components,
        "must_reference_hashes": hashes if feature_types else [],
        "forbidden_claims": forbidden,
    }
    return {
        "case_id": case.get("case_id") or case.get("name"),
        "description": case.get("description") or case.get("name"),
        "input_entity": entity,
        "evidence": evidence,
        "expected": canonical_expected,
    }


def generate() -> list[dict]:
    cases = [normalized(json.loads(path.read_text(encoding="utf-8"))) for path in sorted(CASE_DIR.glob("*.json"))]
    output = []
    for case in cases:
        evidence = dict(case["evidence"])
        evidence.update({
            key: value
            for key, value in case["input_entity"].items()
            if key != "top_templates"
        })
        output.append({
            "description": case["description"],
            "vars": {
                "case_id": case["case_id"],
                "evidence_json": json.dumps(evidence, ensure_ascii=False, separators=(",", ":")),
                "expected_json": json.dumps(case["expected"], ensure_ascii=False, separators=(",", ":")),
            },
        })
    return output


if __name__ == "__main__":
    OUTPUT.write_text(json.dumps(generate(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
