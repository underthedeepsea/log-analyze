from __future__ import annotations

from typing import Any


REQUIRED_FEATURE_FIELDS = (
    "feature_type",
    "title",
    "summary",
    "importance",
    "template_hashes",
    "components",
    "tags",
    "selection_reason",
)


def _text(feature: dict[str, Any]) -> str:
    return " ".join(str(feature.get(field) or "") for field in ("title", "summary", "selection_reason"))


def _templates(feature: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in feature.get("source_templates") or [] if isinstance(item, dict)]


def _schema_valid(features: list[dict[str, Any]]) -> bool:
    for feature in features:
        if not all(field in feature for field in REQUIRED_FEATURE_FIELDS):
            return False
        if not isinstance(feature.get("template_hashes"), list) or not feature["template_hashes"]:
            return False
        if not isinstance(feature.get("components"), list) or not feature["components"]:
            return False
        if not isinstance(feature.get("tags"), list) or not feature["tags"] or not all(
            isinstance(tag, str) and tag.strip() for tag in feature["tags"]
        ):
            return False
        if not isinstance(feature.get("selection_reason"), str) or not feature["selection_reason"].strip():
            return False
    return True


def evaluate_case(case: dict[str, Any], features: list[dict[str, Any]], error: str | None = None) -> dict[str, Any]:
    expected = case.get("expected") or {}
    evidence = case.get("evidence") or {"templates": case.get("input_entity", {}).get("top_templates") or []}
    known_hashes = {
        str(template.get("template_hash"))
        for template in (evidence.get("templates") or [])
        if isinstance(template, dict) and template.get("template_hash")
    }
    feature_types = {str(feature.get("feature_type") or "") for feature in features}
    referenced_hashes = {
        str(template_hash)
        for feature in features
        for template_hash in (feature.get("template_hashes") or [])
    }
    template_text = " ".join(str(template.get("template") or "").lower() for feature in features for template in _templates(feature))
    output_text = " ".join(_text(feature) for feature in features)
    forbidden_claims = expected.get("forbidden_claims", expected.get("must_not_claim", []))
    expected_types = expected.get("expected_feature_types", expected.get("must_include_feature_type", []))
    forbidden_hits = [claim for claim in forbidden_claims if claim and claim in output_text]
    missing_types = [item for item in expected_types if item not in feature_types]
    missing_keywords = [item for item in expected.get("must_include_template_keywords", []) if str(item).lower() not in template_text]
    expected_entity_id = expected.get("expected_entity_id")
    expects_features = not expected.get("expect_empty_features", not bool(expected_types or expected.get("must_include_template_keywords")))
    entity_ok = not expected_entity_id or (not expects_features and not features) or any((feature.get("entity") or {}).get("id") == expected_entity_id for feature in features)
    template_reference_ok = (not expects_features and not features) or (bool(features) and referenced_hashes <= known_hashes and bool(referenced_hashes))
    schema_valid = error is None and _schema_valid(features)
    errors = []
    if error:
        errors.append(error)
    if missing_types:
        errors.append("missing feature_type: " + ", ".join(missing_types))
    if missing_keywords:
        errors.append("missing template keyword: " + ", ".join(missing_keywords))
    if not entity_ok:
        errors.append("expected entity_id not found: " + str(expected_entity_id))
    if not template_reference_ok:
        errors.append("template reference mismatch")
    if not schema_valid:
        errors.append("schema invalid")
    if forbidden_hits:
        errors.append("forbidden claim: " + ", ".join(forbidden_hits))
    allowed_components = set(expected.get("allowed_components") or [])
    if allowed_components and any(set(feature.get("components") or []) - allowed_components for feature in features):
        errors.append("component outside allowed set")
    allowed_importance = set(expected.get("allowed_importance") or [])
    if allowed_importance and any(feature.get("importance") not in allowed_importance for feature in features):
        errors.append("importance outside allowed set")
    required_hashes = set(expected.get("must_reference_hashes") or [])
    if required_hashes and not required_hashes.issubset(referenced_hashes):
        errors.append("required template hash missing")
    return {
        "name": case.get("name") or "",
        "passed": not errors,
        "json_valid": error is None,
        "schema_valid": schema_valid,
        "template_reference_ok": template_reference_ok,
        "forbidden_claim_count": len(forbidden_hits),
        "errors": errors,
        "features": features,
    }


def summarize_results(*, run_id: str, prompt_id: str, model: str, case_results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(case_results)
    passed = sum(1 for result in case_results if result.get("passed"))
    json_valid = sum(1 for result in case_results if result.get("json_valid"))
    schema_valid = sum(1 for result in case_results if result.get("schema_valid"))
    template_ok = sum(1 for result in case_results if result.get("template_reference_ok"))
    forbidden = sum(int(result.get("forbidden_claim_count") or 0) for result in case_results)
    return {
        "run_id": run_id,
        "prompt_id": prompt_id,
        "model": model,
        "cases_total": total,
        "cases_passed": passed,
        "pass_rate": round(passed / total, 4) if total else 0.0,
        "json_valid_rate": round(json_valid / total, 4) if total else 0.0,
        "schema_valid_rate": round(schema_valid / total, 4) if total else 0.0,
        "template_reference_accuracy": round(template_ok / total, 4) if total else 0.0,
        "forbidden_claim_count": forbidden,
        "case_results": case_results,
    }
