from __future__ import annotations

import re
from typing import Any


IMPORTANCE_LEVELS = {"critical", "high", "medium", "low"}
FORBIDDEN_CLAIMS = ("根因是", "建议重启", "应该扩容", "修复方法", "建议扩容", "处置建议", "影响范围")
FEATURE_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]{2,64}$")


def _rule(rule_id: str, rule_name: str, passed: bool, message: str = "") -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "rule_name": rule_name,
        "status": "passed" if passed else "failed",
        "message": message,
    }


def evaluate_feature_output(*, feature: dict, entity: dict, evidence: dict) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    rule_results: list[dict[str, Any]] = []
    templates = evidence.get("templates") if isinstance(evidence, dict) else []
    template_hashes = {str(item.get("template_hash")) for item in templates if isinstance(item, dict) and item.get("template_hash")}
    components = {str(item.get("component")) for item in templates if isinstance(item, dict) and item.get("component")}
    known_entities = {
        str((evidence.get("entity") or {}).get("id") or ""),
        str(entity.get("entity_id") or ""),
        *[str(item) for item in evidence.get("affected_entities", [])],
    }

    missing_hashes = sorted(set(feature.get("template_hashes") or []) - template_hashes)
    if missing_hashes:
        errors.append("template_hash 不存在于 evidence.templates: " + ", ".join(missing_hashes))
    rule_results.append(_rule("template_hash_reference", "模板 Hash 引用", not missing_hashes, errors[-1] if missing_hashes else ""))

    missing_components = sorted(set(feature.get("components") or []) - components)
    if missing_components:
        errors.append("component 不存在于 evidence.templates: " + ", ".join(missing_components))
    rule_results.append(_rule("component_reference", "组件引用", not missing_components, errors[-1] if missing_components else ""))

    importance_ok = feature.get("importance") in IMPORTANCE_LEVELS
    if not importance_ok:
        errors.append("importance 不在允许范围: critical/high/medium/low")
    rule_results.append(_rule("importance_allowed", "重要性枚举", importance_ok, "" if importance_ok else errors[-1]))

    text_ok = bool(str(feature.get("title") or "").strip()) and bool(str(feature.get("summary") or "").strip())
    if not text_ok:
        errors.append("title 和 summary 不能为空")
    rule_results.append(_rule("text_required", "标题摘要必填", text_ok, "" if text_ok else errors[-1]))

    summary = str(feature.get("summary") or "")
    forbidden = [word for word in FORBIDDEN_CLAIMS if word in summary]
    if forbidden:
        errors.append("summary 包含禁止表达: " + ", ".join(forbidden))
    rule_results.append(_rule("forbidden_claim", "禁止 RCA/处置建议", not forbidden, "" if not forbidden else errors[-1]))

    entity_text = " ".join(str(feature.get(field) or "") for field in ("title", "summary", "selection_reason"))
    unknown_entities = sorted(item for item in re.findall(r"\b(?:node|pod)[-/][A-Za-z0-9_.-]+", entity_text) if item not in known_entities)
    if unknown_entities:
        errors.append("引用不存在的 entity: " + ", ".join(unknown_entities))
    rule_results.append(_rule("entity_reference", "实体引用", not unknown_entities, "" if not unknown_entities else errors[-1]))

    feature_type_ok = bool(FEATURE_TYPE_RE.fullmatch(str(feature.get("feature_type") or "")))
    if not feature_type_ok:
        errors.append("feature_type 格式无效")
    rule_results.append(_rule("feature_type_format", "特征类型格式", feature_type_ok, "" if feature_type_ok else errors[-1]))

    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "score": 1.0 if not errors else 0.0,
        "rule_results": rule_results,
    }
