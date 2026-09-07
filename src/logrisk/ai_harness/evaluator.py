from __future__ import annotations

import re
from typing import Any

from logrisk.problem_resolver import resolve_problem


IMPORTANCE_LEVELS = {"critical", "high", "medium", "low"}
FORBIDDEN_CLAIMS = (
    "根因是", "原因是", "可能由于", "建议重启", "应该扩容", "建议扩容",
    "应该检查", "修复方法", "处理建议", "处置建议", "影响范围",
)
FEATURE_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]{2,64}$")


def _rule(rule_id: str, rule_name: str, passed: bool, message: str = "") -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "rule_name": rule_name,
        "status": "passed" if passed else "failed",
        "message": message,
    }


def evaluate_feature_output(
    *, feature: dict, entity: dict, evidence: dict, final_candidate: bool = False,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    rule_results: list[dict[str, Any]] = []
    templates = evidence.get("templates") if isinstance(evidence, dict) else []
    template_hashes = {
        str(value)
        for item in templates
        if isinstance(item, dict)
        for value in (item.get("template_hash"), item.get("template_fingerprint"))
        if value
    }
    components = {str(item.get("component")) for item in templates if isinstance(item, dict) and item.get("component")}
    known_entities = {
        str((evidence.get("entity") or {}).get("id") or ""),
        str(entity.get("entity_id") or ""),
        *[str(item) for item in evidence.get("affected_entities", [])],
    }

    if not final_candidate:
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

        forbidden = [
            f"{field}:{word}"
            for field in ("title", "summary", "selection_reason")
            for word in FORBIDDEN_CLAIMS
            if word in str(feature.get(field) or "")
        ]
        if forbidden:
            errors.append("模型文本字段包含禁止表达: " + ", ".join(forbidden))
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
    else:
        selected_templates = [
            item for item in templates
            if isinstance(item, dict)
            and ({str(item.get("template_hash") or ""), str(item.get("template_fingerprint") or "")}
                 & set(feature.get("template_hashes") or []))
        ]
        selected_components = {
            str(item.get("component")) for item in selected_templates if item.get("component")
        }
        unselected_components = sorted(set(feature.get("components") or []) - selected_components)
        if unselected_components:
            errors.append("component 不属于所选模板: " + ", ".join(unselected_components))
        rule_results.append(_rule(
            "selected_component_reference", "所选模板组件引用", not unselected_components,
            "" if not unselected_components else errors[-1],
        ))

        resolution = resolve_problem(feature, entity)
        claimed_safe = (
            feature.get("match_mode") == "semantic"
            or bool((feature.get("problem_resolution") or {}).get("semantic_safe"))
        )
        semantic_ok = not claimed_safe or resolution.semantic_safe
        if not semantic_ok:
            errors.append("最终候选声称语义安全，但所选模板证据不完整或冲突")
        elif not resolution.semantic_safe:
            diagnostics = [
                *resolution.missing_selected_ids,
                *resolution.unresolved_selected_ids,
            ]
            suffix = f"（模板: {', '.join(diagnostics)}）" if diagnostics else ""
            warnings.append(f"最终候选的所选模板证据不完整，保留为待人工复核。{suffix}")
        rule_results.append(_rule(
            "final_semantic_evidence", "最终候选语义证据", semantic_ok,
            "" if semantic_ok else errors[-1],
        ))

    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "score": 1.0 if not errors else 0.0,
        "rule_results": rule_results,
    }
