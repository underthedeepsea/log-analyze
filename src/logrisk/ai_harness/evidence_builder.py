from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from logrisk.ai_harness.context_budget import EvidenceBudget, EvidenceBuildMeta, estimate_tokens_from_chars


TEMPLATE_FIELDS = (
    "template_hash",
    "template_fingerprint",
    "template_instance_hash",
    "hash_version",
    "component",
    "severity",
    "template",
    "category",
    "count",
    "first_seen",
    "last_seen",
    "feature_hint",
    "semantic_fields",
    "semantic_tags",
    "typed_parameters",
    "semantic_extractor_version",
    "semantic_dictionary_versions",
)
TRUNCATION_MARKER = "[...omitted... ]"
_OPTIONAL_TEMPLATE_FIELDS = (
    "semantic_dictionary_versions",
    "typed_parameters",
    "semantic_fields",
    "risk_semantic",
    "semantic_tags",
    "semantic_extractor_version",
    "feature_hint",
    "last_seen",
    "first_seen",
    "count",
    "category",
    "severity",
    "hash_version",
    "template_instance_hash",
    "template_fingerprint",
)


def sanitized_templates(entity: Dict[str, Any]) -> list[Dict[str, Any]]:
    sanitized = []
    for template in entity.get("top_templates") or []:
        if not isinstance(template, dict):
            continue
        item = {key: template.get(key) for key in TEMPLATE_FIELDS if key in template}
        semantic = template.get("risk_semantic")
        if isinstance(semantic, dict):
            item["risk_semantic"] = {
                key: semantic.get(key)
                for key in (
                    "semantic_rule_id", "semantic_rule_version", "domain", "category", "risk_type",
                    "risk_subtype", "severity", "base_score", "confidence", "semantic_fields", "tags",
                )
                if key in semantic
            }
        sanitized.append(item)
    return sanitized


def _json_chars(value: Dict[str, Any]) -> int:
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _head_tail_excerpt(text: str, limit: int) -> tuple[str, dict[str, Any] | None]:
    if len(text) <= limit:
        return text, None
    if limit <= len(TRUNCATION_MARKER):
        return text[:limit], {
            "strategy": "prefix",
            "original_chars": len(text),
            "omitted_chars": len(text) - limit,
        }
    kept_chars = limit - len(TRUNCATION_MARKER)
    head_chars = kept_chars // 2
    tail_chars = kept_chars - head_chars
    return (
        text[:head_chars] + TRUNCATION_MARKER + text[-tail_chars:],
        {
            "strategy": "head_tail",
            "original_chars": len(text),
            "omitted_chars": len(text) - kept_chars,
        },
    )


def _budget_meta(
    *,
    evidence: Dict[str, Any],
    budget: EvidenceBudget,
    model_profile_id: str | None,
    original_template_count: int,
    original_affected_entity_count: int,
    reasons: list[str],
) -> EvidenceBuildMeta:
    chars = _json_chars(evidence)
    return EvidenceBuildMeta(
        model_profile_id=model_profile_id,
        max_templates=budget.max_templates,
        max_template_chars=budget.max_template_chars,
        max_affected_entities=budget.max_affected_entities,
        max_evidence_chars=budget.max_evidence_chars,
        original_template_count=original_template_count,
        kept_template_count=len(evidence["templates"]),
        original_affected_entity_count=original_affected_entity_count,
        kept_affected_entity_count=len(evidence["affected_entities"]),
        evidence_chars=chars,
        estimated_input_tokens=estimate_tokens_from_chars(chars),
        truncated=bool(reasons),
        truncation_reason=",".join(dict.fromkeys(reasons)) or None,
    )


def _apply_evidence_char_budget(
    evidence: Dict[str, Any],
    template_entries: list[tuple[Dict[str, Any], str | None]],
    max_evidence_chars: int,
    reasons: list[str],
) -> None:
    """Make model-visible Evidence fit exactly, or fail before sending it.

    Lower-priority entity references, extra templates, and template metadata are
    discarded in that order.  The final template excerpt is shortened from its
    full sanitized source while retaining its template hash.  If the required
    envelope itself cannot fit, raising is safer than sending an oversized
    payload and makes the caller choose a usable Profile budget.
    """
    if max_evidence_chars <= 0:
        raise ValueError("max_evidence_chars 必须大于 0")
    if _json_chars(evidence) <= max_evidence_chars:
        return

    affected_entities = evidence["affected_entities"]
    while affected_entities and _json_chars(evidence) > max_evidence_chars:
        affected_entities.pop()
        reasons.append("evidence_char_budget")

    while len(template_entries) > 1 and _json_chars(evidence) > max_evidence_chars:
        template_entries.pop()
        evidence["templates"] = [item for item, _ in template_entries]
        reasons.append("evidence_char_budget")

    for template, _ in template_entries:
        for field in _OPTIONAL_TEMPLATE_FIELDS:
            if _json_chars(evidence) <= max_evidence_chars:
                return
            if field in template:
                template.pop(field)
                reasons.append("evidence_char_budget")

    for template, source_text in template_entries:
        if _json_chars(evidence) <= max_evidence_chars:
            return
        if not isinstance(source_text, str):
            continue
        current_text = template.get("template")
        if not isinstance(current_text, str):
            continue
        for limit in range(min(len(source_text), len(current_text)), -1, -1):
            excerpt, truncation = _head_tail_excerpt(source_text, limit)
            template["template"] = excerpt
            if truncation is None:
                template.pop("truncation", None)
            else:
                template["truncation"] = truncation
            if _json_chars(evidence) <= max_evidence_chars:
                reasons.append("evidence_char_budget")
                return

    if _json_chars(evidence) > max_evidence_chars:
        raise ValueError(
            "max_evidence_chars 无法容纳包含 template_hash 的最小 Evidence envelope"
        )


def build_feature_evidence(
    entity: Dict[str, Any],
    *,
    budget: EvidenceBudget | None = None,
    model_profile_id: str | None = None,
    return_meta: bool = False,
) -> Dict[str, Any] | tuple[Dict[str, Any], EvidenceBuildMeta]:
    sanitized = sanitized_templates(entity)
    template_entries = [
        (dict(template), template.get("template") if isinstance(template.get("template"), str) else None)
        for template in sanitized
    ]
    affected_entities = list(entity.get("affected_entities") or [])
    reasons: list[str] = []
    if budget:
        if len(template_entries) > budget.max_templates:
            reasons.append("template_count_budget")
            template_entries = template_entries[:budget.max_templates]
        for item, source_text in template_entries:
            text = item.get("template")
            if isinstance(text, str) and len(text) > budget.max_template_chars:
                item["template"], item["truncation"] = _head_tail_excerpt(
                    source_text or text, budget.max_template_chars,
                )
                reasons.append("template_char_budget")
        if len(affected_entities) > budget.max_affected_entities:
            reasons.append("affected_entities_budget")
            affected_entities = affected_entities[:budget.max_affected_entities]
    evidence = {
        "window_start": entity.get("window_start"),
        "window_end": entity.get("window_end"),
        "cluster": entity.get("cluster"),
        "entity": {"type": entity.get("entity_type"), "id": entity.get("entity_id")},
        "risk_score": entity.get("risk_score"),
        "risk_level": entity.get("risk_level"),
        "affected_entities": affected_entities,
        "templates": [item for item, _ in template_entries],
    }
    if budget:
        _apply_evidence_char_budget(
            evidence,
            template_entries,
            budget.max_evidence_chars,
            reasons,
        )
    if return_meta:
        effective_budget = budget or EvidenceBudget()
        return evidence, _budget_meta(
            evidence=evidence,
            budget=effective_budget,
            model_profile_id=model_profile_id,
            original_template_count=len(sanitized),
            original_affected_entity_count=len(entity.get("affected_entities") or []),
            reasons=reasons,
        )
    return evidence


def evidence_hash(evidence: Dict[str, Any]) -> str:
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
