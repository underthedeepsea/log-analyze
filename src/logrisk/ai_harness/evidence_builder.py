from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from logrisk.ai_harness.context_budget import EvidenceBudget, EvidenceBuildMeta, estimate_tokens_from_chars


TEMPLATE_FIELDS = (
    "template_hash",
    "component",
    "severity",
    "template",
    "category",
    "count",
    "first_seen",
    "last_seen",
    "feature_hint",
)


def sanitized_templates(entity: Dict[str, Any]) -> list[Dict[str, Any]]:
    return [
        {key: template.get(key) for key in TEMPLATE_FIELDS}
        for template in (entity.get("top_templates") or [])
        if isinstance(template, dict)
    ]


def _json_chars(value: Dict[str, Any]) -> int:
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


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


def build_feature_evidence(
    entity: Dict[str, Any],
    *,
    budget: EvidenceBudget | None = None,
    model_profile_id: str | None = None,
    return_meta: bool = False,
) -> Dict[str, Any] | tuple[Dict[str, Any], EvidenceBuildMeta]:
    templates = sanitized_templates(entity)
    affected_entities = entity.get("affected_entities") or []
    reasons: list[str] = []
    if budget:
        if len(templates) > budget.max_templates:
            reasons.append("template_count_budget")
            templates = templates[:budget.max_templates]
        shortened = []
        for template in templates:
            item = dict(template)
            text = item.get("template")
            if isinstance(text, str) and len(text) > budget.max_template_chars:
                item["template"] = text[:budget.max_template_chars]
                reasons.append("template_char_budget")
            shortened.append(item)
        templates = shortened
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
        "templates": templates,
    }
    if budget:
        while len(evidence["templates"]) > 1 and _json_chars(evidence) > budget.max_evidence_chars:
            evidence["templates"] = evidence["templates"][:-1]
            reasons.append("evidence_char_budget")
        if _json_chars(evidence) > budget.max_evidence_chars:
            for template in evidence["templates"]:
                text = template.get("template")
                if isinstance(text, str) and len(text) > 20:
                    template["template"] = text[:20]
                    reasons.append("evidence_char_budget")
    if return_meta:
        effective_budget = budget or EvidenceBudget()
        return evidence, _budget_meta(
            evidence=evidence,
            budget=effective_budget,
            model_profile_id=model_profile_id,
            original_template_count=len(sanitized_templates(entity)),
            original_affected_entity_count=len(entity.get("affected_entities") or []),
            reasons=reasons,
        )
    return evidence


def evidence_hash(evidence: Dict[str, Any]) -> str:
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
