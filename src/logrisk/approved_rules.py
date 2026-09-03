from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict

from logrisk.approval_dedup import (
    approval_identity,
    build_approval_key,
    derive_problem_code,
    normalize_problem_code,
    normalize_feature_type,
    semantic_resolver_enabled,
)
from logrisk.problem_resolver import resolve_problem


class ApprovedRuleError(RuntimeError):
    """Raised when the approved-rule state cannot be read or written safely."""


class ApprovedRuleIntegrityError(ApprovedRuleError):
    """Raised when a malformed rule occupies an approval identity."""

    def __init__(
        self,
        message: str = "存在损坏规则占用了当前审批 identity，需先完成规则治理修复",
        *,
        rule_id: Any = None,
        approval_key: Any = None,
        signature: Any = None,
        integrity_errors: tuple[str, ...] = (),
        code: str = "malformed_rule_identity_conflict",
        status_code: int = 409,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.rule_id = str(rule_id or "") or None
        self.approval_key = str(approval_key or "") or None
        self.signature = str(signature or "") or None
        self.integrity_errors = tuple(integrity_errors)


class RuleFormat(str, Enum):
    LEGACY_V1 = "legacy_v1"
    VALID_V2 = "valid_v2"
    MALFORMED_V2 = "malformed_v2"


class RuleNormalizationSource(str, Enum):
    DATABASE_MIGRATION = "database_migration"
    LEGACY_IMPORT = "legacy_import"
    LEGACY_FILE = "legacy_file"


@dataclass(frozen=True)
class RuleClassification:
    kind: RuleFormat
    integrity_errors: tuple[str, ...] = ()


_PROCESS_LOCK = threading.RLock()

_LEGACY_SCHEMA_VERSION = "approved_rule_v1"
_V2_SCHEMA_VERSION = "approved_rule_v2"
_RULE_INTEGRITY_ERRORS_KEY = "__logrisk_rule_integrity_errors"
_RULE_STATUSES = {"active", "disabled", "under_review", "deprecated", "archived"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _next_review(timestamp: str) -> str:
    return (datetime.fromisoformat(timestamp) + timedelta(days=30)).isoformat()


def _template_pairs(items: list[Dict[str, Any]]) -> list[Dict[str, str]]:
    pairs: dict[tuple[str, str], Dict[str, str]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        fingerprint = str(item.get("template_fingerprint") or "").strip()
        legacy_hash = str(item.get("template_hash") or "").strip()
        identity = fingerprint or legacy_hash
        if not identity:
            continue
        category = str(item.get("category") or "").strip()
        signature = {"category": category}
        if fingerprint:
            signature["template_fingerprint"] = fingerprint
        if legacy_hash:
            signature["template_hash"] = legacy_hash
        pairs[(identity, category)] = signature
    return [pairs[key] for key in sorted(pairs)]


def _identity_pair(item: Dict[str, Any]) -> tuple[str, str]:
    identity = str(item.get("template_fingerprint") or item.get("template_hash") or "").strip()
    return identity, str(item.get("category") or "").strip()


def rule_signature(feature_type: str, sources: list[Dict[str, Any]]) -> str:
    if not str(feature_type or "").strip():
        raise ApprovedRuleError("批准特征缺少 feature_type 或模板 Hash，无法生成规则")
    normalized_type = normalize_feature_type(feature_type)
    pairs = _template_pairs(sources)
    if not pairs:
        raise ApprovedRuleError("批准特征缺少 feature_type 或模板 Hash，无法生成规则")
    raw = json.dumps([normalized_type, pairs], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _approval_rule_signature(base_signature: str, approval_key: str) -> str:
    raw = json.dumps([base_signature, approval_key], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _lineage(feature: Dict[str, Any]) -> Dict[str, str]:
    aliases = {
        "evidence_hash": ("evidence_hash", "input_evidence_hash"),
    }
    fields = ("job_id", "candidate_id", "trace_id", "prompt_id", "prompt_hash", "provider", "model", "evidence_hash")
    lineage = {}
    for field in fields:
        keys = aliases.get(field, (field,))
        value = next((feature.get(key) for key in keys if feature.get(key)), None)
        if value:
            lineage[field] = str(value)
    return lineage


def _versioned_fields(rule: Dict[str, Any]) -> Dict[str, Any]:
    fields = (
        "signature",
        "feature_type",
        "title",
        "summary",
        "importance",
        "tags",
        "selection_reason",
        "components",
        "template_signatures",
        "problem_code",
        "approval_key",
        "anchor_signatures",
        "supporting_signatures",
        "match_mode",
        "canonical_approval_key",
        "predecessor_rule_id",
        "lineage",
    )
    return {field: copy.deepcopy(rule.get(field)) for field in fields}


def _preferred_rule(rules: list[Dict[str, Any]]) -> Dict[str, Any] | None:
    if not rules:
        return None
    latest = max(str(rule.get("updated_at") or "") for rule in rules)
    return min(
        (rule for rule in rules if str(rule.get("updated_at") or "") == latest),
        key=lambda rule: str(rule.get("rule_id") or ""),
    )


def _is_active(rule: Dict[str, Any]) -> bool:
    return str(rule.get("status") or "active") == "active"


def _rule_canonical_approval_key(rule: Dict[str, Any]) -> str:
    problem_code = normalize_problem_code(rule.get("problem_code")) or derive_problem_code(rule)
    return build_approval_key(
        rule.get("feature_type"),
        problem_code,
        rule.get("components") or rule.get("component_scope") or [],
        rule.get("anchor_signatures") or [],
    )


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _has_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return value is not None and value != [] and value != {} and value != ()


def has_v2_identity_markers(rule: dict[str, Any]) -> bool:
    """Return whether a rule carries any identity field introduced by V2."""

    if not isinstance(rule, dict):
        return False
    for field in (
        "approval_key",
        "canonical_approval_key",
        "problem_code",
        "problemCode",
        "match_mode",
        "risk_type",
        "cause",
        "template_storage_key",
        "strict_storage_key",
    ):
        if _has_value(rule.get(field)):
            return True
    for field in (
        "anchor_signatures",
        "supporting_signatures",
        "component_scope",
    ):
        if field in rule and rule[field] is not None:
            return True
    for field in ("risk_semantic", "semantic_fields"):
        if _has_value(rule.get(field)):
            return True
    return False


def _key_matches_expected(stored: str, expected: str) -> bool:
    return stored == expected or stored.startswith(f"{expected}:replacement-")


def _valid_text_list(value: Any) -> bool:
    return isinstance(value, list) and all(_nonempty_text(item) for item in value)


def _valid_template_list(value: Any) -> bool:
    return isinstance(value, list) and all(
        isinstance(item, dict)
        and _nonempty_text(item.get("template_fingerprint") or item.get("template_hash"))
        and ("category" not in item or isinstance(item.get("category"), str))
        for item in value
    )


def _rule_identity(rule: Dict[str, Any]) -> Dict[str, Any]:
    problem_code = normalize_problem_code(rule.get("problem_code"))
    components = _legacy_components(rule)
    anchors = {
        str(item).strip()
        for item in (rule.get("anchor_signatures") or [])
        if str(item).strip()
    }
    return {
        "problem_code": problem_code,
        "approval_key": build_approval_key(
            rule.get("feature_type"), problem_code,
            sorted(components), sorted(anchors),
        ),
        "component_scope": sorted(components),
        "anchor_signatures": sorted(anchors),
        "match_mode": rule.get("match_mode"),
    }


def validate_v2_rule(rule: dict[str, Any]) -> tuple[str, ...]:
    """Validate the persisted V2 identity without repairing any field."""

    errors: list[str] = []
    if not isinstance(rule, dict):
        return ("rule_not_object",)
    if rule.get("schema_version") != _V2_SCHEMA_VERSION:
        errors.append("schema_version_invalid")
    for field in ("rule_id", "signature", "feature_type", "approval_key"):
        if not _nonempty_text(rule.get(field)):
            errors.append(f"{field}_missing")
    if normalize_feature_type(rule.get("feature_type")) == "unknown_feature":
        errors.append("feature_type_invalid")
    for field in ("components", "component_scope", "anchor_signatures"):
        if field in rule and rule[field] is not None and not _valid_text_list(rule[field]):
            errors.append(f"{field}_invalid")
    for field in ("source_templates", "template_signatures"):
        if field in rule and rule[field] is not None and not _valid_template_list(rule[field]):
            errors.append(f"{field}_invalid")
    if (
        "supporting_signatures" in rule
        and rule["supporting_signatures"] is not None
        and not isinstance(rule["supporting_signatures"], list)
    ):
        errors.append("supporting_signatures_invalid")
    status = str(rule.get("status") or "")
    if status not in _RULE_STATUSES:
        errors.append("status_invalid")
    current_version = rule.get("current_version")
    if isinstance(current_version, bool) or not isinstance(current_version, int) or current_version < 1:
        errors.append("current_version_invalid")
    match_mode = rule.get("match_mode")
    if match_mode not in {"semantic", "template_set"}:
        errors.append("match_mode_missing_or_invalid")
    problem_code = normalize_problem_code(rule.get("problem_code"))
    if not problem_code:
        errors.append("problem_code_missing")

    if match_mode == "semantic":
        if not resolve_problem(rule).semantic_safe:
            errors.append("semantic_problem_code_invalid")
        expected = build_approval_key(
            rule.get("feature_type"), problem_code,
            rule.get("components") or rule.get("component_scope") or [],
            rule.get("anchor_signatures") or [],
        )
        stored = str(rule.get("approval_key") or "").strip()
        if not _key_matches_expected(stored, expected):
            errors.append("approval_key_mismatch")
        canonical = str(rule.get("canonical_approval_key") or "").strip()
        if canonical and canonical != expected:
            errors.append("canonical_approval_key_mismatch")
    elif match_mode == "template_set":
        if not _legacy_components(rule):
            errors.append("template_set_components_missing")
        if "anchor_signatures" not in rule or not isinstance(rule.get("anchor_signatures"), list):
            errors.append("template_set_anchors_missing")
        if not _v2_template_pairs(rule):
            errors.append("template_set_templates_missing")
        identity = _rule_identity(rule)
        expected = _template_set_storage_key(rule, identity)
        stored = str(rule.get("approval_key") or "").strip()
        base_expected = identity["approval_key"]
        if not (_key_matches_expected(stored, expected) or _key_matches_expected(stored, base_expected)):
            errors.append("approval_key_mismatch")
        for field in ("template_storage_key", "strict_storage_key"):
            if field in rule and rule[field] is not None:
                if (
                    not _nonempty_text(rule[field])
                    or not _key_matches_expected(str(rule[field]).strip(), expected)
                ):
                    errors.append(f"{field}_mismatch")
        canonical = str(rule.get("canonical_approval_key") or "").strip()
        resolution = resolve_problem(rule)
        if resolution.semantic_safe:
            if canonical != base_expected:
                errors.append("canonical_approval_key_mismatch")
        elif canonical:
            errors.append("canonical_approval_key_unexpected")
    return tuple(dict.fromkeys(errors))


def _projection_errors(rule: dict[str, Any], projection: dict[str, Any]) -> tuple[str, ...]:
    errors: list[str] = []
    json_schema = str(rule.get("schema_version") or "").strip()
    db_schema = str(projection.get("schema_version") or "").strip()
    if not json_schema and db_schema == _LEGACY_SCHEMA_VERSION:
        if has_v2_identity_markers(rule):
            errors.append("unversioned_v2_identity_markers")
        json_schema = _LEGACY_SCHEMA_VERSION
    elif json_schema != db_schema:
        errors.append("schema_version_projection_mismatch")

    for field in ("rule_id", "signature", "feature_type"):
        json_value = str(rule.get(field) or "").strip()
        db_value = str(projection.get(field) or "").strip()
        if json_value and db_value and json_value != db_value:
            errors.append(f"{field}_projection_mismatch")

    if db_schema == _LEGACY_SCHEMA_VERSION:
        for field in ("problem_code", "approval_key"):
            if projection.get(field) is not None:
                errors.append(f"v1_{field}_projection_not_null")
    elif db_schema == _V2_SCHEMA_VERSION:
        for field in ("problem_code", "approval_key"):
            json_value = str(rule.get(field) or "").strip()
            db_value = str(projection.get(field) or "").strip()
            if json_value != db_value:
                errors.append(f"{field}_projection_mismatch")
    return tuple(dict.fromkeys(errors))


def classify_rule(
    rule: dict[str, Any],
    *,
    persisted_projection: dict[str, Any] | None = None,
) -> RuleClassification:
    """Classify a rule without treating malformed V2 data as legacy V1."""

    if not isinstance(rule, dict):
        return RuleClassification(RuleFormat.MALFORMED_V2, ("rule_not_object",))
    errors = list(rule.get(_RULE_INTEGRITY_ERRORS_KEY) or ())
    candidate = rule
    if persisted_projection is not None:
        errors.extend(_projection_errors(rule, persisted_projection))
        if not str(rule.get("schema_version") or "").strip() and str(persisted_projection.get("schema_version") or "") == _LEGACY_SCHEMA_VERSION:
            candidate = {**rule, "schema_version": _LEGACY_SCHEMA_VERSION}
    version = str(candidate.get("schema_version") or "").strip()
    if version == _LEGACY_SCHEMA_VERSION:
        if errors:
            return RuleClassification(RuleFormat.MALFORMED_V2, tuple(dict.fromkeys(errors)))
        return RuleClassification(RuleFormat.LEGACY_V1)
    if version == _V2_SCHEMA_VERSION:
        errors.extend(validate_v2_rule(candidate))
    else:
        errors.append("schema_version_missing_or_unknown")
    if version == _V2_SCHEMA_VERSION and not errors:
        return RuleClassification(RuleFormat.VALID_V2)
    return RuleClassification(RuleFormat.MALFORMED_V2, tuple(dict.fromkeys(errors)))


def normalize_legacy_rule_version(
    rule: dict[str, Any],
    *,
    source: RuleNormalizationSource,
) -> dict[str, Any]:
    """Normalize an unversioned rule only at an explicitly trusted legacy boundary."""

    try:
        RuleNormalizationSource(source)
    except (TypeError, ValueError) as exc:
        raise ValueError("不支持的 legacy 规则归一化来源") from exc
    if not isinstance(rule, dict):
        raise ApprovedRuleError("legacy 规则必须是 object")
    normalized = copy.deepcopy(rule)
    version = str(normalized.get("schema_version") or "").strip()
    if version == _LEGACY_SCHEMA_VERSION:
        return normalized
    if version == _V2_SCHEMA_VERSION:
        classification = classify_rule(normalized)
        if classification.kind == RuleFormat.VALID_V2:
            return normalized
        raise ApprovedRuleIntegrityError(
            "legacy 规则声明为 V2 但内容损坏",
            rule_id=normalized.get("rule_id"),
            approval_key=normalized.get("approval_key"),
            signature=normalized.get("signature"),
            integrity_errors=classification.integrity_errors,
            code="malformed_rule_version",
            status_code=422,
        )
    if version or has_v2_identity_markers(normalized):
        raise ApprovedRuleIntegrityError(
            "legacy 规则版本或 identity 无法安全归一化",
            rule_id=normalized.get("rule_id"),
            approval_key=normalized.get("approval_key"),
            signature=normalized.get("signature"),
            integrity_errors=("legacy_normalization_not_safe",),
            code="malformed_rule_version",
            status_code=422,
        )
    normalized["schema_version"] = _LEGACY_SCHEMA_VERSION
    return normalized


def _is_legacy_feature(feature: Dict[str, Any], identity: Dict[str, Any]) -> bool:
    version = str(feature.get("schema_version") or "").strip()
    if version == _LEGACY_SCHEMA_VERSION:
        return True
    if version and version != _V2_SCHEMA_VERSION:
        return True
    if version == _V2_SCHEMA_VERSION:
        return False
    key = str(feature.get("approval_key") or "").strip()
    canonical_key = str(feature.get("canonical_approval_key") or "").strip()
    return bool(key and key != identity["approval_key"] and canonical_key != identity["approval_key"])


def _value_list(value: Any) -> list[Any]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return []


def _legacy_template_pairs(value: Dict[str, Any]) -> set[tuple[str, str]]:
    items = value.get("source_templates") or value.get("template_signatures") or value.get("top_templates") or []
    pairs = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        identity = str(item.get("template_fingerprint") or item.get("template_hash") or "").strip()
        if identity:
            pairs.add((identity, str(item.get("category") or "").strip()))
    return pairs


def _legacy_components(value: Dict[str, Any]) -> set[str]:
    components = {
        str(item).strip().lower()
        for item in _value_list(value.get("components") or value.get("component_scope"))
        if str(item).strip()
    }
    if components:
        return components
    items = value.get("source_templates") or value.get("template_signatures") or value.get("top_templates") or []
    return {
        str(item.get("component")).strip().lower()
        for item in items
        if isinstance(item, dict) and str(item.get("component") or "").strip()
    }


def _legacy_anchors(value: Dict[str, Any]) -> set[str]:
    anchors = {
        str(item).strip()
        for item in _value_list(value.get("anchor_signatures"))
        if str(item).strip()
    }
    if anchors:
        return anchors
    return {
        "|".join(item)
        for item in sorted(_legacy_template_pairs(value))
    }


def _legacy_feature_matches(rule: Dict[str, Any], feature: Dict[str, Any]) -> bool:
    rule_key = str(rule.get("approval_key") or "").strip()
    feature_key = str(feature.get("approval_key") or "").strip()
    if rule_key or feature_key:
        return bool(rule_key and feature_key and rule_key == feature_key)
    if normalize_feature_type(rule.get("feature_type")) != normalize_feature_type(feature.get("feature_type")):
        return False
    rule_pairs = _legacy_template_pairs(rule)
    feature_pairs = _legacy_template_pairs(feature)
    if not rule_pairs or rule_pairs != feature_pairs:
        return False
    if _legacy_components(rule) != _legacy_components(feature):
        return False
    return _legacy_anchors(rule) == _legacy_anchors(feature)


def _legacy_entity_matches(rule: Dict[str, Any], entity: Dict[str, Any]) -> bool:
    rule_type = str(rule.get("feature_type") or "").strip()
    entity_type = str(entity.get("feature_type") or "").strip()
    if not rule_type or not entity_type or normalize_feature_type(rule_type) != normalize_feature_type(entity_type):
        return False
    required = _legacy_template_pairs(rule)
    actual = _legacy_template_pairs(entity)
    if not required or not required.issubset(actual):
        return False
    required_components = _legacy_components(rule)
    actual_components = _legacy_components(entity)
    if required_components and (not actual_components or not required_components.issubset(actual_components)):
        return False
    required_anchors = _legacy_anchors(rule)
    actual_anchors = _legacy_anchors(entity)
    return not required_anchors or bool(actual_anchors) and required_anchors.issubset(actual_anchors)


def _v2_template_pairs(value: Dict[str, Any]) -> set[tuple[str, str]]:
    return {
        _identity_pair(item)
        for item in (value.get("source_templates") or value.get("template_signatures") or [])
        if isinstance(item, dict) and _identity_pair(item)[0]
    }


def _persistable_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    value = copy.deepcopy(rule)
    value.pop(_RULE_INTEGRITY_ERRORS_KEY, None)
    return value


def public_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    """Return a rule without internal DB consistency diagnostics."""

    return _persistable_rule(rule)


def hydrate_persisted_rule(
    rule_json: Any,
    *,
    persisted_projection: dict[str, Any],
    lifecycle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Hydrate lifecycle metadata while keeping JSON identity authoritative for comparison."""

    if isinstance(rule_json, str):
        try:
            rule = json.loads(rule_json)
        except json.JSONDecodeError as exc:
            raise ApprovedRuleError("批准规则 JSON 无效") from exc
    else:
        rule = copy.deepcopy(rule_json)
    if not isinstance(rule, dict):
        raise ApprovedRuleError("批准规则 JSON 必须是 object")
    lifecycle_values = dict(lifecycle or {})
    if (
        not str(rule.get("schema_version") or "").strip()
        and str(persisted_projection.get("schema_version") or "") == _LEGACY_SCHEMA_VERSION
        and not has_v2_identity_markers(rule)
    ):
        rule["schema_version"] = _LEGACY_SCHEMA_VERSION
    candidate = {**rule, **{
        field: value
        for field, value in lifecycle_values.items()
        if field in {"status", "current_version", "next_review_at", "approved_at", "updated_at"}
    }}
    classification = classify_rule(candidate, persisted_projection=persisted_projection)
    if classification.kind == RuleFormat.MALFORMED_V2:
        rule[_RULE_INTEGRITY_ERRORS_KEY] = classification.integrity_errors
    rule.update({
        field: value
        for field, value in lifecycle_values.items()
        if field in {"status", "current_version", "next_review_at", "approved_at", "updated_at"}
    })
    if "current_version" in rule and rule["current_version"] is not None:
        rule["current_version"] = int(rule["current_version"])
    rule.setdefault("created_at", rule.get("approved_at"))
    return rule


def _template_set_storage_key(feature: Dict[str, Any], identity: Dict[str, Any]) -> str:
    if identity["match_mode"] != "template_set" or identity.get("semantic_safe"):
        return identity["approval_key"]
    payload = {
        "approval_key": identity["approval_key"],
        "feature_type": normalize_feature_type(feature.get("feature_type")),
        "components": sorted(identity["component_scope"]),
        "anchors": sorted(identity["anchor_signatures"]),
        "templates": sorted(_v2_template_pairs(feature)),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:32]
    return f'{identity["approval_key"]}:template-{digest}'


def _template_set_rule_id(approval_key: str) -> str:
    digest = hashlib.sha256(f"template-set\x1f{approval_key}".encode("utf-8")).hexdigest()[:20]
    return f"rule-{digest}"


def _template_set_rule_complete(rule: Dict[str, Any]) -> bool:
    return (
        rule.get("match_mode") == "template_set"
        and normalize_feature_type(rule.get("feature_type")) != "unknown_feature"
        and bool(_legacy_components(rule))
        and "anchor_signatures" in rule
        and bool(_v2_template_pairs(rule))
    )


def _v2_exact_feature_match(
    rule: Dict[str, Any],
    feature: Dict[str, Any],
    identity: Dict[str, Any],
) -> bool:
    if classify_rule(rule).kind != RuleFormat.VALID_V2:
        return False
    strict_key = _template_set_storage_key(feature, identity)
    keys_match = (
        rule.get("approval_key") == identity["approval_key"]
        or rule.get("approval_key") == strict_key
        or rule.get("canonical_approval_key") == identity["approval_key"]
    )
    if not keys_match:
        return False
    if rule.get("match_mode") == "semantic":
        return identity["match_mode"] == "semantic"
    if identity["match_mode"] != "template_set" or not _template_set_rule_complete(rule):
        return False
    return (
        normalize_feature_type(rule.get("feature_type")) == normalize_feature_type(feature.get("feature_type"))
        and _legacy_components(rule) == set(identity["component_scope"])
        and {
            str(item).strip()
            for item in (rule.get("anchor_signatures") or [])
            if str(item).strip()
        } == set(identity["anchor_signatures"])
        and _v2_template_pairs(rule) == _v2_template_pairs(feature)
    )


def _v2_entity_fields_match(rule: Dict[str, Any], entity: Dict[str, Any]) -> bool:
    if not _template_set_rule_complete(rule):
        return False
    rule_type = str(rule.get("feature_type") or "").strip()
    entity_type = str(entity.get("feature_type") or "").strip()
    if not entity_type or normalize_feature_type(rule_type) != normalize_feature_type(entity_type):
        return False
    required_components = _legacy_components(rule)
    actual_components = _legacy_components(entity)
    if required_components and (not actual_components or not required_components.issubset(actual_components)):
        return False
    required_anchors = {
        str(item).strip()
        for item in (rule.get("anchor_signatures") or [])
        if str(item).strip()
    }
    actual_anchors = _legacy_anchors(entity)
    return not required_anchors or bool(actual_anchors) and required_anchors.issubset(actual_anchors)


def _rule_matches_feature(
    rule: Dict[str, Any],
    feature: Dict[str, Any],
    entity: Dict[str, Any] | None = None,
    *,
    active_only: bool = True,
) -> bool:
    if active_only and not _is_active(rule):
        return False
    classification = classify_rule(rule)
    if classification.kind == RuleFormat.LEGACY_V1:
        return _legacy_feature_matches(rule, feature)
    if classification.kind != RuleFormat.VALID_V2:
        return False
    identity = approval_identity(feature, entity)
    if _is_legacy_feature(feature, identity):
        return False
    if _v2_exact_feature_match(rule, feature, identity):
        return True
    rule_resolution = resolve_problem(rule)
    feature_resolution = resolve_problem(feature, entity)
    if (
        not active_only
        and rule.get("match_mode") == "semantic"
        and rule_resolution.semantic_safe
        and rule_resolution.problem_code == identity["problem_code"]
    ):
        return True
    return (
        rule.get("match_mode") == "semantic"
        and identity["match_mode"] == "semantic"
        and rule_resolution.semantic_safe
        and feature_resolution.semantic_safe
        and rule_resolution.problem_code == feature_resolution.problem_code == identity["problem_code"]
    )


def _rule_matches_entity(rule: Dict[str, Any], entity: Dict[str, Any]) -> bool:
    if not _is_active(rule):
        return False
    classification = classify_rule(rule)
    if classification.kind == RuleFormat.LEGACY_V1:
        return _legacy_entity_matches(rule, entity)
    if classification.kind != RuleFormat.VALID_V2:
        return False
    rule_resolution = resolve_problem(rule)
    entity_resolution = resolve_problem(entity, entity)
    if (
        semantic_resolver_enabled()
        and rule.get("match_mode") == "semantic"
        and rule_resolution.semantic_safe
        and entity_resolution.semantic_safe
    ):
        return rule_resolution.problem_code == entity_resolution.problem_code
    required = {
        _identity_pair(item)
        for item in (rule.get("template_signatures") or [])
        if isinstance(item, dict)
    }
    actual = {
        _identity_pair(item)
        for item in (entity.get("top_templates") or [])
        if isinstance(item, dict)
    }
    if not required or not required.issubset(actual):
        return False
    return _v2_entity_fields_match(rule, entity)


def _replacement_key(approval_key: str, predecessor_rule_id: str) -> str:
    digest = hashlib.sha256(
        f"{approval_key}\x1f{predecessor_rule_id}".encode("utf-8")
    ).hexdigest()[:20]
    return f"{approval_key}:replacement-{digest}"


def _replacement_rule_id(approval_key: str, predecessor_rule_id: str) -> str:
    digest = hashlib.sha256(
        f"replacement\x1f{approval_key}\x1f{predecessor_rule_id}".encode("utf-8")
    ).hexdigest()[:20]
    return f"rule-{digest}"


def rule_matches_feature(
    rule: Dict[str, Any],
    feature: Dict[str, Any],
    entity: Dict[str, Any] | None = None,
    *,
    active_only: bool = True,
) -> bool:
    return _rule_matches_feature(rule, feature, entity, active_only=active_only)


class ApprovedRuleStore:
    def __init__(self, path: str | Path, clock: Callable[[], str] = _now) -> None:
        self.path = Path(path)
        self.clock = clock

    def _read_locked(self) -> list[Dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApprovedRuleError(f"批准规则库无法读取: {exc}") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "1.0"
            or not isinstance(payload.get("rules"), list)
            or not all(isinstance(rule, dict) for rule in payload["rules"])
        ):
            raise ApprovedRuleError("批准规则库结构无效")
        return payload["rules"]

    def _write_locked(self, rules: list[Dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        payload = {"schema_version": "1.0", "rules": [_persistable_rule(rule) for rule in rules]}
        try:
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(temporary, self.path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise ApprovedRuleError(f"批准规则库写入失败: {exc}") from exc

    def list_rules(self) -> list[Dict[str, Any]]:
        with _PROCESS_LOCK:
            return [public_rule(rule) for rule in self._read_locked()]

    def load_legacy_file(self) -> list[Dict[str, Any]]:
        """Load a known legacy file without implicitly rewriting it."""

        with _PROCESS_LOCK:
            return [
                normalize_legacy_rule_version(rule, source=RuleNormalizationSource.LEGACY_FILE)
                for rule in self._read_locked()
            ]

    def upsert_feature(self, feature: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(feature, dict):
            raise ApprovedRuleError("批准特征必须是 object")
        identity = approval_identity(feature)
        base_signature = rule_signature(
            str(feature.get("feature_type") or ""),
            feature.get("source_templates") or [],
        )
        now = self.clock()
        with _PROCESS_LOCK:
            rules = self._read_locked()
            classified = [(rule, classify_rule(rule)) for rule in rules]
            incoming_keys = {
                identity["approval_key"],
                _template_set_storage_key(feature, identity),
            }
            for candidate, classification in classified:
                if classification.kind != RuleFormat.MALFORMED_V2:
                    continue
                candidate_key = str(candidate.get("approval_key") or "").strip()
                candidate_canonical_key = str(candidate.get("canonical_approval_key") or "").strip()
                if (
                    candidate_key in incoming_keys
                    or candidate_canonical_key in incoming_keys
                    or candidate.get("signature") == base_signature
                ):
                    raise ApprovedRuleIntegrityError(
                        rule_id=candidate.get("rule_id"),
                        approval_key=candidate.get("approval_key"),
                        signature=candidate.get("signature"),
                        integrity_errors=classification.integrity_errors,
                    )
            legacy_feature = _is_legacy_feature(feature, identity)
            active = [rule for rule, classification in classified if _is_active(rule) and classification.kind in {
                RuleFormat.LEGACY_V1, RuleFormat.VALID_V2,
            }]
            exact = [
                rule for rule in rules
                if not legacy_feature
                and rule in active
                and classify_rule(rule).kind == RuleFormat.VALID_V2
                and _v2_exact_feature_match(rule, feature, identity)
            ]
            existing = _preferred_rule(exact)
            if (
                existing is None
                and not legacy_feature
                and identity["match_mode"] == "semantic"
            ):
                semantic = [
                    rule for rule in active
                    if classify_rule(rule).kind == RuleFormat.VALID_V2
                    and rule.get("match_mode") == "semantic"
                    and resolve_problem(rule).semantic_safe
                    and resolve_problem(rule).problem_code == identity["problem_code"]
                ]
                existing = _preferred_rule(semantic)
            if existing is None and legacy_feature:
                existing = _preferred_rule([
                    rule for rule in active
                    if classify_rule(rule).kind == RuleFormat.LEGACY_V1
                    and _legacy_feature_matches(rule, feature)
                ])
                if existing is not None:
                    return copy.deepcopy(existing)

            predecessor = None
            if existing is None and not legacy_feature:
                predecessor = _preferred_rule([
                    rule for rule, classification in classified
                    if classification.kind == RuleFormat.VALID_V2
                    and not _is_active(rule)
                    and _rule_matches_feature(rule, feature, active_only=False)
                ])

            approval_key = (
                str(existing.get("approval_key") or identity["approval_key"])
                if existing
                else _template_set_storage_key(feature, identity)
            )
            canonical_approval_key = None
            predecessor_rule_id = None
            if (
                existing is None
                and identity["match_mode"] == "template_set"
                and identity["semantic_safe"]
            ):
                canonical_approval_key = identity["approval_key"]
            if predecessor is not None:
                predecessor_rule_id = str(predecessor.get("rule_id") or "")
                canonical_approval_key = identity["approval_key"]
                if any(rule.get("approval_key") == approval_key for rule in rules):
                    approval_key = _replacement_key(identity["approval_key"], predecessor_rule_id)

            signature = base_signature
            if any(
                rule.get("signature") == signature
                and (existing is None or rule.get("rule_id") != existing.get("rule_id"))
                for rule in rules
            ):
                signature = _approval_rule_signature(signature, approval_key)

            rule_id = existing.get("rule_id") if existing else (
                _template_set_rule_id(approval_key)
                if identity["match_mode"] == "template_set" and identity["semantic_safe"]
                else f"rule-{identity['approval_key'][5:25]}"
            )
            if existing is None and any(rule.get("rule_id") == rule_id for rule in rules):
                rule_id = _replacement_rule_id(identity["approval_key"], str(predecessor_rule_id or rule_id))
            rule = {
                "rule_id": rule_id,
                "signature": signature,
                "feature_type": normalize_feature_type(feature.get("feature_type")),
                "title": str(feature.get("title") or "").strip(),
                "summary": str(feature.get("summary") or "").strip(),
                "importance": str(feature.get("importance") or "medium").strip(),
                "tags": [str(tag).strip() for tag in (feature.get("tags") or []) if str(tag).strip()],
                "selection_reason": str(feature.get("selection_reason") or "").strip(),
                "components": identity["component_scope"],
                "template_signatures": _template_pairs(feature.get("source_templates") or []),
                "problem_code": identity["problem_code"],
                "approval_key": approval_key,
                "anchor_signatures": identity["anchor_signatures"],
                "supporting_signatures": copy.deepcopy(feature.get("supporting_signatures") or []),
                "match_mode": identity["match_mode"],
                "approved_at": existing.get("approved_at") if existing else now,
                "created_at": (existing.get("created_at") or existing.get("approved_at") or now) if existing else now,
                "updated_at": now,
                "reuse_count": int(existing.get("reuse_count") or 0) if existing else 0,
                "last_reused_at": existing.get("last_reused_at") if existing else None,
                "schema_version": "approved_rule_v2",
                "status": "active",
                "next_review_at": existing.get("next_review_at") if existing else _next_review(now),
            }
            lineage = _lineage(feature)
            if lineage:
                rule["lineage"] = lineage
            elif existing and isinstance(existing.get("lineage"), dict):
                rule["lineage"] = copy.deepcopy(existing["lineage"])
            if predecessor_rule_id:
                rule["predecessor_rule_id"] = predecessor_rule_id
                rule["lineage"] = copy.deepcopy(rule.get("lineage") or {})
                rule["lineage"]["predecessor_rule_id"] = predecessor_rule_id
            elif existing:
                for field in ("canonical_approval_key", "predecessor_rule_id"):
                    if existing.get(field) is not None:
                        rule[field] = copy.deepcopy(existing[field])
            current_version = int(existing.get("current_version") or 1) if existing else 1
            if existing and _versioned_fields(existing) != _versioned_fields(rule):
                current_version += 1
            rule["current_version"] = current_version
            if canonical_approval_key:
                rule["canonical_approval_key"] = canonical_approval_key
            if existing:
                rules[rules.index(existing)] = rule
            else:
                rules.append(rule)
            rules.sort(key=lambda item: str(item.get("rule_id")))
            self._write_locked(rules)
            return public_rule(rule)

    def match_entity(self, entity: Dict[str, Any]) -> list[Dict[str, Any]]:
        with _PROCESS_LOCK:
            matches = []
            semantic_matches: dict[str, list[Dict[str, Any]]] = {}
            for rule in self._read_locked():
                if not _rule_matches_entity(rule, entity):
                    continue
                resolution = resolve_problem(rule)
                if (
                    classify_rule(rule).kind == RuleFormat.VALID_V2
                    and rule.get("match_mode") == "semantic"
                    and resolution.semantic_safe
                    and resolution.problem_code
                ):
                    semantic_matches.setdefault(resolution.problem_code, []).append(rule)
                else:
                    matches.append(public_rule(rule))
            for code in sorted(semantic_matches):
                preferred = _preferred_rule(semantic_matches[code])
                if preferred is not None:
                    matches.append(public_rule(preferred))
            return matches

    def match_feature(
        self,
        feature: Dict[str, Any],
        entity: Dict[str, Any] | None = None,
    ) -> list[Dict[str, Any]]:
        identity = approval_identity(feature, entity)
        with _PROCESS_LOCK:
            rules = [
                rule for rule in self._read_locked()
                if _is_active(rule)
            ]
            if _is_legacy_feature(feature, identity):
                legacy = _preferred_rule([
                    rule for rule in rules
                    if classify_rule(rule).kind == RuleFormat.LEGACY_V1
                    and _legacy_feature_matches(rule, feature)
                ])
                return [public_rule(legacy)] if legacy is not None else []
            exact = _preferred_rule([
                rule for rule in rules
                if _v2_exact_feature_match(rule, feature, identity)
            ])
            if exact is not None:
                return [public_rule(exact)]
            incoming_resolution = resolve_problem(feature, entity)
            if identity["match_mode"] == "semantic" and incoming_resolution.semantic_safe:
                semantic_candidates = []
                for rule in rules:
                    if classify_rule(rule).kind != RuleFormat.VALID_V2 or rule.get("match_mode") != "semantic":
                        continue
                    rule_resolution = resolve_problem(rule)
                    if (
                        rule_resolution.semantic_safe
                        and rule_resolution.problem_code == incoming_resolution.problem_code == identity["problem_code"]
                    ):
                        semantic_candidates.append(rule)
                semantic = _preferred_rule(semantic_candidates)
                if semantic is not None:
                    return [public_rule(semantic)]
            return []

    def record_reuse(
        self,
        rule_id: str,
        *,
        job_id: str | None = None,
        entity_id: str | None = None,
        cluster: str | None = None,
    ) -> Dict[str, Any]:
        with _PROCESS_LOCK:
            rules = self._read_locked()
            rule = next((item for item in rules if item.get("rule_id") == rule_id), None)
            if rule is None:
                raise ApprovedRuleError("批准规则不存在")
            classification = classify_rule(rule)
            if classification.kind == RuleFormat.MALFORMED_V2:
                raise ApprovedRuleError("批准规则损坏，不能记录复用")
            if not _is_active(rule):
                raise ApprovedRuleError("只有 active 批准规则可以记录复用")
            rule["reuse_count"] = int(rule.get("reuse_count") or 0) + 1
            rule["last_reused_at"] = self.clock()
            self._write_locked(rules)
            return public_rule(rule)
