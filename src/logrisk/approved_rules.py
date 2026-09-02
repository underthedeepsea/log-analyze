from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict

from logrisk.approval_dedup import (
    approval_identity,
    build_approval_key,
    derive_problem_code,
    is_canonical_problem_code,
    normalize_problem_code,
    normalize_feature_type,
)


class ApprovedRuleError(RuntimeError):
    """Raised when the approved-rule state cannot be read or written safely."""


_PROCESS_LOCK = threading.RLock()


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


def _is_v2_rule(rule: Dict[str, Any]) -> bool:
    if str(rule.get("schema_version") or "") != "approved_rule_v2":
        return False
    if rule.get("match_mode") not in {None, "semantic", "template_set"}:
        return False
    expected = _rule_canonical_approval_key(rule)
    canonical = str(rule.get("canonical_approval_key") or "").strip()
    stored = str(rule.get("approval_key") or "").strip()
    return canonical == expected or (not canonical and stored == expected)


def _is_semantic_v2_rule(rule: Dict[str, Any]) -> bool:
    return _is_v2_rule(rule) and (
        rule.get("match_mode") == "semantic"
        or (rule.get("match_mode") is None and bool(rule.get("approval_key")))
    )


def _is_legacy_feature(feature: Dict[str, Any], identity: Dict[str, Any]) -> bool:
    version = str(feature.get("schema_version") or "")
    if version and version != "approved_rule_v2":
        return True
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
        identity = str(item.get("template_hash") or item.get("template_fingerprint") or "").strip()
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


def _template_set_storage_key(feature: Dict[str, Any], identity: Dict[str, Any]) -> str:
    if identity["match_mode"] != "template_set" or not is_canonical_problem_code(identity["problem_code"]):
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
    )


def _v2_exact_feature_match(
    rule: Dict[str, Any],
    feature: Dict[str, Any],
    identity: Dict[str, Any],
) -> bool:
    if not _is_v2_rule(rule):
        return False
    strict_key = _template_set_storage_key(feature, identity)
    keys_match = (
        rule.get("approval_key") == identity["approval_key"]
        or rule.get("approval_key") == strict_key
        or rule.get("canonical_approval_key") == identity["approval_key"]
    )
    if not keys_match:
        return False
    if _is_semantic_v2_rule(rule):
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
    if not _is_v2_rule(rule):
        if str(rule.get("schema_version") or "") == "approved_rule_v2":
            return False
        return _legacy_feature_matches(rule, feature)
    identity = approval_identity(feature, entity)
    if _is_legacy_feature(feature, identity):
        return False
    if _v2_exact_feature_match(rule, feature, identity):
        return True
    code = derive_problem_code(rule)
    return (
        _is_semantic_v2_rule(rule)
        and identity["match_mode"] == "semantic"
        and is_canonical_problem_code(identity["problem_code"])
        and is_canonical_problem_code(code)
        and code == identity["problem_code"]
    )


def _rule_matches_entity(rule: Dict[str, Any], entity: Dict[str, Any]) -> bool:
    if not _is_active(rule):
        return False
    if not _is_v2_rule(rule):
        if str(rule.get("schema_version") or "") == "approved_rule_v2":
            return False
        return _legacy_entity_matches(rule, entity)
    problem_code = derive_problem_code(rule)
    if _is_semantic_v2_rule(rule) and is_canonical_problem_code(problem_code):
        return problem_code == derive_problem_code(entity, entity)
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
        payload = {"schema_version": "1.0", "rules": rules}
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
            return copy.deepcopy(self._read_locked())

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
            legacy_feature = _is_legacy_feature(feature, identity)
            active = [rule for rule in rules if _is_active(rule)]
            exact = [
                rule for rule in rules
                if not legacy_feature
                and rule in active
                and _v2_exact_feature_match(rule, feature, identity)
            ]
            existing = _preferred_rule(exact)
            if (
                existing is None
                and not legacy_feature
                and identity["match_mode"] == "semantic"
                and is_canonical_problem_code(identity["problem_code"])
            ):
                semantic = [
                    rule for rule in active
                    if _is_v2_rule(rule)
                    and _is_semantic_v2_rule(rule)
                    and is_canonical_problem_code(derive_problem_code(rule))
                    and derive_problem_code(rule) == identity["problem_code"]
                ]
                existing = _preferred_rule(semantic)
            if existing is None and legacy_feature:
                existing = _preferred_rule([
                    rule for rule in active
                    if not _is_v2_rule(rule) and _legacy_feature_matches(rule, feature)
                ])
                if existing is not None:
                    return copy.deepcopy(existing)

            predecessor = None
            if existing is None:
                predecessor = _preferred_rule([
                    rule for rule in rules
                    if not _is_active(rule) and _rule_matches_feature(rule, feature, active_only=False)
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
                and is_canonical_problem_code(identity["problem_code"])
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
                if identity["match_mode"] == "template_set" and is_canonical_problem_code(identity["problem_code"])
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
            return copy.deepcopy(rule)

    def match_entity(self, entity: Dict[str, Any]) -> list[Dict[str, Any]]:
        with _PROCESS_LOCK:
            matches = []
            semantic_matches: dict[str, list[Dict[str, Any]]] = {}
            for rule in self._read_locked():
                if not _rule_matches_entity(rule, entity):
                    continue
                problem_code = derive_problem_code(rule)
                if _is_semantic_v2_rule(rule) and is_canonical_problem_code(problem_code):
                    semantic_matches.setdefault(problem_code, []).append(rule)
                else:
                    matches.append(copy.deepcopy(rule))
            for code in sorted(semantic_matches):
                preferred = _preferred_rule(semantic_matches[code])
                if preferred is not None:
                    matches.append(copy.deepcopy(preferred))
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
                    if not _is_v2_rule(rule) and _legacy_feature_matches(rule, feature)
                ])
                return [copy.deepcopy(legacy)] if legacy is not None else []
            legacy = _preferred_rule([
                rule for rule in rules
                if not _is_v2_rule(rule) and _legacy_feature_matches(rule, feature)
            ])
            if legacy is not None:
                return [copy.deepcopy(legacy)]
            exact = _preferred_rule([
                rule for rule in rules
                if _v2_exact_feature_match(rule, feature, identity)
            ])
            if exact is not None:
                return [copy.deepcopy(exact)]
            if identity["match_mode"] == "semantic" and is_canonical_problem_code(identity["problem_code"]):
                semantic = _preferred_rule([
                    rule for rule in rules
                    if _is_v2_rule(rule)
                    and _is_semantic_v2_rule(rule)
                    and is_canonical_problem_code(derive_problem_code(rule))
                    and derive_problem_code(rule) == identity["problem_code"]
                ])
                if semantic is not None:
                    return [copy.deepcopy(semantic)]
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
            rule["reuse_count"] = int(rule.get("reuse_count") or 0) + 1
            rule["last_reused_at"] = self.clock()
            self._write_locked(rules)
            return copy.deepcopy(rule)
