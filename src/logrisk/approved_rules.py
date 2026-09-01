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
    entity_problem_codes,
    normalize_feature_type,
    normalize_problem_code,
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
        "lineage",
    )
    return {field: copy.deepcopy(rule.get(field)) for field in fields}


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
        signature = rule_signature(
            str(feature.get("feature_type") or ""),
            feature.get("source_templates") or [],
        )
        now = self.clock()
        with _PROCESS_LOCK:
            rules = self._read_locked()
            existing = next(
                (
                    rule for rule in rules
                    if identity["approval_key"] and rule.get("approval_key") == identity["approval_key"]
                ),
                None,
            )
            if existing is None:
                existing = next(
                    (
                        rule for rule in rules
                        if not rule.get("approval_key") and rule.get("signature") == signature
                    ),
                    None,
                )
            if any(
                rule.get("signature") == signature
                and rule.get("approval_key") not in {None, identity["approval_key"]}
                for rule in rules
            ):
                signature = _approval_rule_signature(signature, identity["approval_key"])
            rule = {
                "rule_id": existing.get("rule_id") if existing else f"rule-{identity['approval_key'][5:25]}",
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
                "approval_key": identity["approval_key"],
                "anchor_signatures": identity["anchor_signatures"],
                "supporting_signatures": copy.deepcopy(feature.get("supporting_signatures") or []),
                "match_mode": identity["match_mode"],
                "approved_at": existing.get("approved_at") if existing else now,
                "created_at": existing.get("created_at") or existing.get("approved_at") if existing else now,
                "updated_at": now,
                "reuse_count": int(existing.get("reuse_count") or 0) if existing else 0,
                "last_reused_at": existing.get("last_reused_at") if existing else None,
                "schema_version": "approved_rule_v2",
                "status": str(existing.get("status") or "active") if existing else "active",
                "next_review_at": existing.get("next_review_at") if existing else _next_review(now),
            }
            lineage = _lineage(feature)
            if lineage:
                rule["lineage"] = lineage
            elif existing and isinstance(existing.get("lineage"), dict):
                rule["lineage"] = copy.deepcopy(existing["lineage"])
            current_version = int(existing.get("current_version") or 1) if existing else 1
            if existing and _versioned_fields(existing) != _versioned_fields(rule):
                current_version += 1
            rule["current_version"] = current_version
            if existing:
                rules[rules.index(existing)] = rule
            else:
                rules.append(rule)
            rules.sort(key=lambda item: str(item.get("rule_id")))
            self._write_locked(rules)
            return copy.deepcopy(rule)

    def match_entity(self, entity: Dict[str, Any]) -> list[Dict[str, Any]]:
        entity_pairs = {
            _identity_pair(item)
            for item in _template_pairs(entity.get("top_templates") or [])
        }
        entity_codes = entity_problem_codes(entity)
        entity_components = {
            str(item.get("component") or "").strip().lower()
            for item in (entity.get("top_templates") or [])
            if isinstance(item, dict) and str(item.get("component") or "").strip()
        }
        entity_anchors = {
            f"{str(item.get('template_fingerprint') or item.get('template_hash') or '').strip()}|{str(item.get('category') or '').strip()}".rstrip("|")
            for item in (entity.get("top_templates") or [])
            if isinstance(item, dict) and (item.get("template_fingerprint") or item.get("template_hash"))
        }
        with _PROCESS_LOCK:
            matches = []
            for rule in self._read_locked():
                if str(rule.get("status") or "active") != "active":
                    continue
                problem_code = normalize_problem_code(rule.get("problem_code"))
                semantic_rule = rule.get("match_mode") == "semantic" or (
                    rule.get("match_mode") is None and bool(rule.get("approval_key"))
                )
                if problem_code and entity_codes and semantic_rule:
                    if problem_code not in entity_codes:
                        continue
                    required_components = {
                        str(item).strip().lower()
                        for item in (rule.get("components") or [])
                        if str(item).strip()
                    }
                    if required_components and entity_components and not required_components.issubset(entity_components):
                        continue
                    required_anchors = {
                        str(item).strip()
                        for item in (rule.get("anchor_signatures") or [])
                        if str(item).strip()
                    }
                    if required_anchors and not all(
                        any(required == actual or actual.startswith(required + "|") for actual in entity_anchors)
                        for required in required_anchors
                    ):
                        continue
                required = {
                    _identity_pair(item)
                    for item in (rule.get("template_signatures") or [])
                }
                if problem_code and entity_codes and semantic_rule:
                    matches.append(copy.deepcopy(rule))
                elif required and required.issubset(entity_pairs):
                    matches.append(copy.deepcopy(rule))
            return matches

    def match_feature(
        self,
        feature: Dict[str, Any],
        entity: Dict[str, Any] | None = None,
    ) -> list[Dict[str, Any]]:
        identity = approval_identity(feature, entity)
        with _PROCESS_LOCK:
            matches = []
            for rule in self._read_locked():
                if str(rule.get("status") or "active") != "active":
                    continue
                if rule.get("approval_key") == identity["approval_key"]:
                    matches.append(copy.deepcopy(rule))
            return matches

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
