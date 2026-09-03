from __future__ import annotations

import copy
from collections import defaultdict
from typing import Any, Mapping

from logrisk.approval_dedup import approval_identity


_IMPORTANCE_RANK = {"critical": 4, "high": 3, "medium": 2, "low": 1}
_REPRESENTATIVE_FIELDS = frozenset({
    "candidate_id",
    "job_id",
    "approval_group_id",
    "approval_key",
    "problem_code",
    "schema_version",
    "feature_type",
    "title",
    "summary",
    "importance",
    "tags",
    "reviewer_note",
    "status",
    "resolution_type",
    "resolved_rule_id",
    "rule_id",
    "approved_at",
    "review_scope",
    "entity",
    "entity_id",
    "entity_type",
    "cluster",
    "window_start",
    "window_end",
    "time_range",
    "risk_score",
    "risk_level",
    "template_hashes",
    "anchor_signatures",
    "supporting_signatures",
    "component_scope",
    "components",
    "selection_reason",
    "occurrence_count",
    "affected_entities",
    "source_templates",
    "provider",
    "model",
    "model_profile_id",
    "prompt_id",
    "prompt_hash",
    "trace_id",
    "evidence_hash",
    "parameter_size",
    "thinking_enabled",
    "context_budget",
    "evaluator_result",
    "cache_hit",
    "latency_ms",
    "origin",
    "lineage",
    "created_at",
    "updated_at",
    "job_created_at",
    "job_status",
    "problem_resolution",
})
_FORBIDDEN_EVIDENCE_FIELDS = frozenset({
    "raw",
    "raw_log",
    "raw_logs",
    "raw_record",
    "raw_records",
    "raw_sample",
    "raw_samples",
    "samples",
    "log_stream",
    "raw_stream",
})


def _candidate_occurrence_count(candidate: Mapping[str, Any]) -> int:
    value = candidate.get("occurrence_count")
    if value is not None:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            pass
    total = 0
    for template in (candidate.get("source_templates") or []):
        if not isinstance(template, Mapping):
            continue
        try:
            total += max(0, int(template.get("count") or 0))
        except (TypeError, ValueError):
            continue
    return total


def _entity_key(candidate: Mapping[str, Any]) -> str:
    entity = candidate.get("entity") if isinstance(candidate.get("entity"), Mapping) else {}
    return "|".join((
        str(candidate.get("cluster") or ""),
        str(entity.get("type") or candidate.get("entity_type") or ""),
        str(entity.get("id") or candidate.get("entity_id") or ""),
    ))


def _time_bounds(candidate: Mapping[str, Any]) -> tuple[str | None, str | None]:
    time_range = candidate.get("time_range") if isinstance(candidate.get("time_range"), Mapping) else {}
    first_seen = time_range.get("first_seen") or candidate.get("window_start") or candidate.get("created_at")
    last_seen = time_range.get("last_seen") or candidate.get("window_end") or first_seen
    return (
        str(first_seen) if first_seen else None,
        str(last_seen) if last_seen else None,
    )


def _sanitize_representative_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _sanitize_representative_value(item)
            for key, item in value.items()
            if str(key).lower() not in _FORBIDDEN_EVIDENCE_FIELDS
        }
    if isinstance(value, list):
        return [_sanitize_representative_value(item) for item in value]
    return copy.deepcopy(value)


def _safe_representative(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _sanitize_representative_value(candidate[key])
        for key in sorted(_REPRESENTATIVE_FIELDS)
        if key in candidate
    }


def _representative_key(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    importance = str(candidate.get("importance") or "medium").strip().lower()
    try:
        risk_score = float(candidate.get("risk_score") or 0)
    except (TypeError, ValueError):
        risk_score = 0.0
    return (
        -_IMPORTANCE_RANK.get(importance, 0),
        -risk_score,
        str(candidate.get("created_at") or candidate.get("job_created_at") or ""),
        str(candidate.get("candidate_id") or ""),
    )


def build_review_groups(candidates: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build deterministic, read-only logical groups from pending candidates."""

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    identities: dict[str, dict[str, Any]] = {}
    seen_candidate_ids: set[str] = set()
    for raw_candidate in candidates:
        if not isinstance(raw_candidate, Mapping) or raw_candidate.get("status") != "pending":
            continue
        candidate = copy.deepcopy(dict(raw_candidate))
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id or candidate_id in seen_candidate_ids:
            continue
        seen_candidate_ids.add(candidate_id)
        identity = approval_identity(candidate)
        problem_code = str(identity["problem_code"])
        approval_key = str(candidate.get("approval_key") or identity["approval_key"])
        if identity["semantic_safe"]:
            review_key = f"semantic:{problem_code}"
            match_mode = "semantic"
        else:
            review_key = f"approval:{approval_key}"
            match_mode = "template_set"
        candidate["problem_code"] = problem_code
        candidate.setdefault("approval_key", identity["approval_key"])
        candidate["match_mode"] = match_mode
        candidate["problem_resolution"] = {
            "confidence": identity["resolution_confidence"],
            "semantic_safe": bool(identity["semantic_safe"]),
            "ambiguity": bool(identity["ambiguity"]),
            "evidence_source": identity["resolution_source"],
            "matched_rule": identity["matched_rule"],
            "supporting_codes": list(identity["supporting_codes"]),
        }
        grouped[review_key].append(candidate)
        identities[review_key] = {
            "problem_code": problem_code,
            "match_mode": match_mode,
            "resolution_confidence": identity["resolution_confidence"],
            "resolution_source": identity["resolution_source"],
            "semantic_safe": bool(identity["semantic_safe"]),
            "ambiguity": bool(identity["ambiguity"]),
        }

    result: list[dict[str, Any]] = []
    for review_key in sorted(grouped):
        members = grouped[review_key]
        representative = min(members, key=_representative_key)
        first_seen_values: list[str] = []
        last_seen_values: list[str] = []
        entity_keys: set[str] = set()
        occurrence_count = 0
        for candidate in members:
            first_seen, last_seen = _time_bounds(candidate)
            if first_seen:
                first_seen_values.append(first_seen)
            if last_seen:
                last_seen_values.append(last_seen)
            entity_keys.add(_entity_key(candidate))
            occurrence_count += _candidate_occurrence_count(candidate)
        identity = identities[review_key]
        result.append({
            "review_key": review_key,
            "problem_code": identity["problem_code"],
            "match_mode": identity["match_mode"],
            "resolution_confidence": identity["resolution_confidence"],
            "resolution_source": identity["resolution_source"],
            "semantic_safe": identity["semantic_safe"],
            "ambiguity": identity["ambiguity"],
            "title": str(representative.get("title") or ""),
            "summary": str(representative.get("summary") or ""),
            "importance": str(representative.get("importance") or "medium"),
            "candidate_count": len(members),
            "occurrence_count": occurrence_count,
            "affected_entity_count": len(entity_keys),
            "first_seen": min(first_seen_values) if first_seen_values else None,
            "last_seen": max(last_seen_values) if last_seen_values else None,
            "candidate_ids": sorted(str(item.get("candidate_id") or "") for item in members),
            "representative": _safe_representative(representative),
        })
    return result
