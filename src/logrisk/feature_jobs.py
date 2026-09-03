from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict

from logrisk.approval_dedup import (
    InMemoryApprovalGroupStore,
    approval_identity,
    group_id_for_key,
    same_approval_identity,
)
from logrisk.approved_rules import ApprovedRuleStore
from logrisk.feature_extractor_ollama import (
    DEFAULT_OLLAMA_URL,
    FEATURE_PROMPT_ID,
    IMPORTANCE_LEVELS,
    extract_features_for_entity,
)
from logrisk.processing_metrics import ProcessingMetricsStore


REVIEW_OWNED_FIELDS = (
    "status",
    "reviewer_note",
    "approved_at",
    "resolved_rule_id",
    "rule_id",
    "resolution_type",
    "review_scope",
    "title",
    "summary",
    "importance",
    "tags",
)

_FORBIDDEN_FEATURE_PAYLOAD_KEYS = frozenset({
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
    "raw_message",
    "message",
    "content",
    "api_key",
    "authorization",
    "cookie",
    "database_url",
    "dsn",
    "password",
    "secret",
    "token",
})


class FeatureJobError(RuntimeError):
    """Raised for invalid feature extraction job operations."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code


def _sanitize_feature_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _sanitize_feature_payload(item)
            for key, item in value.items()
            if str(key).strip().lower() not in _FORBIDDEN_FEATURE_PAYLOAD_KEYS
        }
    if isinstance(value, list):
        return [_sanitize_feature_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_feature_payload(item) for item in value]
    return copy.deepcopy(value)


def _merge_review_owned_fields(
    current: Dict[str, Any], incoming: Dict[str, Any]
) -> Dict[str, Any]:
    merged = copy.deepcopy(incoming)
    for field in REVIEW_OWNED_FIELDS:
        if field in current:
            merged[field] = copy.deepcopy(current[field])
    return merged


def _candidate_version_payload(candidate: Dict[str, Any]) -> Dict[str, Any]:
    value = copy.deepcopy(candidate)
    for field in ("job_id", "job_created_at", "job_status", "updated_at", "entity_id", "created_at"):
        value.pop(field, None)
    return value


def _candidate_not_found() -> FeatureJobError:
    return FeatureJobError("候选特征不存在", code="candidate_not_found", status_code=404)


def _candidate_state_conflict() -> FeatureJobError:
    return FeatureJobError("候选特征状态已变化", code="candidate_state_conflict", status_code=409)


def _invalid_feature_update(message: str) -> FeatureJobError:
    return FeatureJobError(message, code="invalid_feature_update", status_code=422)


def _validate_candidate_review_changes(changes: Dict[str, Any]) -> None:
    unknown = set(changes) - set(REVIEW_OWNED_FIELDS)
    if unknown:
        raise FeatureJobError(
            f"不可编辑字段: {sorted(unknown)}",
            code="invalid_feature_update",
            status_code=422,
        )
    if "status" in changes and changes["status"] not in {"pending", "approved", "rejected"}:
        raise FeatureJobError("字段 status 无效", code="invalid_feature_update", status_code=422)


class FeatureJobFileStore:
    """Durable local job snapshots without introducing a database dependency."""

    _lock = threading.RLock()

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @contextmanager
    def _process_lock(self):
        # ponytail: one root-wide lock keeps file CAS cross-process safe; use per-job locks if throughput requires it.
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / ".feature_jobs.lock").open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _atomic_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, path)

    def save(self, job: Dict[str, Any]) -> None:
        with self._lock, self._process_lock():
            job["features"] = self._save_locked(job)

    def _save_locked(self, job: Dict[str, Any]) -> dict[str, Dict[str, Any]]:
        existing = self.load_job(str(job["job_id"]))
        features: dict[str, Dict[str, Any]] = {}
        if existing:
            features.update(copy.deepcopy(existing.get("features") or {}))
        for candidate_id, candidate in (job.get("features") or {}).items():
            if not isinstance(candidate, dict):
                continue
            candidate = _sanitize_feature_payload(candidate)
            key = str(candidate.get("candidate_id") or candidate_id)
            previous = features.get(key)
            merged = (
                _merge_review_owned_fields(previous, candidate)
                if isinstance(previous, dict)
                else copy.deepcopy(candidate)
            )
            merged["candidate_id"] = key
            if (
                isinstance(previous, dict)
                and _candidate_version_payload(previous) == _candidate_version_payload(merged)
                and previous.get("updated_at") is not None
            ):
                merged["updated_at"] = copy.deepcopy(previous["updated_at"])
            else:
                merged["updated_at"] = _now()
            features[key] = merged
        self._write(job, features)
        return features

    def _write(
        self, job: Dict[str, Any], features: dict[str, Dict[str, Any]]
    ) -> None:
        safe_job = _sanitize_feature_payload(
            {key: value for key, value in job.items() if key != "condition"}
        )
        snapshot = {
            key: copy.deepcopy(value)
            for key, value in safe_job.items()
            if key not in {"condition", "events"}
        }
        snapshot["features"] = copy.deepcopy(features)
        job_dir = self.root / str(safe_job["job_id"])
        self._atomic_json(job_dir / "snapshot.json", snapshot)
        events_text = "".join(
            json.dumps(_sanitize_feature_payload(event), ensure_ascii=False, separators=(",", ":")) + "\n"
            for event in safe_job.get("events", [])
        )
        events_path = job_dir / "events.jsonl"
        events_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = events_path.with_name(f".{events_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(events_text, encoding="utf-8")
        os.replace(temporary, events_path)

    @staticmethod
    def _candidate_with_lineage(job: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
        value = _sanitize_feature_payload(candidate)
        value["job_id"] = str(job["job_id"])
        for field in ("model", "provider", "prompt_id", "model_profile_id"):
            if value.get(field) is None and job.get(field) is not None:
                value[field] = copy.deepcopy(job[field])
        value.setdefault("job_created_at", job.get("created_at"))
        value.setdefault("job_status", job.get("status"))
        return value

    def load(self) -> list[Dict[str, Any]]:
        if not self.root.exists():
            return []
        jobs = []
        for snapshot_path in sorted(self.root.glob("*/snapshot.json")):
            try:
                job = _sanitize_feature_payload(json.loads(snapshot_path.read_text(encoding="utf-8")))
                events_path = snapshot_path.with_name("events.jsonl")
                events = []
                if events_path.exists():
                    events = [
                        _sanitize_feature_payload(json.loads(line))
                        for line in events_path.read_text(encoding="utf-8").splitlines()
                        if line.strip()
                    ]
                if isinstance(job, dict) and job.get("job_id") and isinstance(events, list):
                    job["events"] = events
                    jobs.append(job)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
        return jobs

    def load_job(self, job_id: str) -> Dict[str, Any] | None:
        target = str(job_id)
        for job in self.load():
            if str(job.get("job_id")) == target:
                return job
        return None

    def load_candidate(
        self, candidate_id: str, job_id: str | None = None
    ) -> Dict[str, Any] | None:
        with self._lock:
            for job in self.load():
                if job_id is not None and str(job.get("job_id")) != str(job_id):
                    continue
                candidate = (job.get("features") or {}).get(str(candidate_id))
                if isinstance(candidate, dict):
                    return self._candidate_with_lineage(job, candidate)
        return None

    def save_generated_candidate(
        self, job_id: str, candidate: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not isinstance(candidate, dict):
            raise FeatureJobError("候选特征必须是 JSON object")
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id:
            raise FeatureJobError("候选特征缺少 candidate_id")
        candidate = _sanitize_feature_payload(candidate)
        with self._lock, self._process_lock():
            job = self.load_job(str(job_id))
            if job is None:
                raise _candidate_not_found()
            features = job.setdefault("features", {})
            previous = features.get(candidate_id)
            features[candidate_id] = (
                _merge_review_owned_fields(previous, candidate)
                if isinstance(previous, dict)
                else copy.deepcopy(candidate)
            )
            features[candidate_id]["candidate_id"] = candidate_id
            saved_features = self._save_locked(job)
            return self._candidate_with_lineage(job, saved_features[candidate_id])

    def update_candidate_review_state(
        self,
        candidate_id: str,
        changes: Dict[str, Any],
        *,
        expected_status: str,
        job_id: str | None = None,
        expected_updated_at: Any | None = None,
        allow_terminal_rollback: bool = False,
    ) -> Dict[str, Any]:
        if not isinstance(changes, dict):
            raise FeatureJobError(
                "审批内容必须是 JSON object",
                code="invalid_feature_update",
                status_code=422,
            )
        _validate_candidate_review_changes(changes)
        with self._lock, self._process_lock():
            candidate = self.load_candidate(candidate_id, job_id=job_id)
            if candidate is None:
                raise _candidate_not_found()
            current_status = candidate.get("status")
            requested_status = changes.get("status")
            if current_status in {"approved", "rejected"} and requested_status is not None and requested_status != current_status:
                if not (allow_terminal_rollback and current_status == expected_status and requested_status == "pending"):
                    raise _candidate_state_conflict()
            if expected_updated_at is not None and candidate.get("updated_at") != expected_updated_at:
                raise _candidate_state_conflict()
            if current_status != expected_status:
                if current_status == "approved" and requested_status == "approved":
                    return candidate
                raise _candidate_state_conflict()
            if all(candidate.get(field) == value for field, value in changes.items()):
                return candidate
            job = self.load_job(str(candidate["job_id"]))
            if job is None:
                raise _candidate_not_found()
            features = job.get("features") or {}
            current = features.get(str(candidate_id))
            if not isinstance(current, dict):
                raise _candidate_not_found()
            updated = copy.deepcopy(current)
            for field, value in changes.items():
                updated[field] = copy.deepcopy(value)
            updated["candidate_id"] = str(candidate_id)
            if _candidate_version_payload(current) != _candidate_version_payload(updated):
                updated["updated_at"] = _now()
            else:
                updated["updated_at"] = current.get("updated_at") or _now()
            features[str(candidate_id)] = updated
            job["features"] = features
            self._write(job, features)
            return self._candidate_with_lineage(job, updated)

    def rollback_candidate_review_state(
        self,
        candidate_id: str,
        changes: Dict[str, Any],
        *,
        expected_status: str,
        expected_updated_at: Any | None,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
        return self.update_candidate_review_state(
            candidate_id,
            changes,
            expected_status=expected_status,
            job_id=job_id,
            expected_updated_at=expected_updated_at,
            allow_terminal_rollback=True,
        )

    def list_candidates(
        self, status: str | None = None, limit: int | None = None
    ) -> list[Dict[str, Any]]:
        candidates: list[Dict[str, Any]] = []
        for job in self.load():
            for candidate in (job.get("features") or {}).values():
                if not isinstance(candidate, dict):
                    continue
                if status is not None and str(candidate.get("status") or "") != str(status):
                    continue
                candidates.append(self._candidate_with_lineage(job, candidate))
        candidates.sort(
            key=lambda item: (
                str(item.get("created_at") or item.get("job_created_at") or ""),
                str(item.get("candidate_id") or ""),
            )
        )
        if limit is None:
            return candidates
        return candidates[: max(1, int(limit))]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_enabled_default() -> bool:
    return os.environ.get("AI_CACHE_ENABLED", "1").lower() not in {"0", "false", "no", "off"}


def validate_result_document(document: Any) -> Dict[str, Any]:
    if not isinstance(document, dict):
        raise FeatureJobError("result.json 顶层必须是 JSON object")
    entities = document.get("risk_entities")
    if not isinstance(entities, list):
        raise FeatureJobError("result.json 必须包含 risk_entities 数组")
    for index, entity in enumerate(entities):
        if not isinstance(entity, dict):
            raise FeatureJobError(f"risk_entities[{index}] 必须是 object")
        if not entity.get("entity_id"):
            raise FeatureJobError(f"risk_entities[{index}] 缺少 entity_id")
        try:
            float(entity.get("risk_score") or 0)
        except (TypeError, ValueError) as exc:
            raise FeatureJobError(f"risk_entities[{index}].risk_score 无效") from exc
    return document


def _entity_log_count(entity: Dict[str, Any]) -> int:
    return sum(int(template.get("count") or 0) for template in (entity.get("top_templates") or []))


def _template_identity(template: Dict[str, Any]) -> tuple[str, ...]:
    identity = (
        str(template.get("template_fingerprint") or "").strip()
        or str(template.get("template_hash") or "").strip()
        or str(template.get("template_instance_hash") or "").strip()
    )
    if identity:
        return (identity, str(template.get("component") or ""), str(template.get("category") or ""))
    return (json.dumps(template, ensure_ascii=False, sort_keys=True),)


def _merge_templates(
    current: list[Dict[str, Any]], incoming: list[Dict[str, Any]]
) -> list[Dict[str, Any]]:
    merged: dict[tuple[str, ...], Dict[str, Any]] = {}
    for template in [*current, *incoming]:
        if not isinstance(template, dict):
            continue
        key = _template_identity(template)
        if key not in merged:
            merged[key] = copy.deepcopy(template)
            continue
        target = merged[key]
        target["count"] = int(target.get("count") or 0) + int(template.get("count") or 0)
        first_seen = [value for value in (target.get("first_seen"), template.get("first_seen")) if value]
        last_seen = [value for value in (target.get("last_seen"), template.get("last_seen")) if value]
        if first_seen:
            target["first_seen"] = min(first_seen)
        if last_seen:
            target["last_seen"] = max(last_seen)
    return sorted(
        merged.values(),
        key=lambda item: (-int(item.get("count") or 0), _template_identity(item)),
    )[:10]


def _collapse_risk_entities(entities: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """Collapse window-level risk rows to the entity-level job storage contract."""

    level_rank = {"low": 0, "medium": 1, "high": 2, "critical": 3}
    collapsed: dict[str, Dict[str, Any]] = {}
    for raw_source in entities:
        source = copy.deepcopy(raw_source)
        entity_id = str(source.get("entity_id") or "")
        current = collapsed.get(entity_id)
        if current is None:
            source["entity_id"] = entity_id
            collapsed[entity_id] = source
            continue

        current_score = float(current.get("risk_score") or 0)
        source_score = float(source.get("risk_score") or 0)
        preferred = source if source_score > current_score else current
        for key, value in preferred.items():
            if key not in {"window_start", "window_end", "risk_score", "risk_level", "top_templates", "affected_entities"}:
                current[key] = copy.deepcopy(value)

        starts = [value for value in (current.get("window_start"), source.get("window_start")) if value]
        ends = [value for value in (current.get("window_end"), source.get("window_end")) if value]
        if starts:
            current["window_start"] = min(starts)
        if ends:
            current["window_end"] = max(ends)
        current["risk_score"] = max(current_score, source_score)
        if level_rank.get(str(source.get("risk_level") or ""), -1) > level_rank.get(
            str(current.get("risk_level") or ""), -1
        ):
            current["risk_level"] = source.get("risk_level")
        current["top_templates"] = _merge_templates(
            current.get("top_templates") or [], source.get("top_templates") or []
        )
        current["affected_entities"] = sorted({
            *[str(value) for value in (current.get("affected_entities") or []) if value],
            *[str(value) for value in (source.get("affected_entities") or []) if value],
        })

    return sorted(
        collapsed.values(),
        key=lambda item: (-float(item.get("risk_score") or 0), str(item.get("entity_id") or "")),
    )


def _sanitized_templates(entity: Dict[str, Any]) -> list[Dict[str, Any]]:
    allowed = (
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
        "semantic_dictionary_versions",
        "semantic_extractor_version",
        "risk_semantic",
    )
    return [
        {key: template.get(key) for key in allowed}
        for template in (entity.get("top_templates") or [])
        if isinstance(template, dict)
    ]


def _feature_from_rule(rule: Dict[str, Any], entity: Dict[str, Any]) -> Dict[str, Any]:
    templates = _sanitized_templates(entity)

    def matches_signature(signature: Any, template: Dict[str, Any]) -> bool:
        expected = str(signature or "").strip()
        if not expected:
            return False
        expected_identity, separator, expected_category = expected.partition("|")
        actual_identity = str(
            template.get("template_fingerprint") or template.get("template_hash") or ""
        ).strip()
        actual_category = str(template.get("category") or "").strip()
        return bool(
            actual_identity
            and actual_identity == expected_identity
            and (not separator or actual_category == expected_category)
        )

    anchors = [str(item).strip() for item in (rule.get("anchor_signatures") or []) if str(item).strip()]
    sources = [
        template
        for template in templates
        if any(matches_signature(anchor, template) for anchor in anchors)
    ]
    if not sources:
        required = [
            "|".join(
                part for part in (
                    str(item.get("template_fingerprint") or item.get("template_hash") or "").strip(),
                    str(item.get("category") or "").strip(),
                )
                if part
            )
            for item in (rule.get("template_signatures") or [])
            if isinstance(item, dict)
        ]
        sources = [
            template
            for template in templates
            if any(matches_signature(signature, template) for signature in required)
        ]
    if not sources:
        required_components = {
            str(item).strip().lower()
            for item in (rule.get("components") or [])
            if str(item).strip()
        }
        sources = [
            template
            for template in templates
            if not required_components
            or str(template.get("component") or "").strip().lower() in required_components
        ] or templates[:1]
    material = "|".join([
        str(rule.get("rule_id") or ""),
        str(entity.get("cluster") or ""),
        str(entity.get("entity_type") or ""),
        str(entity.get("entity_id") or ""),
        str(entity.get("window_start") or ""),
    ])
    first_seen = [item.get("first_seen") for item in sources if item.get("first_seen")]
    last_seen = [item.get("last_seen") for item in sources if item.get("last_seen")]
    return {
        "candidate_id": hashlib.sha256(material.encode("utf-8")).hexdigest()[:20],
        "status": "approved",
        "reviewer_note": "历史批准规则自动复用",
        "approved_at": rule.get("approved_at"),
        "cluster": entity.get("cluster"),
        "entity": {"type": entity.get("entity_type"), "id": entity.get("entity_id")},
        "window_start": entity.get("window_start"),
        "window_end": entity.get("window_end"),
        "risk_score": entity.get("risk_score"),
        "risk_level": entity.get("risk_level"),
        "affected_entities": copy.deepcopy(entity.get("affected_entities") or []),
        "feature_type": rule.get("feature_type"),
        "title": rule.get("title"),
        "summary": rule.get("summary"),
        "importance": rule.get("importance"),
        "template_hashes": [item.get("template_hash") for item in sources],
        "components": sorted({str(item.get("component")) for item in sources if item.get("component")}) or copy.deepcopy(rule.get("components") or []),
        "tags": copy.deepcopy(rule.get("tags") or []),
        "selection_reason": rule.get("selection_reason") or "命中历史人工批准规则",
        "occurrence_count": sum(int(item.get("count") or 0) for item in sources),
        "time_range": {
            "first_seen": min(first_seen) if first_seen else entity.get("window_start"),
            "last_seen": max(last_seen) if last_seen else entity.get("window_end"),
        },
        "source_templates": sources,
        "provider": "approved_rule",
        "model": None,
        "origin": "approved_rule",
        "rule_id": rule.get("rule_id"),
        "problem_code": rule.get("problem_code"),
        "approval_key": rule.get("canonical_approval_key") or rule.get("approval_key"),
        "anchor_signatures": copy.deepcopy(rule.get("anchor_signatures") or []),
        "supporting_signatures": copy.deepcopy(rule.get("supporting_signatures") or []),
        "match_mode": rule.get("match_mode"),
        "resolution_type": "rule_matched",
        "resolved_rule_id": rule.get("rule_id"),
    }


class FeatureJobManager:
    def __init__(
        self,
        extractor: Callable[..., list[Dict[str, Any]]] = extract_features_for_entity,
        rule_store: ApprovedRuleStore | None = None,
        metrics_store: ProcessingMetricsStore | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        auto_start: bool = True,
        persistence: FeatureJobFileStore | None = None,
        observability: Any | None = None,
        interrupt_on_restore: bool = True,
        approval_group_store: Any | None = None,
    ) -> None:
        self.extractor = extractor
        self.rule_store = rule_store
        self.metrics_store = metrics_store
        self.monotonic = monotonic
        self.auto_start = auto_start
        self.persistence = persistence
        self.observability = observability
        self.interrupt_on_restore = bool(interrupt_on_restore)
        self.approval_group_store = approval_group_store if approval_group_store is not None else InMemoryApprovalGroupStore()
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._restore_jobs()

    def _restore_jobs(self) -> None:
        if not self.persistence:
            return
        for job in self.persistence.load():
            condition = threading.Condition(self._lock)
            job["condition"] = condition
            job.setdefault("events", [])
            job.setdefault("processed_samples", [])
            job["started_monotonic"] = self.monotonic()
            interrupted = self.interrupt_on_restore and job.get("status") in {"queued", "running"}
            if self.interrupt_on_restore:
                for record in job.get("entities", []):
                    if record.get("status") in {"queued", "running"}:
                        record["status"] = "interrupted"
                        record["error"] = "服务重启中断，需人工重试"
                        interrupted = True
            if interrupted:
                job["status"] = "interrupted"
                job["completed_at"] = None
                job["events"].append({
                    "sequence": len(job["events"]),
                    "type": "job_interrupted",
                    "timestamp": _now(),
                    "job_id": job["job_id"],
                })
            self._jobs[str(job["job_id"])] = job
            if interrupted:
                self.persistence.save(job)
        with self._lock:
            changed_jobs: set[str] = set()
            for job in self._jobs.values():
                if self._rebuild_approval_groups_locked(job):
                    changed_jobs.add(str(job["job_id"]))
            for job in self._jobs.values():
                for candidate_id, feature, record in self._review_candidates_locked(job):
                    if feature.get("status") != "pending":
                        continue
                    source = record.get("source") or {}
                    matches = []
                    if self.rule_store:
                        match_feature = getattr(self.rule_store, "match_feature", None)
                        if callable(match_feature):
                            matches = match_feature(feature, source)
                        if not matches:
                            matches = self.rule_store.match_entity(source)
                    for rule in matches[:1]:
                        resolved_rule = copy.deepcopy(rule)
                        if not resolved_rule.get("approval_key"):
                            resolved_rule.update(approval_identity(resolved_rule, source))
                        self._reconcile_pending_candidates_locked(resolved_rule)
                        break
            for job in self._jobs.values():
                if str(job["job_id"]) in changed_jobs:
                    self.persistence.save(job)

    def _hydrate_persisted_job_locked(self, persisted: Dict[str, Any]) -> Dict[str, Any]:
        job = copy.deepcopy(persisted)
        job.setdefault("events", [])
        job.setdefault("processed_samples", [])
        job["condition"] = threading.Condition(self._lock)
        job["started_monotonic"] = self.monotonic()
        job.setdefault("entities", [])
        job.setdefault("features", {})
        return job

    def _save_generated_candidate_locked(
        self, job: Dict[str, Any], candidate: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not self.persistence:
            return candidate
        saver = getattr(self.persistence, "save_generated_candidate", None)
        if not callable(saver):
            return candidate
        saved = saver(str(job["job_id"]), copy.deepcopy(candidate))
        if isinstance(saved, dict):
            candidate_id = str(saved.get("candidate_id") or candidate.get("candidate_id") or "")
            if candidate_id:
                job.setdefault("features", {})[candidate_id] = copy.deepcopy(saved)
            return copy.deepcopy(saved)
        return candidate

    def _load_review_candidate_locked(
        self, job: Dict[str, Any], candidate_id: str
    ) -> Dict[str, Any]:
        loader = getattr(self.persistence, "load_candidate", None) if self.persistence else None
        if callable(loader):
            persisted = loader(str(candidate_id))
            if persisted is None or str(persisted.get("job_id") or job["job_id"]) != str(job["job_id"]):
                raise _candidate_not_found()
            feature = copy.deepcopy(persisted)
            job.setdefault("features", {})[str(candidate_id)] = feature
            return feature
        try:
            return copy.deepcopy(job["features"][str(candidate_id)])
        except KeyError as exc:
            raise _candidate_not_found() from exc

    def _persist_review_state_locked(
        self,
        candidate_id: str,
        changes: Dict[str, Any],
        *,
        expected_status: str,
        expected_updated_at: Any | None = None,
    ) -> Dict[str, Any] | None:
        if not self.persistence:
            return None
        updater = getattr(self.persistence, "update_candidate_review_state", None)
        if not callable(updater):
            return None
        parameters: Dict[str, Any] = {"expected_status": expected_status}
        if expected_updated_at is not None:
            parameters["expected_updated_at"] = expected_updated_at
        updated = updater(
            str(candidate_id),
            copy.deepcopy(changes),
            **parameters,
        )
        return copy.deepcopy(updated) if isinstance(updated, dict) else None

    def _rollback_review_state_locked(
        self,
        candidate_id: str,
        changes: Dict[str, Any],
        *,
        expected_status: str,
        expected_updated_at: Any,
    ) -> Dict[str, Any] | None:
        if not self.persistence:
            return None
        rollback = getattr(self.persistence, "rollback_candidate_review_state", None)
        if not callable(rollback):
            return None
        updated = rollback(
            str(candidate_id),
            copy.deepcopy(changes),
            expected_status=expected_status,
            expected_updated_at=expected_updated_at,
        )
        return copy.deepcopy(updated) if isinstance(updated, dict) else None

    def create_job(
        self,
        document: Dict[str, Any],
        model: str,
        min_score: float = 40,
        base_url: str = DEFAULT_OLLAMA_URL,
        timeout: float = 120,
        prompt_id: str = FEATURE_PROMPT_ID,
        cache_enabled: bool | None = None,
        model_profile_id: str | None = None,
        retry_count: int = 0,
        provider: str = "ollama",
        connection_snapshot: dict[str, Any] | None = None,
        profile_snapshot: dict[str, Any] | None = None,
    ) -> str:
        validate_result_document(document)
        if not isinstance(model, str) or not model.strip():
            raise FeatureJobError("必须指定模型")
        if timeout <= 0:
            raise FeatureJobError("模型 timeout 必须大于 0")
        try:
            normalized_retry_count = int(retry_count)
        except (TypeError, ValueError) as exc:
            raise FeatureJobError("自动重试次数必须是非负整数") from exc
        if normalized_retry_count < 0:
            raise FeatureJobError("自动重试次数必须是非负整数")

        job_id = uuid.uuid4().hex
        records = []
        initial_matches: list[tuple[Dict[str, Any], list[Dict[str, Any]]]] = []
        started_monotonic = self.monotonic()
        processed_samples: list[tuple[float, int]] = []
        sources = [
            _sanitize_feature_payload(source)
            for source in _collapse_risk_entities(document["risk_entities"])
        ]
        source_summary = _sanitize_feature_payload(document.get("summary") or {})
        safe_connection_snapshot = _sanitize_feature_payload(connection_snapshot) if connection_snapshot else None
        safe_profile_snapshot = _sanitize_feature_payload(profile_snapshot) if profile_snapshot else None
        for source in sorted(
            sources,
            key=lambda item: float(item.get("risk_score") or 0),
            reverse=True,
        ):
            score = float(source.get("risk_score") or 0)
            matches = self.rule_store.match_entity(source) if self.rule_store and score >= min_score else []
            record = {
                "entity_id": str(source.get("entity_id")),
                "entity_type": source.get("entity_type"),
                "cluster": source.get("cluster"),
                "window_start": source.get("window_start"),
                "window_end": source.get("window_end"),
                "risk_score": score,
                "risk_level": source.get("risk_level"),
                "log_count": _entity_log_count(source),
                "affected_entities": copy.deepcopy(source.get("affected_entities") or []),
                "status": "rule_matched" if matches else ("queued" if score >= min_score else "skipped"),
                "error": None,
                "feature_ids": [],
                "matched_rule_ids": [str(rule.get("rule_id")) for rule in matches],
                "llm_counted": False,
                "cache_hit": False,
                "source": copy.deepcopy(source),
            }
            if matches:
                initial_matches.append((record, matches))
                processed_samples.append((started_monotonic, record["log_count"]))
            records.append(record)

        condition = threading.Condition(self._lock)
        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "created_at": _now(),
                "completed_at": None,
                "model": model.strip(),
                "provider": str(provider or "ollama"),
                "base_url": base_url,
                "timeout": float(timeout),
                "prompt_id": str(prompt_id or FEATURE_PROMPT_ID),
                "model_profile_id": model_profile_id,
                "connection_snapshot": safe_connection_snapshot,
                "profile_snapshot": safe_profile_snapshot,
                "cache_enabled": _cache_enabled_default() if cache_enabled is None else bool(cache_enabled),
                "retry_count": normalized_retry_count,
                "min_score": float(min_score),
                "source_summary": {
                    **source_summary,
                    "feature_job_entity_count": len(sources),
                },
                "entities": records,
                "features": {},
                "events": [],
                "started_monotonic": started_monotonic,
                "processed_samples": processed_samples,
                "condition": condition,
            }
            self._emit_locked(self._jobs[job_id], "job_created")
            for record, matches in initial_matches:
                self._apply_rule_matches_locked(self._jobs[job_id], record, matches)
            for record in records:
                if record["status"] == "rule_matched":
                    self._emit_locked(
                        self._jobs[job_id],
                        "entity_rule_matched",
                        entity_id=record["entity_id"],
                        rule_ids=record["matched_rule_ids"],
                    )

        if self.auto_start:
            threading.Thread(target=self.run_job, args=(job_id,), daemon=True).start()
        return job_id

    @staticmethod
    def _feature_times(feature: Dict[str, Any], record: Dict[str, Any]) -> tuple[str | None, str | None]:
        time_range = feature.get("time_range") if isinstance(feature.get("time_range"), dict) else {}
        first_seen = time_range.get("first_seen") or feature.get("window_start") or record.get("window_start")
        last_seen = time_range.get("last_seen") or feature.get("window_end") or record.get("window_end")
        return (str(first_seen) if first_seen else None, str(last_seen) if last_seen else None)

    @staticmethod
    def _entity_key(feature: Dict[str, Any], record: Dict[str, Any]) -> str:
        entity = feature.get("entity") if isinstance(feature.get("entity"), dict) else {}
        return "|".join((
            str(feature.get("cluster") or record.get("cluster") or ""),
            str(entity.get("type") or record.get("entity_type") or ""),
            str(entity.get("id") or record.get("entity_id") or ""),
        ))

    def _prepare_feature(
        self,
        feature: Dict[str, Any],
        record: Dict[str, Any],
        *,
        job_id: str,
    ) -> Dict[str, Any]:
        prepared = _sanitize_feature_payload(feature)
        identity = approval_identity(prepared, record.get("source") or {})
        prepared.update({
            "problem_code": identity["problem_code"],
            "approval_key": identity["approval_key"],
            "anchor_signatures": identity["anchor_signatures"],
            "component_scope": identity["component_scope"],
            "match_mode": identity["match_mode"],
        })
        prepared.setdefault("job_id", job_id)
        if prepared.get("status") == "pending":
            prepared.setdefault("resolution_type", "manual")
        return prepared

    def _register_feature_group_locked(
        self,
        job: Dict[str, Any],
        record: Dict[str, Any],
        feature: Dict[str, Any],
        *,
        persist: bool = True,
        resolve_existing_rule: bool = True,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        raw_feature = copy.deepcopy(feature)
        candidate_id = str(raw_feature.get("candidate_id") or "")
        existing_group_id = None
        candidate_group_id = getattr(self.approval_group_store, "candidate_group_id", None)
        if candidate_id and callable(candidate_group_id):
            existing_group_id = candidate_group_id(candidate_id)
        existing_group_id = existing_group_id or raw_feature.get("approval_group_id")
        preserved_key = str(raw_feature.get("approval_key") or "") if existing_group_id else ""
        if existing_group_id and not preserved_key:
            get_by_id = getattr(self.approval_group_store, "get_by_id", None)
            if callable(get_by_id):
                historical_group = get_by_id(str(existing_group_id))
                preserved_key = str(historical_group.get("approval_key") or "") if historical_group else ""
        feature = self._prepare_feature(feature, record, job_id=str(job["job_id"]))
        if preserved_key:
            feature["approval_key"] = preserved_key
            feature["approval_group_id"] = str(existing_group_id)
        candidate_id = str(feature.get("candidate_id") or candidate_id)
        if not candidate_id:
            raise FeatureJobError("候选特征缺少 candidate_id")
        approval_key = str(feature["approval_key"])
        existing = None
        if existing_group_id:
            get_by_id = getattr(self.approval_group_store, "get_by_id", None)
            if callable(get_by_id):
                existing = get_by_id(str(existing_group_id))
        if existing is None:
            existing = self.approval_group_store.get_by_key(approval_key)
        first_seen, last_seen = self._feature_times(feature, record)
        entity_key = self._entity_key(feature, record)
        is_new_candidate = not self.approval_group_store.has_candidate(candidate_id)
        was_pending = feature.get("status") == "pending"
        resolved_rule = None
        if was_pending and resolve_existing_rule and self.rule_store:
            match_feature = getattr(self.rule_store, "match_feature", None)
            if callable(match_feature):
                matches = match_feature(feature, record.get("source") or {})
                if matches:
                    resolved_rule = matches[0]
                    feature.update({
                        "status": "approved",
                        "resolution_type": "group_matched",
                        "resolved_rule_id": resolved_rule.get("rule_id"),
                        "rule_id": resolved_rule.get("rule_id"),
                        "approved_at": resolved_rule.get("approved_at") or _now(),
                    })
        if existing is None:
            now = _now()
            group = {
                "approval_group_id": str(existing_group_id or group_id_for_key(approval_key)),
                "approval_key": approval_key,
                "problem_code": feature["problem_code"],
                "feature_type": feature.get("feature_type"),
                "title": feature.get("title") or "",
                "summary": feature.get("summary") or "",
                "importance": feature.get("importance") or "medium",
                "status": "approved" if feature.get("status") == "approved" else "pending",
                "rule_id": feature.get("resolved_rule_id") or feature.get("rule_id"),
                "first_seen": first_seen or now,
                "last_seen": last_seen or first_seen or now,
                "occurrence_count": int(feature.get("occurrence_count") or 0),
                "affected_entity_count": 1,
                "candidate_count": 1,
                "primary_candidate_id": candidate_id,
                "candidate_ids": [candidate_id],
                "entity_keys": [entity_key],
                "created_at": now,
                "updated_at": now,
                "schema_version": "approval_group_v1",
            }
        else:
            group = copy.deepcopy(existing)
            if is_new_candidate:
                group["candidate_count"] = int(group.get("candidate_count") or 0) + 1
                group["occurrence_count"] = int(group.get("occurrence_count") or 0) + int(feature.get("occurrence_count") or 0)
                group.setdefault("candidate_ids", []).append(candidate_id)
            group.setdefault("entity_keys", [])
            if entity_key not in group["entity_keys"]:
                group["entity_keys"].append(entity_key)
            group["affected_entity_count"] = len(group["entity_keys"])
            if first_seen and (not group.get("first_seen") or first_seen < group["first_seen"]):
                group["first_seen"] = first_seen
            if last_seen and (not group.get("last_seen") or last_seen > group["last_seen"]):
                group["last_seen"] = last_seen
            group["updated_at"] = _now()

        if feature.get("status") == "approved" and (feature.get("resolved_rule_id") or feature.get("rule_id")):
            group["status"] = "approved"
            group["rule_id"] = feature.get("resolved_rule_id") or feature.get("rule_id")
        group.setdefault("candidate_ids", [])
        if candidate_id not in group["candidate_ids"]:
            group["candidate_ids"].append(candidate_id)

        if (
            group.get("status") in {"approved", "auto_resolved"}
            and group.get("rule_id")
            and (not was_pending or resolve_existing_rule)
        ):
            feature.update({
                "status": "approved",
                "resolution_type": "group_matched",
                "resolved_rule_id": group["rule_id"],
                "rule_id": group["rule_id"],
                "approved_at": feature.get("approved_at") or group.get("updated_at") or _now(),
            })
        if was_pending:
            feature["resolution_type"] = "manual" if resolved_rule is None and is_new_candidate and existing is None else "group_matched"
        primary = str(group.get("primary_candidate_id") or "")
        if primary and primary != candidate_id:
            feature["duplicate_of"] = primary
        feature["approval_group_id"] = group["approval_group_id"]
        if persist:
            self._persist_feature_group_locked(job, record, group, candidate_id)
        return feature, group

    def _persist_feature_group_locked(
        self,
        job: Dict[str, Any],
        record: Dict[str, Any],
        group: Dict[str, Any],
        candidate_id: str,
    ) -> None:
        self.approval_group_store.save(group)
        self.approval_group_store.attach_candidate(
            group["approval_group_id"],
            candidate_id,
            job_id=str(job["job_id"]),
            entity_id=str(record.get("entity_id") or ""),
        )

    def _apply_rule_matches_locked(
        self,
        job: Dict[str, Any],
        record: Dict[str, Any],
        matches: list[Dict[str, Any]],
    ) -> None:
        record["status"] = "rule_matched"
        record["matched_rule_ids"] = [str(rule.get("rule_id")) for rule in matches]
        record["feature_ids"] = []
        for rule in matches:
            feature = _feature_from_rule(rule, record["source"])
            feature, _ = self._register_feature_group_locked(job, record, feature)
            candidate_id = str(feature["candidate_id"])
            job["features"][candidate_id] = copy.deepcopy(feature)
            feature = self._save_generated_candidate_locked(job, feature)
            record["feature_ids"].append(candidate_id)
            if self.rule_store:
                self.rule_store.record_reuse(
                    str(rule["rule_id"]),
                    job_id=str(job["job_id"]),
                    entity_id=record["entity_id"],
                    cluster=record.get("cluster"),
                )
                self._reconcile_pending_candidates_locked(rule)

    def _rebuild_approval_groups_locked(self, job: Dict[str, Any]) -> bool:
        changed = False
        for candidate_id, raw_feature in list((job.get("features") or {}).items()):
            feature = copy.deepcopy(raw_feature)
            entity_id = str((feature.get("entity") or {}).get("id") or "")
            record = next((item for item in job.get("entities") or [] if str(item.get("entity_id")) == entity_id), None)
            if record is None:
                record = {
                    "entity_id": entity_id,
                    "entity_type": (feature.get("entity") or {}).get("type"),
                    "cluster": feature.get("cluster"),
                    "window_start": feature.get("window_start"),
                    "window_end": feature.get("window_end"),
                    "source": {},
                }
            feature["candidate_id"] = str(feature.get("candidate_id") or candidate_id)
            prepared, _ = self._register_feature_group_locked(
                job,
                record,
                feature,
                resolve_existing_rule=False,
            )
            job.setdefault("features", {})[candidate_id] = prepared
            changed = changed or prepared != raw_feature
        return changed

    def reconcile_pending_candidates(self, rule: Dict[str, Any]) -> Dict[str, int]:
        with self._lock:
            return self._reconcile_pending_candidates_locked(rule)

    @staticmethod
    def _review_candidates_locked(
        job: Dict[str, Any],
    ) -> list[tuple[str, Dict[str, Any], Dict[str, Any]]]:
        records_by_candidate: dict[str, Dict[str, Any]] = {}
        for record in job.get("entities") or []:
            for candidate_id in record.get("feature_ids") or []:
                records_by_candidate[str(candidate_id)] = record
        candidates = []
        for candidate_id, raw_feature in (job.get("features") or {}).items():
            if not isinstance(raw_feature, dict):
                continue
            candidate_key = str(raw_feature.get("candidate_id") or candidate_id)
            record = records_by_candidate.get(candidate_key)
            if record is None:
                entity = raw_feature.get("entity") if isinstance(raw_feature.get("entity"), dict) else {}
                record = {
                    "entity_id": entity.get("id") or raw_feature.get("entity_id"),
                    "entity_type": entity.get("type") or raw_feature.get("entity_type"),
                    "cluster": raw_feature.get("cluster"),
                    "window_start": raw_feature.get("window_start"),
                    "window_end": raw_feature.get("window_end"),
                    "source": {},
                }
            candidates.append((candidate_key, raw_feature, record))
        return candidates

    def _load_persisted_candidate_jobs_locked(self, status: str = "pending") -> None:
        if not self.persistence:
            return
        list_candidates = getattr(self.persistence, "list_candidates", None)
        if not callable(list_candidates):
            return
        candidates = list_candidates(status=status, limit=None)
        for candidate in candidates:
            target = str(candidate.get("job_id") or "")
            if not target or target in self._jobs:
                continue
            job = self._job(target)
            changed = self._rebuild_approval_groups_locked(job)
            if changed:
                self.persistence.save(job)

    def _reconcile_pending_candidates_locked(self, rule: Dict[str, Any]) -> Dict[str, int]:
        if str(rule.get("status") or "active") != "active" or not (rule.get("approval_key") or rule.get("problem_code")):
            return {"auto_resolved_candidates": 0, "auto_resolved_groups": 0}
        self._load_persisted_candidate_jobs_locked()
        resolved = 0
        groups: set[str] = set()
        for job in self._jobs.values():
            for candidate_id, feature, record in self._review_candidates_locked(job):
                if feature.get("status") != "pending":
                    continue
                if not same_approval_identity(feature, rule):
                    continue
                expected_updated_at = feature.get("updated_at")
                feature.update({
                    "status": "approved",
                    "approved_at": feature.get("approved_at") or rule.get("approved_at") or _now(),
                    "resolution_type": "group_matched",
                    "resolved_rule_id": rule.get("rule_id"),
                    "rule_id": rule.get("rule_id"),
                })
                try:
                    persisted = self._persist_review_state_locked(
                        str(candidate_id),
                        {
                            "status": "approved",
                            "approved_at": feature.get("approved_at"),
                            "resolution_type": "group_matched",
                            "resolved_rule_id": rule.get("rule_id"),
                            "rule_id": rule.get("rule_id"),
                        },
                        expected_status="pending",
                        expected_updated_at=expected_updated_at,
                    )
                except FeatureJobError as exc:
                    if getattr(exc, "code", None) not in {"candidate_not_found", "candidate_state_conflict"}:
                        raise
                    loader = getattr(self.persistence, "load_candidate", None) if self.persistence else None
                    latest = loader(str(candidate_id)) if callable(loader) else None
                    if isinstance(latest, dict):
                        job["features"][candidate_id] = copy.deepcopy(latest)
                    continue
                if persisted is not None:
                    feature = persisted
                job["features"][candidate_id] = feature
                group = self.approval_group_store.get_by_key(str(feature.get("approval_key") or ""))
                if group:
                    group["rule_id"] = rule.get("rule_id")
                    if group.get("status") == "pending":
                        group["status"] = "auto_resolved"
                        groups.add(str(group.get("approval_group_id")))
                    group["updated_at"] = _now()
                    self.approval_group_store.save(group)
                resolved += 1
                self._emit_locked(
                    job,
                    "pending_candidate_reconciled",
                    candidate_id=str(candidate_id),
                    entity_id=str(record.get("entity_id") or ""),
                    approval_group_id=feature.get("approval_group_id"),
                    approval_key=feature.get("approval_key"),
                    rule_id=rule.get("rule_id"),
                )
        return {"auto_resolved_candidates": resolved, "auto_resolved_groups": len(groups)}

    def _active_rule_for_feature_locked(self, feature: Dict[str, Any]) -> Dict[str, Any] | None:
        if not self.rule_store:
            return None
        rule_id = feature.get("resolved_rule_id") or feature.get("rule_id")
        if not rule_id:
            return None
        for rule in self.rule_store.list_rules():
            if str(rule.get("rule_id") or "") == str(rule_id) and str(rule.get("status") or "active") == "active":
                return copy.deepcopy(rule)
        return None

    def _set_group_resolution_locked(
        self,
        feature: Dict[str, Any],
        *,
        status: str,
        rule_id: str | None = None,
    ) -> None:
        group = None
        group_id = feature.get("approval_group_id")
        if group_id:
            get_by_id = getattr(self.approval_group_store, "get_by_id", None)
            if callable(get_by_id):
                group = get_by_id(str(group_id))
        if group is None:
            get_by_key = getattr(self.approval_group_store, "get_by_key", None)
            if callable(get_by_key) and feature.get("approval_key"):
                group = get_by_key(str(feature["approval_key"]))
        if group is None:
            return
        changed = False
        if status == "approved":
            if group.get("status") != "approved":
                group["status"] = "approved"
                changed = True
            if rule_id and group.get("rule_id") != rule_id:
                group["rule_id"] = rule_id
                changed = True
        elif status == "rejected" and group.get("status") == "pending":
            group["status"] = "rejected"
            changed = True
        if changed:
            group["updated_at"] = _now()
            self.approval_group_store.save(group)

    def _reject_pending_identity_locked(self, selected: Dict[str, Any]) -> int:
        self._load_persisted_candidate_jobs_locked()
        rejected = 0
        for job in self._jobs.values():
            for candidate_id, feature, record in self._review_candidates_locked(job):
                if feature.get("status") != "pending":
                    continue
                if not same_approval_identity(feature, selected):
                    continue
                expected_status = str(feature.get("status") or "pending")
                expected_updated_at = feature.get("updated_at")
                feature.update({
                    "status": "rejected",
                    "approved_at": None,
                    "resolution_type": "group_rejected",
                    "review_scope": "approval_identity",
                })
                try:
                    persisted = self._persist_review_state_locked(
                        str(candidate_id),
                        {
                            "status": "rejected",
                            "approved_at": None,
                            "resolution_type": "group_rejected",
                            "review_scope": "approval_identity",
                        },
                        expected_status=expected_status,
                        expected_updated_at=expected_updated_at,
                    )
                except FeatureJobError as exc:
                    if getattr(exc, "code", None) not in {"candidate_not_found", "candidate_state_conflict"}:
                        raise
                    loader = getattr(self.persistence, "load_candidate", None) if self.persistence else None
                    latest = loader(str(candidate_id)) if callable(loader) else None
                    if isinstance(latest, dict):
                        job["features"][candidate_id] = copy.deepcopy(latest)
                    continue
                if persisted is not None:
                    feature = persisted
                job["features"][candidate_id] = feature
                group = self.approval_group_store.get_by_key(str(feature.get("approval_key") or ""))
                if group and group.get("status") == "pending":
                    group["status"] = "rejected"
                    group["updated_at"] = _now()
                    self.approval_group_store.save(group)
                self._emit_locked(
                    job,
                    "candidate_group_rejected",
                    candidate_id=str(candidate_id),
                    entity_id=str(record.get("entity_id") or ""),
                    approval_group_id=feature.get("approval_group_id"),
                    approval_key=feature.get("approval_key"),
                )
                rejected += 1
        return rejected

    def list_persisted_candidates(
        self, status: str | None = None, limit: int | None = None
    ) -> list[Dict[str, Any]]:
        if self.persistence:
            list_candidates = getattr(self.persistence, "list_candidates", None)
            if callable(list_candidates):
                return copy.deepcopy(list_candidates(status=status, limit=limit))
        with self._lock:
            candidates = []
            for job in self._jobs.values():
                for feature in (job.get("features") or {}).values():
                    if not isinstance(feature, dict):
                        continue
                    if status is not None and str(feature.get("status") or "") != str(status):
                        continue
                    value = copy.deepcopy(feature)
                    value["job_id"] = str(job["job_id"])
                    for field in ("model", "provider", "prompt_id", "model_profile_id"):
                        if value.get(field) is None and job.get(field) is not None:
                            value[field] = copy.deepcopy(job[field])
                    value.setdefault("job_created_at", job.get("created_at"))
                    value.setdefault("job_status", job.get("status"))
                    candidates.append(value)
            candidates.sort(
                key=lambda item: (
                    str(item.get("created_at") or item.get("job_created_at") or ""),
                    str(item.get("candidate_id") or ""),
                )
            )
            if limit is None:
                return candidates
            return candidates[: max(1, int(limit))]

    def list_approval_groups(self, status: str | None = None) -> list[Dict[str, Any]]:
        with self._lock:
            return self.approval_group_store.list_groups(status=status)

    def _job(self, job_id: str) -> Dict[str, Any]:
        target = str(job_id)
        job = self._jobs.get(target)
        if job is None and self.persistence:
            loader = getattr(self.persistence, "load_job", None)
            persisted = loader(target) if callable(loader) else next(
                (item for item in self.persistence.load() if str(item.get("job_id")) == target),
                None,
            )
            if persisted is not None:
                job = self._hydrate_persisted_job_locked(persisted)
                self._jobs[target] = job
        if job is None:
            raise FeatureJobError("任务不存在")
        return job

    def _emit_locked(self, job: Dict[str, Any], event_type: str, **payload: Any) -> None:
        event = {
            "sequence": len(job["events"]),
            "type": event_type,
            "timestamp": _now(),
            "job_id": job["job_id"],
            **payload,
        }
        job["events"].append(event)
        self._record_observability_event(job, event_type, payload)
        if self.persistence:
            self.persistence.save(job)
        job["condition"].notify_all()

    def _record_observability_event(
        self,
        job: Dict[str, Any],
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:
        if self.observability is None:
            return
        stages = {
            "job_created": [
                ("input", "input-ready", "success"),
                ("normalize", "normalization-completed", "success"),
                ("drain3", "template-mining-completed", "success"),
                ("aggregate", "risk-aggregation-completed", "success"),
            ],
            "job_started": [("aggregate", "feature-job", "success")],
            "entity_rule_matched": [("rule_cache", "approved-rule", "skipped")],
            "candidate_auto_resolved": [("approval", "duplicate-candidate-resolved", "success")],
            "pending_candidate_reconciled": [("approval", "pending-candidate-reconciled", "success")],
            "entity_started": [
                ("rule_cache", "rule-cache-checked", "success"),
                ("evidence", "evidence-built", "success"),
                ("prompt", "prompt-locked", "success"),
                ("model", "model-call-started", "running"),
            ],
            "entity_retrying": [("model", "model-retry", "failed")],
            "entity_cache_hit": [("rule_cache", "ai-cache", "cache_hit")],
            "entity_completed": [
                ("model", "model-call-completed", "success"),
                ("parse", "json-parsed", "success"),
                ("schema", "schema-validated", "success"),
                ("evaluator", "quality-evaluated", "success"),
                ("candidate", "candidate-generation", "success"),
            ],
            "entity_failed": [("model", "model-call", "failed")],
            "feature_updated": [("approval", "manual-review", "success")],
            "job_completed": [("candidate", "feature-job", "success")],
        }
        event_stages = stages.get(event_type)
        if event_stages is None:
            return
        if event_type == "entity_failed":
            error = str(payload.get("error") or "").lower()
            if "evaluator" in error:
                event_stages = [("evaluator", "quality-evaluated", "failed")]
            elif "json" in error or "解析" in error:
                event_stages = [("parse", "json-parsed", "failed")]
            elif "schema" in error or "特征字段" in error or "features 数组" in error:
                event_stages = [("schema", "schema-validated", "failed")]
        try:
            observation = self.observability.ensure_observation(
                job_id=job["job_id"],
                attributes={
                    "provider": job.get("provider"),
                    "model": job.get("model"),
                    "prompt_id": job.get("prompt_id"),
                },
            )
            sequence = len(job.get("events") or []) - 1
            for stage, name, status in event_stages:
                self.observability.record(
                    observation_id=observation["observation_id"],
                    name=name,
                    stage=stage,
                    status=status,
                    trace_id=payload.get("trace_id"),
                    attributes={"event_type": event_type, **payload},
                    idempotency_key=f"{job['job_id']}:{sequence}:{stage}:{name}",
                )
            job["observation_id"] = observation["observation_id"]
            if event_type == "job_completed":
                self.observability.finish_observation(
                    observation["observation_id"],
                    failed=payload.get("status") == "completed_with_errors",
                )
        except Exception:
            return

    def run_job(self, job_id: str, only_entity_id: str | None = None) -> None:
        with self._lock:
            job = self._job(job_id)
            job["status"] = "running"
            self._emit_locked(job, "job_started")
            records = [
                record
                for record in job["entities"]
                if record["status"] == "queued"
                and (only_entity_id is None or record["entity_id"] == only_entity_id)
            ]

        for record in records:
            with self._lock:
                matches = self.rule_store.match_entity(record["source"]) if self.rule_store else []
                if matches:
                    self._apply_rule_matches_locked(job, record, matches)
                    job["processed_samples"].append((self.monotonic(), record["log_count"]))
                    self._emit_locked(
                        job,
                        "entity_rule_matched",
                        entity_id=record["entity_id"],
                        rule_ids=record["matched_rule_ids"],
                    )
                    continue
                record["status"] = "running"
                self._emit_locked(job, "entity_started", entity_id=record["entity_id"])
            try:
                retry_count = int(job.get("retry_count") or 0)
                for attempt in range(retry_count + 1):
                    try:
                        features = self.extractor(
                            copy.deepcopy(record["source"]),
                            model=job["model"],
                            base_url=job["base_url"],
                            timeout=job["timeout"],
                            prompt_id=job["prompt_id"],
                            job_id=job["job_id"],
                            cache_enabled=job["cache_enabled"],
                            model_profile_id=job.get("model_profile_id"),
                            provider=job.get("provider", "ollama"),
                            connection_snapshot=copy.deepcopy(job.get("connection_snapshot")),
                            profile_snapshot=copy.deepcopy(job.get("profile_snapshot")),
                        )
                        break
                    except Exception as exc:
                        if attempt >= retry_count:
                            raise
                        with self._lock:
                            record["error"] = str(exc)
                            self._emit_locked(
                                job,
                                "entity_retrying",
                                entity_id=record["entity_id"],
                                attempt=attempt + 1,
                                retry_count=retry_count,
                                error=str(exc),
                            )
                with self._lock:
                    record["cache_hit"] = any(feature.get("cache_hit") for feature in features)
                    if record["cache_hit"]:
                        self._emit_locked(job, "entity_cache_hit", entity_id=record["entity_id"])
                    elif not record["llm_counted"]:
                        if self.metrics_store:
                            self.metrics_store.add_llm_logs(record["log_count"])
                        record["llm_counted"] = True
                    record["feature_ids"] = []
                    for raw_feature in features:
                        feature = copy.deepcopy(raw_feature)
                        feature.setdefault("origin", job.get("provider", "ollama"))
                        feature, _ = self._register_feature_group_locked(job, record, feature)
                        candidate_id = str(feature["candidate_id"])
                        job["features"][candidate_id] = copy.deepcopy(feature)
                        feature = self._save_generated_candidate_locked(job, feature)
                        record["feature_ids"].append(candidate_id)
                    record["status"] = "completed"
                    record["error"] = None
                    job["processed_samples"].append((self.monotonic(), record["log_count"]))
                    self._emit_locked(
                        job,
                        "entity_completed",
                        entity_id=record["entity_id"],
                        feature_count=len(features),
                        trace_id=next((item.get("trace_id") for item in features if item.get("trace_id")), None),
                        latency_ms=max((int(item.get("latency_ms") or 0) for item in features), default=0),
                    )
            except Exception as exc:
                with self._lock:
                    if not record["llm_counted"]:
                        if self.metrics_store:
                            self.metrics_store.add_llm_logs(record["log_count"])
                        record["llm_counted"] = True
                    record["status"] = "failed"
                    record["error"] = str(exc)
                    job["processed_samples"].append((self.monotonic(), record["log_count"]))
                    self._emit_locked(job, "entity_failed", entity_id=record["entity_id"], error=str(exc))

        with self._lock:
            eligible = [record for record in job["entities"] if record["status"] != "skipped"]
            has_errors = any(record["status"] == "failed" for record in eligible)
            still_running = any(record["status"] in {"queued", "running"} for record in eligible)
            if not still_running:
                job["status"] = "completed_with_errors" if has_errors else "completed"
                job["completed_at"] = _now()
                self._emit_locked(job, "job_completed", status=job["status"])

    def _progress_locked(self, job: Dict[str, Any]) -> Dict[str, int]:
        eligible = [record for record in job["entities"] if record["status"] != "skipped"]
        ollama_completed = sum(record["status"] == "completed" for record in eligible)
        rule_matched = sum(record["status"] == "rule_matched" for record in eligible)
        completed = ollama_completed + rule_matched
        failed = sum(record["status"] == "failed" for record in eligible)
        finished = completed + failed
        total = len(eligible)
        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "percent": round(finished / total * 100) if total else 100,
            "rule_matched": rule_matched,
            "ollama_completed": ollama_completed,
        }

    def _log_statistics_locked(self, job: Dict[str, Any]) -> Dict[str, int | float]:
        summary = job["source_summary"]
        original = int(summary.get("total_raw_logs") or sum(record["log_count"] for record in job["entities"]))
        template_windows = int(summary.get("total_template_windows") or 0)
        reduced = int(summary.get("drain3_reduced_logs") or max(0, original - template_windows))
        ratio = summary.get("drain3_compression_ratio_percent")
        if ratio is None:
            ratio = round(reduced / original * 100, 2) if original else 0.0
        eligible = [record for record in job["entities"] if record["status"] != "skipped"]
        return {
            "original_logs": original,
            "normalized_logs": int(summary.get("total_normalized_logs") or 0),
            "template_events": int(summary.get("total_template_events") or 0),
            "template_windows": template_windows,
            "drain3_reduced_logs": reduced,
            "drain3_compression_ratio_percent": float(ratio),
            "eligible_logs": sum(record["log_count"] for record in eligible),
            "analyzed_logs": sum(
                record["log_count"]
                for record in eligible
                if record["status"] in {"completed", "rule_matched"}
            ),
            "pending_logs": sum(
                record["log_count"]
                for record in eligible
                if record["status"] in {"queued", "running", "failed"}
            ),
            "skipped_logs": sum(
                record["log_count"] for record in job["entities"] if record["status"] == "skipped"
            ),
            "reused_logs": sum(
                record["log_count"] for record in eligible if record["status"] == "rule_matched"
            ),
            "ollama_logs": sum(
                record["log_count"] for record in eligible if record["status"] == "completed" and not record.get("cache_hit")
            ),
            "cache_hit_logs": sum(
                record["log_count"] for record in eligible if record.get("cache_hit")
            ),
        }

    def _live_metrics_locked(self, job: Dict[str, Any]) -> Dict[str, int | float]:
        now = self.monotonic()
        elapsed = max(0.0, now - float(job["started_monotonic"]))
        samples = job["processed_samples"]
        processed_logs = sum(int(count) for _, count in samples)
        rolling_logs = sum(int(count) for timestamp, count in samples if timestamp >= now - 60)
        current_rate = processed_logs / elapsed if elapsed > 0 else 0.0
        rolling_duration = min(60.0, elapsed)
        rolling_rate = rolling_logs / rolling_duration if rolling_duration > 0 else 0.0
        pending_logs = sum(
            record["log_count"]
            for record in job["entities"]
            if record["status"] in {"queued", "running"}
        )
        rule_matched = [record for record in job["entities"] if record["status"] == "rule_matched"]
        cache_hit = [record for record in job["entities"] if record.get("cache_hit")]
        return {
            "today_llm_logs": self.metrics_store.today_llm_logs() if self.metrics_store else 0,
            "cache_hit_calls": len(cache_hit),
            "cache_hit_logs": sum(record["log_count"] for record in cache_hit),
            "saved_llm_calls": len(rule_matched) + len(cache_hit),
            "saved_llm_logs": sum(record["log_count"] for record in rule_matched + cache_hit),
            "processing_logs_per_second": round(current_rate, 2),
            "rolling_60s_logs_per_second": round(rolling_rate, 2),
            "eta_seconds": round(pending_logs / current_rate) if current_rate > 0 else (None if pending_logs else 0),
        }

    def get_job(self, job_id: str) -> Dict[str, Any]:
        with self._lock:
            job = self._job(job_id)
            entities = [
                {key: copy.deepcopy(value) for key, value in record.items() if key != "source"}
                for record in job["entities"]
            ]
            return {
                "job_id": job["job_id"],
                "status": job["status"],
                "created_at": job["created_at"],
                "completed_at": job["completed_at"],
                "model": job["model"],
                "provider": job.get("provider", "ollama"),
                "prompt_id": job["prompt_id"],
                "model_profile_id": job.get("model_profile_id"),
                "connection_snapshot": copy.deepcopy(job.get("connection_snapshot")),
                "profile_snapshot": copy.deepcopy(job.get("profile_snapshot")),
                "retry_count": job.get("retry_count", 0),
                "min_score": job["min_score"],
                "source_summary": copy.deepcopy(job["source_summary"]),
                "progress": self._progress_locked(job),
                "log_statistics": self._log_statistics_locked(job),
                "live_metrics": self._live_metrics_locked(job),
                "entities": entities,
                "features": copy.deepcopy(list(job["features"].values())),
            }

    def get_agent_evidence(self, job_id: str, entity_id: str) -> Dict[str, Any]:
        """Return the same sanitized template facts used by model extraction, never raw samples."""
        with self._lock:
            job = self._job(job_id)
            record = next((item for item in job["entities"] if item["entity_id"] == str(entity_id)), None)
            if record is None:
                raise FeatureJobError("风险实体不存在")
            source = record["source"]
            return {
                "schema_version": "1.0",
                "entity": {
                    "type": source.get("entity_type"),
                    "id": source.get("entity_id"),
                    "cluster": source.get("cluster"),
                },
                "window": {"start": source.get("window_start"), "end": source.get("window_end")},
                "risk_score": source.get("risk_score"),
                "risk_level": source.get("risk_level"),
                "affected_entities": copy.deepcopy(source.get("affected_entities") or []),
                "templates": copy.deepcopy(_sanitized_templates(source)),
            }

    def register_agent_candidate(
        self,
        job_id: str,
        entity_id: str,
        feature: Dict[str, Any],
        *,
        run_id: str,
    ) -> Dict[str, Any]:
        evidence = self.get_agent_evidence(job_id, entity_id)
        known_hashes = {
            str(item.get("template_hash"))
            for item in evidence["templates"]
            if item.get("template_hash")
        }
        known_components = {
            str(item.get("component"))
            for item in evidence["templates"]
            if item.get("component")
        }
        hashes = feature.get("template_hashes")
        components = feature.get("components")
        if not isinstance(hashes, list) or not hashes or set(map(str, hashes)) - known_hashes:
            raise FeatureJobError("Agent Candidate 的 template_hash 无效")
        if not isinstance(components, list) or not components or set(map(str, components)) - known_components:
            raise FeatureJobError("Agent Candidate 的 component 无效")
        required_strings = ("feature_type", "title", "summary", "importance", "selection_reason")
        if any(not isinstance(feature.get(field), str) or not feature[field].strip() for field in required_strings):
            raise FeatureJobError("Agent Candidate 缺少必填字段")
        if feature["importance"] not in IMPORTANCE_LEVELS:
            raise FeatureJobError("Agent Candidate importance 无效")
        tags = feature.get("tags")
        if not isinstance(tags, list) or not tags or any(not isinstance(item, str) or not item.strip() for item in tags):
            raise FeatureJobError("Agent Candidate tags 无效")
        material = "|".join((str(run_id), str(entity_id), feature["feature_type"], *sorted(map(str, hashes))))
        candidate = {
            **copy.deepcopy(feature),
            "candidate_id": hashlib.sha256(material.encode("utf-8")).hexdigest()[:20],
            "status": "pending",
            "reviewer_note": "",
            "approved_at": None,
            "agent_run_id": str(run_id),
            "origin": "agentic",
            "provider": "agentic",
            "model": None,
            "cluster": evidence["entity"].get("cluster"),
            "entity": {"type": evidence["entity"].get("type"), "id": evidence["entity"].get("id")},
            "window_start": evidence["window"].get("start"),
            "window_end": evidence["window"].get("end"),
            "risk_score": evidence.get("risk_score"),
            "risk_level": evidence.get("risk_level"),
            "affected_entities": copy.deepcopy(evidence.get("affected_entities") or []),
            "occurrence_count": sum(
                int(item.get("count") or 0) for item in evidence["templates"]
                if item.get("template_hash") in hashes
            ),
            "source_templates": [
                copy.deepcopy(item) for item in evidence["templates"]
                if item.get("template_hash") in hashes
            ],
        }
        with self._lock:
            job = self._job(job_id)
            record = next((item for item in job["entities"] if item["entity_id"] == str(entity_id)), None)
            if record is None:
                raise FeatureJobError("风险实体不存在")
            if candidate["candidate_id"] not in job["features"]:
                candidate, _ = self._register_feature_group_locked(job, record, candidate)
                job["features"][candidate["candidate_id"]] = copy.deepcopy(candidate)
                candidate = self._save_generated_candidate_locked(job, candidate)
                if candidate["candidate_id"] not in record["feature_ids"]:
                    record["feature_ids"].append(candidate["candidate_id"])
                self._emit_locked(
                    job,
                    "agent_candidate_registered",
                    entity_id=str(entity_id),
                    candidate_id=candidate["candidate_id"],
                    agent_run_id=str(run_id),
                )
        return copy.deepcopy(candidate)

    def list_jobs(self) -> list[Dict[str, Any]]:
        with self._lock:
            job_ids = sorted(self._jobs, key=lambda item: self._jobs[item]["created_at"], reverse=True)
        return [self.get_job(job_id) for job_id in job_ids]

    def refresh_from_persistence(self, job_id: str) -> None:
        """Refresh one job written by another process without changing its lifecycle state."""
        if not self.persistence:
            return
        target = str(job_id)
        with self._lock:
            loader = getattr(self.persistence, "load_job", None)
            persisted = loader(target) if callable(loader) else next(
                (job for job in self.persistence.load() if str(job.get("job_id")) == target),
                None,
            )
            if persisted is None:
                raise FeatureJobError("任务不存在")
            current = self._jobs.get(target)
            if current is not None:
                current_events = len(current.get("events") or [])
                persisted_events = len(persisted.get("events") or [])
                if persisted_events <= current_events:
                    current_features = current.get("features") or {}
                    persisted_features = persisted.get("features") or {}
                    for candidate_id, persisted_candidate in persisted_features.items():
                        live_candidate = current_features.get(candidate_id)
                        if not isinstance(live_candidate, dict) or not isinstance(persisted_candidate, dict):
                            continue
                        if not any(
                            live_candidate.get(field) != persisted_candidate.get(field)
                            for field in REVIEW_OWNED_FIELDS
                        ):
                            continue
                        live_updated_at = str(live_candidate.get("updated_at") or "")
                        persisted_updated_at = str(persisted_candidate.get("updated_at") or "")
                        if not persisted_updated_at or persisted_updated_at <= live_updated_at:
                            continue
                        for field in REVIEW_OWNED_FIELDS:
                            if field in persisted_candidate:
                                live_candidate[field] = copy.deepcopy(persisted_candidate[field])
                        if persisted_updated_at:
                            live_candidate["updated_at"] = persisted_updated_at
                    return
                persisted = copy.deepcopy(persisted)
                persisted_features = persisted.setdefault("features", {})
                for candidate_id, live_candidate in (current.get("features") or {}).items():
                    if not isinstance(live_candidate, dict):
                        continue
                    persisted_candidate = persisted_features.get(candidate_id)
                    if isinstance(persisted_candidate, dict):
                        live_updated_at = str(live_candidate.get("updated_at") or "")
                        persisted_updated_at = str(persisted_candidate.get("updated_at") or "")
                        if live_updated_at > persisted_updated_at:
                            persisted_features[candidate_id] = _merge_review_owned_fields(
                                live_candidate, persisted_candidate
                            )
                    else:
                        persisted_features[candidate_id] = copy.deepcopy(live_candidate)
            self._jobs[target] = self._hydrate_persisted_job_locked(persisted)

    def wait_for_events(
        self,
        job_id: str,
        cursor: int,
        timeout: float = 15,
    ) -> tuple[list[Dict[str, Any]], int]:
        with self._lock:
            job = self._job(job_id)
            if cursor >= len(job["events"]) and timeout > 0:
                job["condition"].wait(timeout)
            events = copy.deepcopy(job["events"][cursor:])
            return events, len(job["events"])

    def list_events(self, job_id: str, limit: int = 100) -> list[Dict[str, Any]]:
        with self._lock:
            job = self._job(job_id)
            return copy.deepcopy(job["events"][-max(1, min(int(limit), 200)):])

    def retry_entity(self, job_id: str, entity_id: str, start: bool = True) -> None:
        with self._lock:
            job = self._job(job_id)
            matches = [record for record in job["entities"] if record["entity_id"] == entity_id]
            if not matches:
                raise FeatureJobError("风险实体不存在")
            record = matches[0]
            if record["status"] not in {"failed", "interrupted"}:
                raise FeatureJobError("只能重试失败或已中断的风险实体")
            record["status"] = "queued"
            record["error"] = None
            job["status"] = "queued"
            job["completed_at"] = None
            self._emit_locked(job, "entity_queued", entity_id=entity_id)
        if start:
            threading.Thread(target=self.run_job, args=(job_id, entity_id), daemon=True).start()

    def update_feature(self, job_id: str, candidate_id: str, changes: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(changes, dict):
            raise _invalid_feature_update("审批内容必须是 JSON object")
        candidate_id = str(candidate_id)
        allowed = {"title", "summary", "importance", "tags", "reviewer_note", "status", "review_scope"}
        unknown = set(changes) - allowed
        if unknown:
            raise _invalid_feature_update(f"不可编辑字段: {sorted(unknown)}")
        if changes.get("review_scope") not in {None, "approval_identity"}:
            raise _invalid_feature_update("字段 review_scope 无效")
        with self._lock:
            job = self._job(job_id)
            for field in ("title", "summary", "reviewer_note"):
                if field in changes:
                    if not isinstance(changes[field], str) or (field != "reviewer_note" and not changes[field].strip()):
                        raise _invalid_feature_update(f"字段 {field} 无效")
            if "importance" in changes and changes["importance"] not in IMPORTANCE_LEVELS:
                raise _invalid_feature_update("字段 importance 无效")
            if "tags" in changes:
                tags = changes["tags"]
                if not isinstance(tags, list) or not all(isinstance(tag, str) and tag.strip() for tag in tags):
                    raise _invalid_feature_update("字段 tags 必须是字符串数组")
            if "status" in changes and changes["status"] not in {"pending", "approved", "rejected"}:
                raise _invalid_feature_update("字段 status 无效")

            persisted_status: str | None = None
            expected_updated_at: Any | None = None
            original_feature: Dict[str, Any]
            if callable(getattr(self.persistence, "update_candidate_review_state", None)):
                feature = self._load_review_candidate_locked(job, candidate_id)
                original_feature = copy.deepcopy(feature)
                expected_status = str(feature.get("status") or "")
                persisted_status = expected_status
                expected_updated_at = feature.get("updated_at")
            else:
                try:
                    feature = copy.deepcopy(job["features"][candidate_id])
                except KeyError as exc:
                    raise _candidate_not_found() from exc
                original_feature = copy.deepcopy(feature)
            current_status = str(feature.get("status") or "")
            requested_status = changes.get("status")
            substantive_changes = set(changes) - {"status", "review_scope"}
            if requested_status == "approved" and current_status == "approved" and not substantive_changes:
                active_rule = self._active_rule_for_feature_locked(feature)
                if active_rule is not None:
                    self._reconcile_pending_candidates_locked(active_rule)
                    self._set_group_resolution_locked(
                        feature,
                        status="approved",
                        rule_id=str(active_rule.get("rule_id") or ""),
                    )
                    return copy.deepcopy(feature)
            if requested_status == "rejected" and current_status == "rejected" and not substantive_changes:
                if changes.get("review_scope") == "approval_identity":
                    self._reject_pending_identity_locked(feature)
                return copy.deepcopy(feature)
            record = next(
                (item for item in job.get("entities") or [] if candidate_id in (item.get("feature_ids") or [])),
                {
                    "entity_id": (feature.get("entity") or {}).get("id"),
                    "entity_type": (feature.get("entity") or {}).get("type"),
                    "cluster": feature.get("cluster"),
                    "window_start": feature.get("window_start"),
                    "window_end": feature.get("window_end"),
                    "source": {},
                },
            )
            feature, group = self._register_feature_group_locked(
                job, record, feature, persist=False
            )

            for field in ("title", "summary", "reviewer_note"):
                if field in changes:
                    feature[field] = changes[field].strip()
            if "importance" in changes:
                feature["importance"] = changes["importance"]
            if "tags" in changes:
                tags = changes["tags"]
                feature["tags"] = list(dict.fromkeys(tag.strip() for tag in tags))
            if "review_scope" in changes:
                feature["review_scope"] = changes["review_scope"]
            if "status" in changes:
                feature["status"] = changes["status"]
                feature["approved_at"] = _now() if changes["status"] == "approved" else None
                if changes["status"] == "approved":
                    feature["resolution_type"] = "manual"
                elif changes["status"] == "rejected":
                    feature["resolution_type"] = (
                        "group_rejected"
                        if changes.get("review_scope") == "approval_identity"
                        else "manual"
                    )

            review_state: Dict[str, Any] = {}
            for field in ("title", "summary", "reviewer_note", "importance", "review_scope", "tags"):
                if field in changes:
                    review_state[field] = copy.deepcopy(feature.get(field))
            for field in ("status", "approved_at", "resolved_rule_id", "rule_id", "resolution_type"):
                if feature.get(field) != original_feature.get(field):
                    review_state[field] = copy.deepcopy(feature.get(field))
            winner_updated_at: Any | None = None
            if persisted_status is not None:
                updated = self._persist_review_state_locked(
                    str(candidate_id),
                    review_state,
                    expected_status=persisted_status,
                    expected_updated_at=expected_updated_at,
                )
                if updated is not None:
                    winner_updated_at = updated.get("updated_at")
                    for field, value in feature.items():
                        if field not in REVIEW_OWNED_FIELDS and field not in updated:
                            updated[field] = copy.deepcopy(value)
                    feature = updated
                else:
                    winner_updated_at = expected_updated_at

                loader = getattr(self.persistence, "load_candidate", None) if self.persistence else None
                latest = loader(str(candidate_id)) if callable(loader) else None
                if isinstance(latest, dict) and str(latest.get("job_id") or job["job_id"]) == str(job["job_id"]):
                    for field, value in feature.items():
                        if field not in REVIEW_OWNED_FIELDS and field not in latest:
                            latest[field] = copy.deepcopy(value)
                    feature = latest

            rule: Dict[str, Any] | None = None
            if changes.get("status") == "approved" and self.rule_store:
                feature["job_id"] = job["job_id"]
                try:
                    rule = self.rule_store.upsert_feature(feature)
                except Exception:
                    if persisted_status is not None and winner_updated_at is not None:
                        rollback_changes = {
                            field: copy.deepcopy(original_feature[field])
                            for field in REVIEW_OWNED_FIELDS
                            if field in original_feature
                        }
                        rollback_changes.setdefault("status", original_feature.get("status", "pending"))
                        for field in ("approved_at", "resolved_rule_id", "rule_id", "resolution_type"):
                            rollback_changes.setdefault(field, None)
                        try:
                            restored = self._rollback_review_state_locked(
                                candidate_id,
                                rollback_changes,
                                expected_status="approved",
                                expected_updated_at=winner_updated_at,
                            )
                        except FeatureJobError:
                            try:
                                restored = self._rollback_review_state_locked(
                                    candidate_id,
                                    {
                                        "status": copy.deepcopy(original_feature.get("status", "pending")),
                                        "approved_at": copy.deepcopy(original_feature.get("approved_at")),
                                        "resolved_rule_id": copy.deepcopy(original_feature.get("resolved_rule_id")),
                                        "rule_id": copy.deepcopy(original_feature.get("rule_id")),
                                        "resolution_type": copy.deepcopy(original_feature.get("resolution_type")),
                                    },
                                    expected_status="approved",
                                    expected_updated_at=None,
                                )
                            except FeatureJobError:
                                restored = None
                        if restored is not None:
                            feature = restored
                        else:
                            feature = copy.deepcopy(original_feature)
                        job["features"][candidate_id] = feature
                    raise
                feature["rule_id"] = rule["rule_id"]
                feature["resolved_rule_id"] = rule["rule_id"]
                feature["resolution_type"] = "manual"
                if rule.get("lineage"):
                    feature["lineage"] = copy.deepcopy(rule["lineage"])
                if persisted_status is not None:
                    try:
                        updated = self._persist_review_state_locked(
                            candidate_id,
                            {
                                "rule_id": rule["rule_id"],
                                "resolved_rule_id": rule["rule_id"],
                                "resolution_type": "manual",
                            },
                            expected_status="approved",
                        )
                    except FeatureJobError as exc:
                        if getattr(exc, "code", None) not in {"candidate_not_found", "candidate_state_conflict"}:
                            raise
                        updated = None
                        loader = getattr(self.persistence, "load_candidate", None) if self.persistence else None
                        latest = loader(candidate_id) if callable(loader) else None
                        if isinstance(latest, dict):
                            feature = latest
                    if updated is not None:
                        for field, value in feature.items():
                            if field not in REVIEW_OWNED_FIELDS and field not in updated:
                                updated[field] = copy.deepcopy(value)
                        feature = updated

            job["features"][candidate_id] = feature
            if rule is not None:
                group["status"] = "approved"
                group["rule_id"] = rule["rule_id"]
                group["updated_at"] = _now()
            elif changes.get("status") == "approved":
                group["status"] = "approved"
                group["updated_at"] = _now()
            elif (
                changes.get("status") == "rejected"
                and changes.get("review_scope") == "approval_identity"
                and group.get("status") == "pending"
            ):
                group["status"] = "rejected"
                group["updated_at"] = _now()
            self._persist_feature_group_locked(job, record, group, candidate_id)

            if rule is not None:
                reconcile = self._reconcile_pending_candidates_locked(rule)
                feature["auto_resolved_count"] = reconcile["auto_resolved_candidates"]
                job["features"][candidate_id] = feature
            if changes.get("status") == "rejected" and changes.get("review_scope") == "approval_identity":
                self._reject_pending_identity_locked(feature)
                self._emit_locked(
                    job,
                    "feature_updated",
                    candidate_id=candidate_id,
                    status=feature["status"],
                    review_scope="approval_identity",
                    approval_group_id=feature.get("approval_group_id"),
                    auto_resolved_count=0,
                )
                return copy.deepcopy(job["features"][candidate_id])
            self._emit_locked(
                job,
                "feature_updated",
                candidate_id=candidate_id,
                status=feature["status"],
                approval_group_id=feature.get("approval_group_id"),
                auto_resolved_count=int(feature.get("auto_resolved_count") or 0),
            )
            return copy.deepcopy(feature)

    def list_rules(self) -> list[Dict[str, Any]]:
        return self.rule_store.list_rules() if self.rule_store else []

    def get_system_metrics(self) -> Dict[str, int]:
        return {
            "today_llm_logs": self.metrics_store.today_llm_logs() if self.metrics_store else 0,
        }

    def export_approved(self, job_id: str) -> Dict[str, Any]:
        with self._lock:
            job = self._job(job_id)
            features = list(job["features"].values())
            approved = [copy.deepcopy(feature) for feature in features if feature["status"] == "approved"]
            if not approved:
                raise FeatureJobError("导出前至少批准一条特征")
            statistics = {
                "total": len(features),
                "approved": len(approved),
                "rejected": sum(feature["status"] == "rejected" for feature in features),
                "pending": sum(feature["status"] == "pending" for feature in features),
            }
            approved_ids = {feature["candidate_id"] for feature in approved}
            nodes: Dict[tuple[str, str], Dict[str, Any]] = {}
            for record in job["entities"]:
                source = record["source"]
                if source.get("entity_type") != "node":
                    continue
                feature_ids = sorted(approved_ids.intersection(record["feature_ids"]))
                if not feature_ids:
                    continue
                key = (str(source.get("cluster") or ""), str(source.get("entity_id") or ""))
                node = nodes.get(key)
                if node is None:
                    node = {
                        "node_id": source.get("entity_id"),
                        "cluster": source.get("cluster"),
                        "risk_score": source.get("risk_score"),
                        "risk_level": source.get("risk_level"),
                        "window_start": source.get("window_start"),
                        "window_end": source.get("window_end"),
                        "log_count": 0,
                        "approved_feature_ids": [],
                        "affected_entities": [],
                    }
                    nodes[key] = node
                if float(source.get("risk_score") or 0) > float(node.get("risk_score") or 0):
                    node["risk_score"] = source.get("risk_score")
                    node["risk_level"] = source.get("risk_level")
                node["window_start"] = min(
                    value for value in (node.get("window_start"), source.get("window_start")) if value
                )
                node["window_end"] = max(
                    value for value in (node.get("window_end"), source.get("window_end")) if value
                )
                node["log_count"] += record["log_count"]
                node["approved_feature_ids"] = sorted(set(node["approved_feature_ids"]) | set(feature_ids))
                node["affected_entities"] = sorted(
                    set(node["affected_entities"]) | set(source.get("affected_entities") or [])
                )
            return {
                "schema_version": "1.0",
                "generated_at": _now(),
                "source_summary": copy.deepcopy(job["source_summary"]),
                "model": {"provider": "ollama", "name": job["model"]},
                "review_statistics": statistics,
                "approved_risk_nodes": sorted(nodes.values(), key=lambda item: str(item["node_id"])),
                "approved_features": approved,
            }
