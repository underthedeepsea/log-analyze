from __future__ import annotations

import copy
import hashlib
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict

from logrisk.approved_rules import ApprovedRuleStore
from logrisk.feature_extractor_ollama import (
    DEFAULT_OLLAMA_URL,
    FEATURE_PROMPT_ID,
    IMPORTANCE_LEVELS,
    extract_features_for_entity,
)
from logrisk.processing_metrics import ProcessingMetricsStore


class FeatureJobError(RuntimeError):
    """Raised for invalid feature extraction job operations."""


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


def _sanitized_templates(entity: Dict[str, Any]) -> list[Dict[str, Any]]:
    allowed = (
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
    return [
        {key: template.get(key) for key in allowed}
        for template in (entity.get("top_templates") or [])
        if isinstance(template, dict)
    ]


def _feature_from_rule(rule: Dict[str, Any], entity: Dict[str, Any]) -> Dict[str, Any]:
    required = {
        (str(item.get("template_hash") or ""), str(item.get("category") or ""))
        for item in (rule.get("template_signatures") or [])
    }
    sources = [
        template
        for template in _sanitized_templates(entity)
        if (
            str(template.get("template_hash") or ""),
            str(template.get("category") or ""),
        ) in required
    ]
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
        "components": sorted({str(item.get("component")) for item in sources if item.get("component")}),
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
    }


class FeatureJobManager:
    def __init__(
        self,
        extractor: Callable[..., list[Dict[str, Any]]] = extract_features_for_entity,
        rule_store: ApprovedRuleStore | None = None,
        metrics_store: ProcessingMetricsStore | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        auto_start: bool = True,
    ) -> None:
        self.extractor = extractor
        self.rule_store = rule_store
        self.metrics_store = metrics_store
        self.monotonic = monotonic
        self.auto_start = auto_start
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

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
    ) -> str:
        validate_result_document(document)
        if not isinstance(model, str) or not model.strip():
            raise FeatureJobError("必须指定 Ollama 模型")
        if timeout <= 0:
            raise FeatureJobError("Ollama timeout 必须大于 0")
        try:
            normalized_retry_count = int(retry_count)
        except (TypeError, ValueError) as exc:
            raise FeatureJobError("自动重试次数必须是非负整数") from exc
        if normalized_retry_count < 0:
            raise FeatureJobError("自动重试次数必须是非负整数")

        records = []
        initial_features: Dict[str, Dict[str, Any]] = {}
        started_monotonic = self.monotonic()
        processed_samples: list[tuple[float, int]] = []
        for source in sorted(
            document["risk_entities"],
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
            for rule in matches:
                feature = _feature_from_rule(rule, source)
                initial_features[feature["candidate_id"]] = feature
                record["feature_ids"].append(feature["candidate_id"])
                self.rule_store.record_reuse(str(rule["rule_id"]))
            if matches:
                processed_samples.append((started_monotonic, record["log_count"]))
            records.append(record)

        job_id = uuid.uuid4().hex
        condition = threading.Condition(self._lock)
        with self._lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "created_at": _now(),
                "completed_at": None,
                "model": model.strip(),
                "base_url": base_url,
                "timeout": float(timeout),
                "prompt_id": str(prompt_id or FEATURE_PROMPT_ID),
                "model_profile_id": model_profile_id,
                "cache_enabled": _cache_enabled_default() if cache_enabled is None else bool(cache_enabled),
                "retry_count": normalized_retry_count,
                "min_score": float(min_score),
                "source_summary": copy.deepcopy(document.get("summary") or {}),
                "entities": records,
                "features": initial_features,
                "events": [],
                "started_monotonic": started_monotonic,
                "processed_samples": processed_samples,
                "condition": condition,
            }
            self._emit_locked(self._jobs[job_id], "job_created")
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

    def _job(self, job_id: str) -> Dict[str, Any]:
        try:
            return self._jobs[job_id]
        except KeyError as exc:
            raise FeatureJobError("任务不存在") from exc

    def _emit_locked(self, job: Dict[str, Any], event_type: str, **payload: Any) -> None:
        event = {
            "sequence": len(job["events"]),
            "type": event_type,
            "timestamp": _now(),
            "job_id": job["job_id"],
            **payload,
        }
        job["events"].append(event)
        job["condition"].notify_all()

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
                    for feature in features:
                        feature.setdefault("origin", "ollama")
                        candidate_id = feature["candidate_id"]
                        job["features"][candidate_id] = copy.deepcopy(feature)
                        record["feature_ids"].append(candidate_id)
                    record["status"] = "completed"
                    record["error"] = None
                    job["processed_samples"].append((self.monotonic(), record["log_count"]))
                    self._emit_locked(
                        job,
                        "entity_completed",
                        entity_id=record["entity_id"],
                        feature_count=len(features),
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
                "prompt_id": job["prompt_id"],
                "model_profile_id": job.get("model_profile_id"),
                "retry_count": job.get("retry_count", 0),
                "min_score": job["min_score"],
                "source_summary": copy.deepcopy(job["source_summary"]),
                "progress": self._progress_locked(job),
                "log_statistics": self._log_statistics_locked(job),
                "live_metrics": self._live_metrics_locked(job),
                "entities": entities,
                "features": copy.deepcopy(list(job["features"].values())),
            }

    def list_jobs(self) -> list[Dict[str, Any]]:
        with self._lock:
            job_ids = sorted(self._jobs, key=lambda item: self._jobs[item]["created_at"], reverse=True)
        return [self.get_job(job_id) for job_id in job_ids]

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
            if record["status"] != "failed":
                raise FeatureJobError("只能重试失败的风险实体")
            record["status"] = "queued"
            record["error"] = None
            job["status"] = "queued"
            job["completed_at"] = None
            self._emit_locked(job, "entity_queued", entity_id=entity_id)
        if start:
            threading.Thread(target=self.run_job, args=(job_id, entity_id), daemon=True).start()

    def update_feature(self, job_id: str, candidate_id: str, changes: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(changes, dict):
            raise FeatureJobError("审批内容必须是 JSON object")
        allowed = {"title", "summary", "importance", "tags", "reviewer_note", "status"}
        unknown = set(changes) - allowed
        if unknown:
            raise FeatureJobError(f"不可编辑字段: {sorted(unknown)}")
        with self._lock:
            job = self._job(job_id)
            try:
                feature = copy.deepcopy(job["features"][candidate_id])
            except KeyError as exc:
                raise FeatureJobError("候选特征不存在") from exc

            for field in ("title", "summary", "reviewer_note"):
                if field in changes:
                    if not isinstance(changes[field], str) or (field != "reviewer_note" and not changes[field].strip()):
                        raise FeatureJobError(f"字段 {field} 无效")
                    feature[field] = changes[field].strip()
            if "importance" in changes:
                if changes["importance"] not in IMPORTANCE_LEVELS:
                    raise FeatureJobError("字段 importance 无效")
                feature["importance"] = changes["importance"]
            if "tags" in changes:
                tags = changes["tags"]
                if not isinstance(tags, list) or not all(isinstance(tag, str) and tag.strip() for tag in tags):
                    raise FeatureJobError("字段 tags 必须是字符串数组")
                feature["tags"] = list(dict.fromkeys(tag.strip() for tag in tags))
            if "status" in changes:
                if changes["status"] not in {"pending", "approved", "rejected"}:
                    raise FeatureJobError("字段 status 无效")
                feature["status"] = changes["status"]
                feature["approved_at"] = _now() if changes["status"] == "approved" else None
                if changes["status"] == "approved" and self.rule_store:
                    feature["job_id"] = job["job_id"]
                    rule = self.rule_store.upsert_feature(feature)
                    feature["rule_id"] = rule["rule_id"]
                    if rule.get("lineage"):
                        feature["lineage"] = copy.deepcopy(rule["lineage"])
            job["features"][candidate_id] = feature
            self._emit_locked(job, "feature_updated", candidate_id=candidate_id, status=feature["status"])
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
