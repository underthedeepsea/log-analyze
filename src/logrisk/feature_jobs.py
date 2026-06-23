from __future__ import annotations

import copy
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict

from logrisk.feature_extractor_ollama import (
    DEFAULT_OLLAMA_URL,
    IMPORTANCE_LEVELS,
    extract_features_for_entity,
)


class FeatureJobError(RuntimeError):
    """Raised for invalid feature extraction job operations."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


class FeatureJobManager:
    def __init__(
        self,
        extractor: Callable[..., list[Dict[str, Any]]] = extract_features_for_entity,
        auto_start: bool = True,
    ) -> None:
        self.extractor = extractor
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
    ) -> str:
        validate_result_document(document)
        if not isinstance(model, str) or not model.strip():
            raise FeatureJobError("必须指定 Ollama 模型")
        if timeout <= 0:
            raise FeatureJobError("Ollama timeout 必须大于 0")

        records = []
        for source in sorted(
            document["risk_entities"],
            key=lambda item: float(item.get("risk_score") or 0),
            reverse=True,
        ):
            score = float(source.get("risk_score") or 0)
            records.append({
                "entity_id": str(source.get("entity_id")),
                "entity_type": source.get("entity_type"),
                "cluster": source.get("cluster"),
                "window_start": source.get("window_start"),
                "window_end": source.get("window_end"),
                "risk_score": score,
                "risk_level": source.get("risk_level"),
                "log_count": _entity_log_count(source),
                "affected_entities": copy.deepcopy(source.get("affected_entities") or []),
                "status": "queued" if score >= min_score else "skipped",
                "error": None,
                "feature_ids": [],
                "source": copy.deepcopy(source),
            })

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
                "min_score": float(min_score),
                "source_summary": copy.deepcopy(document.get("summary") or {}),
                "entities": records,
                "features": {},
                "events": [],
                "condition": condition,
            }
            self._emit_locked(self._jobs[job_id], "job_created")

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
                features = self.extractor(
                    copy.deepcopy(record["source"]),
                    model=job["model"],
                    base_url=job["base_url"],
                    timeout=job["timeout"],
                )
                with self._lock:
                    record["feature_ids"] = []
                    for feature in features:
                        candidate_id = feature["candidate_id"]
                        job["features"][candidate_id] = copy.deepcopy(feature)
                        record["feature_ids"].append(candidate_id)
                    record["status"] = "completed"
                    record["error"] = None
                    self._emit_locked(
                        job,
                        "entity_completed",
                        entity_id=record["entity_id"],
                        feature_count=len(features),
                    )
            except Exception as exc:
                with self._lock:
                    record["status"] = "failed"
                    record["error"] = str(exc)
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
        completed = sum(record["status"] == "completed" for record in eligible)
        failed = sum(record["status"] == "failed" for record in eligible)
        finished = completed + failed
        total = len(eligible)
        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "percent": round(finished / total * 100) if total else 100,
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
                record["log_count"] for record in eligible if record["status"] == "completed"
            ),
            "pending_logs": sum(
                record["log_count"]
                for record in eligible
                if record["status"] in {"queued", "running", "failed"}
            ),
            "skipped_logs": sum(
                record["log_count"] for record in job["entities"] if record["status"] == "skipped"
            ),
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
                "min_score": job["min_score"],
                "source_summary": copy.deepcopy(job["source_summary"]),
                "progress": self._progress_locked(job),
                "log_statistics": self._log_statistics_locked(job),
                "entities": entities,
                "features": copy.deepcopy(list(job["features"].values())),
            }

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
                feature = job["features"][candidate_id]
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
            self._emit_locked(job, "feature_updated", candidate_id=candidate_id, status=feature["status"])
            return copy.deepcopy(feature)

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
