from __future__ import annotations

import copy
import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.approved_rules import ApprovedRuleStore
from logrisk.database import SQLiteDatabase, utc_now
from logrisk.input_jobs import InputJobConfig, InputJobStore
from logrisk.upload_sessions import UploadConfig, UploadSessionStore
from logrisk.semantic.store import SemanticDictionaryStore
from logrisk.semantic.schema import SemanticValidationError
from logrisk.drain_eval.annotation_store import AnnotationStore
from logrisk.drain_eval.config_store import DrainConfigStore
from logrisk.drain_eval.dataset import DatasetStore
from logrisk.drain_eval.schema import DrainQualityError, now_iso, require_object, validate_gold_record
from logrisk.drain_eval.service import DrainQualityService
from logrisk.drain_eval.template_store import TemplateStore


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class SQLiteFeatureJobStore:
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def save(self, job: dict[str, Any]) -> None:
        snapshot = {key: copy.deepcopy(value) for key, value in job.items() if key not in {"condition", "events"}}
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO feature_jobs(job_id, status, model_profile_id, connection_snapshot_json, profile_snapshot_json, "
                "job_json, created_at, completed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, model_profile_id=excluded.model_profile_id, "
                "connection_snapshot_json=excluded.connection_snapshot_json, profile_snapshot_json=excluded.profile_snapshot_json, "
                "job_json=excluded.job_json, completed_at=excluded.completed_at, updated_at=excluded.updated_at",
                (
                    job["job_id"],
                    job.get("status", "unknown"),
                    job.get("model_profile_id"),
                    _json(job.get("connection_snapshot")) if job.get("connection_snapshot") else None,
                    _json(job.get("profile_snapshot")) if job.get("profile_snapshot") else None,
                    _json(snapshot),
                    job.get("created_at") or now,
                    job.get("completed_at"),
                    now,
                ),
            )
            connection.execute("DELETE FROM feature_job_entities WHERE job_id=?", (job["job_id"],))
            connection.execute("DELETE FROM feature_candidates WHERE job_id=?", (job["job_id"],))
            connection.execute("DELETE FROM feature_job_events WHERE job_id=?", (job["job_id"],))
            for entity in job.get("entities", []):
                connection.execute(
                    "INSERT INTO feature_job_entities(job_id, entity_id, status, risk_score, entity_json, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (job["job_id"], entity["entity_id"], entity.get("status", "unknown"), entity.get("risk_score"), _json(entity), now),
                )
            for candidate_id, candidate in (job.get("features") or {}).items():
                connection.execute(
                    "INSERT INTO feature_candidates(candidate_id, job_id, entity_id, status, candidate_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        candidate_id,
                        job["job_id"],
                        (candidate.get("entity") or {}).get("id") or candidate.get("entity_id"),
                        candidate.get("status"),
                        _json(candidate),
                        candidate.get("created_at") or now,
                        now,
                    ),
                )
            for event in job.get("events", []):
                connection.execute(
                    "INSERT INTO feature_job_events(job_id, sequence, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)",
                    (job["job_id"], int(event.get("sequence", 0)), str(event.get("type") or "event"), _json(event), event.get("timestamp") or now),
                )

    def load(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT job_id, job_json FROM feature_jobs ORDER BY created_at").fetchall()
            jobs = []
            for row in rows:
                job = json.loads(row["job_json"])
                job["entities"] = [
                    json.loads(item[0]) for item in connection.execute(
                        "SELECT entity_json FROM feature_job_entities WHERE job_id=? ORDER BY updated_at, entity_id", (row["job_id"],)
                    )
                ]
                job["features"] = {
                    item[0]: json.loads(item[1]) for item in connection.execute(
                        "SELECT candidate_id, candidate_json FROM feature_candidates WHERE job_id=? ORDER BY created_at, candidate_id", (row["job_id"],)
                    )
                }
                job["events"] = [
                    json.loads(item[0]) for item in connection.execute(
                        "SELECT event_json FROM feature_job_events WHERE job_id=? ORDER BY sequence", (row["job_id"],)
                    )
                ]
                jobs.append(job)
        return jobs


class SQLiteApprovedRuleStore(ApprovedRuleStore):
    def __init__(self, database: SQLiteDatabase, clock: Callable[[], str] = utc_now) -> None:
        self.database = database
        self.clock = clock

    def _read_locked(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT rule_json, status, current_version, next_review_at, schema_version, approved_at, updated_at "
                "FROM approved_rules ORDER BY rule_id"
            )
            rules = []
            for row in rows:
                rule = json.loads(row["rule_json"])
                rule.update({
                    "status": row["status"],
                    "current_version": int(row["current_version"]),
                    "next_review_at": row["next_review_at"],
                    "schema_version": row["schema_version"],
                    "approved_at": row["approved_at"],
                    "created_at": rule.get("created_at") or row["approved_at"],
                    "updated_at": row["updated_at"],
                })
                rules.append(rule)
            return rules

    def _write_locked(self, rules: list[dict[str, Any]]) -> None:
        with self.database.transaction() as connection:
            for rule in rules:
                connection.execute(
                    "INSERT INTO approved_rules(rule_id, signature, feature_type, rule_json, approved_at, updated_at, "
                    "status, current_version, next_review_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(rule_id) DO UPDATE SET signature=excluded.signature, feature_type=excluded.feature_type, "
                    "rule_json=excluded.rule_json, updated_at=excluded.updated_at, status=excluded.status, "
                    "current_version=excluded.current_version, next_review_at=excluded.next_review_at, "
                    "schema_version=excluded.schema_version",
                    (
                        rule["rule_id"], rule["signature"], rule["feature_type"], _json(rule),
                        rule["approved_at"], rule["updated_at"], rule.get("status", "active"),
                        int(rule.get("current_version") or 1), rule.get("next_review_at"),
                        rule.get("schema_version", "approved_rule_v2"),
                    ),
                )
                version = int(rule.get("current_version") or 1)
                change_type = "rule_created" if version == 1 else "rule_updated"
                created = connection.execute(
                    "INSERT INTO rule_versions(rule_id, version, rule_json, change_type, change_reason, "
                    "operator, created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(rule_id, version) DO NOTHING",
                    (
                        rule["rule_id"], version, _json(rule), change_type,
                        "人工批准特征" if version == 1 else "人工审批更新规则",
                        "manual-approval", rule["updated_at"], "rule_version_v1",
                    ),
                )
                if created.rowcount:
                    connection.execute(
                        "INSERT INTO rule_audit_events(event_id, rule_id, event_type, from_version, to_version, "
                        "event_json, operator, created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            f"rule-event-{uuid.uuid4().hex}", rule["rule_id"], change_type,
                            version - 1 if version > 1 else None, version,
                            _json({"reason": "人工审批写入规则库"}), "manual-approval",
                            rule["updated_at"], "rule_audit_event_v1",
                        ),
                    )

    def record_reuse(
        self,
        rule_id: str,
        *,
        job_id: str | None = None,
        entity_id: str | None = None,
        cluster: str | None = None,
    ) -> dict[str, Any]:
        rule = super().record_reuse(rule_id, job_id=job_id, entity_id=entity_id, cluster=cluster)
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO rule_reuse_events(rule_id, job_id, entity_id, reused_at, cluster, schema_version) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (rule_id, job_id, entity_id, rule["last_reused_at"], cluster, "rule_reuse_event_v2"),
            )
        return rule


class SQLiteAITraceLogger(AITraceLogger):
    def __init__(self, database: SQLiteDatabase, enabled: bool = True) -> None:
        self.database = database
        self.path = database.state_root / "ai_traces.jsonl"
        self.enabled = enabled

    def append(self, trace: dict[str, Any]) -> None:
        if not self.enabled:
            return
        trace_id = str(trace.get("trace_id") or "")
        if not trace_id:
            raise ValueError("trace_id 不能为空")
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO ai_traces(trace_id, job_id, provider, model, status, prompt_id, prompt_hash, latency_ms, trace_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(trace_id) DO UPDATE SET "
                "job_id=excluded.job_id, provider=excluded.provider, model=excluded.model, status=excluded.status, "
                "prompt_id=excluded.prompt_id, prompt_hash=excluded.prompt_hash, latency_ms=excluded.latency_ms, "
                "trace_json=excluded.trace_json, created_at=excluded.created_at",
                (
                    trace_id,
                    trace.get("job_id"),
                    trace.get("provider"),
                    trace.get("model"),
                    trace.get("status"),
                    trace.get("prompt_id"),
                    trace.get("prompt_hash"),
                    trace.get("latency_ms"),
                    _json(trace),
                    trace.get("created_at") or utc_now(),
                ),
            )

    def _read(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT trace_json FROM ai_traces")]


class SQLiteAICache:
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def get(self, signature: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute("SELECT value_json FROM ai_cache_entries WHERE signature=?", (signature,)).fetchone()
        return json.loads(row[0]) if row else None

    def set(self, signature: str, value: dict[str, Any]) -> None:
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO ai_cache_entries(signature, value_json, created_at, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(signature) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
                (signature, _json(value), now, now),
            )


class SQLiteProcessingMetricsStore:
    def __init__(self, database: SQLiteDatabase, today: Callable[[], date] = date.today) -> None:
        self.database = database
        self.today = today

    def today_llm_logs(self) -> int:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT llm_logs FROM processing_metrics_daily WHERE metric_date=?", (self.today().isoformat(),)
            ).fetchone()
        return int(row[0]) if row else 0

    def add_llm_logs(self, count: int) -> int:
        value = int(count)
        if value < 0:
            raise ValueError("LLM 关联日志量不能为负数")
        key = self.today().isoformat()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO processing_metrics_daily(metric_date, llm_logs, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(metric_date) DO UPDATE SET llm_logs=llm_logs+excluded.llm_logs, updated_at=excluded.updated_at",
                (key, value, utc_now()),
            )
            return int(connection.execute(
                "SELECT llm_logs FROM processing_metrics_daily WHERE metric_date=?", (key,)
            ).fetchone()[0])


class SQLiteUploadSessionStore(UploadSessionStore):
    def __init__(self, config: UploadConfig, database: SQLiteDatabase) -> None:
        super().__init__(config)
        self.database = database

    def get(self, upload_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT manifest_json FROM upload_sessions WHERE upload_id=?", (upload_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"Upload not found: {upload_id}")
        return json.loads(row[0])

    def _write_manifest(self, upload_id: str, manifest: dict[str, Any]) -> None:
        source = self._root(upload_id) / "source.log"
        source_reference = manifest.get("artifact_relative_path") or (str(source) if source.is_file() else None)
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO upload_sessions(upload_id, status, filename, source_path, size_bytes, sha256, manifest_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(upload_id) DO UPDATE SET status=excluded.status, "
                "source_path=excluded.source_path, sha256=excluded.sha256, manifest_json=excluded.manifest_json, updated_at=excluded.updated_at",
                (
                    upload_id,
                    manifest["status"],
                    manifest["filename"],
                    source_reference,
                    int(manifest["size_bytes"]),
                    manifest.get("sha256"),
                    _json(manifest),
                    manifest["created_at"],
                    manifest["updated_at"],
                ),
            )

    def complete(self, *, upload_id: str, final_sha256: str | None = None) -> dict[str, Any]:
        manifest = super().complete(upload_id=upload_id, final_sha256=final_sha256)
        (self._root(upload_id) / "upload.done").unlink(missing_ok=True)
        return manifest


class SQLiteInputJobStore(InputJobStore):
    def __init__(self, config: InputJobConfig, database: SQLiteDatabase) -> None:
        super().__init__(config)
        self.database = database

    def get_job(self, input_job_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT job_json FROM input_jobs WHERE input_job_id=?", (input_job_id,)).fetchone()
        if row is None:
            raise KeyError(f"Input job not found: {input_job_id}")
        return json.loads(row[0])

    def get_progress(self, input_job_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT job_json, progress_json FROM input_jobs WHERE input_job_id=?", (input_job_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"Input job not found: {input_job_id}")
        return {**json.loads(row[0]), **json.loads(row[1])}

    def get_result(self, input_job_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT result_json FROM input_jobs WHERE input_job_id=?", (input_job_id,)).fetchone()
        if row is None or row[0] is None:
            raise KeyError(f"Input job result not found: {input_job_id}")
        return json.loads(row[0])

    def write_job(self, input_job_id: str, job: dict[str, Any]) -> None:
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO input_jobs(input_job_id, upload_id, status, stage, job_json, progress_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, '{}', ?, ?) ON CONFLICT(input_job_id) DO UPDATE SET status=excluded.status, "
                "stage=excluded.stage, job_json=excluded.job_json, updated_at=excluded.updated_at",
                (
                    input_job_id,
                    job.get("upload_id"),
                    job.get("status", "queued"),
                    job.get("stage", "queued"),
                    _json(job),
                    job.get("created_at") or now,
                    now,
                ),
            )

    def write_progress(self, input_job_id: str, progress: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE input_jobs SET status=?, stage=?, progress_json=?, updated_at=? WHERE input_job_id=?",
                (
                    progress.get("status", "queued"),
                    progress.get("stage", "queued"),
                    _json(progress),
                    utc_now(),
                    input_job_id,
                ),
            )

    def write_result(self, input_job_id: str, result: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE input_jobs SET result_json=?, updated_at=? WHERE input_job_id=?",
                (_json(result), utc_now(), input_job_id),
            )


class SQLiteSemanticDictionaryStore(SemanticDictionaryStore):
    def __init__(self, database: SQLiteDatabase, builtin_root: str | Path) -> None:
        self.database = database
        super().__init__(database.state_root / "semantic-artifacts", builtin_root)
        self._seed_database()

    def _seed_database(self) -> None:
        now = utc_now()
        with self.database.transaction() as connection:
            for dictionary_id, builtin in self._builtins.items():
                metadata = {"latest_version": 1, "active_version": 1, "versions": [1]}
                connection.execute(
                    "INSERT INTO semantic_dictionaries(dictionary_id, display_name, active_version, dictionary_json, updated_at) VALUES (?, ?, 1, ?, ?) "
                    "ON CONFLICT(dictionary_id) DO NOTHING",
                    (dictionary_id, builtin.get("name"), _json(metadata), now),
                )
                payload = {"dictionary_id": dictionary_id, "version": 1, "custom_rules": []}
                connection.execute(
                    "INSERT INTO semantic_dictionary_versions(dictionary_id, version, status, dictionary_json, content_hash, created_at) VALUES (?, 1, 'published', ?, ?, ?) "
                    "ON CONFLICT(dictionary_id, version) DO NOTHING",
                    (dictionary_id, _json(payload), builtin.get("content_hash") or hashlib.sha256(_json(builtin).encode()).hexdigest(), now),
                )

    def _catalog(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT dictionary_id, dictionary_json, active_version FROM semantic_dictionaries").fetchall()
        items = {}
        for row in rows:
            metadata = json.loads(row["dictionary_json"])
            metadata["active_version"] = int(row["active_version"])
            items[row["dictionary_id"]] = metadata
        return {"schema_version": "semantic_catalog_v1", "items": items}

    def _write_catalog(self, catalog: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            for dictionary_id, metadata in catalog["items"].items():
                connection.execute(
                    "UPDATE semantic_dictionaries SET active_version=?, dictionary_json=?, updated_at=? WHERE dictionary_id=?",
                    (int(metadata.get("active_version", 1)), _json(metadata), utc_now(), dictionary_id),
                )

    def _read_version_payload(self, dictionary_id: str, version: int) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT dictionary_json FROM semantic_dictionary_versions WHERE dictionary_id=? AND version=?",
                (dictionary_id, int(version)),
            ).fetchone()
        if row is None:
            raise SemanticValidationError("语义词典版本不存在")
        return json.loads(row[0])

    def _write_version_payload(self, dictionary_id: str, version: int, payload: dict[str, Any]) -> None:
        digest = hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO semantic_dictionary_versions(dictionary_id, version, status, dictionary_json, content_hash, created_at) VALUES (?, ?, 'candidate', ?, ?, ?)",
                (dictionary_id, int(version), _json(payload), digest, utc_now()),
            )

    def _write_validation(self, dictionary_id: str, version: int, report: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO semantic_validation_runs(dictionary_id, version, validation_json, created_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(dictionary_id, version) DO UPDATE SET validation_json=excluded.validation_json, created_at=excluded.created_at",
                (dictionary_id, int(version), _json(report), utc_now()),
            )

    def _read_validation(self, dictionary_id: str, version: int) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT validation_json FROM semantic_validation_runs WHERE dictionary_id=? AND version=?",
                (dictionary_id, int(version)),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def _append_event(self, event: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO semantic_events(dictionary_id, version, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (event.get("dictionary_id"), event.get("version"), event.get("action", "event"), _json(event), event.get("created_at") or utc_now()),
            )


class SQLiteDatasetStore(DatasetStore):
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def _read(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            items = [json.loads(row[0]) for row in connection.execute("SELECT dataset_json FROM drain_datasets ORDER BY created_at")]
        return {"schema_version": "drain_dataset_index_v1", "items": items}

    def create(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        name, records = source.get("name"), source.get("records")
        if not isinstance(name, str) or not name.strip() or not isinstance(records, list) or not records:
            raise DrainQualityError("Dataset name 和 records 不能为空")
        validated = [validate_gold_record(record) for record in records]
        if len({row["record_id"] for row in validated}) != len(validated):
            raise DrainQualityError("Dataset record_id 不可重复")
        now = now_iso()
        item = {
            "schema_version": "drain_dataset_v1",
            "dataset_id": str(source.get("dataset_id") or f"dataset_{uuid.uuid4().hex[:12]}"),
            "name": name.strip(), "description": str(source.get("description") or ""),
            "version": str(source.get("version") or "1.0.0"), "split": str(source.get("split") or "validation"),
            "record_count": len(validated), "records": validated, "created_at": now, "updated_at": now,
        }
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_datasets(dataset_id, name, version, dataset_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (item["dataset_id"], item["name"], item["version"], _json(item), now, now),
            )
        return item


class SQLiteAnnotationStore(AnnotationStore):
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def _annotations(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT annotation_json FROM drain_annotations ORDER BY created_at")]

    def _reviews(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT review_json FROM drain_reviews ORDER BY created_at")]

    def append(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        action, cluster_id = source.get("action"), source.get("cluster_id")
        if action not in self.ACTIONS or not isinstance(cluster_id, str) or not cluster_id:
            raise DrainQualityError("标注 action 无效或 cluster_id 为空")
        if action in {"split", "merge"} and not source.get("target_cluster_ids"):
            raise DrainQualityError("拆分或合并必须提供 target_cluster_ids")
        event = {
            "schema_version": "drain_annotation_event_v1", "annotation_id": f"annotation_{uuid.uuid4().hex}",
            "cluster_id": cluster_id, "action": action, "template": source.get("template"),
            "target_cluster_ids": list(source.get("target_cluster_ids") or []),
            "reviewer": str(source.get("reviewer") or "local-operator"), "note": str(source.get("note") or ""),
            "created_at": now_iso(),
        }
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_annotations(annotation_id, dataset_id, annotation_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (event["annotation_id"], source.get("dataset_id"), _json(event), event["created_at"], event["created_at"]),
            )
        return event

    def review(self, annotation_id: str, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        if source.get("decision") not in {"approved", "rejected", "changes_requested"}:
            raise DrainQualityError("复核 decision 无效")
        if not any(item["annotation_id"] == annotation_id for item in self._annotations()):
            raise DrainQualityError("标注事件不存在")
        event = {
            "schema_version": "drain_annotation_review_v1", "review_id": f"review_{uuid.uuid4().hex}",
            "annotation_id": annotation_id, "decision": source["decision"],
            "reviewer": str(source.get("reviewer") or "local-reviewer"), "note": str(source.get("note") or ""),
            "created_at": now_iso(),
        }
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_reviews(review_id, annotation_id, review_json, created_at) VALUES (?, ?, ?, ?)",
                (event["review_id"], annotation_id, _json(event), event["created_at"]),
            )
        return event

    def replay(self) -> dict[str, dict[str, Any]]:
        state, by_annotation = {}, {}
        for event in self._annotations():
            current = state.setdefault(event["cluster_id"], {"cluster_id": event["cluster_id"]})
            current.update({"status": {"accept": "accepted", "edit": "edited", "split": "split", "merge": "merged", "ignore": "ignored"}[event["action"]], "annotation_id": event["annotation_id"], "updated_at": event["created_at"]})
            for key in ("template", "target_cluster_ids"):
                if event.get(key): current[key] = event[key]
            by_annotation[event["annotation_id"]] = event["cluster_id"]
        for review in self._reviews():
            cluster_id = by_annotation.get(review["annotation_id"])
            if cluster_id:
                state[cluster_id].update({"review_status": review["decision"], "reviewed_at": review["created_at"]})
        return state

    def events(self) -> list[dict[str, Any]]:
        return self._annotations()


class SQLiteTemplateStore(TemplateStore):
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    def _catalog(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            items = {row[0]: json.loads(row[1]) for row in connection.execute("SELECT template_hash, template_json FROM drain_templates")}
        return {"schema_version": "drain_template_catalog_v1", "items": items}

    def _write_catalog(self, catalog: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            for template_hash, item in catalog["items"].items():
                connection.execute(
                    "INSERT INTO drain_templates(template_hash, component, status, template_json, updated_at) VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(template_hash) DO UPDATE SET component=excluded.component, status=excluded.status, template_json=excluded.template_json, updated_at=excluded.updated_at",
                    (template_hash, item.get("component"), item.get("status"), _json(item), item.get("updated_at") or utc_now()),
                )
                connection.execute(
                    "INSERT INTO drain_template_versions(template_hash, version, template_json, created_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(template_hash, version) DO NOTHING",
                    (template_hash, int(item.get("version", 1)), _json(item), item.get("updated_at") or utc_now()),
                )

    def _events(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT event_json FROM drain_template_events ORDER BY event_id")]

    def _append_event(self, event: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_template_events(template_hash, event_type, event_json, created_at) VALUES (?, ?, ?, ?)",
                (event.get("template_hash"), event.get("action", "event"), _json(event), event.get("created_at") or utc_now()),
            )


class SQLiteDrainConfigStore(DrainConfigStore):
    def __init__(self, database: SQLiteDatabase, baseline_path: str | Path) -> None:
        self.database = database
        super().__init__(database.state_root / "drain-config-artifacts", baseline_path)

    def _catalog(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT config_id, config_json FROM drain_config_versions WHERE (config_id, version) IN "
                "(SELECT config_id, MAX(version) FROM drain_config_versions GROUP BY config_id)"
            ).fetchall()
        return {"schema_version": "drain_config_catalog_v1", "items": {row["config_id"]: json.loads(row["config_json"]) for row in rows}}

    def _write_catalog(self, catalog: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            for config_id, metadata in catalog["items"].items():
                for version_meta in metadata.get("versions", []):
                    version = int(version_meta["version"])
                    path = self.configs_root / config_id / f"{version}.ini"
                    content = path.read_text(encoding="utf-8")
                    connection.execute(
                        "INSERT INTO drain_config_versions(config_id, version, status, content, content_hash, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(config_id, version) DO UPDATE SET status=excluded.status, config_json=excluded.config_json",
                        (config_id, version, metadata.get("status", "candidate"), content, self._content_hash(content), _json(metadata), version_meta.get("created_at") or metadata.get("created_at") or utc_now()),
                    )

    def get_version(self, config_id: str, version: int) -> dict[str, Any]:
        if config_id != "baseline":
            path = self.configs_root / config_id / f"{int(version)}.ini"
            if not path.is_file():
                with self.database.connect() as connection:
                    row = connection.execute("SELECT content FROM drain_config_versions WHERE config_id=? AND version=?", (config_id, int(version))).fetchone()
                if row:
                    self._write_ini(path, row[0])
        return super().get_version(config_id, version)

    def _active_reference(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT value_json FROM app_settings WHERE setting_key='active_drain_config'").fetchone()
        return json.loads(row[0]) if row else {"config_id": "baseline", "version": 1}

    def _append_event(self, event: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_config_events(config_id, version, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (event.get("config_id"), event.get("version"), event.get("action", "event"), _json(event), event.get("updated_at") or utc_now()),
            )

    def _activate(self, config_id: str, version: int, payload: dict[str, Any], action: str) -> dict[str, Any]:
        if payload.get("confirmed") is not True:
            raise DrainQualityError("配置变更需要人工确认")
        if config_id == "baseline" and action == "publish":
            raise DrainQualityError("系统基线无需发布")
        snapshot = self.get_version(config_id, version)
        now = now_iso()
        reference = {"schema_version": "drain_active_config_v1", "config_id": config_id, "version": int(version), "content_hash": snapshot["content_hash"], "updated_at": now, "updated_by": str(payload.get("operator") or "local-operator")}
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO app_settings(setting_key, value_json, updated_at) VALUES ('active_drain_config', ?, ?) "
                "ON CONFLICT(setting_key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
                (_json(reference), now),
            )
        self._append_event({"schema_version": "drain_config_event_v1", "event_id": f"config_event_{uuid.uuid4().hex}", "action": action, **reference})
        return dict(snapshot, status="published", updated_at=now)


class SQLiteDrainQualityService(DrainQualityService):
    def __init__(self, database: SQLiteDatabase, profiles_root: str | Path, baseline_path: str | Path) -> None:
        super().__init__(database.state_root / "drain-quality-artifacts", profiles_root, baseline_path)
        self.database = database
        self.datasets = SQLiteDatasetStore(database)
        self.annotations = SQLiteAnnotationStore(database)
        self.templates = SQLiteTemplateStore(database)
        self.configs = SQLiteDrainConfigStore(database, baseline_path)

    def create_eval_run(self, payload: Any) -> dict[str, Any]:
        result = super().create_eval_run(payload)
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_eval_runs(evaluation_id, status, evaluation_json, created_at, completed_at) VALUES (?, ?, ?, ?, ?)",
                (result["run_id"], result["status"], _json(result), result["created_at"], result["updated_at"]),
            )
        return result

    def get_eval_run(self, run_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT evaluation_json FROM drain_eval_runs WHERE evaluation_id=?", (run_id,)).fetchone()
        if row is None:
            raise DrainQualityError("评测任务不存在")
        return json.loads(row[0])

    def list_eval_runs(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT evaluation_json FROM drain_eval_runs ORDER BY created_at DESC")]

    def create_tune_run(self, payload: Any) -> dict[str, Any]:
        result = super().create_tune_run(payload)
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_tune_runs(tune_run_id, status, tune_json, created_at, completed_at) VALUES (?, ?, ?, ?, ?)",
                (result["run_id"], result["status"], _json(result), result["created_at"], result["updated_at"]),
            )
        return result

    def get_tune_run(self, run_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT tune_json FROM drain_tune_runs WHERE tune_run_id=?", (run_id,)).fetchone()
        if row is None:
            raise DrainQualityError("调参任务不存在")
        return json.loads(row[0])

    def _profile_events(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT event_json FROM drain_config_events WHERE event_type LIKE 'profile_%' ORDER BY event_id")]

    def _append_profile_event(self, event: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO drain_config_events(config_id, event_type, event_json, created_at) VALUES (?, ?, ?, ?)",
                (event.get("profile_id"), "profile_" + event.get("status", "event"), _json(event), event.get("created_at") or utc_now()),
            )
