from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from logrisk.approved_rules import (
    ApprovedRuleError,
    RuleFormat,
    RuleNormalizationSource,
    classify_rule,
    normalize_legacy_rule_version,
    public_rule,
)
from logrisk.database import SQLiteDatabase, utc_now


class LegacyStateImporter:
    """One-time importer for mutable JSON/JSONL state left by pre-SQLite releases."""

    def __init__(self, database: SQLiteDatabase, state_root: str | Path, input_jobs_root: str | Path | None = None) -> None:
        self.database = database
        self.state_root = Path(state_root)
        self.input_jobs_root = Path(input_jobs_root) if input_jobs_root else None

    @staticmethod
    def _read_json(path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    @staticmethod
    def _digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def run(self) -> dict[str, int]:
        candidates = [
            self.state_root / "approved_rules.json",
            self.state_root / "ai_traces.jsonl",
            self.state_root / "ai_cache.json",
            self.state_root / "processing_metrics.json",
            self.state_root / "prompt_versions.json",
            self.state_root / "drain_quality" / "template_overrides.json",
            self.state_root / "drain_quality" / "template_events.jsonl",
            self.state_root / "drain_quality" / "datasets.json",
            self.state_root / "drain_quality" / "annotations.jsonl",
            self.state_root / "drain_quality" / "reviews.jsonl",
            self.state_root / "drain_quality" / "config_catalog.json",
            self.state_root / "drain_quality" / "active_config.json",
            self.state_root / "semantic_dictionaries" / "catalog.json",
            self.state_root / "semantic_dictionaries" / "events.jsonl",
        ]
        candidates += sorted((self.state_root / "feature_jobs").glob("*/snapshot.json"))
        candidates += sorted((self.state_root / "uploads").glob("*/manifest.json"))
        candidates += sorted((self.state_root / "drain_quality" / "eval_runs").glob("*/summary.json"))
        candidates += sorted((self.state_root / "drain_quality" / "tune_runs").glob("*/summary.json"))
        candidates += sorted((self.state_root / "semantic_dictionaries").glob("*/versions/*.json"))
        candidates += sorted((self.state_root / "semantic_dictionaries" / "validations").glob("*/*.json"))
        if self.input_jobs_root:
            candidates += sorted(self.input_jobs_root.glob("*/job.json"))
        existing = [path for path in candidates if path.is_file()]
        with self.database.connect() as connection:
            imported = {row[0]: row[1] for row in connection.execute("SELECT source_path, source_sha256 FROM legacy_imports")}
        pending = [(path, self._digest(path)) for path in existing if imported.get(str(path)) != self._digest(path)]
        if not pending:
            return {"files_imported": 0, "records_imported": 0}
        records = 0
        with self.database.transaction() as connection:
            for path, digest in pending:
                count = self._import_path(connection, path)
                connection.execute(
                    "INSERT INTO legacy_imports(source_path, source_sha256, records_imported, imported_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(source_path) DO UPDATE SET source_sha256=excluded.source_sha256, records_imported=excluded.records_imported, imported_at=excluded.imported_at",
                    (str(path), digest, count, utc_now()),
                )
                records += count
        return {"files_imported": len(pending), "records_imported": records}

    def _import_path(self, connection: Any, path: Path) -> int:
        if path.name == "approved_rules.json":
            rules = self._read_json(path).get("rules") or []
            imported = 0
            for rule in rules:
                try:
                    normalized = normalize_legacy_rule_version(
                        rule,
                        source=RuleNormalizationSource.LEGACY_IMPORT,
                    )
                except ApprovedRuleError:
                    continue
                approved_at = normalized.get("approved_at") or normalized.get("created_at") or utc_now()
                updated_at = normalized.get("updated_at") or approved_at
                snapshot = {
                    **normalized,
                    "status": str(normalized.get("status") or "active"),
                    "current_version": int(normalized.get("current_version") or 1),
                    "created_at": normalized.get("created_at") or approved_at,
                    "next_review_at": normalized.get("next_review_at"),
                }
                classification = classify_rule(snapshot)
                if classification.kind == RuleFormat.MALFORMED_V2:
                    continue
                persisted = public_rule(snapshot)
                projection = (None, None) if classification.kind == RuleFormat.LEGACY_V1 else (
                    persisted.get("problem_code"), persisted.get("approval_key")
                )
                connection.execute(
                    "INSERT INTO approved_rules(rule_id, signature, feature_type, rule_json, approved_at, "
                    "updated_at, status, current_version, next_review_at, schema_version, problem_code, approval_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (
                        persisted["rule_id"], persisted["signature"], persisted["feature_type"],
                        json.dumps(persisted, ensure_ascii=False), approved_at, updated_at,
                        snapshot["status"], snapshot["current_version"], snapshot["next_review_at"],
                        persisted["schema_version"], projection[0], projection[1],
                    ),
                )
                connection.execute(
                    "INSERT INTO rule_versions(rule_id, version, rule_json, change_type, change_reason, "
                    "operator, created_at, schema_version) VALUES (?, ?, ?, 'legacy_import', ?, 'legacy-importer', ?, 'rule_version_v1') ON CONFLICT DO NOTHING",
                    (
                        persisted["rule_id"], snapshot["current_version"], json.dumps(persisted, ensure_ascii=False),
                        "由旧批准规则文件导入", updated_at,
                    ),
                )
                imported += 1
            return imported
        if path.name == "ai_traces.jsonl":
            traces = self._read_jsonl(path)
            for trace in traces:
                connection.execute(
                    "INSERT INTO ai_traces(trace_id, job_id, provider, model, status, prompt_id, prompt_hash, latency_ms, trace_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (
                        trace["trace_id"], trace.get("job_id"), trace.get("provider"), trace.get("model"),
                        trace.get("status"), trace.get("prompt_id"), trace.get("prompt_hash"), trace.get("latency_ms"),
                        json.dumps(trace, ensure_ascii=False), trace.get("created_at") or utc_now(),
                    ),
                )
            return len(traces)
        if path.name == "ai_cache.json":
            values = self._read_json(path)
            now = utc_now()
            for signature, value in values.items():
                connection.execute(
                    "INSERT INTO ai_cache_entries(signature, value_json, created_at, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (signature, json.dumps(value, ensure_ascii=False), now, now),
                )
            return len(values)
        if path.name == "processing_metrics.json":
            days = self._read_json(path).get("days") or {}
            for metric_date, count in days.items():
                connection.execute(
                    "INSERT INTO processing_metrics_daily(metric_date, llm_logs, updated_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                    (metric_date, int(count), utc_now()),
                )
            return len(days)
        if path.name == "prompt_versions.json":
            history = self._read_json(path)
            imported = 0
            for prompt_id, versions in history.items():
                current = connection.execute(
                    "SELECT t.current_version, v.content, v.content_sha256 FROM prompt_templates t JOIN prompt_versions v "
                    "ON v.prompt_id=t.prompt_id AND v.version=t.current_version WHERE t.prompt_id=?", (prompt_id,)
                ).fetchone()
                if current is None:
                    continue
                ordered = list(reversed(versions if isinstance(versions, list) else []))
                connection.execute("DELETE FROM prompt_versions WHERE prompt_id=?", (prompt_id,))
                for number, item in enumerate(ordered, 1):
                    content = str(item.get("content") or "")
                    digest = str(item.get("sha256") or hashlib.sha256(content.encode()).hexdigest())
                    connection.execute(
                        "INSERT INTO prompt_versions(prompt_id, version, content, content_sha256, note, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (prompt_id, number, content, digest, item.get("note"), item.get("saved_at") or utc_now()),
                    )
                number = len(ordered) + 1
                connection.execute(
                    "INSERT INTO prompt_versions(prompt_id, version, content, content_sha256, note, created_at) VALUES (?, ?, ?, ?, 'legacy-current', ?)",
                    (prompt_id, number, current[1], current[2], utc_now()),
                )
                connection.execute("UPDATE prompt_templates SET current_version=?, updated_at=? WHERE prompt_id=?", (number, utc_now(), prompt_id))
                imported += len(ordered)
            return imported
        if path.name == "snapshot.json" and path.parent.parent.name == "feature_jobs":
            job = self._read_json(path)
            events_path = path.with_name("events.jsonl")
            events = self._read_jsonl(events_path) if events_path.is_file() else []
            snapshot = dict(job)
            connection.execute(
                "INSERT INTO feature_jobs(job_id, status, model_profile_id, job_json, created_at, completed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (job["job_id"], job.get("status", "unknown"), job.get("model_profile_id"), json.dumps(snapshot, ensure_ascii=False), job.get("created_at") or utc_now(), job.get("completed_at"), utc_now()),
            )
            for entity in job.get("entities", []):
                connection.execute(
                    "INSERT INTO feature_job_entities(job_id, entity_id, status, risk_score, entity_json, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (job["job_id"], entity["entity_id"], entity.get("status", "unknown"), entity.get("risk_score"), json.dumps(entity, ensure_ascii=False), utc_now()),
                )
            for candidate_id, candidate in (job.get("features") or {}).items():
                connection.execute(
                    "INSERT INTO feature_candidates(candidate_id, job_id, entity_id, status, candidate_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (candidate_id, job["job_id"], (candidate.get("entity") or {}).get("id"), candidate.get("status"), json.dumps(candidate, ensure_ascii=False), candidate.get("created_at") or utc_now(), utc_now()),
                )
            for event in events:
                connection.execute(
                    "INSERT INTO feature_job_events(job_id, sequence, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (job["job_id"], int(event.get("sequence", 0)), event.get("type", "event"), json.dumps(event, ensure_ascii=False), event.get("timestamp") or utc_now()),
                )
            return 1 + len(events)
        if path.name == "manifest.json" and path.parent.parent.name == "uploads":
            manifest = self._read_json(path)
            source = path.parent / "source.log"
            connection.execute(
                "INSERT INTO upload_sessions(upload_id, status, filename, source_path, size_bytes, sha256, manifest_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (
                    manifest["upload_id"], manifest["status"], manifest["filename"], str(source) if source.is_file() else None,
                    int(manifest["size_bytes"]), manifest.get("sha256"), json.dumps(manifest, ensure_ascii=False),
                    manifest["created_at"], manifest["updated_at"],
                ),
            )
            return 1
        if path.name == "job.json" and self.input_jobs_root and path.parent.parent == self.input_jobs_root:
            job = self._read_json(path)
            progress_path, result_path = path.with_name("progress.json"), path.with_name("result.json")
            progress = self._read_json(progress_path) if progress_path.is_file() else {}
            result = self._read_json(result_path) if result_path.is_file() else None
            upload_id = job.get("upload_id")
            if upload_id and connection.execute(
                "SELECT 1 FROM upload_sessions WHERE upload_id=?", (upload_id,)
            ).fetchone() is None:
                upload_id = None
            connection.execute(
                "INSERT INTO input_jobs(input_job_id, upload_id, status, stage, job_json, progress_json, result_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (job["input_job_id"], upload_id, job.get("status", "unknown"), job.get("stage", "unknown"), json.dumps(job, ensure_ascii=False), json.dumps(progress, ensure_ascii=False), json.dumps(result, ensure_ascii=False) if result is not None else None, job.get("created_at") or utc_now(), job.get("completed_at") or utc_now()),
            )
            return 1
        if path.name == "template_overrides.json":
            items = self._read_json(path).get("items") or {}
            for template_hash, item in items.items():
                connection.execute(
                    "INSERT INTO drain_templates(template_hash, component, status, template_json, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (template_hash, item.get("component"), item.get("status"), json.dumps(item, ensure_ascii=False), item.get("updated_at") or utc_now()),
                )
                connection.execute(
                    "INSERT INTO drain_template_versions(template_hash, version, template_json, created_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (template_hash, int(item.get("version", 1)), json.dumps(item, ensure_ascii=False), item.get("updated_at") or utc_now()),
                )
            return len(items)
        if path.name == "template_events.jsonl":
            events = self._read_jsonl(path)
            for event in events:
                connection.execute(
                    "INSERT INTO drain_template_events(template_hash, event_type, event_json, created_at) VALUES (?, ?, ?, ?)",
                    (event.get("template_hash"), event.get("action", "event"), json.dumps(event, ensure_ascii=False), event.get("created_at") or utc_now()),
                )
            return len(events)
        if path.name == "datasets.json":
            items = self._read_json(path).get("items") or []
            for item in items:
                connection.execute(
                    "INSERT INTO drain_datasets(dataset_id, name, version, dataset_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (item["dataset_id"], item["name"], item.get("version"), json.dumps(item, ensure_ascii=False), item.get("created_at") or utc_now(), item.get("updated_at") or utc_now()),
                )
            return len(items)
        if path.name in {"annotations.jsonl", "reviews.jsonl"}:
            items = self._read_jsonl(path)
            for item in items:
                if path.name == "annotations.jsonl":
                    connection.execute(
                        "INSERT INTO drain_annotations(annotation_id, annotation_json, created_at, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (item["annotation_id"], json.dumps(item, ensure_ascii=False), item.get("created_at") or utc_now(), item.get("created_at") or utc_now()),
                    )
                else:
                    connection.execute(
                        "INSERT INTO drain_reviews(review_id, annotation_id, review_json, created_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (item["review_id"], item.get("annotation_id"), json.dumps(item, ensure_ascii=False), item.get("created_at") or utc_now()),
                    )
            return len(items)
        if path.name == "config_catalog.json":
            items = self._read_json(path).get("items") or {}
            for config_id, metadata in items.items():
                for version_meta in metadata.get("versions", []):
                    version = int(version_meta["version"])
                    ini = path.parent / "configs" / config_id / f"{version}.ini"
                    if not ini.is_file():
                        continue
                    content = ini.read_text(encoding="utf-8")
                    connection.execute(
                    "INSERT INTO drain_config_versions(config_id, version, status, content, content_hash, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        (config_id, version, metadata.get("status", "candidate"), content, hashlib.sha256(content.encode()).hexdigest(), json.dumps(metadata, ensure_ascii=False), version_meta.get("created_at") or utc_now()),
                    )
            return len(items)
        if path.name == "active_config.json":
            connection.execute("INSERT INTO app_settings(setting_key, value_json, updated_at) VALUES ('active_drain_config', ?, ?) ON CONFLICT(setting_key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at", (path.read_text(encoding="utf-8"), utc_now()))
            return 1
        if path.name == "summary.json" and path.parent.parent.name in {"eval_runs", "tune_runs"}:
            item = self._read_json(path)
            if path.parent.parent.name == "eval_runs":
                connection.execute("INSERT INTO drain_eval_runs(evaluation_id, status, evaluation_json, created_at, completed_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING", (item["run_id"], item.get("status", "completed"), json.dumps(item, ensure_ascii=False), item.get("created_at") or utc_now(), item.get("updated_at")))
            else:
                connection.execute("INSERT INTO drain_tune_runs(tune_run_id, status, tune_json, created_at, completed_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING", (item["run_id"], item.get("status", "completed"), json.dumps(item, ensure_ascii=False), item.get("created_at") or utc_now(), item.get("updated_at")))
            return 1
        if path.name == "catalog.json" and path.parent.name == "semantic_dictionaries":
            catalog = self._read_json(path).get("items") or {}
            for dictionary_id, metadata in catalog.items():
                connection.execute("UPDATE semantic_dictionaries SET active_version=?, dictionary_json=?, updated_at=? WHERE dictionary_id=?", (int(metadata.get("active_version", 1)), json.dumps(metadata, ensure_ascii=False), utc_now(), dictionary_id))
            return len(catalog)
        if path.parent.name == "versions" and path.parent.parent.parent.name == "semantic_dictionaries":
            item = self._read_json(path)
            dictionary_id, version = item["dictionary_id"], int(item["version"])
            digest = hashlib.sha256(json.dumps(item, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
            connection.execute("INSERT INTO semantic_dictionary_versions(dictionary_id, version, status, dictionary_json, content_hash, created_at) VALUES (?, ?, 'candidate', ?, ?, ?) ON CONFLICT DO NOTHING", (dictionary_id, version, json.dumps(item, ensure_ascii=False), digest, utc_now()))
            return 1
        if path.parent.parent.name == "validations":
            item = self._read_json(path)
            connection.execute("INSERT INTO semantic_validation_runs(dictionary_id, version, validation_json, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(dictionary_id, version) DO UPDATE SET validation_json=excluded.validation_json, created_at=excluded.created_at", (item["dictionary_id"], int(item["version"]), json.dumps(item, ensure_ascii=False), utc_now()))
            return 1
        if path.name == "events.jsonl" and path.parent.name == "semantic_dictionaries":
            events = self._read_jsonl(path)
            for event in events:
                connection.execute("INSERT INTO semantic_events(dictionary_id, version, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)", (event.get("dictionary_id"), event.get("version"), event.get("action", "event"), json.dumps(event, ensure_ascii=False), event.get("created_at") or utc_now()))
            return len(events)
        return 0
