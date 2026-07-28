from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from logrisk.ai_harness.prompt_registry import PromptRegistry
from logrisk.ai_harness.model_client import parse_content_json
from logrisk.database import Database, utc_now


SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "content",
    "database_url",
    "dsn",
    "message",
    "password",
    "raw_log",
    "raw_sample",
    "samples",
    "secret",
    "token",
}
LOGGER = logging.getLogger(__name__)


def sanitize_observability_attributes(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): sanitize_observability_attributes(item)
            for key, item in value.items()
            if str(key).lower() not in SENSITIVE_KEYS
        }
    if isinstance(value, list):
        return [sanitize_observability_attributes(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _json(value: Any) -> str:
    return json.dumps(
        sanitize_observability_attributes(value),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    parsed = json.loads(str(value))
    return parsed if isinstance(parsed, dict) else {}


class ObservabilityRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def create_observation(
        self,
        *,
        job_id: str,
        input_job_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not str(job_id).strip():
            raise ValueError("job_id 不能为空")
        with self.database.transaction() as connection:
            current = connection.execute(
                "SELECT * FROM observability_runs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if current is None:
                now = utc_now()
                connection.execute(
                    "INSERT INTO observability_runs("
                    "observation_id, job_id, input_job_id, status, attributes_json, "
                    "schema_version, created_at, updated_at"
                    ") VALUES (?, ?, ?, 'running', ?, 'observation_v2', ?, ?)",
                    (
                        "obs-" + uuid.uuid4().hex,
                        job_id,
                        input_job_id,
                        _json(attributes or {}),
                        now,
                        now,
                    ),
                )
                current = connection.execute(
                    "SELECT * FROM observability_runs WHERE job_id=?",
                    (job_id,),
                ).fetchone()
        return self._observation(current)

    def get_observation(self, observation_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM observability_runs WHERE observation_id=?",
                (observation_id,),
            ).fetchone()
        return self._observation(row) if row else None

    def get_observation_by_job(self, job_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM observability_runs WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return self._observation(row) if row else None

    def update_observation_status(self, observation_id: str, status: str) -> dict[str, Any]:
        if status not in {"running", "completed", "failed"}:
            raise ValueError("Observation status 无效")
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE observability_runs SET status=?, updated_at=? WHERE observation_id=?",
                (status, now, observation_id),
            )
            row = connection.execute(
                "SELECT * FROM observability_runs WHERE observation_id=?",
                (observation_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Observation 不存在: {observation_id}")
        return self._observation(row)

    def start_span(
        self,
        *,
        observation_id: str,
        name: str,
        stage: str,
        parent_span_id: str | None = None,
        trace_id: str | None = None,
        attributes: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        started_at: str | None = None,
    ) -> dict[str, Any]:
        if not name or not stage:
            raise ValueError("Span name 和 stage 不能为空")
        with self.database.transaction() as connection:
            current = None
            if idempotency_key:
                current = connection.execute(
                    "SELECT * FROM observability_spans WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
            if current is None:
                now = utc_now()
                span_id = "span-" + uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO observability_spans("
                    "span_id, observation_id, trace_id, parent_span_id, name, stage, "
                    "status, started_at, attributes_json, idempotency_key, schema_version, "
                    "created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, 'span_v2', ?, ?)",
                    (
                        span_id,
                        observation_id,
                        trace_id,
                        parent_span_id,
                        name,
                        stage,
                        started_at or now,
                        _json(attributes or {}),
                        idempotency_key,
                        now,
                        now,
                    ),
                )
                current = connection.execute(
                    "SELECT * FROM observability_spans WHERE span_id=?",
                    (span_id,),
                ).fetchone()
        return self._span(current)

    def finish_span(
        self,
        span_id: str,
        *,
        status: str,
        attributes: dict[str, Any] | None = None,
        ended_at: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM observability_spans WHERE span_id=?",
                (span_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Span 不存在: {span_id}")
            end = ended_at or utc_now()
            start = datetime.fromisoformat(str(row["started_at"]))
            finish = datetime.fromisoformat(end)
            duration_ms = max(0, int((finish - start).total_seconds() * 1000))
            merged = _object(row["attributes_json"])
            merged.update(sanitize_observability_attributes(attributes or {}))
            connection.execute(
                "UPDATE observability_spans SET status=?, ended_at=?, duration_ms=?, "
                "attributes_json=?, trace_id=COALESCE(?, trace_id), updated_at=? WHERE span_id=?",
                (status, end, duration_ms, _json(merged), trace_id, end, span_id),
            )
            updated = connection.execute(
                "SELECT * FROM observability_spans WHERE span_id=?",
                (span_id,),
            ).fetchone()
        return self._span(updated)

    def list_spans(
        self,
        observation_id: str,
        *,
        stage: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM observability_spans WHERE observation_id=?"
        parameters: list[Any] = [observation_id]
        if stage:
            sql += " AND stage=?"
            parameters.append(stage)
        if status:
            sql += " AND status=?"
            parameters.append(status)
        sql += " ORDER BY started_at, span_id"
        with self.database.connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [self._span(row) for row in rows]

    def list_observations(
        self,
        *,
        job_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM observability_runs WHERE 1=1"
        parameters: list[Any] = []
        if job_id:
            sql += " AND job_id=?"
            parameters.append(job_id)
        if status:
            sql += " AND status=?"
            parameters.append(status)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        parameters.append(max(1, min(int(limit), 200)))
        with self.database.connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [self._observation(row) for row in rows]

    def get_span(self, span_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM observability_spans WHERE span_id=?",
                (span_id,),
            ).fetchone()
        return self._span(row) if row else None

    def create_replay(
        self,
        *,
        source_trace_id: str,
        mode: str,
        idempotency_key: str,
        snapshot: dict[str, Any],
        observation_id: str | None = None,
    ) -> dict[str, Any]:
        with self.database.transaction() as connection:
            current = connection.execute(
                "SELECT * FROM replay_runs WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if current is None:
                now = utc_now()
                replay_id = "replay-" + uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO replay_runs("
                    "replay_id, observation_id, source_trace_id, mode, status, "
                    "idempotency_key, snapshot_json, result_json, schema_version, "
                    "created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, 'queued', ?, ?, '{}', 'replay_v2', ?, ?)",
                    (
                        replay_id,
                        observation_id,
                        source_trace_id,
                        mode,
                        idempotency_key,
                        _json(snapshot),
                        now,
                        now,
                    ),
                )
                current = connection.execute(
                    "SELECT * FROM replay_runs WHERE replay_id=?",
                    (replay_id,),
                ).fetchone()
        return self._replay(current)

    def update_replay(
        self,
        replay_id: str,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        completed_at = now if status in {"completed", "failed"} else None
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE replay_runs SET status=?, result_json=?, error_code=?, "
                "error_message=?, updated_at=?, completed_at=? WHERE replay_id=?",
                (
                    status,
                    _json(result or {}),
                    error_code,
                    error_message,
                    now,
                    completed_at,
                    replay_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM replay_runs WHERE replay_id=?",
                (replay_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Replay 不存在: {replay_id}")
        return self._replay(row)

    def get_replay(self, replay_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM replay_runs WHERE replay_id=?",
                (replay_id,),
            ).fetchone()
        return self._replay(row) if row else None

    def append_replay_event(
        self,
        replay_id: str,
        event_type: str,
        event: dict[str, Any],
    ) -> None:
        with self.database.transaction() as connection:
            sequence = int(connection.execute(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM replay_events WHERE replay_id=?",
                (replay_id,),
            ).fetchone()[0])
            connection.execute(
                "INSERT INTO replay_events(replay_id, sequence, event_type, event_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (replay_id, sequence, event_type, _json(event), utc_now()),
            )

    def list_replay_events(self, replay_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM replay_events WHERE replay_id=? ORDER BY sequence",
                (replay_id,),
            ).fetchall()
        return [{
            "sequence": row["sequence"],
            "event_type": row["event_type"],
            "event": _object(row["event_json"]),
            "created_at": row["created_at"],
        } for row in rows]

    @staticmethod
    def _observation(row: Any) -> dict[str, Any]:
        return {
            "observation_id": row["observation_id"],
            "job_id": row["job_id"],
            "input_job_id": row["input_job_id"],
            "status": row["status"],
            "attributes": _object(row["attributes_json"]),
            "schema_version": row["schema_version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _span(row: Any) -> dict[str, Any]:
        return {
            "span_id": row["span_id"],
            "observation_id": row["observation_id"],
            "trace_id": row["trace_id"],
            "parent_span_id": row["parent_span_id"],
            "name": row["name"],
            "stage": row["stage"],
            "status": row["status"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "duration_ms": row["duration_ms"],
            "attributes": _object(row["attributes_json"]),
            "schema_version": row["schema_version"],
        }

    @staticmethod
    def _replay(row: Any) -> dict[str, Any]:
        return {
            "replay_id": row["replay_id"],
            "observation_id": row["observation_id"],
            "source_trace_id": row["source_trace_id"],
            "mode": row["mode"],
            "status": row["status"],
            "snapshot": _object(row["snapshot_json"]),
            "result": _object(row["result_json"]),
            "error_code": row["error_code"],
            "error_message": row["error_message"],
            "schema_version": row["schema_version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }


class SpanRecorder:
    def __init__(self, repository: ObservabilityRepository) -> None:
        self.repository = repository
        self.failure_count = 0

    def ensure_observation(
        self,
        *,
        job_id: str,
        input_job_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.repository.create_observation(
            job_id=job_id,
            input_job_id=input_job_id,
            attributes=attributes,
        )

    def record(
        self,
        *,
        observation_id: str,
        name: str,
        stage: str,
        status: str,
        parent_span_id: str | None = None,
        trace_id: str | None = None,
        attributes: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any] | None:
        try:
            span = self.repository.start_span(
                observation_id=observation_id,
                name=name,
                stage=stage,
                parent_span_id=parent_span_id,
                trace_id=trace_id,
                attributes=sanitize_observability_attributes(attributes or {}),
                idempotency_key=idempotency_key,
            )
            return self.repository.finish_span(
                span["span_id"],
                status=status,
                trace_id=trace_id,
            )
        except Exception as exc:
            self.failure_count += 1
            LOGGER.warning("observability span write failed: %s", type(exc).__name__)
            return None

    def finish_observation(self, observation_id: str, *, failed: bool = False) -> None:
        try:
            self.repository.update_observation_status(
                observation_id,
                "failed" if failed else "completed",
            )
        except Exception as exc:
            self.failure_count += 1
            LOGGER.warning("observability status write failed: %s", type(exc).__name__)


class PromptSnapshotResolver:
    def __init__(self, registry: PromptRegistry) -> None:
        self.registry = registry

    def resolve(self, prompt_id: str, prompt_hash: str) -> dict[str, Any]:
        try:
            load_by_hash = getattr(self.registry, "load_by_hash", None)
            prompt = (
                load_by_hash(prompt_id, prompt_hash)
                if callable(load_by_hash)
                else self.registry.load(prompt_id)
            )
            if prompt.sha256 != prompt_hash:
                raise FileNotFoundError(prompt_id)
            return {
                "status": "exact",
                "prompt_content": prompt.content,
                "prompt_path": prompt.path,
                "prompt_version": prompt.version,
            }
        except FileNotFoundError:
            return {
                "status": "unavailable",
                "prompt_content": "",
                "prompt_path": "",
                "prompt_version": None,
            }


class ReplayError(RuntimeError):
    def __init__(self, message: str, *, code: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def compare_replay(source: dict[str, Any], replay: dict[str, Any]) -> dict[str, Any]:
    before = source.get("parsed_output") if isinstance(source.get("parsed_output"), dict) else {}
    after = replay.get("parsed_output") if isinstance(replay.get("parsed_output"), dict) else {}
    before_features = before.get("features") if isinstance(before.get("features"), list) else []
    after_features = after.get("features") if isinstance(after.get("features"), list) else []
    fields = (
        "feature_type",
        "title",
        "summary",
        "importance",
        "template_hashes",
        "components",
        "tags",
        "selection_reason",
    )

    def feature_key(feature: Any) -> str:
        if not isinstance(feature, dict):
            return json.dumps(feature, ensure_ascii=False, sort_keys=True)
        hashes = sorted(str(item) for item in feature.get("template_hashes") or [])
        return f"{feature.get('feature_type') or ''}:{','.join(hashes)}"

    before_by_key = {feature_key(item): item for item in before_features}
    after_by_key = {feature_key(item): item for item in after_features}
    added_keys = sorted(set(after_by_key) - set(before_by_key))
    removed_keys = sorted(set(before_by_key) - set(after_by_key))
    field_changes = []
    for key in sorted(set(before_by_key) & set(after_by_key)):
        changed_fields = [
            field
            for field in fields
            if before_by_key[key].get(field) != after_by_key[key].get(field)
        ]
        if changed_fields:
            field_changes.append({"feature_key": key, "fields": changed_fields})
    return {
        "feature_count": {"before": len(before_features), "after": len(after_features)},
        "added": len(added_keys),
        "removed": len(removed_keys),
        "modified": len(field_changes),
        "added_feature_keys": added_keys,
        "removed_feature_keys": removed_keys,
        "field_changes": field_changes,
        "template_references": {
            "before": sorted({
                str(value)
                for feature in before_features
                if isinstance(feature, dict)
                for value in feature.get("template_hashes") or []
            }),
            "after": sorted({
                str(value)
                for feature in after_features
                if isinstance(feature, dict)
                for value in feature.get("template_hashes") or []
            }),
        },
        "validation": {
            "before": source.get("validation_result") or {},
            "after": replay.get("validation_result") or {},
        },
        "evaluator": {
            "before": source.get("evaluator_result") or {},
            "after": replay.get("evaluator_result") or {},
        },
        "latency_ms": {
            "before": source.get("latency_ms"),
            "after": replay.get("latency_ms"),
        },
        "usage": {"before": source.get("usage") or {}, "after": replay.get("usage") or {}},
        "estimated_cost": {
            "before": source.get("estimated_cost"),
            "after": replay.get("estimated_cost"),
        },
        "changed": before != after,
    }


class ReplayService:
    def __init__(
        self,
        repository: ObservabilityRepository,
        trace_store: Any,
        prompt_resolver: PromptSnapshotResolver,
        *,
        model_runner: Any | None = None,
        validation_runner: Any | None = None,
    ) -> None:
        self.repository = repository
        self.trace_store = trace_store
        self.prompt_resolver = prompt_resolver
        self.model_runner = model_runner
        self.validation_runner = validation_runner

    def create(self, payload: dict[str, Any], *, idempotency_key: str) -> dict[str, Any]:
        source_trace_id = str(payload.get("source_trace_id") or "")
        mode = str(payload.get("mode") or "")
        if mode not in {"historical", "model"}:
            raise ReplayError("Replay mode 无效", code="invalid_replay_mode", status_code=422)
        if not idempotency_key:
            raise ReplayError("缺少 Idempotency-Key", code="missing_idempotency_key", status_code=400)
        if payload.get("confirmed") is not True:
            raise ReplayError("Replay 必须人工确认", code="confirmation_required", status_code=422)
        trace = self.trace_store.get_trace(source_trace_id)
        if not trace:
            raise ReplayError("来源 Trace 不存在", code="trace_not_found", status_code=404)
        prompt = self.prompt_resolver.resolve(
            str(trace.get("prompt_id") or ""),
            str(trace.get("prompt_hash") or ""),
        )
        if mode == "model" and prompt["status"] != "exact":
            raise ReplayError("历史 Prompt 快照不可用", code="prompt_snapshot_unavailable", status_code=422)
        snapshot = {
            "source_trace": trace,
            "prompt": prompt,
            "operator": str(payload.get("operator") or "local-operator"),
        }
        return self.repository.create_replay(
            source_trace_id=source_trace_id,
            mode=mode,
            idempotency_key=idempotency_key,
            snapshot=snapshot,
            observation_id=trace.get("observation_id"),
        )

    def execute(self, replay_id: str) -> dict[str, Any]:
        replay = self.repository.get_replay(replay_id)
        if not replay:
            raise ReplayError("Replay 不存在", code="replay_not_found", status_code=404)
        source = replay["snapshot"]["source_trace"]
        self.repository.append_replay_event(replay_id, "started", {"mode": replay["mode"]})
        try:
            if replay["mode"] == "historical":
                parsed = source.get("parsed_output")
                if not isinstance(parsed, dict) or not isinstance(parsed.get("features"), list):
                    raw = str(source.get("raw_output") or "")
                    parsed = parse_content_json(raw)
                if callable(self.validation_runner):
                    result = self.validation_runner(replay["snapshot"], parsed)
                else:
                    result = {
                        "parsed_output": parsed,
                        "validation_result": {
                            "valid": isinstance(parsed.get("features"), list),
                            "errors": [],
                            "warnings": [],
                        },
                        "evaluator_result": source.get("evaluator_result") or {},
                    }
                result["model_called"] = False
                result["latency_ms"] = 0
            else:
                if not callable(self.model_runner):
                    raise ReplayError(
                        "当前运行时未配置模型 Replay 执行器",
                        code="model_replay_unavailable",
                        status_code=409,
                    )
                result = self.model_runner(replay["snapshot"])
                result["model_called"] = True
            result["diff"] = compare_replay(source, result)
            completed = self.repository.update_replay(
                replay_id,
                status="completed",
                result=result,
            )
            self.repository.append_replay_event(replay_id, "completed", {"status": "completed"})
            return completed
        except Exception as exc:
            code = exc.code if isinstance(exc, ReplayError) else "replay_failed"
            error_message = str(exc) if isinstance(exc, ReplayError) else "Replay 执行失败"
            self.repository.append_replay_event(replay_id, "failed", {"error_code": code})
            self.repository.update_replay(
                replay_id,
                status="failed",
                error_code=code,
                error_message=error_message,
            )
            raise
