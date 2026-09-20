from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from logrisk.database import Database, utc_now
from logrisk.incremental_sources import SourceCursor, SourceDescriptor


class StreamingStateError(ValueError):
    """A streaming state error that is safe to show in the Dashboard."""


class StreamingConflictError(StreamingStateError):
    """A persisted source or configuration no longer matches the task snapshot."""


class StreamingTaskBusyError(StreamingStateError):
    """A streaming task is already claimed by another worker."""


class StreamingIncompleteError(StreamingStateError):
    """Committed prefix evidence is missing and cannot be resumed safely."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "STREAMING_PREFIX_INCOMPLETE",
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = {"error_code": code, **dict(details or {})}


@dataclass(frozen=True)
class PrefixEvidence:
    task_id: str
    frontier: dict[str, Any]
    committed_batches: int
    record_count: int
    result_completeness: str
    manifest_history: str
    summary_unknown_fields: tuple[str, ...]
    summary_invalid_fields: tuple[str, ...]
    evidence_digest: str

    def integrity(self, *, lease_token: str) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "frontier": self.frontier,
            "commit_count": self.committed_batches,
            "record_count": self.record_count,
            "prefix_digest": self.evidence_digest,
            "lease_token": str(lease_token),
        }


_RAW_LOG_KEYS = {"raw_sample", "samples", "message", "content", "raw_log", "log_line"}


class StreamingStateRepository:
    def __init__(self, database: Database, ledger_repository: Any | None = None) -> None:
        self.database = database
        self.ledger_repository = ledger_repository

    def create_or_load(
        self,
        *,
        descriptor: SourceDescriptor,
        config_hash: str,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        if not config_hash:
            raise StreamingStateError("缺少 Drain3 配置摘要")
        if task_id:
            try:
                return self.get_task(task_id)
            except KeyError:
                pass
        now = utc_now()
        task_id = task_id or "stream_" + uuid.uuid4().hex
        task = {
            "schema_version": "streaming_task_v1",
            "task_id": task_id,
            "source": descriptor.to_dict(),
            "config_hash": config_hash,
            "status": "queued",
            "stage": "READING",
            "cursor": SourceCursor.empty().to_dict(),
            "windows_committed": 0,
            "records_processed": 0,
            "pending_external_commit": None,
            "result": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO streaming_tasks(task_id, source_kind, source_identity_json, config_hash, status, stage, cursor_json, task_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    descriptor.kind,
                    _json(descriptor.to_dict()),
                    config_hash,
                    task["status"],
                    task["stage"],
                    _json(task["cursor"]),
                    _json(task),
                    now,
                    now,
                ),
            )
            self._append_event(connection, task_id, "task_created", {"source_kind": descriptor.kind, "config_hash": config_hash}, now)
        return task

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(f"Streaming task not found: {task_id}")
        return _decode_json(row[0])

    def list_tasks(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT task_json FROM streaming_tasks ORDER BY updated_at DESC, task_id DESC LIMIT ?", (max(1, min(limit, 500)),)
            ).fetchall()
        return [_decode_json(row[0]) for row in rows]

    def mark_running(self, task_id: str) -> dict[str, Any]:
        return self._update_task(task_id, status="running", stage="READING", event_type="task_started")

    def count_active_tasks(self, *, source_kind: str) -> int:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM streaming_tasks WHERE source_kind=? AND status IN ('queued', 'running')",
                (source_kind,),
            ).fetchone()
        return int(row[0])

    def claim_task(self, task_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            row = connection.execute(
                "SELECT status,task_json FROM streaming_tasks WHERE task_id=?" + lock, (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row["task_json"])
            previous_status = str(row["status"] or task.get("status") or "")
            if previous_status == "running" or task.get("status") == "running":
                raise StreamingTaskBusyError("流式任务已被其他 Worker 占用")
            task.update({
                "status": "running", "stage": "READING", "error": None,
                "lease_token": uuid.uuid4().hex, "updated_at": now,
            })
            updated = connection.execute(
                "UPDATE streaming_tasks SET status='running', stage='READING', task_json=?, updated_at=? "
                "WHERE task_id=? AND status=?",
                (_json(task), now, task_id, previous_status),
            )
            if updated.rowcount != 1:
                raise StreamingTaskBusyError("流式任务已被其他 Worker 占用")
            self._append_event(connection, task_id, "task_claimed", {}, now)
        return task

    def mark_stage(self, task_id: str, stage: str) -> dict[str, Any]:
        allowed = {"READING", "SPOOLING", "MINING", "AGGREGATING"}
        if stage not in allowed:
            raise StreamingStateError("流式任务阶段无效")
        return self._update_task(task_id, status="running", stage=stage, event_type="stage_changed")

    def mark_claim_stage(self, task_id: str, lease_token: str, stage: str) -> dict[str, Any]:
        allowed = {"READING", "SPOOLING", "MINING", "AGGREGATING"}
        if stage not in allowed:
            raise StreamingStateError("流式任务阶段无效")
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            row = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?" + lock, (str(task_id),)
            ).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if task.get("status") != "running" or str(task.get("lease_token") or "") != str(lease_token):
                raise StreamingConflictError("流式任务租约已变化，迟到 Worker 不能更新阶段")
            task.update({"status": "running", "stage": stage, "error": None, "updated_at": now})
            connection.execute(
                "UPDATE streaming_tasks SET status='running', stage=?, task_json=?, updated_at=? WHERE task_id=?",
                (stage, _json(task), now, str(task_id)),
            )
            self._append_event(connection, str(task_id), "stage_changed", {}, now)
        return task

    def attach_input_job(self, task_id: str, input_job_id: str) -> dict[str, Any]:
        if not input_job_id:
            raise StreamingStateError("输入任务标识不能为空")
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            task["input_job_id"] = input_job_id
            task["updated_at"] = now
            connection.execute(
                "UPDATE streaming_tasks SET task_json=?, updated_at=? WHERE task_id=?",
                (_json(task), now, task_id),
            )
            self._append_event(connection, task_id, "input_job_attached", {"input_job_id": input_job_id}, now)
        return task

    def mark_completed(self, task_id: str) -> dict[str, Any]:
        raise StreamingIncompleteError("流式任务只能在完整结果代次核验后完成")

    def mark_failed(self, task_id: str, error: str, *, conflict: bool = False) -> dict[str, Any]:
        return self._update_task(
            task_id,
            status="conflict" if conflict else "failed",
            stage="CONFLICT" if conflict else "FAILED",
            error=error,
            event_type="task_conflict" if conflict else "task_failed",
        )

    def mark_interrupted(self, task_id: str) -> dict[str, Any]:
        return self._update_task(task_id, status="interrupted", stage="FAILED", error="服务重启导致任务中断", event_type="task_interrupted")

    def finish_claim(
        self,
        task_id: str,
        lease_token: str,
        *,
        error: str,
        interrupted: bool = False,
        conflict: bool = False,
        error_details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish a claimed task's terminal error without overwriting a new owner."""
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            row = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?" + lock, (str(task_id),)
            ).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if task.get("status") != "running" or str(task.get("lease_token") or "") != str(lease_token):
                return task
            if conflict:
                status, stage, event_type = "conflict", "CONFLICT", "task_conflict"
            elif interrupted:
                status, stage, event_type = "interrupted", "FAILED", "task_interrupted"
            else:
                status, stage, event_type = "failed", "FAILED", "task_failed"
            task.update({"status": status, "stage": stage, "error": error, "updated_at": now})
            if error_details is not None:
                task["error_details"] = dict(error_details)
            connection.execute(
                "UPDATE streaming_tasks SET status=?, stage=?, task_json=?, updated_at=? WHERE task_id=?",
                (status, stage, _json(task), now, str(task_id)),
            )
            event = {"error": error}
            if error_details is not None:
                event["details"] = dict(error_details)
            self._append_event(connection, str(task_id), event_type, event, now)
        return task

    def complete_claim(
        self,
        task_id: str,
        lease_token: str,
        *,
        result_reference: Mapping[str, Any] | None = None,
        prefix_evidence: PrefixEvidence | None = None,
    ) -> dict[str, Any]:
        """Complete only the still-running lease that produced the result."""
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            row = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?" + lock, (str(task_id),)
            ).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if task.get("status") != "running" or str(task.get("lease_token") or "") != str(lease_token):
                raise StreamingConflictError("流式任务租约已变化，迟到 Worker 不能完成任务")
            if result_reference is None or prefix_evidence is None:
                raise StreamingIncompleteError("任务缺少结果完整性证据，不能完成")
            reference = dict(result_reference)
            saved_reference = dict(
                task.get("result_reference") or (task.get("result") or {}).get("result_ref") or {}
            )
            if saved_reference != reference or reference.get("task_id") != str(task_id):
                raise StreamingIncompleteError("结果引用尚未绑定到当前任务")
            generation_lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            generation = connection.execute(
                "SELECT status,frontier_json,summary_json FROM streaming_result_generations "
                "WHERE task_id=? AND generation=?" + generation_lock,
                (str(task_id), str(reference.get("generation") or "")),
            ).fetchone()
            if generation is None or generation["status"] != "ready":
                raise StreamingIncompleteError("结果代次尚未就绪")
            frontier = _decode_json(generation["frontier_json"])
            if frontier != task.get("cursor") or frontier != prefix_evidence.frontier:
                raise StreamingIncompleteError("结果代次与当前提交前沿不一致")
            generation_summary = _decode_json(generation["summary_json"])
            expected_integrity = prefix_evidence.integrity(lease_token=lease_token)
            if generation_summary.get("_integrity") != expected_integrity:
                raise StreamingIncompleteError("结果完整性证据与当前任务不一致")
            commit_count = connection.execute(
                "SELECT COUNT(*) FROM streaming_window_commits WHERE task_id=?", (str(task_id),)
            ).fetchone()[0]
            if int(commit_count) != prefix_evidence.committed_batches:
                raise StreamingIncompleteError("完成任务前已提交批次数发生变化")
            task.update({"status": "completed", "stage": "COMPLETED", "error": None, "updated_at": now})
            connection.execute(
                "UPDATE streaming_tasks SET status='completed', stage='COMPLETED', task_json=?, updated_at=? WHERE task_id=?",
                (_json(task), now, str(task_id)),
            )
            self._append_event(connection, str(task_id), "task_completed", {}, now)
        return task

    def interrupt_running_tasks(self) -> int:
        count = 0
        for task in self.list_tasks(limit=500):
            if task.get("status") == "running":
                self.mark_interrupted(str(task["task_id"]))
                count += 1
        return count

    def clear_pending_external_commit(
        self,
        task_id: str,
        cursor: SourceCursor | Mapping[str, Any],
        *,
        expected_lease_token: str | None = None,
    ) -> dict[str, Any]:
        cursor_value = cursor.to_dict() if isinstance(cursor, SourceCursor) else SourceCursor.from_dict(cursor).to_dict()
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            row = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?" + lock, (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if expected_lease_token is not None and (
                task.get("status") != "running"
                or str(task.get("lease_token") or "") != str(expected_lease_token)
            ):
                raise StreamingConflictError("流式任务租约已变化，迟到 Worker 不能确认外部提交")
            if task.get("pending_external_commit") == cursor_value:
                task["pending_external_commit"] = None
                task["updated_at"] = now
                connection.execute(
                    "UPDATE streaming_tasks SET task_json=?, updated_at=? WHERE task_id=?",
                    (_json(task), now, task_id),
                )
                self._append_event(connection, task_id, "external_commit_completed", {}, now)
        return task

    def save_result(
        self,
        task_id: str,
        result: Mapping[str, Any],
        *,
        expected_lease_token: str | None = None,
        result_reference: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        safe_result = dict(result)
        _reject_raw_fields(safe_result)
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else ""
            row = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?" + lock, (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if expected_lease_token is not None and (
                task.get("status") != "running"
                or str(task.get("lease_token") or "") != str(expected_lease_token)
            ):
                raise StreamingConflictError("流式任务租约已变化，迟到 Worker 不能保存结果")
            task["result"] = safe_result
            if result_reference is not None:
                task["result_reference"] = dict(result_reference)
            task["updated_at"] = now
            connection.execute(
                "UPDATE streaming_tasks SET task_json=?, updated_at=? WHERE task_id=?",
                (_json(task), now, task_id),
            )
            self._append_event(connection, task_id, "result_saved", {}, now)
        return task

    def commit_window(
        self,
        task_id: str,
        *,
        window_id: str,
        cursor: SourceCursor | Mapping[str, Any],
        templates: list[Mapping[str, Any]],
        windows: list[Mapping[str, Any]] | None = None,
        summary: Mapping[str, Any] | None = None,
        ledger_batch: Mapping[str, Any] | None = None,
        fencing_token: str | None = None,
        miner_generation: Mapping[str, Any] | None = None,
        expected_cursor: SourceCursor | Mapping[str, Any] | None = None,
    ) -> bool:
        if not window_id:
            raise StreamingStateError("窗口标识不能为空")
        cursor_value = cursor.to_dict() if isinstance(cursor, SourceCursor) else SourceCursor.from_dict(cursor).to_dict()
        sanitized_templates = [_safe_template(item) for item in templates]
        sanitized_windows = [_safe_window(item) for item in (windows if windows is not None else templates)]
        commit_summary = dict(summary or {})
        _validate_new_summary(commit_summary)
        payload_hash = _payload_hash(cursor_value, sanitized_windows, commit_summary)
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database,"provider","sqlite") == "postgres" else ""
            task_row = connection.execute("SELECT task_json, config_hash FROM streaming_tasks WHERE task_id=?" + lock, (task_id,)).fetchone()
            if task_row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(task_row[0])
            if fencing_token is not None and (
                task.get("status") != "running"
                or str(task.get("lease_token") or "") != str(fencing_token)
            ):
                raise StreamingConflictError("流式任务租约已变化，迟到 Worker 不能提交")
            if self.ledger_repository is not None and ledger_batch is None:
                raise StreamingStateError("启用 operational ledger 时窗口缺少 ingestion receipt")
            if self.ledger_repository is not None and ledger_batch is not None:
                receipt_payload = dict(ledger_batch)
                receipt_payload["connection"] = connection
                self.ledger_repository.record_ingestion_batch(**receipt_payload)
            existing = connection.execute(
                "SELECT window_id, payload_hash FROM streaming_window_commits WHERE task_id=? AND window_id=?", (task_id, window_id)
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] and str(existing["payload_hash"]) != payload_hash:
                    raise StreamingConflictError("相同批次标识对应不同内容")
                return False
            current_cursor = SourceCursor.from_dict(task.get("cursor"))
            if expected_cursor is not None:
                expected = expected_cursor.to_dict() if isinstance(expected_cursor, SourceCursor) else SourceCursor.from_dict(expected_cursor).to_dict()
                if expected != current_cursor.to_dict():
                    raise StreamingConflictError("流式提交前沿已变化")
            if not _cursor_is_at_least(cursor_value, current_cursor):
                raise StreamingConflictError("新批次不能回退检查点")
            state_manifest = miner_generation if miner_generation is not None else task.get("miner_generation")
            connection.execute(
                "INSERT INTO streaming_window_commits(task_id, window_id, cursor_json, summary_json, committed_at, payload_hash, state_manifest_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, window_id, _json(cursor_value), _json(commit_summary), now, payload_hash,
                 _json(state_manifest) if state_manifest is not None else None),
            )
            for item_index, window in enumerate(sanitized_windows):
                connection.execute(
                    "INSERT INTO streaming_batch_windows(task_id, window_id, item_index, window_json) VALUES (?, ?, ?, ?)",
                    (task_id, window_id, item_index, _json(window)),
                )
            config_hash = str(task_row[1])
            for template in sanitized_templates:
                connection.execute(
                    "INSERT INTO unknown_template_queue(task_id, template_hash, component, window_start, config_hash, occurrence_count, template_json, status, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?) "
                    "ON CONFLICT(task_id, template_hash, window_start) DO UPDATE SET occurrence_count=unknown_template_queue.occurrence_count + excluded.occurrence_count, "
                    "template_json=excluded.template_json, updated_at=excluded.updated_at",
                    (
                        task_id,
                        template["template_hash"],
                        template["component"],
                        template["window_start"],
                        config_hash,
                        template["count"],
                        _json(template),
                        now,
                        now,
                    ),
                )
            task.update(
                {
                    "status": "running",
                    "stage": "AGGREGATING",
                    "cursor": cursor_value,
                    "windows_committed": int(task.get("windows_committed") or 0) + 1,
                    "records_processed": int(task.get("records_processed") or 0) + int(commit_summary.get("record_count") or 0),
                    "pending_external_commit": cursor_value,
                    "updated_at": now,
                }
            )
            if miner_generation is not None:
                task["miner_generation"] = dict(miner_generation)
            connection.execute(
                "UPDATE streaming_tasks SET status=?, stage=?, cursor_json=?, task_json=?, updated_at=? WHERE task_id=?",
                (task["status"], task["stage"], _json(cursor_value), _json(task), now, task_id),
            )
            self._append_event(connection, task_id, "window_committed", {"window_id": window_id, "template_count": len(sanitized_templates)}, now)
        return True

    def list_unknown_templates(self, *, task_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if task_id:
            where = " WHERE task_id=?"
            params.append(task_id)
        params.append(max(1, min(limit, 500)))
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT task_id, template_hash, component, window_start, config_hash, occurrence_count, template_json, status, created_at, updated_at "
                "FROM unknown_template_queue" + where + " ORDER BY updated_at DESC, template_hash LIMIT ?",
                params,
            ).fetchall()
        return [
            {
                "task_id": row[0],
                "template_hash": row[1],
                "component": row[2],
                "window_start": row[3],
                "config_hash": row[4],
                "occurrence_count": int(row[5]),
                "template": _decode_json(row[6]),
                "status": row[7],
                "created_at": row[8],
                "updated_at": row[9],
            }
            for row in rows
        ]

    def list_commits(self, task_id: str) -> list[str]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT window_id FROM streaming_window_commits WHERE task_id=? ORDER BY committed_at, window_id", (task_id,)
            ).fetchall()
        return [str(row[0]) for row in rows]

    def committed_summary(self, task_id: str) -> dict[str, Any]:
        sums = ("record_count", "template_count", "unknown_template_count", "risk_semantic_matches",
                "template_event_count", "partition_count")
        required = (*sums, "worker_count", "parallel", "node_risk_enabled", "process_start_method")
        totals: dict[str, Any] = {key: 0 for key in sums}
        totals.update(worker_count=0, parallel=False, node_risk_enabled=False)
        missing: set[str] = set()
        invalid: set[str] = set()
        methods: set[str] = set()
        method_unknown = False
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT summary_json FROM streaming_window_commits WHERE task_id=?", (task_id,)
            )
            for row in rows:
                summary = _decode_json(row[0])
                missing.update(key for key in required if key not in summary or summary.get(key) is None)
                for key in sums:
                    value = summary.get(key)
                    if _is_nonnegative_int(value):
                        totals[key] += value
                    elif value is not None:
                        invalid.add(key)
                workers = summary.get("worker_count")
                if _is_nonnegative_int(workers):
                    totals["worker_count"] = max(totals["worker_count"], workers)
                elif workers is not None:
                    invalid.add("worker_count")
                for key in ("parallel", "node_risk_enabled"):
                    value = summary.get(key)
                    if isinstance(value, bool):
                        totals[key] = totals[key] or value
                    elif value is not None:
                        invalid.add(key)
                method = summary.get("process_start_method")
                if isinstance(method, str) and method in {"spawn", "fork", "forkserver"}:
                    methods.add(method)
                elif method == "not_applicable":
                    pass
                elif method is not None:
                    invalid.add("process_start_method")
                else:
                    method_unknown = True
        for key in missing:
            if key in {"parallel", "node_risk_enabled"} and totals[key] is True:
                continue
            totals[key] = None
        for key in invalid:
            totals[key] = None
        if method_unknown:
            totals["process_start_method"] = "unknown"
        elif len(methods) > 1:
            totals["process_start_method"] = "mixed"
        elif methods:
            totals["process_start_method"] = next(iter(methods))
        else:
            totals["process_start_method"] = "not_applicable"
        totals["unknown_fields"] = sorted(missing)
        totals["invalid_fields"] = sorted(invalid)
        totals["completeness"] = "legacy_partial" if missing or invalid else "complete"
        return totals

    def has_legacy_partial_commits(self, task_id: str) -> bool:
        try:
            self.require_complete_prefix(task_id)
        except StreamingIncompleteError:
            return True
        return False

    def require_complete_prefix(self, task_id: str) -> PrefixEvidence:
        summary = self.committed_summary(task_id)
        with self.database.connect() as connection:
            task_row = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if task_row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(task_row[0])
            commits = connection.execute(
                "SELECT window_id,cursor_json,summary_json,payload_hash,state_manifest_json "
                "FROM streaming_window_commits WHERE task_id=? ORDER BY committed_at,window_id",
                (task_id,),
            )
            digest = hashlib.sha256()
            commit_count = 0
            record_count = 0
            previous_cursor = SourceCursor.empty()
            manifest_unknown = False
            last_manifest: dict[str, Any] | None = None
            for row in commits:
                commit_count += 1
                cursor = _decode_json(row["cursor_json"])
                commit_summary = _decode_json(row["summary_json"])
                value = commit_summary.get("record_count")
                if not _is_nonnegative_int(value):
                    self._raise_incomplete("批摘要缺少可信 record_count", reason="record_count")
                record_count += value
                if not _cursor_is_at_least(cursor, previous_cursor):
                    self._raise_incomplete("提交批次的来源前沿不单调", reason="frontier")
                previous_cursor = SourceCursor.from_dict(cursor)
                payload_hash = str(row["payload_hash"] or "")
                if not payload_hash:
                    self._raise_incomplete("历史批次缺少 payload hash", reason="legacy_hash")
                actual_hash, window_count = _stored_payload_hash(
                    connection, task_id, str(row["window_id"]), cursor, commit_summary
                )
                if actual_hash != payload_hash:
                    self._raise_incomplete("已提交窗口与 payload hash 不一致", reason="payload_hash")
                template_count = commit_summary.get("template_count")
                if _is_nonnegative_int(template_count) and template_count != window_count:
                    self._raise_incomplete("批摘要窗口数与持久事实不一致", reason="window_count")
                if row["state_manifest_json"] is None:
                    manifest_unknown = True
                    last_manifest = None
                else:
                    manifest = _decode_json(row["state_manifest_json"])
                    if (
                        not isinstance(manifest, dict)
                        or not str(manifest.get("generation") or "")
                        or not isinstance(manifest.get("files"), dict)
                    ):
                        self._raise_incomplete("批次状态 manifest 无效", reason="manifest")
                    last_manifest = manifest
                digest.update(str(row["window_id"]).encode("utf-8"))
                digest.update(payload_hash.encode("ascii"))
                digest.update(_json(cursor).encode("utf-8"))
            task_cursor = SourceCursor.from_dict(task.get("cursor"))
            if commit_count == 0:
                if (
                    task_cursor != SourceCursor.empty()
                    or int(task.get("windows_committed") or 0) != 0
                    or int(task.get("records_processed") or 0) != 0
                    or task.get("pending_external_commit") is not None
                    or task.get("miner_generation") is not None
                ):
                    self._raise_incomplete("无提交批次的任务包含非初始前沿", reason="cursor_only")
            elif task_cursor.to_dict() != previous_cursor.to_dict():
                self._raise_incomplete("任务来源前沿与最后提交批次不一致", reason="frontier")
            if last_manifest is not None and task.get("miner_generation") != last_manifest:
                self._raise_incomplete("任务 miner generation 与最后提交批次不一致", reason="manifest_frontier")
            if int(task.get("windows_committed") or 0) != commit_count:
                self._raise_incomplete("任务批次数与持久提交不一致", reason="commit_count")
            if not _is_nonnegative_int(task.get("records_processed")) or int(task["records_processed"]) != record_count:
                self._raise_incomplete("任务记录数与持久提交不一致", reason="record_count")
        digest.update(_json(task_cursor.to_dict()).encode("utf-8"))
        digest.update(str(commit_count).encode("ascii"))
        digest.update(str(record_count).encode("ascii"))
        return PrefixEvidence(
            task_id=task_id,
            frontier=task_cursor.to_dict(),
            committed_batches=commit_count,
            record_count=record_count,
            result_completeness="complete",
            manifest_history="legacy_unknown" if manifest_unknown else "bound",
            summary_unknown_fields=tuple(summary["unknown_fields"]),
            summary_invalid_fields=tuple(summary["invalid_fields"]),
            evidence_digest=digest.hexdigest(),
        )

    @staticmethod
    def _raise_incomplete(message: str, *, reason: str) -> None:
        raise StreamingIncompleteError(
            "legacy_partial: " + message + "；请在原始文件仍可验证时显式创建新 Run 重算",
            details={
                "status": "failed",
                "error_code": "STREAMING_PREFIX_INCOMPLETE",
                "result_authoritative": False,
                "streaming_result_completeness": "legacy_partial",
                "reason": reason,
                "recovery": {
                    "resume_allowed": False,
                    "recommended_action": "explicit_recompute",
                    "source_verification_required": True,
                },
            },
        )

    def iter_committed_windows(
        self,
        task_id: str,
        *,
        after_key: tuple[str, int] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["task_id=?"]
        parameters: list[Any] = [task_id]
        if after_key is not None:
            clauses.append("(window_id>? OR (window_id=? AND item_index>?))")
            parameters.extend([after_key[0], after_key[0], int(after_key[1])])
        query = (
            "SELECT window_id,item_index,window_json FROM streaming_batch_windows WHERE " + " AND ".join(clauses)
            + " ORDER BY window_id, item_index"
        )
        if limit is not None:
            query += " LIMIT ?"
            parameters.append(max(1, min(int(limit), 10000)))
        with self.database.connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [dict(_decode_json(row["window_json"]), source_batch_id=row["window_id"], _commit_item_index=row["item_index"]) for row in rows]

    def _update_task(
        self,
        task_id: str,
        *,
        status: str,
        stage: str,
        event_type: str,
        error: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            task.update({"status": status, "stage": stage, "error": error, "updated_at": now})
            connection.execute(
                "UPDATE streaming_tasks SET status=?, stage=?, task_json=?, updated_at=? WHERE task_id=?",
                (status, stage, _json(task), now, task_id),
            )
            self._append_event(connection, task_id, event_type, {"error": error} if error else {}, now)
        return task

    def _append_event(self, connection: Any, task_id: str, event_type: str, payload: Mapping[str, Any], created_at: str) -> None:
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM streaming_task_events WHERE task_id=?", (task_id,)
        ).fetchone()
        connection.execute(
            "INSERT INTO streaming_task_events(task_id, sequence, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (task_id, int(row[0]), event_type, _json(dict(payload)), created_at),
        )


def _safe_template(value: Mapping[str, Any]) -> dict[str, Any]:
    _reject_raw_fields(value)
    template_hash = str(value.get("template_hash") or "")
    if not template_hash:
        raise StreamingStateError("未知模板缺少 template_hash")
    window_start = str(value.get("window_start") or "").strip()
    if not window_start:
        raise StreamingStateError("未知模板缺少 window_start")
    if window_start != "unknown":
        try:
            parsed_window_start = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StreamingStateError("未知模板 window_start 无效") from exc
        if parsed_window_start.tzinfo is None:
            raise StreamingStateError("未知模板 window_start 必须包含时区")
    return {
        "template_hash": template_hash,
        "component": str(value.get("component") or "unknown"),
        "template": str(value.get("template") or ""),
        "count": max(0, int(value.get("count") or 0)),
        "window_start": window_start,
        "window_end": str(value.get("window_end") or ""),
        "time_quality": value.get("time_quality") or ("unknown" if window_start == "unknown" else "event"),
        "severity": value.get("severity"),
        "category": value.get("category"),
        "semantic_fields": value.get("semantic_fields") or {},
    }


def _safe_window(value: Mapping[str, Any]) -> dict[str, Any]:
    def scrub(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): scrub(child) for key, child in item.items() if str(key).lower() not in _RAW_LOG_KEYS}
        if isinstance(item, list):
            return [scrub(child) for child in item]
        return item

    safe = json.loads(json.dumps(scrub(dict(value)), ensure_ascii=False, default=str))
    if not str(safe.get("template_hash") or ""):
        raise StreamingStateError("窗口缺少 template_hash")
    window_start = safe.get("window_start")
    if window_start:
        try:
            parsed = datetime.fromisoformat(str(window_start).replace("Z", "+00:00"))
        except ValueError as exc:
            raise StreamingStateError("窗口 window_start 无效") from exc
        if parsed.tzinfo is None:
            raise StreamingStateError("窗口 window_start 必须包含时区")
    elif safe.get("time_quality") != "unknown":
        raise StreamingStateError("窗口缺少可信 window_start 时必须标记 time_quality=unknown")
    return safe


def _payload_hash(cursor: Mapping[str, Any], windows: list[dict[str, Any]], summary: Mapping[str, Any]) -> str:
    payload = {"cursor": dict(cursor), "windows": windows, "summary": dict(summary)}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _stored_payload_hash(
    connection: Any,
    task_id: str,
    window_id: str,
    cursor: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> tuple[str, int]:
    """Reproduce the historical payload hash without materializing a whole batch."""

    digest = hashlib.sha256()
    digest.update(b'{"cursor":')
    digest.update(_canonical_json(dict(cursor)).encode("utf-8"))
    digest.update(b',"summary":')
    digest.update(_canonical_json(dict(summary)).encode("utf-8"))
    digest.update(b',"windows":[')
    after = -1
    count = 0
    while True:
        rows = connection.execute(
            "SELECT item_index,window_json FROM streaming_batch_windows "
            "WHERE task_id=? AND window_id=? AND item_index>? ORDER BY item_index LIMIT 250",
            (task_id, window_id, after),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            index = int(row["item_index"])
            if index != count:
                raise StreamingIncompleteError(
                    "legacy_partial: 批次窗口 item_index 不连续",
                    details={"error_code": "STREAMING_PREFIX_INCOMPLETE", "reason": "window_index"},
                )
            if count:
                digest.update(b",")
            digest.update(_canonical_json(_decode_json(row["window_json"])).encode("utf-8"))
            count += 1
            after = index
    digest.update(b"]}")
    return digest.hexdigest(), count


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _validate_new_summary(summary: Mapping[str, Any]) -> None:
    if summary.get("summary_schema_version") is None:
        return
    if summary.get("summary_schema_version") != 2:
        raise StreamingStateError("批摘要 schema version 无效")
    integer_fields = (
        "record_count", "template_count", "unknown_template_count", "risk_semantic_matches",
        "template_event_count", "partition_count", "worker_count",
    )
    for key in integer_fields:
        if not _is_nonnegative_int(summary.get(key)):
            raise StreamingStateError(f"批摘要 {key} 必须是非负整数")
    for key in ("parallel", "node_risk_enabled"):
        if not isinstance(summary.get(key), bool):
            raise StreamingStateError(f"批摘要 {key} 必须是布尔值")
    method = summary.get("process_start_method")
    if method not in {"spawn", "fork", "forkserver", "not_applicable"}:
        raise StreamingStateError("批摘要 process_start_method 无效")


def _cursor_is_at_least(candidate: Mapping[str, Any], current: SourceCursor) -> bool:
    """Return whether a duplicate receipt cursor does not move state backward."""

    incoming = SourceCursor.from_dict(candidate)
    if not current.kind or incoming.kind != current.kind:
        return not current.kind
    if incoming.kind == "file":
        return (
            int(incoming.value.get("offset") or 0),
            int(incoming.value.get("line") or 0),
        ) >= (
            int(current.value.get("offset") or 0),
            int(current.value.get("line") or 0),
        )
    if incoming.kind == "kafka":
        incoming_high_water = incoming.value.get("high_water")
        current_high_water = current.value.get("high_water")
        if isinstance(incoming_high_water, Mapping) and isinstance(current_high_water, Mapping):
            incoming_values = {str(key): int(value) for key, value in incoming_high_water.items()}
            current_values = {str(key): int(value) for key, value in current_high_water.items()}
            return set(incoming_values) >= set(current_values) and all(
                incoming_values[key] >= current_values[key] for key in current_values
            )
        incoming_partition = incoming.value.get("partition")
        current_partition = current.value.get("partition")
        if incoming_partition is not None and current_partition is not None and str(incoming_partition) == str(current_partition):
            return int(incoming.value.get("offset") or 0) >= int(current.value.get("offset") or 0)
    return incoming.to_dict() == current.to_dict()


def _reject_raw_fields(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in _RAW_LOG_KEYS:
                raise StreamingStateError("未知模板不能保存原始日志字段")
            _reject_raw_fields(item)
    elif isinstance(value, list):
        for item in value:
            _reject_raw_fields(item)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _decode_json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return dict(json.loads(value))
    if isinstance(value, Mapping):
        return dict(value)
    raise StreamingStateError("数据库中的流式任务状态无效")
