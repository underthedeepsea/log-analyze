from __future__ import annotations

import hashlib
import json
import uuid
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
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if task.get("status") == "running":
                raise StreamingTaskBusyError("流式任务已被其他 Worker 占用")
            task.update({
                "status": "running", "stage": "READING", "error": None,
                "lease_token": uuid.uuid4().hex, "updated_at": now,
            })
            connection.execute(
                "UPDATE streaming_tasks SET status='running', stage='READING', task_json=?, updated_at=? WHERE task_id=?",
                (_json(task), now, task_id),
            )
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
        return self._update_task(task_id, status="completed", stage="COMPLETED", event_type="task_completed")

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
            connection.execute(
                "UPDATE streaming_tasks SET status=?, stage=?, task_json=?, updated_at=? WHERE task_id=?",
                (status, stage, _json(task), now, str(task_id)),
            )
            self._append_event(connection, str(task_id), event_type, {"error": error}, now)
        return task

    def complete_claim(self, task_id: str, lease_token: str) -> dict[str, Any]:
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
        payload_hash = _payload_hash(cursor_value, sanitized_windows, commit_summary)
        now = utc_now()
        with self.database.transaction() as connection:
            lock = " FOR UPDATE" if getattr(self.database,"provider","sqlite") == "postgres" else ""
            task_row = connection.execute("SELECT task_json, config_hash FROM streaming_tasks WHERE task_id=?" + lock, (task_id,)).fetchone()
            if task_row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(task_row[0])
            if fencing_token is not None and str(task.get("lease_token") or "") != str(fencing_token):
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
        required = (*sums, "worker_count", "parallel", "node_risk_enabled")
        totals: dict[str, Any] = {key: 0 for key in sums}
        totals.update(worker_count=0, parallel=False, node_risk_enabled=False)
        missing: set[str] = set()
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT summary_json FROM streaming_window_commits WHERE task_id=?", (task_id,)
            )
            for row in rows:
                summary = _decode_json(row[0])
                missing.update(key for key in required if key not in summary)
                for key in sums:
                    totals[key] += int(summary.get(key) or 0)
                totals["worker_count"] = max(totals["worker_count"], int(summary.get("worker_count") or 0))
                for key in ("parallel", "node_risk_enabled"):
                    totals[key] = bool(totals[key] or summary.get(key))
        for key in missing:
            totals[key] = None
        totals["completeness"] = "legacy_partial" if missing else "complete"
        return totals

    def has_legacy_partial_commits(self, task_id: str) -> bool:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS legacy_count FROM streaming_window_commits "
                "WHERE task_id=? AND payload_hash IS NULL",
                (task_id,),
            ).fetchone()
            task = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            summaries = connection.execute("SELECT summary_json FROM streaming_window_commits WHERE task_id=?", (task_id,))
            records = sum(int(_decode_json(item[0]).get("record_count") or 0) for item in summaries)
        return int(row["legacy_count"] or 0) > 0 or (
            task is not None and int(_decode_json(task[0]).get("records_processed") or 0) != records
        )

    def require_complete_prefix(self, task_id: str) -> None:
        if self.has_legacy_partial_commits(task_id):
            raise StreamingIncompleteError(
                "legacy_partial: 已提交前缀缺少完整事实，禁止继续读取或生成完整结果；"
                "请在原始文件仍可验证时显式创建新 Run 重算"
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
