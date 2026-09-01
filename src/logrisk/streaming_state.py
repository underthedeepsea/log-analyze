from __future__ import annotations

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


_RAW_LOG_KEYS = {"raw_sample", "samples", "message", "content", "raw_log", "log_line"}


class StreamingStateRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

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

    def claim_task(self, task_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if task.get("status") == "running":
                raise StreamingTaskBusyError("流式任务已被其他 Worker 占用")
            task.update({"status": "running", "stage": "READING", "error": None, "updated_at": now})
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

    def interrupt_running_tasks(self) -> int:
        count = 0
        for task in self.list_tasks(limit=500):
            if task.get("status") == "running":
                self.mark_interrupted(str(task["task_id"]))
                count += 1
        return count

    def clear_pending_external_commit(self, task_id: str, cursor: SourceCursor | Mapping[str, Any]) -> dict[str, Any]:
        cursor_value = cursor.to_dict() if isinstance(cursor, SourceCursor) else SourceCursor.from_dict(cursor).to_dict()
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
            if task.get("pending_external_commit") == cursor_value:
                task["pending_external_commit"] = None
                task["updated_at"] = now
                connection.execute(
                    "UPDATE streaming_tasks SET task_json=?, updated_at=? WHERE task_id=?",
                    (_json(task), now, task_id),
                )
                self._append_event(connection, task_id, "external_commit_completed", {}, now)
        return task

    def save_result(self, task_id: str, result: Mapping[str, Any]) -> dict[str, Any]:
        safe_result = dict(result)
        _reject_raw_fields(safe_result)
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            task = _decode_json(row[0])
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
        summary: Mapping[str, Any] | None = None,
    ) -> bool:
        if not window_id:
            raise StreamingStateError("窗口标识不能为空")
        cursor_value = cursor.to_dict() if isinstance(cursor, SourceCursor) else SourceCursor.from_dict(cursor).to_dict()
        sanitized_templates = [_safe_template(item) for item in templates]
        commit_summary = dict(summary or {})
        now = utc_now()
        with self.database.transaction() as connection:
            task_row = connection.execute("SELECT task_json, config_hash FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
            if task_row is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            existing = connection.execute(
                "SELECT window_id FROM streaming_window_commits WHERE task_id=? AND window_id=?", (task_id, window_id)
            ).fetchone()
            if existing is not None:
                task = _decode_json(task_row[0])
                task["cursor"] = cursor_value
                task["updated_at"] = now
                connection.execute(
                    "UPDATE streaming_tasks SET cursor_json=?, task_json=?, updated_at=? WHERE task_id=?",
                    (_json(cursor_value), _json(task), now, task_id),
                )
                return False
            connection.execute(
                "INSERT INTO streaming_window_commits(task_id, window_id, cursor_json, summary_json, committed_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, window_id, _json(cursor_value), _json(commit_summary), now),
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
            task = _decode_json(task_row[0])
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
        "severity": value.get("severity"),
        "category": value.get("category"),
        "semantic_fields": value.get("semantic_fields") or {},
    }


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
