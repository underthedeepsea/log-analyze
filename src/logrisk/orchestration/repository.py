from __future__ import annotations

import json
import uuid
from typing import Any, Mapping, Sequence

from logrisk.database import Database, utc_now
from logrisk.runtime.repository import sanitize_runtime_metadata


_STATUSES = {
    "pending_dispatch",
    "dispatched",
    "running",
    "cancel_requested",
    "completed",
    "failed",
    "cancelled",
    "dispatch_failed",
}
_TRANSITIONS = {
    "pending_dispatch": {"dispatched", "cancel_requested", "dispatch_failed", "failed"},
    "dispatched": {"running", "cancel_requested", "completed", "failed", "cancelled", "dispatch_failed"},
    "running": {"cancel_requested", "completed", "failed", "cancelled"},
    "cancel_requested": {"cancelled", "failed"},
    "dispatch_failed": {"pending_dispatch", "cancel_requested", "failed"},
}
_EDITABLE_COLUMNS = {
    "external_dag_id",
    "external_run_id",
    "attempt",
    "last_heartbeat_at",
    "started_at",
    "completed_at",
    "error_code",
    "error_summary",
}
_SENSITIVE_ERROR_MARKERS = (
    "api_key",
    "authorization",
    "bearer",
    "cookie",
    "password",
    "secret",
    "token",
    "://",
)


class OrchestrationConflict(RuntimeError):
    """Raised when another worker has already advanced an orchestration run."""

    code = "orchestration_state_conflict"


def _json(value: Sequence[str]) -> str:
    return json.dumps([str(item) for item in value if str(item)], ensure_ascii=False, separators=(",", ":"))


def _roles(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if not value:
        return []
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(item) for item in decoded] if isinstance(decoded, list) else []


class OrchestrationRepository:
    """Persist scheduler state without storing task payloads, logs, or credentials."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def create_pending(
        self,
        *,
        job_id: str,
        orchestrator: str,
        request_id: str,
        actor: str,
        roles: Sequence[str] = (),
    ) -> dict[str, Any]:
        normalized_job = str(job_id).strip()
        normalized_orchestrator = str(orchestrator).strip()
        normalized_request = str(request_id).strip()
        normalized_actor = str(actor).strip()
        if not normalized_job or not normalized_request or not normalized_actor:
            raise ValueError("job_id、request_id 和 actor 不能为空")
        if normalized_orchestrator != "airflow":
            raise ValueError("当前仅支持 airflow 编排器")
        now = utc_now()
        with self.database.transaction() as connection:
            run_id = "orchestration-" + uuid.uuid4().hex
            connection.execute(
                "INSERT INTO orchestration_runs(orchestration_run_id, job_id, orchestrator, status, request_id, actor, roles_json, created_at, updated_at) "
                "VALUES (?, ?, ?, 'pending_dispatch', ?, ?, ?, ?, ?) "
                "ON CONFLICT(job_id, orchestrator) DO NOTHING",
                (run_id, normalized_job, normalized_orchestrator, normalized_request, normalized_actor, _json(roles), now, now),
            )
            row = connection.execute(
                "SELECT * FROM orchestration_runs WHERE job_id=? AND orchestrator=?",
                (normalized_job, normalized_orchestrator),
            ).fetchone()
            if row is None:
                raise RuntimeError("无法创建编排运行记录")
            return self._run(row)

    def get(self, orchestration_run_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM orchestration_runs WHERE orchestration_run_id=?",
                (str(orchestration_run_id),),
            ).fetchone()
        if row is None:
            raise KeyError("编排运行记录不存在")
        return self._run(row)

    def for_job(self, job_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM orchestration_runs WHERE job_id=? AND orchestrator='airflow'",
                (str(job_id),),
            ).fetchone()
        return self._run(row) if row is not None else None

    def transition(
        self,
        orchestration_run_id: str,
        *,
        from_status: str,
        to_status: str,
        expected_version: int,
        **changes: Any,
    ) -> dict[str, Any]:
        current = self._status(from_status)
        target = self._status(to_status)
        if target == current and current == "running":
            pass
        elif target not in _TRANSITIONS.get(current, set()):
            raise ValueError(f"不允许将编排状态从 {current} 切换到 {target}")
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version 必须是正整数")
        unknown = sorted(set(changes) - _EDITABLE_COLUMNS)
        if unknown:
            raise ValueError("不允许修改编排字段: " + ", ".join(unknown))
        values: list[Any] = [target]
        assignments = ["status=?", "state_version=state_version+1", "updated_at=?"]
        values.append(utc_now())
        for name in sorted(changes):
            assignments.append(name + "=?")
            values.append(self._value(name, changes[name]))
        values.extend((str(orchestration_run_id), current, expected_version))
        with self.database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE orchestration_runs SET " + ", ".join(assignments)
                + " WHERE orchestration_run_id=? AND status=? AND state_version=?",
                values,
            )
            if cursor.rowcount != 1:
                raise OrchestrationConflict("编排状态已被其他执行者更新")
            row = connection.execute(
                "SELECT * FROM orchestration_runs WHERE orchestration_run_id=?",
                (str(orchestration_run_id),),
            ).fetchone()
            if row is None:
                raise RuntimeError("编排运行记录在状态更新后不存在")
            return self._run(row)

    def list_reconcilable(self, *, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 500))
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM orchestration_runs WHERE status IN ('pending_dispatch', 'dispatch_failed') "
                "ORDER BY updated_at, orchestration_run_id LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return [self._run(row) for row in rows]

    @staticmethod
    def _status(value: Any) -> str:
        status = str(value).strip()
        if status not in _STATUSES:
            raise ValueError("无效编排状态")
        return status

    @staticmethod
    def _value(name: str, value: Any) -> Any:
        if name == "attempt":
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("attempt 必须是非负整数")
            return value
        if name in {"error_code", "error_summary"}:
            return _safe_error_value(name, value)
        return None if value is None else str(value)[:1000]

    @staticmethod
    def _run(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "orchestration_run_id": str(row["orchestration_run_id"]),
            "job_id": str(row["job_id"]),
            "orchestrator": str(row["orchestrator"]),
            "external_dag_id": row["external_dag_id"],
            "external_run_id": row["external_run_id"],
            "status": str(row["status"]),
            "attempt": int(row["attempt"]),
            "state_version": int(row["state_version"]),
            "request_id": str(row["request_id"]),
            "actor": str(row["actor"]),
            "roles": _roles(row["roles_json"]),
            "last_heartbeat_at": row["last_heartbeat_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "error_code": row["error_code"],
            "error_summary": row["error_summary"],
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }


def _safe_error_value(name: str, value: Any) -> str | None:
    if value is None:
        return None
    cleaned = sanitize_runtime_metadata({"summary": str(value)})
    text = str(cleaned.get("summary") or "").strip()[:1000]
    if any(marker in text.casefold() for marker in _SENSITIVE_ERROR_MARKERS):
        return "orchestration_error" if name == "error_code" else "编排器返回了受保护的错误详情"
    return text
