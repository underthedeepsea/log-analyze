from __future__ import annotations

import json
import uuid
from typing import Any

from logrisk.database import Database, utc_now

from .errors import AgenticError
from .models import AgentPlan, AgentRunRequest


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _db_json(database: Database, value: Any) -> Any:
    return value if getattr(database, "provider", "sqlite") == "postgres" else _json(value)


def _decode(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


class AgentRepository:
    """Minimal cross-database persistence for recoverable sequential Agent runs."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def create_run(self, request: AgentRunRequest, *, locked_snapshot: dict[str, Any]) -> dict[str, Any]:
        existing = self._by_idempotency(request.actor, request.idempotency_key)
        if existing:
            return {**self.get_run(existing), "idempotent_replay": True}
        run_id = uuid.uuid4().hex
        now = utc_now()
        try:
            with self.database.transaction() as connection:
                connection.execute(
                    "INSERT INTO agent_runs(run_id, schema_version, parent_run_id, source_job_id, entity_id, entity_type, "
                    "model_profile_id, prompt_id, status, max_steps, max_tool_calls, used_tool_calls, timeout_seconds, "
                    "allowed_tools_json, locked_snapshot_json, goal, error_code, error_summary, idempotency_key, actor, roles_json, "
                    "request_id, state_version, created_at, updated_at, started_at, completed_at) "
                    "VALUES (?, '1.0', ?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, 1, ?, ?, NULL, NULL)",
                    (
                        run_id, request.parent_run_id, request.source_job_id, request.entity_id, request.entity_type,
                        request.model_profile_id, request.prompt_id, request.max_steps, request.max_tool_calls,
                        request.timeout_seconds, _db_json(self.database, list(request.allowed_tools)),
                        _db_json(self.database, locked_snapshot), request.idempotency_key, request.actor,
                        _db_json(self.database, list(request.roles)), request.request_id, now, now,
                    ),
                )
                self._append_event(connection, run_id, "run_created", {"status": "queued"}, now)
        except Exception:
            existing = self._by_idempotency(request.actor, request.idempotency_key)
            if existing:
                return {**self.get_run(existing), "idempotent_replay": True}
            raise
        return {**self.get_run(run_id), "idempotent_replay": False}

    def replace_plan(self, run_id: str, plan: AgentPlan) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            if not connection.execute("SELECT 1 FROM agent_runs WHERE run_id=?", (run_id,)).fetchone():
                raise AgenticError("Agent Run 不存在", code="agent_run_not_found", status_code=404)
            connection.execute("DELETE FROM agent_run_steps WHERE run_id=?", (run_id,))
            for sequence, step in enumerate(plan.steps):
                connection.execute(
                    "INSERT INTO agent_run_steps(run_id, step_id, sequence, tool_name, arguments_json, status, attempt, "
                    "result_summary_json, error_code, error_summary, started_at, completed_at, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, NULL, NULL, ?, ?)",
                    (run_id, step.step_id, sequence, step.tool_name, _db_json(self.database, step.arguments), _db_json(self.database, {}), now, now),
                )
            connection.execute("UPDATE agent_runs SET goal=?, updated_at=?, state_version=state_version+1 WHERE run_id=?", (plan.goal, now, run_id))
        return self.get_run(run_id)

    def append_event(self, run_id: str, event_type: str, attributes: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        with self.database.transaction() as connection:
            if idempotency_key:
                existing = connection.execute(
                    "SELECT * FROM agent_run_events WHERE run_id=? AND idempotency_key=?", (run_id, idempotency_key)
                ).fetchone()
                if existing:
                    return self._event(existing)
            event = self._append_event(connection, run_id, event_type, attributes, utc_now(), idempotency_key=idempotency_key)
        return event

    def add_artifact(self, run_id: str, artifact_type: str, payload: dict[str, Any], *, step_id: str | None = None, fingerprint: str | None = None) -> dict[str, Any]:
        artifact_id = uuid.uuid4().hex
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO agent_artifacts(artifact_id, run_id, step_id, artifact_type, payload_json, fingerprint, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (artifact_id, run_id, step_id, artifact_type, _db_json(self.database, payload), fingerprint, now),
            )
        return {"artifact_id": artifact_id, "run_id": run_id, "step_id": step_id, "artifact_type": artifact_type, "payload": payload, "fingerprint": fingerprint, "created_at": now}

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM agent_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise AgenticError("Agent Run 不存在", code="agent_run_not_found", status_code=404)
            steps = connection.execute("SELECT * FROM agent_run_steps WHERE run_id=? ORDER BY sequence", (run_id,)).fetchall()
            tool_calls = connection.execute("SELECT * FROM agent_tool_calls WHERE run_id=? ORDER BY created_at, tool_call_id", (run_id,)).fetchall()
            artifacts = connection.execute("SELECT * FROM agent_artifacts WHERE run_id=? ORDER BY created_at, artifact_id", (run_id,)).fetchall()
            events = connection.execute("SELECT * FROM agent_run_events WHERE run_id=? ORDER BY sequence", (run_id,)).fetchall()
        result = self._run(row)
        result["steps"] = [self._step(item) for item in steps]
        result["tool_calls"] = [self._tool_call(item) for item in tool_calls]
        result["artifacts"] = [self._artifact(item) for item in artifacts]
        result["events"] = [self._event(item) for item in events]
        return result

    def list_runs(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT run_id FROM agent_runs ORDER BY created_at DESC, run_id DESC LIMIT ?", (max(1, min(int(limit), 500)),)).fetchall()
        return [self.get_run(str(row["run_id"])) for row in rows]

    def transition(
        self,
        run_id: str,
        status: str,
        *,
        allowed_from: set[str],
        error_code: str | None = None,
        error_summary: str | None = None,
        event_idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            locked = connection.execute(
                "UPDATE agent_runs SET state_version=state_version WHERE run_id=?", (run_id,)
            )
            if locked.rowcount != 1:
                raise AgenticError("Agent Run 不存在", code="agent_run_not_found", status_code=404)
            if event_idempotency_key and connection.execute(
                "SELECT 1 FROM agent_run_events WHERE run_id=? AND idempotency_key=?", (run_id, event_idempotency_key)
            ).fetchone():
                return_value = True
            else:
                return_value = False
            row = connection.execute("SELECT status, state_version FROM agent_runs WHERE run_id=?", (run_id,)).fetchone()
            if not return_value and str(row["status"]) not in allowed_from:
                raise AgenticError("Agent Run 状态不允许此操作", code="agent_state_conflict", status_code=409)
            if return_value:
                changed = None
            else:
                started_at = now if status in {"planning", "running"} else None
                completed_at = now if status in {"completed", "failed", "cancelled"} else None
                changed = connection.execute(
                    "UPDATE agent_runs SET status=?, error_code=?, error_summary=?, "
                    "started_at=COALESCE(started_at, ?), completed_at=COALESCE(?, completed_at), updated_at=?, state_version=state_version+1 "
                    "WHERE run_id=? AND state_version=?",
                    (status, error_code, error_summary, started_at, completed_at, now, run_id, row["state_version"]),
                )
            if changed is not None and changed.rowcount != 1:
                raise AgenticError("Agent Run 状态已变化", code="agent_state_conflict", status_code=409)
            if not return_value:
                self._append_event(
                    connection, run_id, "run_" + status, {"status": status, "error_code": error_code}, now,
                    idempotency_key=event_idempotency_key,
                )
        result = self.get_run(run_id)
        if event_idempotency_key:
            result["idempotent_replay"] = return_value
        return result

    def start_step(self, run_id: str, step_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            changed = connection.execute(
                "UPDATE agent_run_steps SET status='running', attempt=attempt+1, started_at=?, updated_at=? "
                "WHERE run_id=? AND step_id=? AND status='pending'",
                (now, now, run_id, step_id),
            )
            if changed.rowcount != 1:
                raise AgenticError("Agent Step 状态不允许执行", code="agent_step_conflict", status_code=409)
        return next(item for item in self.get_run(run_id)["steps"] if item["step_id"] == step_id)

    def finish_step(
        self,
        run_id: str,
        step_id: str,
        *,
        status: str,
        result_summary: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            changed = connection.execute(
                "UPDATE agent_run_steps SET status=?, result_summary_json=?, error_code=?, error_summary=?, completed_at=?, updated_at=? "
                "WHERE run_id=? AND step_id=? AND status='running'",
                (status, _db_json(self.database, result_summary or {}), error_code, error_summary, now, now, run_id, step_id),
            )
            if changed.rowcount != 1:
                raise AgenticError("Agent Step 状态已变化", code="agent_step_conflict", status_code=409)
        return next(item for item in self.get_run(run_id)["steps"] if item["step_id"] == step_id)

    def record_tool_call(
        self,
        run_id: str,
        step_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        status: str,
        idempotency_key: str,
        cost_units: int = 1,
        result: dict[str, Any] | None = None,
        latency_ms: int | None = None,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        existing: dict[str, Any] | None = None
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM agent_tool_calls WHERE run_id=? AND idempotency_key=?", (run_id, idempotency_key)
            ).fetchone()
            if row:
                existing = self._tool_call(row)
            else:
                tool_call_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO agent_tool_calls(tool_call_id, run_id, step_id, tool_name, arguments_summary_json, result_summary_json, "
                    "status, cost_units, latency_ms, error_code, error_summary, idempotency_key, created_at, completed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (tool_call_id, run_id, step_id, tool_name, _db_json(self.database, arguments), _db_json(self.database, result or {}),
                     status, int(cost_units), latency_ms, error_code, error_summary, idempotency_key, now, now if status != "running" else None),
                )
                connection.execute(
                    "UPDATE agent_runs SET used_tool_calls=used_tool_calls+?, updated_at=?, state_version=state_version+1 WHERE run_id=?",
                    (int(cost_units), now, run_id),
                )
        if existing is not None:
            return existing
        return next(item for item in self.get_run(run_id)["tool_calls"] if item["idempotency_key"] == idempotency_key)

    def _by_idempotency(self, actor: str, key: str) -> str | None:
        with self.database.connect() as connection:
            row = connection.execute("SELECT run_id FROM agent_runs WHERE actor=? AND idempotency_key=?", (actor, key)).fetchone()
        return str(row["run_id"]) if row else None

    def list_active_run_ids(self) -> list[str]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT run_id FROM agent_runs WHERE status IN ('queued','planning','running') ORDER BY created_at, run_id"
            ).fetchall()
        return [str(row["run_id"]) for row in rows]

    def recover_active_runs(self) -> list[str]:
        """Reset interrupted steps so startup recovery can replay them idempotently."""
        now = utc_now()
        with self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT run_id FROM agent_runs WHERE status IN ('queued','planning','running') ORDER BY created_at, run_id"
            ).fetchall()
            run_ids = [str(row["run_id"]) for row in rows]
            for run_id in run_ids:
                connection.execute(
                    "UPDATE agent_run_steps SET status='pending', error_code=NULL, error_summary=NULL, completed_at=NULL, updated_at=? "
                    "WHERE run_id=? AND status='running'",
                    (now, run_id),
                )
                self._append_event(connection, run_id, "run_recovered", {"status": "recoverable"}, now)
        return run_ids

    def recover_run(self, run_id: str) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT status FROM agent_runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise AgenticError("Agent Run 不存在", code="agent_run_not_found", status_code=404)
            if str(row["status"]) not in {"queued", "planning", "running"}:
                raise AgenticError("Agent Run 状态不允许恢复", code="agent_state_conflict", status_code=409)
            connection.execute(
                "UPDATE agent_run_steps SET status='pending', error_code=NULL, error_summary=NULL, completed_at=NULL, updated_at=? "
                "WHERE run_id=? AND status='running'", (now, run_id),
            )
            self._append_event(connection, run_id, "run_recovered", {"status": str(row["status"])}, now)
        return self.get_run(run_id)

    def _append_event(
        self, connection: Any, run_id: str, event_type: str, attributes: dict[str, Any], now: str,
        *, idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        locked = connection.execute("UPDATE agent_runs SET state_version=state_version WHERE run_id=?", (run_id,))
        if locked.rowcount != 1:
            raise AgenticError("Agent Run 不存在", code="agent_run_not_found", status_code=404)
        sequence = int(connection.execute("SELECT COALESCE(MAX(sequence), -1) + 1 FROM agent_run_events WHERE run_id=?", (run_id,)).fetchone()[0])
        event_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO agent_run_events(event_id, run_id, sequence, event_type, attributes_json, idempotency_key, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_id, run_id, sequence, event_type, _db_json(self.database, attributes), idempotency_key, now),
        )
        return {"event_id": event_id, "run_id": run_id, "sequence": sequence, "type": event_type, "attributes": attributes, "idempotency_key": idempotency_key, "created_at": now}

    @staticmethod
    def _run(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["allowed_tools"] = _decode(value.pop("allowed_tools_json"), [])
        value["locked_snapshot"] = _decode(value.pop("locked_snapshot_json"), {})
        value["roles"] = _decode(value.pop("roles_json"), [])
        return value

    @staticmethod
    def _step(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["arguments"] = _decode(value.pop("arguments_json"), {})
        value["result_summary"] = _decode(value.pop("result_summary_json"), {})
        return value

    @staticmethod
    def _tool_call(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["arguments_summary"] = _decode(value.pop("arguments_summary_json"), {})
        value["result_summary"] = _decode(value.pop("result_summary_json"), {})
        return value

    @staticmethod
    def _artifact(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["payload"] = _decode(value.pop("payload_json"), {})
        return value

    @staticmethod
    def _event(row: Any) -> dict[str, Any]:
        value = dict(row)
        value["type"] = value.pop("event_type")
        value["attributes"] = _decode(value.pop("attributes_json"), {})
        return value
