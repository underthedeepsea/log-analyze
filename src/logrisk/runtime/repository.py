from __future__ import annotations

import json
import uuid
from typing import Any, Mapping

from logrisk.database import Database, utc_now


_SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "content",
    "cookie",
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


class RuntimeConflictError(RuntimeError):
    """Raised when a runtime asset changed before a requested write."""

    code = "runtime_version_conflict"


def sanitize_runtime_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): sanitize_runtime_metadata(item)
            for key, item in value.items()
            if str(key).lower() not in _SENSITIVE_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_runtime_metadata(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _json(value: Any) -> str:
    return json.dumps(sanitize_runtime_metadata(value), ensure_ascii=False, separators=(",", ":"))


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    parsed = json.loads(str(value))
    return parsed if isinstance(parsed, dict) else {}


def _array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if not value:
        return []
    parsed = json.loads(str(value))
    return parsed if isinstance(parsed, list) else []


class RuntimeRepository:
    policy_id = "default"

    def __init__(self, database: Database) -> None:
        self.database = database

    def get_policy(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_policies WHERE policy_id=?", (self.policy_id,)
            ).fetchone()
        if row is None:
            return {"policy_id": self.policy_id, "policy": {}, "version": 0, "created_at": None, "updated_at": None}
        return self._policy(row)

    def save_policy(
        self,
        policy: Mapping[str, Any],
        *,
        expected_version: int,
        actor: str | None,
        request_id: str,
        roles: tuple[str, ...] | list[str] = (),
    ) -> dict[str, Any]:
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 0:
            raise ValueError("expected_version 必须是非负整数")
        now = utc_now()
        stored = sanitize_runtime_metadata(dict(policy))
        with self.database.transaction() as connection:
            current = connection.execute(
                "SELECT * FROM runtime_policies WHERE policy_id=?", (self.policy_id,)
            ).fetchone()
            current_version = int(current["version"]) if current else 0
            if current_version != expected_version:
                raise RuntimeConflictError("运行时策略已被其他操作更新")
            next_version = current_version + 1
            if current is None:
                connection.execute(
                    "INSERT INTO runtime_policies(policy_id, policy_json, version, schema_version, created_at, updated_at) VALUES (?, ?, ?, 'runtime_policy_v1', ?, ?)",
                    (self.policy_id, _json(stored), next_version, now, now),
                )
            else:
                connection.execute(
                    "UPDATE runtime_policies SET policy_json=?, version=?, updated_at=? WHERE policy_id=?",
                    (_json(stored), next_version, now, self.policy_id),
                )
            self._append_audit(
                connection,
                action="policy.updated",
                resource_type="runtime_policy",
                resource_id=self.policy_id,
                actor=actor,
                roles=roles,
                request_id=request_id,
                outcome="success",
                attributes={"version": next_version, "policy": stored},
                created_at=now,
            )
            row = connection.execute(
                "SELECT * FROM runtime_policies WHERE policy_id=?", (self.policy_id,)
            ).fetchone()
        return self._policy(row)

    def append_audit(
        self,
        action: str,
        resource_type: str,
        actor: str | None,
        request_id: str,
        attributes: Mapping[str, Any] | None = None,
        *,
        resource_id: str | None = None,
        roles: tuple[str, ...] | list[str] = (),
        outcome: str = "success",
    ) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            return self._append_audit(
                connection,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                actor=actor,
                roles=roles,
                request_id=request_id,
                outcome=outcome,
                attributes=attributes or {},
                created_at=now,
            )

    def _append_audit(
        self,
        connection: Any,
        *,
        action: str,
        resource_type: str,
        resource_id: str | None,
        actor: str | None,
        roles: tuple[str, ...] | list[str],
        request_id: str,
        outcome: str,
        attributes: Mapping[str, Any],
        created_at: str,
    ) -> dict[str, Any]:
        audit_id = "runtime-audit-" + uuid.uuid4().hex
        cleaned_roles = [str(role) for role in roles if str(role)]
        cleaned_attributes = sanitize_runtime_metadata(dict(attributes))
        connection.execute(
            "INSERT INTO runtime_audit_events(audit_id, action, resource_type, resource_id, actor, roles_json, request_id, outcome, attributes_json, schema_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'runtime_audit_v1', ?)",
            (
                audit_id,
                str(action),
                str(resource_type),
                resource_id,
                actor,
                _json(cleaned_roles),
                str(request_id),
                str(outcome),
                _json(cleaned_attributes),
                created_at,
            ),
        )
        return {
            "audit_id": audit_id,
            "action": str(action),
            "resource_type": str(resource_type),
            "resource_id": resource_id,
            "actor": actor,
            "roles": cleaned_roles,
            "request_id": str(request_id),
            "outcome": str(outcome),
            "attributes": cleaned_attributes,
            "created_at": created_at,
        }

    def list_audits(self, *, limit: int = 100, before: str | None = None) -> dict[str, Any]:
        limit = max(1, min(int(limit), 500))
        parameters: list[Any] = []
        where = ""
        if before:
            where = " WHERE created_at < ?"
            parameters.append(before)
        parameters.append(limit)
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_audit_events" + where + " ORDER BY created_at DESC, audit_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        items = [self._audit(row) for row in rows]
        return {"items": items, "next_before": items[-1]["created_at"] if len(items) == limit else None}

    def start_maintenance(
        self, *, action: str, mode: str, actor: str | None, request_id: str
    ) -> dict[str, Any]:
        if mode not in {"dry_run", "execute"}:
            raise ValueError("维护模式必须是 dry_run 或 execute")
        now = utc_now()
        run_id = "maintenance-" + uuid.uuid4().hex
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO runtime_maintenance_runs(run_id, action, mode, status, summary_json, error_code, error_message, actor, request_id, schema_version, created_at, updated_at, completed_at) VALUES (?, ?, ?, 'running', '{}', NULL, NULL, ?, ?, 'runtime_maintenance_v1', ?, ?, NULL)",
                (run_id, action, mode, actor, request_id, now, now),
            )
            row = connection.execute(
                "SELECT * FROM runtime_maintenance_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return self._maintenance(row)

    def finish_maintenance(
        self,
        run_id: str,
        *,
        status: str,
        summary: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"completed", "failed"}:
            raise ValueError("维护状态必须是 completed 或 failed")
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE runtime_maintenance_runs SET status=?, summary_json=?, error_code=?, error_message=?, updated_at=?, completed_at=? WHERE run_id=?",
                (status, _json(dict(summary or {})), error_code, error_message, now, now, run_id),
            )
            row = connection.execute(
                "SELECT * FROM runtime_maintenance_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError("维护任务不存在")
        return self._maintenance(row)

    def record_quota_snapshot(self, usage: Mapping[str, Any]) -> dict[str, Any]:
        now = utc_now()
        snapshot_id = "quota-" + uuid.uuid4().hex
        sanitized = sanitize_runtime_metadata(dict(usage))
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO runtime_quota_snapshots(snapshot_id, usage_json, schema_version, created_at) VALUES (?, ?, 'runtime_quota_v1', ?)",
                (snapshot_id, _json(sanitized), now),
            )
        return {"snapshot_id": snapshot_id, "usage": sanitized, "created_at": now}

    def latest_quota_snapshot(self) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_quota_snapshots ORDER BY created_at DESC, snapshot_id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {"snapshot_id": row["snapshot_id"], "usage": _object(row["usage_json"]), "created_at": row["created_at"]}

    @staticmethod
    def _policy(row: Any) -> dict[str, Any]:
        return {
            "policy_id": row["policy_id"],
            "policy": _object(row["policy_json"]),
            "version": int(row["version"]),
            "schema_version": row["schema_version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _audit(row: Any) -> dict[str, Any]:
        return {
            "audit_id": row["audit_id"],
            "action": row["action"],
            "resource_type": row["resource_type"],
            "resource_id": row["resource_id"],
            "actor": row["actor"],
            "roles": _array(row["roles_json"]),
            "request_id": row["request_id"],
            "outcome": row["outcome"],
            "attributes": _object(row["attributes_json"]),
            "created_at": row["created_at"],
        }

    @staticmethod
    def _maintenance(row: Any) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "action": row["action"],
            "mode": row["mode"],
            "status": row["status"],
            "summary": _object(row["summary_json"]),
            "error_code": row["error_code"],
            "error_message": row["error_message"],
            "actor": row["actor"],
            "request_id": row["request_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }
