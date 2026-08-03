from __future__ import annotations

import json
import re
import uuid
from typing import Any, Mapping, Sequence

from logrisk.database import Database, utc_now
from logrisk.runtime.repository import sanitize_runtime_metadata


_STATUS_VALUES = {"passed", "warning", "blocked"}
_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


def _json(value: Any) -> str:
    return json.dumps(sanitize_runtime_metadata(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    parsed = json.loads(str(value))
    return parsed if isinstance(parsed, dict) else {}


class ReleaseReadinessRepository:
    """Persist sanitized, immutable release validation results."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def record_validation(
        self,
        *,
        target_version: str,
        idempotency_key: str,
        status: str,
        summary: Mapping[str, Any],
        checks: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        target = str(target_version).strip()
        key = str(idempotency_key).strip()
        if not _VERSION_PATTERN.fullmatch(target):
            raise ValueError("target_version 必须是语义版本号")
        if not key:
            raise ValueError("idempotency_key 不能为空")
        validated_status = self._status(status, field="验证")
        normalized_checks = [self._check(item, position) for position, item in enumerate(checks)]
        if not normalized_checks:
            raise ValueError("至少需要一项发布检查")
        now = utc_now()
        with self.database.transaction() as connection:
            duplicate = connection.execute(
                "SELECT validation_id FROM release_validations WHERE idempotency_key=?", (key,)
            ).fetchone()
            if duplicate is not None:
                return self._validation(connection, str(duplicate["validation_id"]))
            validation_id = "release-validation-" + uuid.uuid4().hex
            connection.execute(
                "INSERT INTO release_validations(validation_id, target_version, idempotency_key, status, summary_json, schema_version, created_at) VALUES (?, ?, ?, ?, ?, 'release_validation_v1', ?)",
                (validation_id, target, key, validated_status, _json(dict(summary)), now),
            )
            for item in normalized_checks:
                connection.execute(
                    "INSERT INTO release_validation_checks(validation_id, check_id, position, title, status, code, message, evidence_json, schema_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'release_validation_check_v1', ?)",
                    (
                        validation_id,
                        item["check_id"],
                        item["position"],
                        item["title"],
                        item["status"],
                        item["code"],
                        item["message"],
                        _json(item["evidence"]),
                        now,
                    ),
                )
            return self._validation(connection, validation_id)

    def latest(self) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT validation_id FROM release_validations ORDER BY created_at DESC, validation_id DESC LIMIT 1"
            ).fetchone()
            return self._validation(connection, str(row["validation_id"])) if row is not None else None

    def by_idempotency_key(self, idempotency_key: str) -> dict[str, Any] | None:
        """Return the immutable prior result for a client retry, when present."""
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT validation_id FROM release_validations WHERE idempotency_key=?",
                (str(idempotency_key).strip(),),
            ).fetchone()
            return self._validation(connection, str(row["validation_id"])) if row is not None else None

    def get(self, validation_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            return self._validation(connection, str(validation_id))

    def list_history(self, *, limit: int = 30) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT validation_id FROM release_validations ORDER BY created_at DESC, validation_id DESC LIMIT ?",
                (max(1, min(int(limit), 200)),),
            ).fetchall()
            items = [self._validation(connection, str(row["validation_id"])) for row in rows]
        return {"schema_version": "release_validation_history_v1", "items": items}

    def _validation(self, connection: Any, validation_id: str) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM release_validations WHERE validation_id=?", (validation_id,)
        ).fetchone()
        if row is None:
            raise KeyError("发布校验记录不存在")
        check_rows = connection.execute(
            "SELECT * FROM release_validation_checks WHERE validation_id=? ORDER BY position, check_id",
            (validation_id,),
        ).fetchall()
        return {
            "validation_id": str(row["validation_id"]),
            "target_version": str(row["target_version"]),
            "status": str(row["status"]),
            "summary": _object(row["summary_json"]),
            "schema_version": str(row["schema_version"]),
            "created_at": str(row["created_at"]),
            "checks": [
                {
                    "check_id": str(check["check_id"]),
                    "title": str(check["title"]),
                    "status": str(check["status"]),
                    "code": str(check["code"]),
                    "message": str(check["message"]),
                    "evidence": _object(check["evidence_json"]),
                    "schema_version": str(check["schema_version"]),
                }
                for check in check_rows
            ],
        }

    @staticmethod
    def _status(value: Any, *, field: str) -> str:
        candidate = str(value)
        if candidate not in _STATUS_VALUES:
            raise ValueError(f"{field}状态必须是 passed、warning 或 blocked")
        return candidate

    def _check(self, value: Mapping[str, Any], position: int) -> dict[str, Any]:
        check_id = str(value.get("check_id") or "").strip()
        if not check_id or len(check_id) > 120:
            raise ValueError("check_id 必须为 1 到 120 个字符")
        return {
            "check_id": check_id,
            "position": position,
            "title": str(value.get("title") or check_id).strip()[:240],
            "status": self._status(value.get("status"), field="检查"),
            "code": str(value.get("code") or check_id).strip()[:120],
            "message": str(value.get("message") or "").strip()[:1000],
            "evidence": sanitize_runtime_metadata(dict(value.get("evidence") or {})),
        }
