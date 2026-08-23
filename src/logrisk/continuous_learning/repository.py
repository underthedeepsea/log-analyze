from __future__ import annotations

import json
import uuid
from typing import Any, Callable

from logrisk.database import Database, utc_now

from .models import (
    DATASET_SCHEMA_VERSION,
    FEEDBACK_SCHEMA_VERSION,
    ContinuousLearningError,
    canonical_json,
    content_sha256,
    reject_forbidden_keys,
)


_FEEDBACK_OUTCOMES = frozenset({"approved", "rejected"})
_DATASET_STATUSES = frozenset({"candidate", "approved", "retired"})
_DATASET_TRANSITIONS = {
    "candidate": frozenset({"approved", "retired"}),
    "approved": frozenset({"retired"}),
    "retired": frozenset(),
}
_MAX_ID_LENGTH = 255
_MAX_REASON_LENGTH = 120
_MAX_NOTE_LENGTH = 2000


def _db_json(database: Database, value: Any) -> Any:
    return value if getattr(database, "provider", "sqlite") == "postgres" else canonical_json(value)


def _decode(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return default
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


def _required(value: Any, field: str, *, limit: int = _MAX_ID_LENGTH) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > limit:
        raise ContinuousLearningError(f"{field}不能为空且长度不能超过 {limit} 个字符")
    return value.strip()


class ContinuousLearningRepository:
    """Provider-neutral SQL persistence for append-only learning metadata."""

    def __init__(self, database: Database, clock: Callable[[], str] = utc_now) -> None:
        self.database = database
        self.clock = clock

    def append_feedback(
        self,
        *,
        candidate_id: str,
        job_id: str,
        outcome: str,
        reason_code: str,
        note: str,
        actor: str,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        candidate_id = _required(candidate_id, "candidate_id")
        job_id = _required(job_id, "job_id")
        actor = _required(actor, "actor")
        request_id = _required(request_id, "request_id")
        idempotency_key = _required(idempotency_key, "idempotency_key")
        reason_code = _required(reason_code, "reason_code", limit=_MAX_REASON_LENGTH)
        if outcome not in _FEEDBACK_OUTCOMES:
            raise ContinuousLearningError("outcome 必须是 approved 或 rejected")
        if not isinstance(note, str) or len(note) > _MAX_NOTE_LENGTH:
            raise ContinuousLearningError("note 长度不能超过 2000 个字符")
        with self.database.transaction() as connection:
            candidate = connection.execute(
                "SELECT candidate_id, job_id FROM feature_candidates WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            if candidate is None:
                raise ContinuousLearningError("Feature Candidate 不存在", code="candidate_not_found", status_code=404)
            if str(candidate["job_id"]) != job_id:
                raise ContinuousLearningError("Candidate 与 Job 不匹配", code="candidate_job_mismatch", status_code=409)
            existing = connection.execute(
                "SELECT * FROM feature_candidate_feedback WHERE candidate_id=? AND idempotency_key=?",
                (candidate_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                return self._feedback(existing)
            feedback_id = f"feedback-{uuid.uuid4().hex}"
            now = self.clock()
            connection.execute(
                "INSERT INTO feature_candidate_feedback(feedback_id, candidate_id, job_id, outcome, reason_code, note, "
                "actor, request_id, idempotency_key, created_at, schema_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(candidate_id, idempotency_key) DO NOTHING",
                (
                    feedback_id,
                    candidate_id,
                    job_id,
                    outcome,
                    reason_code,
                    note,
                    actor,
                    request_id,
                    idempotency_key,
                    now,
                    FEEDBACK_SCHEMA_VERSION,
                ),
            )
            row = connection.execute(
                "SELECT * FROM feature_candidate_feedback WHERE candidate_id=? AND idempotency_key=?",
                (candidate_id, idempotency_key),
            ).fetchone()
        return self._feedback(row)

    def list_feedback(
        self,
        *,
        candidate_id: str | None = None,
        job_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        try:
            limit = max(1, min(int(limit), 500))
        except (TypeError, ValueError) as exc:
            raise ContinuousLearningError("limit 必须是整数") from exc
        where: list[str] = []
        parameters: list[Any] = []
        if candidate_id is not None:
            where.append("candidate_id=?")
            parameters.append(_required(candidate_id, "candidate_id"))
        if job_id is not None:
            where.append("job_id=?")
            parameters.append(_required(job_id, "job_id"))
        clause = " WHERE " + " AND ".join(where) if where else ""
        parameters.append(limit)
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM feature_candidate_feedback" + clause + " ORDER BY created_at DESC, feedback_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [self._feedback(row) for row in rows]

    def create_dataset_revision(
        self,
        *,
        family_id: str,
        name: str,
        description: str,
        split: str,
        records: list[dict[str, Any]],
        parent_dataset_id: str | None,
        actor: str,
        request_id: str,
    ) -> dict[str, Any]:
        family_id = _required(family_id, "family_id")
        name = _required(name, "name")
        actor = _required(actor, "actor")
        request_id = _required(request_id, "request_id")
        if not isinstance(description, str) or len(description) > _MAX_NOTE_LENGTH:
            raise ContinuousLearningError("description 长度不能超过 2000 个字符")
        split = _required(split, "split", limit=64)
        if not isinstance(records, list) or not records:
            raise ContinuousLearningError("records 必须是非空数组")
        reject_forbidden_keys(records)
        if any(not isinstance(record, dict) for record in records):
            raise ContinuousLearningError("records 项必须是 JSON object")
        record_ids = [record.get("record_id") for record in records]
        if any(not isinstance(record_id, str) or not record_id.strip() for record_id in record_ids):
            raise ContinuousLearningError("records 项必须包含非空 record_id")
        if len(record_ids) != len(set(record_ids)):
            raise ContinuousLearningError("record_id 不可重复")
        parent_dataset_id = parent_dataset_id.strip() if isinstance(parent_dataset_id, str) and parent_dataset_id.strip() else None
        now = self.clock()
        digest = content_sha256(records)
        dataset_id = f"dataset-{uuid.uuid4().hex}"
        with self.database.transaction() as connection:
            self._ensure_legacy_metadata(connection)
            current = connection.execute(
                "SELECT COALESCE(MAX(revision_number), 0) AS revision_number FROM drain_datasets WHERE dataset_family_id=?",
                (family_id,),
            ).fetchone()
            revision_number = int(current["revision_number"] or 0) + 1
            if revision_number > 1 and parent_dataset_id is None:
                raise ContinuousLearningError("已有 Dataset family 必须提供 parent_dataset_id", code="dataset_parent_required", status_code=409)
            if parent_dataset_id is not None:
                parent = connection.execute(
                    "SELECT dataset_id, dataset_family_id, revision_number FROM drain_datasets WHERE dataset_id=?",
                    (parent_dataset_id,),
                ).fetchone()
                if parent is None:
                    raise ContinuousLearningError("父 Dataset 不存在", code="dataset_parent_not_found", status_code=404)
                if str(parent["dataset_family_id"]) != family_id:
                    raise ContinuousLearningError("父 Dataset family 不匹配", code="dataset_parent_mismatch", status_code=409)
                if int(parent["revision_number"] or 0) >= revision_number:
                    raise ContinuousLearningError("父 Dataset revision 无效", code="dataset_parent_mismatch", status_code=409)
            payload = {
                "schema_version": "drain_dataset_v1",
                "dataset_id": dataset_id,
                "dataset_family_id": family_id,
                "revision_number": revision_number,
                "name": name,
                "description": description,
                "version": str(revision_number),
                "split": split,
                "record_count": len(records),
                "records": records,
                "created_at": now,
                "updated_at": now,
            }
            connection.execute(
                "INSERT INTO drain_datasets(dataset_id, name, version, dataset_json, created_at, updated_at, "
                "dataset_family_id, revision_number, content_sha256, parent_dataset_id, lifecycle_status, "
                "source_type, source_id, source_version, description, split, record_count, actor, request_id, schema_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'candidate', 'continuous_learning', ?, '1', ?, ?, ?, ?, ?, ?)",
                (
                    dataset_id,
                    name,
                    str(revision_number),
                    _db_json(self.database, payload),
                    now,
                    now,
                    family_id,
                    revision_number,
                    digest,
                    parent_dataset_id,
                    dataset_id,
                    description,
                    split,
                    len(records),
                    actor,
                    request_id,
                    DATASET_SCHEMA_VERSION,
                ),
            )
            row = connection.execute("SELECT * FROM drain_datasets WHERE dataset_id=?", (dataset_id,)).fetchone()
        return self._dataset(row)

    def get_dataset_revision(self, dataset_id: str) -> dict[str, Any]:
        dataset_id = _required(dataset_id, "dataset_id")
        with self.database.transaction() as connection:
            self._ensure_legacy_metadata(connection)
            row = connection.execute("SELECT * FROM drain_datasets WHERE dataset_id=?", (dataset_id,)).fetchone()
        if row is None:
            raise ContinuousLearningError("Dataset 不存在", code="dataset_not_found", status_code=404)
        return self._dataset(row)

    def list_dataset_revisions(self, family_id: str) -> list[dict[str, Any]]:
        family_id = _required(family_id, "family_id")
        with self.database.transaction() as connection:
            self._ensure_legacy_metadata(connection)
            rows = connection.execute(
                "SELECT * FROM drain_datasets WHERE dataset_family_id=? ORDER BY revision_number DESC, dataset_id DESC",
                (family_id,),
            ).fetchall()
        return [self._dataset(row) for row in rows]

    def transition_dataset_revision(
        self,
        dataset_id: str,
        lifecycle_status: str,
        *,
        actor: str,
        request_id: str,
    ) -> dict[str, Any]:
        dataset_id = _required(dataset_id, "dataset_id")
        actor = _required(actor, "actor")
        request_id = _required(request_id, "request_id")
        if lifecycle_status not in _DATASET_STATUSES:
            raise ContinuousLearningError("lifecycle_status 无效")
        now = self.clock()
        with self.database.transaction() as connection:
            self._ensure_legacy_metadata(connection)
            row = connection.execute("SELECT * FROM drain_datasets WHERE dataset_id=?", (dataset_id,)).fetchone()
            if row is None:
                raise ContinuousLearningError("Dataset 不存在", code="dataset_not_found", status_code=404)
            current_status = str(row["lifecycle_status"])
            if lifecycle_status == current_status:
                return self._dataset(row)
            if lifecycle_status not in _DATASET_TRANSITIONS.get(current_status, frozenset()):
                raise ContinuousLearningError("Dataset 状态不允许此转换", code="dataset_state_conflict", status_code=409)
            approved_at = now if lifecycle_status == "approved" else row["approved_at"]
            approved_by = actor if lifecycle_status == "approved" else row["approved_by"]
            changed = connection.execute(
                "UPDATE drain_datasets SET lifecycle_status=?, approved_by=?, approved_at=?, actor=?, request_id=?, updated_at=? "
                "WHERE dataset_id=? AND lifecycle_status=?",
                (lifecycle_status, approved_by, approved_at, actor, request_id, now, dataset_id, current_status),
            )
            if changed.rowcount != 1:
                raise ContinuousLearningError("Dataset 状态已变化", code="dataset_state_conflict", status_code=409)
            row = connection.execute("SELECT * FROM drain_datasets WHERE dataset_id=?", (dataset_id,)).fetchone()
        return self._dataset(row)

    def _ensure_legacy_metadata(self, connection: Any) -> None:
        rows = connection.execute("SELECT * FROM drain_datasets").fetchall()
        for row in rows:
            payload = _decode(row["dataset_json"], {})
            records = payload.get("records") if isinstance(payload, dict) else []
            if not isinstance(records, list):
                records = []
            family_id = row["dataset_family_id"] or row["dataset_id"]
            revision_number = int(row["revision_number"] or 1)
            digest = content_sha256(records)
            if row["content_sha256"] and str(row["content_sha256"]) != digest:
                raise ContinuousLearningError(
                    "Dataset 内容摘要不匹配",
                    code="dataset_hash_mismatch",
                    status_code=409,
                )
            name = row["name"] or (payload.get("name") if isinstance(payload, dict) else "") or str(row["dataset_id"])
            description = row["description"] if "description" in row.keys() and row["description"] is not None else (payload.get("description", "") if isinstance(payload, dict) else "")
            split = row["split"] if "split" in row.keys() and row["split"] is not None else (payload.get("split", "validation") if isinstance(payload, dict) else "validation")
            source_type = row["source_type"] or "legacy"
            source_id = row["source_id"] or row["dataset_id"]
            source_version = row["source_version"] or row["version"] or "1"
            schema_version = row["schema_version"] or DATASET_SCHEMA_VERSION
            record_count = int(row["record_count"] if "record_count" in row.keys() and row["record_count"] is not None else len(records))
            connection.execute(
                "UPDATE drain_datasets SET dataset_family_id=?, revision_number=?, content_sha256=?, lifecycle_status=COALESCE(lifecycle_status, 'approved'), "
                "source_type=?, source_id=?, source_version=?, description=?, split=?, record_count=?, schema_version=? WHERE dataset_id=?",
                (family_id, revision_number, digest, source_type, source_id, source_version, description, split, record_count, schema_version, row["dataset_id"]),
            )

    @staticmethod
    def _feedback(row: Any) -> dict[str, Any]:
        return {key: row[key] for key in row.keys()}

    @staticmethod
    def _dataset(row: Any) -> dict[str, Any]:
        payload = _decode(row["dataset_json"], {})
        records = payload.get("records", []) if isinstance(payload, dict) else []
        if not isinstance(records, list):
            records = []
        family_id = row["dataset_family_id"]
        return {
            "dataset_id": row["dataset_id"],
            "family_id": family_id,
            "dataset_family_id": family_id,
            "revision_number": int(row["revision_number"] or 1),
            "name": row["name"],
            "description": row["description"] if "description" in row.keys() else payload.get("description", ""),
            "version": row["version"],
            "split": row["split"] if "split" in row.keys() else payload.get("split", "validation"),
            "record_count": int(row["record_count"] if "record_count" in row.keys() and row["record_count"] is not None else len(records)),
            "records": records,
            "content_sha256": row["content_sha256"],
            "dataset_sha256": row["content_sha256"],
            "parent_dataset_id": row["parent_dataset_id"],
            "lifecycle_status": row["lifecycle_status"],
            "source_type": row["source_type"],
            "source_id": row["source_id"],
            "source_version": row["source_version"],
            "actor": row["actor"] if "actor" in row.keys() else None,
            "request_id": row["request_id"] if "request_id" in row.keys() else None,
            "approved_by": row["approved_by"] if "approved_by" in row.keys() else None,
            "approved_at": row["approved_at"] if "approved_at" in row.keys() else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "schema_version": row["schema_version"],
        }
