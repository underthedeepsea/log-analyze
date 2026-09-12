from __future__ import annotations

import copy
import hashlib
import json
import uuid
from contextlib import contextmanager, nullcontext
from typing import Any, Iterator

from logrisk.approval_dedup import approval_identity
from logrisk.database import utc_now


@contextmanager
def approval_transaction(database: Any, connection: Any = None, identities: Any = ()) -> Iterator[Any]:
    """Every approval writer takes the registry lock before identity/row locks."""
    with (nullcontext(connection) if connection is not None else database.transaction()) as current:
        # ponytail: serial registry writes bound cross-job snapshot reconciliation;
        # remove this gate only after all writers use a stable per-identity row set.
        keys = ["!approval_registry", *sorted(set(str(key) for key in identities if key))]
        for key in keys:
            current.execute(
                "INSERT INTO approval_identity_locks(approval_key, created_at) VALUES (?, ?) "
                "ON CONFLICT(approval_key) DO NOTHING", (key, utc_now()),
            )
            suffix = " FOR UPDATE" if database.provider == "postgres" else ""
            current.execute("SELECT approval_key FROM approval_identity_locks WHERE approval_key=?" + suffix, (key,)).fetchone()
        yield current


class ApprovalService:
    def __init__(self, manager: Any) -> None:
        self.manager = manager
        self.pending_observations: list[tuple[dict[str, Any], str, dict[str, Any], int]] | None = None

    @contextmanager
    def _unit_of_work(self, identities: Any = (), *, job_ids: set[str] | None = None) -> Iterator[Any]:
        manager = self.manager
        database = getattr(manager.persistence, "database", None)
        if database is None:
            yield None
            return
        current = getattr(manager.persistence, "connection", None)
        if current is not None:
            yield current
            return
        stores = {name: getattr(manager, name) for name in ("persistence", "rule_store", "approval_group_store")}
        existing_job_ids = set(manager._jobs)
        # ponytail: review snapshots cached jobs for cross-job convergence;
        # use a mutation journal if the cached review backlog becomes large.
        jobs = {key: {field: copy.deepcopy(value) if field != "condition" else value for field, value in job.items()}
                for key, job in manager._jobs.items() if job_ids is None or key in job_ids}
        observations: list[tuple[dict[str, Any], str, dict[str, Any], int]] = []
        try:
            with approval_transaction(database, identities=identities) as connection:
                for name, store in stores.items():
                    if callable(getattr(store, "bind", None)):
                        setattr(manager, name, store.bind(connection))
                # Optional observations use their own transactions after commit;
                # the durable approval audit remains on this shared connection.
                self.pending_observations = observations
                yield connection
        except Exception:
            for key in list(manager._jobs):
                if key not in existing_job_ids:
                    del manager._jobs[key]
                    continue
                if key not in jobs:
                    continue
                job = manager._jobs[key]
                saved = jobs[key]
                # Workers retain job/entity references while extracting outside
                # the manager lock. Restore those objects rather than detach them.
                records = {item.get("entity_id"): item for item in job.get("entities", [])}
                for index, record in enumerate(saved.get("entities", [])):
                    current_record = records.get(record.get("entity_id"))
                    if current_record is not None:
                        current_record.clear()
                        current_record.update(record)
                        saved["entities"][index] = current_record
                job.clear()
                job.update(saved)
            raise
        finally:
            for name, store in stores.items():
                setattr(manager, name, store)
            self.pending_observations = None
        for job, event_type, payload, sequence in observations:
            manager._record_observability_event(job, event_type, payload, sequence=sequence)

    def register(self, candidate: dict[str, Any], source: dict[str, Any], *, job: dict[str, Any],
                 record: dict[str, Any], persist: bool = True, resolve_existing_rule: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
        manager = self.manager
        with manager._lock, self._unit_of_work([approval_identity(candidate, source)["approval_key"]], job_ids={str(job["job_id"])}):
            feature, group = manager._register_feature_group_in_transaction_locked(
                job, record, candidate, persist=persist, resolve_existing_rule=resolve_existing_rule,
            )
            if persist and callable(getattr(manager.persistence, "save_generated_candidate", None)):
                feature = manager._save_generated_candidate_locked(job, feature)
            return feature, group

    def review(self, candidate_id: str, changes: dict[str, Any], expected_updated_at: Any = None,
               request_key: str | None = None, actor_scope: str = "local-operator", *, job_id: str | None = None) -> dict[str, Any]:
        from logrisk.feature_jobs import FeatureJobError, _invalid_feature_update

        manager = self.manager
        if actor_scope is None:
            raise _invalid_feature_update("字段 actor_scope 无效")
        for name, value in (("request_key", request_key), ("actor_scope", actor_scope), ("expected_updated_at", expected_updated_at)):
            if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > 512):
                raise _invalid_feature_update(f"字段 {name} 无效")
        payload = {"candidate_id": str(candidate_id), "job_id": job_id, "changes": changes,
                   "expected_updated_at": expected_updated_at}
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with manager._lock, self._unit_of_work() as connection:
            if request_key and connection is not None:
                previous = connection.execute(
                    "SELECT payload_hash, result_json FROM approval_decisions WHERE request_key=? AND actor_scope=?",
                    (request_key, actor_scope),
                ).fetchone()
                if previous is not None:
                    if previous["payload_hash"] != digest:
                        raise FeatureJobError("审批请求标识已用于不同内容", code="approval_request_conflict", status_code=409)
                    result = json.loads(previous["result_json"])
                    return {**result, "idempotent_replay": True}
            if job_id is None:
                candidate = manager.persistence.load_candidate(str(candidate_id)) if manager.persistence else None
                if candidate is None:
                    raise FeatureJobError("候选特征不存在", code="candidate_not_found", status_code=404)
                job_id = str(candidate["job_id"])
            candidate = manager._load_review_candidate_locked(manager._job(job_id), str(candidate_id))
            if expected_updated_at is not None and candidate.get("updated_at") != expected_updated_at:
                raise FeatureJobError("候选特征状态已变化", code="candidate_state_conflict", status_code=409)
            if connection is not None:
                with approval_transaction(manager.persistence.database, connection, [approval_identity(candidate)["approval_key"]]):
                    result = manager._review_feature_in_transaction(job_id, candidate_id, changes)
            else:
                result = manager._review_feature_in_transaction(job_id, candidate_id, changes)
            result.update({"decision_id": "decision-" + uuid.uuid4().hex, "decision_version": result.get("updated_at"),
                           "idempotent_replay": False, "affected_candidate_count": 1 + int(result.get("auto_resolved_count") or 0)})
            if connection is not None:
                connection.execute(
                    "INSERT INTO approval_decisions(decision_id, request_key, actor_scope, payload_hash, candidate_id, "
                    "before_version, result_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (result["decision_id"], request_key or result["decision_id"], actor_scope, digest, str(candidate_id),
                     candidate.get("updated_at"), json.dumps(result, ensure_ascii=False, separators=(",", ":")), utc_now()),
                )
            return result
