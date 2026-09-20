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
        # Serial registry writes bound cross-job snapshot reconciliation;
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
        self._job_snapshots: dict[str, dict[str, Any]] | None = None
        self._proxy_stores: dict[int, tuple[Any, Any]] = {}
        self._proxy_restore_store: Any = None

    def journal_job(self, job: dict[str, Any]) -> None:
        """Snapshot only caches this transaction actually mutates, including siblings."""
        if self._job_snapshots is None:
            return
        key = str(job["job_id"])
        if key not in self._job_snapshots:
            memo: dict[int, Any] = {}
            self._job_snapshots[key] = {
                field: copy.deepcopy(value, memo) if field != "condition" else value
                for field, value in job.items()
            }
        # Paged readers must observe their own transaction's candidates/events;
        # otherwise a second event can reuse a still-uncommitted sequence.
        connection = getattr(self.manager.persistence, "connection", None)
        if connection is not None:
            for field in ("entities", "features", "events"):
                collection = job.get(field)
                store = getattr(collection, "store", None)
                if callable(getattr(store, "bind", None)) and id(collection) not in self._proxy_stores:
                    # A cache miss inside this transaction hydrates paged
                    # collections from the temporarily bound manager store.
                    # Restore those proxies to the original unbound store;
                    # otherwise they retain the connection after commit closes
                    # it and every later detail/event/export read fails.
                    restore_store = self._proxy_restore_store or store
                    self._proxy_stores[id(collection)] = (collection, restore_store)
                    collection.store = restore_store.bind(connection)

    @staticmethod
    def _restore_job(job: dict[str, Any], saved: dict[str, Any]) -> None:
        entities = job.get("entities")
        if job.get("entities_paged"):
            saved_entities = saved["entities"]
            current_records = dict(entities._loaded)
            active = job.get("_active_record")
            if active is not None:
                current_records[str(active["entity_id"])] = active
            for key, record in list(saved_entities._loaded.items()):
                if key in current_records:
                    current_records[key].clear()
                    current_records[key].update(record)
                    saved_entities._loaded[key] = current_records[key]
            saved_active = saved.get("_active_record")
            if saved_active is not None and active is not None:
                active.clear()
                active.update(saved_active)
                saved["_active_record"] = active
            entities._loaded.clear()
            entities._loaded.update(saved_entities._loaded)
            saved["entities"] = entities
            for field, cache in (("features", "dirty"), ("events", "pending")):
                collection = job[field]
                current_cache = getattr(collection, cache)
                current_cache.clear()
                saved_cache = getattr(saved[field], cache)
                if isinstance(current_cache, dict):
                    current_cache.update(saved_cache)
                else:
                    current_cache.extend(saved_cache)
                saved[field] = collection
        else:
            records = {item.get("entity_id"): item for item in entities or []}
            restored = []
            for record in saved.get("entities", []):
                current_record = records.get(record.get("entity_id"))
                if current_record is not None:
                    current_record.clear()
                    current_record.update(record)
                    record = current_record
                restored.append(record)
            if isinstance(entities, list):
                entities[:] = restored
                saved["entities"] = entities
        job.clear()
        job.update(saved)

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
        self._job_snapshots = {}
        self._proxy_stores = {}
        self._proxy_restore_store = stores["persistence"]
        observations: list[tuple[dict[str, Any], str, dict[str, Any], int]] = []
        try:
            with approval_transaction(database, identities=identities) as connection:
                for name, store in stores.items():
                    if callable(getattr(store, "bind", None)):
                        setattr(manager, name, store.bind(connection))
                for key, job in manager._jobs.items():
                    if job_ids is None or key in job_ids:
                        self.journal_job(job)
                # Optional observations use their own transactions after commit;
                # the durable approval audit remains on this shared connection.
                self.pending_observations = observations
                yield connection
        except BaseException:
            for key in list(manager._jobs):
                if key not in existing_job_ids:
                    del manager._jobs[key]
                    continue
                if key not in self._job_snapshots:
                    continue
                job = manager._jobs[key]
                self._restore_job(job, self._job_snapshots[key])
            raise
        finally:
            for collection, store in self._proxy_stores.values():
                collection.store = store
            for name, store in stores.items():
                setattr(manager, name, store)
            self.pending_observations = None
            self._job_snapshots = None
            self._proxy_stores = {}
            self._proxy_restore_store = None
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
        database = getattr(manager.persistence, "database", None)
        if request_key and database is not None:
            with database.connect() as lookup:
                previous = lookup.execute(
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
        with manager._lock, self._unit_of_work(job_ids={str(job_id)}) as connection:
            if request_key and connection is not None:
                # Close the race between the lightweight lookup and this write.
                previous = connection.execute(
                    "SELECT payload_hash, result_json FROM approval_decisions WHERE request_key=? AND actor_scope=?",
                    (request_key, actor_scope),
                ).fetchone()
                if previous is not None:
                    if previous["payload_hash"] != digest:
                        raise FeatureJobError("审批请求标识已用于不同内容", code="approval_request_conflict", status_code=409)
                    return {**json.loads(previous["result_json"]), "idempotent_replay": True}
            job = manager._job(job_id)
            self.journal_job(job)
            candidate = manager._load_review_candidate_locked(job, str(candidate_id))
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
