from __future__ import annotations

import base64
import hashlib
import json
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Callable, Iterator, Mapping

from logrisk.kafka_records import REJECTION_CODES
from logrisk.database import Database, utc_now
from logrisk.incremental_sources import SourceDescriptor
from logrisk.streaming_observability.contracts import ALIGN_SECONDS, MAX_SNAPSHOT_BYTES, broker_sample_current, current, elapsed


SAFE_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,249}$")
SOURCE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
ERROR_CODES = frozenset({
    "timeout", "authentication", "authorization", "topic_unavailable", "metadata_unsupported",
    "topic_identity_unavailable", "topic_identity_changed", "partition_set_changed", "partition_limit",
    "offset_out_of_range", "group_frontier_changed", "checkpoint_conflict", "consumer_failed",
    "broker_unavailable", "config_unavailable", "sample_budget", "missing_partition",
    "batch_time_budget_exceeded",
})


class ObservabilityError(ValueError):
    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def json_dict(value: Any) -> dict[str, Any]:
    return json.loads(value) if isinstance(value, str) else dict(value or {})


def digest(value: Any) -> str:
    return hashlib.sha256(json_text(value).encode("utf-8")).hexdigest()


def writer_key(configuration: Mapping[str, Any]) -> str:
    return "|".join(str(configuration.get(field) or "") for field in ("bootstrap_env", "topic", "consumer_group"))


def assignment_key(configuration: Mapping[str, Any]) -> str:
    return "|".join(str(configuration.get(field) or "") for field in ("topic", "consumer_group"))


def _worker_source_current(task: Mapping[str, Any], source: Mapping[str, Any]) -> bool:
    batch = task.get("kafka_batch")
    return not batch or (
        source.get("mode") == "continuous_bounded"
        and source.get("source_id") == batch.get("source_id")
        and bool(source.get("enabled"))
        and source.get("active_task_id") == task.get("task_id")
        and source.get("control_epoch") == task.get("kafka_control_epoch")
    )


def source_id_for(descriptor: SourceDescriptor) -> str:
    return "kafka_" + digest(descriptor.to_dict())[:32]


def safe_label(value: Any) -> str | None:
    return value if isinstance(value, str) and SAFE_ID.fullmatch(value) else None


def public_source(row: Mapping[str, Any], *, kafka_enabled: bool) -> dict[str, Any]:
    descriptor = json_dict(row["descriptor_json"])
    config = descriptor.get("configuration") or {}
    identity = descriptor.get("identity") or {}
    mode = row["mode"]
    return {
        "source_id": row["source_id"], "source_epoch": row.get("latest_epoch") or row["broker_identity"] or row["source_id"],
        "environment": safe_label(identity.get("environment")),
        "cluster_id": safe_label(config.get("cluster_id")), "topic": safe_label(config.get("topic")),
        "consumer_group": safe_label(config.get("consumer_group")),
        "mode": mode, "enabled": bool(row["enabled"]) if mode == "continuous_bounded" else kafka_enabled,
        "block_reason": row["block_reason"], "block_stage": row["block_stage"],
    }


def encode_cursor(value: list[Any]) -> str:
    return base64.urlsafe_b64encode(json_text(value).encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None, size: int) -> list[Any] | None:
    if not cursor:
        return None
    try:
        if len(cursor) > 512:
            raise ValueError
        value = json.loads(base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True))
        if not isinstance(value, list) or len(value) != size:
            raise ValueError
        return value
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ObservabilityError("invalid_cursor", "无效的分页游标") from exc


class ObservabilityRepository:
    def __init__(self, database: Database, clock: Callable[[], str] = utc_now) -> None:
        self.database = database
        self.clock = clock

    @property
    def row_lock(self) -> str:
        return " FOR UPDATE" if self.database.provider == "postgres" else ""

    def register(
        self, descriptor: SourceDescriptor, *, initial_offset: str, exclusive_group: bool,
    ) -> str:
        source_id = source_id_for(descriptor)
        if initial_offset not in {"earliest", "latest", "committed"} or not exclusive_group:
            raise ObservabilityError("invalid_source_contract", "需要显式选择首次位点，并确认消费组独占")
        cfg = descriptor.configuration
        if (any(safe_label(cfg.get(key)) is None for key in ("topic", "consumer_group", "cluster_id"))
                or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", str(cfg.get("bootstrap_env") or ""))):
            raise ObservabilityError("invalid_source_contract", "Kafka 来源标识或环境变量名不符合配置要求")
        with self.control_lock(writer_key(cfg)):
            with self.database.transaction() as connection:
                row = connection.execute(
                    "SELECT source_id, initial_offset FROM kafka_stream_sources WHERE writer_key=? AND mode='continuous_bounded'",
                    (writer_key(cfg),),
                ).fetchone()
                if row is not None:
                    if row["source_id"] != source_id or row["initial_offset"] != initial_offset:
                        raise ObservabilityError("source_conflict", "已登记的消费来源合同不可覆盖", 409)
                    return source_id
                legacy = connection.execute(
                    "SELECT 1 FROM kafka_stream_sources WHERE writer_key=? AND mode='bounded_high_water' LIMIT 1",
                    (writer_key(cfg),),
                ).fetchone()
                if legacy is not None and initial_offset != "committed":
                    raise ObservabilityError("source_conflict", "已有旧批次使用该消费组；续接时须显式选择 committed", 409)
                active_legacy = connection.execute(
                    "SELECT 1 FROM kafka_stream_sources s JOIN streaming_tasks t ON s.active_task_id=t.task_id "
                    "WHERE s.writer_key=? AND s.mode='bounded_high_water' AND t.status IN ('queued','running') LIMIT 1",
                    (writer_key(cfg),),
                ).fetchone()
                if active_legacy is not None:
                    raise ObservabilityError("source_busy", "请等待旧批次结束后再登记为连续来源", 409)
                connection.execute(
                    "INSERT INTO kafka_stream_sources(source_id, descriptor_json, mode, writer_key, assignment_key, writer_scope, initial_offset) "
                    "VALUES (?, ?, 'continuous_bounded', ?, ?, 'exclusive_configured', ?)",
                    (source_id, json_text(descriptor.to_dict()), writer_key(cfg), assignment_key(cfg), initial_offset),
                )
        return source_id

    def get(self, source_id: str, connection: Any = None, *, lock: bool = False) -> Mapping[str, Any]:
        if connection is None:
            with self.database.connect() as conn:
                return self.get(source_id, conn)
        if SOURCE_ID.fullmatch(source_id) is None:
            raise ObservabilityError("source_not_found", "未找到 Kafka 来源", 404)
        row = connection.execute(
            "SELECT * FROM kafka_stream_sources WHERE source_id=?" + (self.row_lock if lock else ""), (source_id,),
        ).fetchone()
        if row is None:
            raise ObservabilityError("source_not_found", "未找到 Kafka 来源", 404)
        return dict(row)

    def list_sources(self, *, after: str, limit: int, kafka_enabled: bool) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT s.*, o.latest_epoch FROM kafka_stream_sources s "
                "LEFT JOIN kafka_observer_state o ON s.source_id=o.source_id "
                "WHERE s.source_id>? ORDER BY s.source_id LIMIT ?", (after, limit + 1),
            ).fetchall()
        return {
            "sources": [public_source(dict(row), kafka_enabled=kafka_enabled) for row in rows[:limit]],
            "next_cursor": rows[limit - 1]["source_id"] if len(rows) > limit else None,
        }

    def set_enabled(self, source_id: str, enabled: bool) -> None:
        if self.database.provider == "sqlite":
            self.get(source_id)
        with self._sqlite_commit_gate(source_id), self.database.transaction() as connection:
            row = self.get(source_id, connection, lock=True)
            if row["mode"] != "continuous_bounded":
                raise ObservabilityError("source_not_continuous", "该来源仅支持原固定高水位任务", 409)
            connection.execute(
                "UPDATE kafka_stream_sources SET enabled=?, control_epoch=control_epoch+1, scheduler_token=NULL, "
                "scheduler_phase=NULL, scheduler_control_epoch=NULL WHERE source_id=? AND enabled<>?",
                (int(enabled), source_id, int(enabled)),
            )

    @contextmanager
    def _sqlite_commit_gate(self, source_id: str) -> Iterator[None]:
        if self.database.provider == "sqlite":
            with self._sqlite_lock("commit:" + source_id, wait=True):
                yield
        else:
            # PostgreSQL serializes the control change using the source row.
            yield

    @contextmanager
    def commit_control(self, source_id: str) -> Iterator[Mapping[str, Any]]:
        """Serialize a Broker commit with the enabled/epoch transition."""
        if self.database.provider == "postgres":
            with self.database.transaction() as connection:
                yield self.get(source_id, connection, lock=True)
        else:
            with self._sqlite_commit_gate(source_id):
                yield self.get(source_id)

    def start_scheduler(self, source_id: str, control_epoch: int) -> str | None:
        token = uuid.uuid4().hex
        with self.database.transaction() as connection:
            result = connection.execute(
                "UPDATE kafka_stream_sources SET scheduler_token=?, scheduler_heartbeat_at=?, scheduler_phase='checking', "
                "scheduler_control_epoch=? WHERE source_id=? AND enabled=1 AND control_epoch=?",
                (token, self.clock(), control_epoch, source_id, control_epoch),
            )
        return token if result.rowcount == 1 else None

    def scheduler_heartbeat(self, source_id: str, token: str, phase: str) -> bool:
        with self.database.transaction() as connection:
            result = connection.execute(
                "UPDATE kafka_stream_sources SET scheduler_heartbeat_at=?, scheduler_phase=? "
                "WHERE source_id=? AND enabled=1 AND scheduler_token=? AND control_epoch=scheduler_control_epoch",
                (self.clock(), phase, source_id, token),
            )
        return result.rowcount == 1

    def finish_scheduler(self, source_id: str, token: str) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE kafka_stream_sources SET scheduler_token=NULL, scheduler_phase=NULL, scheduler_control_epoch=NULL "
                "WHERE source_id=? AND scheduler_token=?", (source_id, token),
            )

    def require_legacy_writer(self, configuration: Mapping[str, Any]) -> None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM kafka_stream_sources WHERE writer_key=? AND mode='continuous_bounded'",
                (writer_key(configuration),),
            ).fetchone()
        if row is not None:
            raise ObservabilityError("source_conflict", "该 Kafka 消费组已登记为连续来源", 409)

    def managed_assignment_exists(self, configuration: Mapping[str, Any], *, broker_key: str | None = None) -> bool:
        with self.database.connect() as connection:
            if broker_key is None:
                sentence = "assignment_key=? AND broker_writer_key IS NOT NULL"
                parameter = assignment_key(configuration)
            else:
                sentence = "broker_writer_key=?"
                parameter = broker_key
            return connection.execute(
                "SELECT 1 FROM kafka_stream_sources WHERE " + sentence + " LIMIT 1", (parameter,),
            ).fetchone() is not None

    @contextmanager
    def control_lock(self, key: str) -> Iterator[Callable[[], None]]:
        """Hold ownership on the database's lock namespace until work exits."""
        if self.database.provider == "postgres":
            key_hash = hashlib.sha256(("logrisk-kafka-writer:" + key).encode()).digest()
            lock_id = int.from_bytes(key_hash[:8], byteorder="big", signed=True)
            with self.database.connect() as connection:
                result = connection.execute("SELECT pg_try_advisory_lock(?)", (lock_id,)).fetchone()
                connection.commit()
                if not result or not result[0]:
                    raise ObservabilityError("source_busy", "Kafka 来源已有运行中的 Worker", 409)
                def check() -> None:
                    connection.execute("SELECT 1")
                    connection.commit()

                yield check
            return

        with self._sqlite_lock(key):
            yield lambda: None

    @contextmanager
    def _sqlite_lock(self, key: str, *, wait: bool = False) -> Iterator[None]:
        import fcntl

        key_hash = hashlib.sha256(("logrisk-kafka-writer:" + key).encode()).hexdigest()
        path = self.database.path.resolve()
        lock_path = path.with_name(f".{path.name}.kafka-{key_hash[:32]}.lock")
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ObservabilityError("source_busy", "Kafka 来源已有运行中的 Worker", 409) from exc
            yield

    def observer_claim(self, source_id: str, *, lease_seconds: int = 60) -> str | None:
        now = self.clock()
        expires = (datetime.fromisoformat(now) + timedelta(seconds=lease_seconds)).isoformat()
        token = uuid.uuid4().hex
        with self.database.transaction() as connection:
            self.get(source_id, connection, lock=True)
            connection.execute(
                "INSERT INTO kafka_observer_state(source_id) VALUES (?) ON CONFLICT(source_id) DO NOTHING", (source_id,),
            )
            result = connection.execute(
                "UPDATE kafka_observer_state SET owner_token=?, expires_at=? WHERE source_id=? "
                "AND (owner_token IS NULL OR expires_at<=?) AND (next_due_at IS NULL OR next_due_at<=?)",
                (token, expires, source_id, now, now),
            )
        return token if result.rowcount == 1 else None

    def observer_snapshot(self, connection: Any, source_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT snapshot_json FROM kafka_observer_state WHERE source_id=?", (source_id,)).fetchone()
        return json_dict(row[0]) if row else {}

    def observer_publish(self, source_id: str, token: str, snapshot: Mapping[str, Any]) -> bool:
        with self.database.transaction() as connection:
            now = self.clock()
            owned = connection.execute(
                "SELECT failed_samples, recovering, status, latest_epoch, "
                "CASE WHEN status='recovering' THEN snapshot_json END AS prior_snapshot_json FROM kafka_observer_state "
                "WHERE source_id=? AND owner_token=? AND expires_at>?"
                + self.row_lock, (source_id, token, now),
            ).fetchone()
            if owned is None:
                return False
            status = snapshot.get("status")
            failed = (status in {"error", "forbidden"} or
                      snapshot.get("end_error") is not None or snapshot.get("group_error") is not None)
            failures = min(int(owned["failed_samples"]) + 1, 3) if failed else 0
            sampled_at, epoch = snapshot.get("sampled_at"), snapshot.get("epoch")
            valid_success = not failed and status == "known" and bool(epoch) and broker_sample_current(now, snapshot)
            confirmed = False
            if valid_success and owned["status"] == "recovering" and epoch == owned["latest_epoch"]:
                previous = json_dict(owned["prior_snapshot_json"])
                confirmed = broker_sample_current(now, previous) and all(
                    (elapsed(snapshot[field], previous[field]) or 0) > 0
                    for field in ("sampled_at", "end_at", "group_at")
                )
            needs_baseline = bool(owned["recovering"]) or (status == "known" and not valid_success)
            recovery_pending = not failed and needs_baseline and not confirmed
            recovering = failed or recovery_pending
            if failed and status == "known":
                status = "error"
            elif status == "known" and not valid_success:
                status = "stale"
            elif valid_success and recovery_pending:
                status = "recovering"
            payload = json_text({**snapshot, "recovery_pending": recovery_pending})
            if len(payload.encode("utf-8")) > MAX_SNAPSHOT_BYTES:
                raise ObservabilityError("sample_budget", "Kafka 采样超出快照预算", 422)
            cadence = 15 * 2 ** max(0, failures - 1)
            next_due = (datetime.fromisoformat(now) + timedelta(seconds=cadence)).isoformat()
            result = connection.execute(
                "UPDATE kafka_observer_state SET snapshot_json=?, latest_epoch=?, sampled_at=?, status=?, "
                "owner_token=NULL, expires_at=NULL, next_due_at=?, failed_samples=?, recovering=? "
                "WHERE source_id=? AND owner_token=? AND expires_at>?",
                (payload, epoch, sampled_at, status,
                 next_due, failures, int(recovering), source_id, token, now),
            )
        return result.rowcount == 1

    def observer_schedule(self, source_id: str) -> tuple[str | None, float]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT status, next_due_at, expires_at FROM kafka_observer_state WHERE source_id=?", (source_id,),
            ).fetchone()
        if row is None:
            return None, 15
        now = self.clock()
        wait = max(elapsed(row[1], now) or 0, elapsed(row[2], now) or 0)
        return row[0], max(1, wait)

    def worker_publish(self, task_id: str, lease: str, sample: Mapping[str, Any]) -> bool:
        with self.database.transaction() as connection:
            task = connection.execute(
                "SELECT task_json FROM streaming_tasks WHERE task_id=?" + self.row_lock, (task_id,),
            ).fetchone()
            task_data = json_dict(task[0]) if task else {}
            if task_data.get("status") != "running" or task_data.get("lease_token") != lease:
                return False
            batch = task_data.get("kafka_batch")
            if batch:
                source = connection.execute(
                    "SELECT source_id, mode, enabled, active_task_id, control_epoch FROM kafka_stream_sources "
                    "WHERE source_id=?" + self.row_lock, (batch["source_id"],),
                ).fetchone()
                if source is None or not _worker_source_current(task_data, dict(source)):
                    return False
            connection.execute(
                "INSERT INTO kafka_worker_observations(task_id, worker_epoch, observed_at, snapshot_json) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(task_id) DO UPDATE SET worker_epoch=excluded.worker_epoch, observed_at=excluded.observed_at, "
                "snapshot_json=excluded.snapshot_json",
                (task_id, lease, sample["last_heartbeat_at"], json_text(sample)),
            )
        return True

    def facts(self, source_id: str) -> tuple[dict[str, Any], ...]:
        with self.database.connect() as connection:
            if self.database.provider == "postgres":
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else:
                connection.execute("BEGIN")
            # The first SELECT pins the snapshot. A slow round trip leaves its
            # exact timing uncertain even if the rest of the reads are quick.
            started_at = self.clock()
            source = self.get(source_id, connection)
            returned_at = self.clock()
            evidence = {"durable_observed_at": returned_at if current(returned_at, started_at, ALIGN_SECONDS) else None}
            ids = [value for value in (source["active_task_id"], source["completed_task_id"]) if value]
            if not ids:
                return source, self.observer_snapshot(connection, source_id), {}, {}, {}, evidence
            rows = connection.execute(
                "SELECT t.task_json, w.worker_epoch, w.snapshot_json FROM streaming_tasks t "
                "LEFT JOIN kafka_worker_observations w ON w.task_id=t.task_id "
                "WHERE t.task_id IN (?, ?)", (ids[0], ids[-1]),
            ).fetchall()
            tasks = {task["task_id"]: (task, row) for row in rows if (task := json_dict(row[0]))}
            active, worker_row = tasks.get(source["active_task_id"], ({}, None))
            completed, _ = tasks.get(source["completed_task_id"], ({}, None))
            task = active or completed
            worker = json_dict(worker_row["snapshot_json"]) if (
                worker_row is not None and task.get("status") == "running"
                and worker_row["worker_epoch"] == task.get("lease_token") and _worker_source_current(task, source)
            ) else {}
            result = self._result(connection, task) or self._result(connection, completed)
            commit_time = task.get("first_durable_commit_at")
            if not commit_time and task and int(task.get("windows_committed") or 0):
                commit_time = connection.execute(
                    "SELECT MIN(committed_at) FROM streaming_window_commits WHERE task_id=?", (task["task_id"],),
                ).fetchone()[0]
            evidence["first_durable_commit_at"] = commit_time
            if result.get("task_id") == task.get("task_id") and result.get("covered_commit_at"):
                evidence["first_uncovered_at"] = connection.execute(
                    "SELECT MIN(committed_at) FROM streaming_window_commits WHERE task_id=? AND committed_at>?",
                    (task["task_id"], result["covered_commit_at"]),
                ).fetchone()[0]
            return source, self.observer_snapshot(connection, source_id), task, completed, worker, {**result, **evidence}

    @staticmethod
    def _result(connection: Any, task: Mapping[str, Any]) -> dict[str, Any]:
        ref = task.get("result_reference") or (task.get("result") or {}).get("result_ref") or {}
        if not ref or ref.get("task_id") != task.get("task_id") or not ref.get("generation"):
            return {}
        row = connection.execute(
            "SELECT frontier_json FROM streaming_result_generations WHERE task_id=? AND generation=? AND status='ready'",
            (task["task_id"], ref["generation"]),
        ).fetchone()
        if row is None:
            return {}
        return {
            "task_id": task["task_id"], "generation": ref["generation"], "frontier": json_dict(row[0]),
            "published_at": task.get("result_published_at"),
            "covered_commit_at": task.get("result_durable_at"),
        }

    def events(self, source_id: str, *, cursor: str | None, limit: int) -> dict[str, Any]:
        page = decode_cursor(cursor, 4)
        values: list[Any] = [source_id]
        clause = ""
        if page:
            if (page[0] != source_id or elapsed(page[1], page[1]) != 0
                    or not isinstance(page[2], str) or not SOURCE_ID.fullmatch(page[2])
                    or type(page[3]) is not int or not 0 < page[3] < 2**63):
                raise ObservabilityError("invalid_cursor", "无效的分页游标")
            clause = " AND (e.created_at, e.task_id, e.sequence) < (?, ?, ?)"
            values.extend(page[1:])
        with self.database.connect() as connection:
            self.get(source_id, connection)
            rows = connection.execute(
                "SELECT e.task_id, e.sequence, e.event_type, e.created_at, e.event_json FROM streaming_task_events e "
                "WHERE e.kafka_source_id=?" + clause +
                " ORDER BY e.created_at DESC, e.task_id DESC, e.sequence DESC LIMIT ?", (*values, limit + 1),
            ).fetchall()
        events = []
        for row in rows[:limit]:
            detail = json_dict(row["event_json"])
            nested = detail.get("details")
            reason = nested.get("error_code") if isinstance(nested, Mapping) else None
            event = {
                "event_type": row["event_type"],
                "reason_code": reason if isinstance(reason, str) and reason in ERROR_CODES else None,
                "created_at": row["created_at"], "task_id": row["task_id"],
            }
            rejected = detail.get("rejected_records")
            sample = detail.get("rejected_positions")
            if type(rejected) is int and 0 < rejected <= 10_000 and isinstance(sample, list):
                event["rejected_records"] = rejected
                event["rejected_positions"] = [
                    {"partition": item["partition"], "offset": str(item["offset"]), "reason_code": item["reason_code"]}
                    for item in sample[:20] if isinstance(item, dict)
                    and type(item.get("partition")) is int and 0 <= item["partition"] < 2**31
                    and type(item.get("offset")) is int and 0 <= item["offset"] < 2**63
                    and isinstance(item.get("reason_code"), str) and item["reason_code"] in REJECTION_CODES
                ]
            events.append(event)
        last = rows[limit - 1] if len(rows) > limit else None
        return {"events": events, "next_cursor": encode_cursor(
            [source_id, last["created_at"], last["task_id"], last["sequence"]]
        ) if last is not None else None}
