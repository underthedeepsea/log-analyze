from __future__ import annotations

import hashlib
import logging
import threading
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Callable, Mapping

from logrisk.incremental_sources import KafkaIncrementalSource
from logrisk.streaming_observability.contracts import offset_vector
from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError, classify
from logrisk.streaming_observability.ownership import assignment_lock_key
from logrisk.streaming_observability.repository import ObservabilityError, ObservabilityRepository, digest, json_dict
from logrisk.streaming_observability.sampler import task_cursor
from logrisk.streaming_state import StreamingStateRepository


LOG = logging.getLogger(__name__)
MAX_BATCH_OFFSETS = 10_000
BATCH_ATTEMPT_SECONDS = 90
NORMAL_WAIT_SECONDS = 30
INITIAL_RETRY_SECONDS = 5
MAX_RETRY_SECONDS = 60
RETRYABLE_BROKER_ERRORS = frozenset({"timeout", "broker_unavailable"})


def bounded_target(
    start: Mapping[int, int], end: Mapping[int, int], budget: int, *, rotate_after: int = -1,
) -> tuple[dict[int, int], int]:
    target = dict(start)
    active = [partition for partition in sorted(start) if end[partition] > start[partition]]
    active.sort(key=lambda partition: partition <= rotate_after)
    last = rotate_after
    for index, partition in enumerate(active):
        if budget == 0:
            break
        width = min(end[partition] - start[partition], max(1, budget // (len(active) - index)))
        target[partition] += width
        budget -= width
        last = partition
    return target, last


class ContinuousKafka:
    """Schedule at most one fixed-target batch per explicitly enabled source."""

    def __init__(
        self, repository: ObservabilityRepository, streaming: StreamingStateRepository,
        *, source_factory: Callable[..., KafkaIncrementalSource], run_task: Callable[..., Any],
        config_snapshot: Callable[[], Mapping[str, Any]],
        max_batch_offsets: int = MAX_BATCH_OFFSETS, monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.repository = repository
        self.streaming = streaming
        self.source_factory = source_factory
        self.run_task = run_task
        self.config_snapshot = config_snapshot
        if max_batch_offsets < 1:
            raise ValueError("Kafka 批次位点预算必须为正数")
        self.max_batch_offsets = max_batch_offsets
        self.monotonic = monotonic

    def run(
        self, source_id: str, *, once: bool = False, report: Callable[[str | None], None] | None = None,
        wait: Callable[[float], None] = time.sleep, heartbeat_interval: float = 15,
    ) -> None:
        row = self.repository.get(source_id)
        if row["mode"] != "continuous_bounded":
            raise ObservabilityError("source_not_continuous", "该来源仅支持原固定高水位任务", 409)
        with self.repository.control_lock("scheduler:" + source_id) as check_owner:
            epoch = self.repository.get(source_id)["control_epoch"]
            token = self.repository.start_scheduler(source_id, epoch)
            if token is None:
                return
            closing = threading.Event()
            phase = "checking"
            pulse_lock = threading.Lock()

            def pulse(next_phase: str | None = None) -> bool:
                nonlocal phase
                with pulse_lock:
                    if next_phase:
                        phase = next_phase
                    try:
                        return self.repository.scheduler_heartbeat(source_id, token, phase)
                    except Exception:
                        LOG.warning("Kafka source scheduler heartbeat failed, source=%s", source_id)
                        return True

            def heartbeat() -> None:
                while not closing.wait(heartbeat_interval):
                    if not pulse():
                        return

            def check_scheduler() -> None:
                check_owner()
                current = self.repository.get(source_id)
                if not current["enabled"] or current["control_epoch"] != epoch or current["scheduler_token"] != token:
                    raise KafkaBoundaryError("batch_stopped", "Kafka 连续来源已停止，保留当前批次")

            beacon = threading.Thread(target=heartbeat, daemon=True, name="logrisk-kafka-source-heartbeat")
            beacon.start()
            try:
                retry_delay = INITIAL_RETRY_SECONDS
                while True:
                    check_scheduler()
                    if not pulse("checking"):
                        return
                    pause = NORMAL_WAIT_SECONDS
                    waiting_phase = "awaiting_batch"
                    try:
                        task_id = self.step(source_id, check_scheduler=check_scheduler)
                    except KafkaBoundaryError as exc:
                        if once or exc.code not in RETRYABLE_BROKER_ERRORS | {"batch_time_budget_exceeded"}:
                            raise
                        task_id = None
                        if exc.code in RETRYABLE_BROKER_ERRORS:
                            pause = retry_delay
                            retry_delay = min(retry_delay * 2, MAX_RETRY_SECONDS)
                            waiting_phase = "retry_backoff"
                    else:
                        retry_delay = INITIAL_RETRY_SECONDS
                        if task_id and self._filled_batch(task_id):
                            pause = 0
                    if report and (task_id or once):
                        report(task_id)
                    if once or not pulse(waiting_phase):
                        return
                    for _ in range(pause):
                        wait(1)
                        check_scheduler()
            except KafkaBoundaryError as exc:
                if exc.code != "batch_stopped":
                    raise
            finally:
                closing.set()
                try:
                    self.repository.finish_scheduler(source_id, token)
                except Exception:
                    # An unclean exit leaves only an expiring heartbeat.
                    LOG.warning("Kafka source scheduler exit observation failed, source=%s", source_id)

    def _filled_batch(self, task_id: str) -> bool:
        task = self.streaming.get_task(task_id)
        batch = task.get("kafka_batch") or {}
        start = offset_vector(batch.get("start"))
        target = offset_vector(batch.get("target"))
        return task.get("status") == "completed" and sum(target[key] - start[key] for key in target) >= self.max_batch_offsets

    def step(self, source_id: str, *, check_scheduler: Callable[[], None] | None = None) -> str | None:
        row = self.repository.get(source_id)
        if row["mode"] != "continuous_bounded":
            raise ObservabilityError("source_not_continuous", "该来源仅支持原固定高水位任务", 409)
        with self.repository.control_lock(row["writer_key"]) as check_owner:
            row = self.repository.get(source_id)
            if not row["enabled"]:
                return None
            descriptor = json_dict(row["descriptor_json"])
            identity = descriptor.get("identity") or {}
            source = self.source_factory(
                descriptor["configuration"], environment=identity.get("environment", "production"),
                scope_key=identity.get("scope_key", "default"),
            )
            deadline = self.monotonic() + BATCH_ATTEMPT_SECONDS

            def check_active() -> None:
                check_owner()
                if check_scheduler is not None:
                    check_scheduler()
                current = self.repository.get(source_id)
                if not current["enabled"] or current["control_epoch"] != row["control_epoch"]:
                    raise KafkaBoundaryError("batch_stopped", "Kafka 连续来源已停止，保留当前批次")
                if source.batch and current["active_task_id"] != source.batch["task_id"]:
                    raise KafkaBoundaryError("checkpoint_conflict", "Kafka 连续来源批次归属已变化")
                if self.monotonic() >= deadline:
                    raise KafkaBoundaryError("batch_time_budget_exceeded", "Kafka 本轮处理预算已到，保留原批次待续")

            source.check_active = check_active
            source.control_epoch = row["control_epoch"]

            def external_commit(operation: Callable[[], None]) -> None:
                # The durable intent already exists. Keep only this source's
                # control gate occupied during the synchronous Broker operation.
                with self.repository.commit_control(source_id) as current:
                    check_owner()
                    if not current["enabled"] or current["control_epoch"] != row["control_epoch"]:
                        raise KafkaBoundaryError("batch_stopped", "Kafka 连续来源已停止，保留当前批次")
                    if current["active_task_id"] != source.batch["task_id"]:
                        raise KafkaBoundaryError("checkpoint_conflict")
                    operation()
                    check_owner()

            source.external_commit = external_commit
            adapter = source.adapters[source.configuration["adapter_id"]]
            try:
                if row["active_task_id"]:
                    task = self.streaming.get_task(row["active_task_id"])
                    source.batch = {**task["kafka_batch"], "task_id": task["task_id"]}
                    if task.get("status") == "running":
                        # The database-scoped writer lock proves that the
                        # previous owner is no longer executing this source.
                        self.streaming.finish_claim(
                            task["task_id"], task["lease_token"],
                            error="连续 Kafka Worker 中断，重新领取原批次", interrupted=True,
                        )
                else:
                    task = self._prepare(row, source, adapter)
                    if task is None:
                        return None
                    source.batch = {**task["kafka_batch"], "task_id": task["task_id"]}
                confirmed = ((task.get("last_external_commit_cursor") or {}).get("value") or {}).get("partitions")
                source.group_frontier = (offset_vector(confirmed) if confirmed else
                                         {int(key): value for key, value in source.batch["group_start"].items()})
                source.check_active()
                self.run_task(task["task_id"], source.configuration, source=source)
                return task["task_id"]
            except KafkaBoundaryError as exc:
                if exc.code == "batch_stopped":
                    return None
                self._block(source_id, exc.code, exc.stage or "source_check", row["control_epoch"])
                raise
            except Exception as exc:
                self._block(source_id, classify(exc), "source_check", row["control_epoch"])
                raise

    def _block(self, source_id: str, reason: str, stage: str, control_epoch: int) -> None:
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE kafka_stream_sources SET block_reason=?, block_stage=? WHERE source_id=? AND enabled=1 "
                "AND control_epoch=?", (reason, stage, source_id, control_epoch),
            )

    def _prepare(self, row: Mapping[str, Any], source: KafkaIncrementalSource, adapter: Any) -> dict[str, Any] | None:
        try:
            measured = adapter.offset_reader.read(source.configuration, require_beginning=True)
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage="source_check") from None
        if not measured.identity_verified:
            raise KafkaBoundaryError("topic_identity_unavailable")
        if row["broker_identity"] and row["broker_identity"] != measured.identity:
            raise KafkaBoundaryError("topic_identity_changed")
        completed = self.streaming.get_task(row["completed_task_id"]) if row["completed_task_id"] else {}
        partitions = set(measured.partitions)
        if completed:
            start = offset_vector(task_cursor(completed).get("partitions"))
            if set(start) != partitions:
                raise KafkaBoundaryError("partition_set_changed")
            if dict(measured.group) != start:
                raise KafkaBoundaryError("group_frontier_changed")
        elif row["initial_offset"] == "committed":
            if set(measured.group) != partitions or any(value is None for value in measured.group.values()):
                raise KafkaBoundaryError("group_frontier_changed", "首次 committed 起点需要每个分区的已提交位点")
            start = dict(measured.group)
        else:
            if any(value is not None for value in measured.group.values()):
                raise KafkaBoundaryError("group_frontier_changed", "Kafka 连续来源的首次消费组必须没有既有提交")
            start = dict(measured.beginning if row["initial_offset"] == "earliest" else measured.end)
        if not partitions or set(start) != partitions or any(
            not measured.beginning[key] <= start[key] <= measured.end[key] for key in partitions
        ):
            raise KafkaBoundaryError("offset_out_of_range")
        source.check_active()
        source_id = row["source_id"]
        if completed and start == measured.end:
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE kafka_stream_sources SET idle_at=?, block_reason=NULL, block_stage=NULL "
                    "WHERE source_id=? AND enabled=1 AND active_task_id IS NULL AND control_epoch=?",
                    (self.repository.clock(), source_id, source.control_epoch),
                )
            return None
        config = self.config_snapshot()
        config_hash = hashlib.sha256(Path(config["path"]).read_bytes()).hexdigest()
        target, rotate_after = bounded_target(
            start, measured.end, self.max_batch_offsets,
            rotate_after=(completed.get("kafka_batch") or {}).get("rotate_after", -1),
        )
        batch = {
            "source_id": source_id, "identity": measured.identity, "locked_at": measured.end_at,
            "start": {str(key): value for key, value in start.items()},
            "target": {str(key): value for key, value in target.items()},
            "group_start": {str(key): value for key, value in measured.group.items()},
            "config_id": config.get("config_id"), "config_version": config.get("version"),
            "broker_writer_key": digest([measured.identity, source.configuration["consumer_group"]]),
            "plan_control_epoch": source.control_epoch,
            "rotate_after": rotate_after,
        }
        if completed.get("config_hash") == config_hash and completed.get("miner_generation"):
            batch["miner_generation"] = completed["miner_generation"]
        # The first binding shares an arbitration lock with legacy readers of
        # this topic/group name. They may proceed only before a binding exists
        # or after determining that their physical destination differs.
        assignment = (nullcontext() if row["broker_writer_key"] else
                      self.repository.control_lock(assignment_lock_key(source.configuration)))
        with assignment:
            with self.repository.control_lock("physical:" + batch["broker_writer_key"]):
                return self.streaming.create_or_load(
                    descriptor=source.descriptor(), config_hash=config_hash, kafka_source_id=source_id, kafka_batch=batch,
                )
