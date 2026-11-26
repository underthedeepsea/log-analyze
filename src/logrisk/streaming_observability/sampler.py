from __future__ import annotations

import logging
import uuid
from typing import Any, Mapping

from logrisk.streaming_observability.contracts import ALIGN_SECONDS, FRESH_SECONDS, offset_vector, elapsed
from logrisk.streaming_observability.kafka_offsets import KafkaOffsetReader, classify
from logrisk.streaming_observability.repository import ObservabilityError, ObservabilityRepository, json_dict


LOG = logging.getLogger(__name__)
RATE_BASELINE_FIELDS = (
    "sampled_at", "durable_at", "records_processed", "worker_epoch", "epoch",
    "durable_epoch", "durable_task_id", "partitions",
)


def task_cursor(task: Mapping[str, Any]) -> dict[str, Any]:
    cursor = (task.get("cursor") or {}).get("value") or {}
    if "partitions" not in cursor and "partition" in cursor and "offset" in cursor:
        return {**cursor, "partitions": {str(cursor["partition"]): cursor["offset"]}}
    return cursor


def durable_vector(task: Mapping[str, Any], completed: Mapping[str, Any]) -> dict[int, int]:
    return offset_vector(task_cursor(task).get("partitions")) or offset_vector(task_cursor(completed).get("partitions"))


class KafkaSampler:
    def __init__(self, repository: ObservabilityRepository, reader: KafkaOffsetReader) -> None:
        self.repository = repository
        self.reader = reader

    def sample(self, source_id: str) -> bool:
        try:
            with self.repository.control_lock("observer:" + source_id):
                return self._sample_owned(source_id)
        except ObservabilityError as exc:
            if exc.code == "source_busy":
                return False
            raise

    def _sample_owned(self, source_id: str) -> bool:
        token = self.repository.observer_claim(source_id)
        if not token:
            return False
        source, previous, *_ = self.repository.facts(source_id)
        configuration = json_dict(source["descriptor_json"])["configuration"]
        try:
            measured = self.reader.read(configuration)
        except Exception as exc:
            reason = classify(exc)
            LOG.warning("Kafka offset observation failed (%s), source=%s", reason, source_id)
            failed = {
                "status": "forbidden" if reason == "authorization" else "error",
                "reason_code": reason, "sampled_at": self.repository.clock(), "epoch": previous.get("epoch"),
                "last_success": previous.get("last_success") or {},
                "partitions": previous.get("partitions") or (), "partition_count": previous.get("partition_count"),
                "sample_id": uuid.uuid4().hex,
            }
            return self.repository.observer_publish(source_id, token, failed)

        # Read actual DB and worker evidence after the Broker operations and
        # retain the separate observation times for the comparison budget.
        _, _, task, completed, worker, evidence = self.repository.facts(source_id)
        at = evidence["durable_observed_at"]
        identity = (task.get("kafka_batch") or completed.get("kafka_batch") or {}).get("identity")
        positions = durable_vector(task, completed)
        prior = previous.get("last_success") or {}
        same_identity = (measured.identity_verified and previous.get("epoch") == measured.identity and
                         previous.get("partition_count") == measured.partition_count and
                         previous.get("partitions") == list(measured.partitions))
        was_fresh = elapsed(at, previous.get("sampled_at"))
        same_series = same_identity and was_fresh is not None and 0 < was_fresh <= FRESH_SECONDS
        compact_durable = {str(key): value for key, value in positions.items() if key in measured.partitions}
        record_count = (task.get("records_processed") if identity == measured.identity else None)
        snapshot: dict[str, Any] = {
            "sample_id": uuid.uuid4().hex,
            "status": "known" if measured.end_error is None else
                ("forbidden" if measured.end_error == "authorization" else "error"),
            "reason_code": measured.end_error,
            "epoch": measured.identity, "identity_verified": measured.identity_verified,
            "partitions": list(measured.partitions), "partition_count": measured.partition_count,
            "sampled_at": measured.end_at or measured.group_at or at or self.repository.clock(),
            "end": measured.end, "end_at": measured.end_at, "end_error": measured.end_error,
            "group": measured.group, "group_at": measured.group_at, "group_error": measured.group_error,
            "durable": compact_durable, "durable_at": at, "durable_epoch": identity,
            "durable_task_id": task.get("task_id") or completed.get("task_id"),
            "records_processed": record_count,
            "worker_epoch": worker.get("worker_epoch"),
            "previous": {key: previous.get(key) for key in RATE_BASELINE_FIELDS} if same_series else {},
        }

        if same_identity:
            last_end = {**(prior.get("end") or {}), **{str(key): [value, measured.end_at] for key, value in measured.end.items()}}
            last_group = {**(prior.get("group") or {}), **{str(key): [value, measured.group_at] for key, value in measured.group.items() if value is not None}}
        else:
            last_end = {str(key): [value, measured.end_at] for key, value in measured.end.items()}
            last_group = {str(key): [value, measured.group_at] for key, value in measured.group.items() if value is not None}
        snapshot["last_success"] = {"end": last_end, "group": last_group}

        observed_skew = elapsed(at, measured.end_at)
        if (measured.identity_verified and measured.identity == identity and observed_skew is not None
                and 0 <= observed_skew <= ALIGN_SECONDS):
            stationary_since = {}
            stalled = []
            prior_positions = previous.get("durable") or {}
            previously_behind = offset_vector(previous.get("end"))
            for key in measured.partitions:
                name = str(key)
                end, durable = measured.end.get(key), positions.get(key)
                if end is None or durable is None or end <= durable:
                    continue
                since = at
                if (same_series and previous.get("durable_epoch") == identity
                        and prior_positions.get(name) == durable and previously_behind.get(key, 0) > durable):
                    since = (previous.get("stationary_since") or {}).get(name) or at
                stationary_since[name] = since
                stationary_for = elapsed(at, since)
                if stationary_for is not None and stationary_for >= 90:
                    stalled.append(key)
            snapshot.update(stalled_partitions=stalled, stationary_since=stationary_since)
        return self.repository.observer_publish(source_id, token, snapshot)
