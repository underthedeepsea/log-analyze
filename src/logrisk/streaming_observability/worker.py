from __future__ import annotations

import logging
import threading
from typing import Any, Mapping

from logrisk.streaming_observability.contracts import MAX_PARTITIONS, offset
from logrisk.streaming_observability.repository import ObservabilityRepository


LOG = logging.getLogger(__name__)


class WorkerObservation:
    """Coalesce actual worker boundary events into independent heartbeats."""

    def __init__(
        self, repository: ObservabilityRepository, task_id: str, worker_epoch: str, interval: float = 15,
    ) -> None:
        self.repository = repository
        self.task_id = task_id
        self.worker_epoch = worker_epoch
        self.interval = interval
        self.closing = threading.Event()
        self.lock = threading.Lock()
        self.snapshot: dict[str, Any] = {
            "worker_epoch": worker_epoch, "phase": "READING", "delivered": {}, "delivered_at": {},
        }
        self.thread = threading.Thread(target=self._heartbeat, name="logrisk-kafka-heartbeat", daemon=True)

    def __enter__(self) -> WorkerObservation:
        self.thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.closing.set()

    def event(self, name: str, data: Mapping[str, Any] | None = None) -> None:
        with self.lock:
            if name == "phase":
                self.snapshot["phase"] = data["phase"]
                return
            now = self.repository.clock()
            if name == "yield":
                partition, position = offset(data.get("partition")), offset(data.get("next_offset"))
                if partition is not None and position is not None:
                    key = str(partition)
                    positions = self.snapshot["delivered"]
                    if key in positions or len(positions) < MAX_PARTITIONS:
                        positions[key] = max(positions.get(key, 0), position)
                        self.snapshot["delivered_at"][key] = now
                self.snapshot["last_record_received_at"] = now
            elif name == "poll":
                self.snapshot["last_poll_returned_at"] = now
                self.snapshot["phase"] = "READING"

    def _heartbeat(self) -> None:
        failures = 0
        while not self.closing.is_set():
            with self.lock:
                snapshot = {
                    **self.snapshot,
                    "delivered": dict(self.snapshot["delivered"]),
                    "delivered_at": dict(self.snapshot["delivered_at"]),
                    "last_heartbeat_at": self.repository.clock(),
                }
            try:
                if not self.repository.worker_publish(self.task_id, self.worker_epoch, snapshot):
                    return
            except Exception:
                failures += 1
                LOG.warning("Kafka worker observation write failed (%s), task=%s", failures, self.task_id)
            self.closing.wait(self.interval)
