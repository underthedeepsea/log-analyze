from __future__ import annotations

import json
import hashlib
import os
import re
import threading
import time
from collections.abc import Iterator, Mapping
from importlib.util import find_spec
from types import MappingProxyType
from typing import Any, Callable

from logrisk.incremental_sources import IncrementalSourceError, SourceCursor, SourceRecord
from logrisk.kafka_records import KafkaCursorBounds, KafkaCursorDelta
from logrisk.normalizer import _identity_text
from logrisk.streaming_observability.contracts import offset_vector
from logrisk.streaming_observability.kafka_offsets import (
    KafkaBoundaryError, KafkaOffsetReader, REQUEST_TIMEOUT_MS, classify,
)


KAFKA_PYTHON_ADAPTER_ID = "kafka-python"
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_POLL_RECORDS = 1000


def decode_kafka_value(value: bytes | bytearray | str | None) -> dict[str, Any]:
    if isinstance(value, (bytes, bytearray)):
        text = bytes(value).decode("utf-8", errors="replace")
    else:
        text = str(value or "")
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return {"message": text}
    return dict(decoded) if isinstance(decoded, dict) else {"message": text}


def _source_record(
    value: bytes | bytearray | str | None,
    cursor: SourceCursor | KafkaCursorDelta,
    metadata: dict[str, int],
    configuration: Mapping[str, str],
) -> SourceRecord:
    try:
        record = decode_kafka_value(value)
        # Escaped surrogate code points can decode into a Python string that
        # cannot be represented as UTF-8 on the normalization/spooling path.
        if isinstance(value, (bytes, bytearray, str)) and (
            ("\\" in value if isinstance(value, str) else b"\\" in value) or isinstance(value, str)
        ):
            json.dumps(record, ensure_ascii=False).encode("utf-8")
    except (RecursionError, ValueError):
        return SourceRecord({}, cursor, metadata, rejection_code="record_decode_failed")
    # The default profile passes the first supplied host identity to the
    # window grouping key. The configured cloud profile resolves host hints
    # itself and can still use a later valid alias or source metadata.
    host = None if configuration.get("source_profile") else (
        record.get("node") or record.get("host") or record.get("hostname")
    )
    identity = host or record.get("pod")
    if (
        (identity and not isinstance(identity, (str, int, float, bool)))
        or (record.get("labels") and not isinstance(record["labels"], Mapping))
        or (
            (configuration.get("source_profile") or configuration.get("cluster_id"))
            and record.get("cluster") is not None and not _identity_text(record["cluster"])
        )
    ):
        return SourceRecord({}, cursor, metadata, rejection_code="record_shape_invalid")
    return SourceRecord(record, cursor, metadata)


class KafkaPythonConsumerAdapter:
    """Read a finite Kafka high-water mark with database-first checkpointing."""

    adapter_id = KAFKA_PYTHON_ADAPTER_ID

    def __init__(
        self, consumer_factory: Callable[..., Any] | None = None, *, offset_reader: KafkaOffsetReader | None = None,
    ) -> None:
        self._consumer_factory = consumer_factory
        self.offset_reader = offset_reader or KafkaOffsetReader()
        self._consumers: dict[int, Any] = {}
        self._lock = threading.Lock()

    @property
    def dependency_ready(self) -> bool:
        """Inspect the local dependency only; never create a Consumer or probe a Broker."""
        return self._consumer_factory is not None or find_spec("kafka") is not None

    def read(
        self,
        configuration: Mapping[str, str],
        cursor: SourceCursor,
    ) -> Iterator[SourceRecord]:
        return self.read_observed(configuration, cursor, None)

    def read_observed(
        self, configuration: Mapping[str, str], cursor: SourceCursor, observe: Callable[..., None] | None,
    ) -> Iterator[SourceRecord]:
        try:
            consumer = self._new_consumer(configuration)
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), "Kafka Consumer 初始化失败", stage="consume") from None
        thread_id = threading.get_ident()
        with self._lock:
            self._consumers[thread_id] = consumer
        try:
            topic = self._topic(configuration)
            partition_ids = consumer.partitions_for_topic(topic)
            if not partition_ids:
                raise IncrementalSourceError("Kafka Topic 不存在或没有可用分区")
            partitions = tuple(
                TopicPartition(topic, int(partition))
                for partition in sorted(partition_ids)
            )
            consumer.assign(partitions)
            offsets = _cursor_offsets(cursor)
            for partition in partitions:
                if str(partition.partition) in offsets:
                    consumer.seek(partition, offsets[str(partition.partition)])
                else:
                    consumer.seek_to_beginning(partition)
            bootstrap_fingerprint = self._bootstrap_fingerprint(configuration)
            stored_fingerprint = str((cursor.value or {}).get("bootstrap_fingerprint") or "")
            if stored_fingerprint and stored_fingerprint != bootstrap_fingerprint:
                raise IncrementalSourceError("Kafka Bootstrap 来源已变化，不能继续恢复")
            stored_high_water = _offset_map((cursor.value or {}).get("high_water"), "Kafka 高水位 Checkpoint 无效")
            if stored_high_water:
                if set(stored_high_water) != {str(partition.partition) for partition in partitions}:
                    raise IncrementalSourceError("Kafka Topic 分区已变化，不能继续恢复")
                end_offsets = {
                    partition: stored_high_water[str(partition.partition)]
                    for partition in partitions
                }
            else:
                end_offsets = consumer.end_offsets(partitions)
                end_offsets = {
                    partition: int(offset)
                    for partition, offset in end_offsets.items()
                }
            current = dict(offsets)
            while True:
                records = consumer.poll(timeout_ms=500, max_records=_MAX_POLL_RECORDS)
                if observe is not None:
                    observe("poll")
                for partition in sorted(records, key=lambda item: (item.topic, item.partition)):
                    for message in records[partition]:
                        if message.offset >= end_offsets[partition]:
                            continue
                        current[str(partition.partition)] = int(message.offset) + 1
                        yield _source_record(
                            message.value,
                            SourceCursor("kafka", {
                                "partitions": dict(current),
                                "high_water": {
                                    str(item.partition): int(end_offsets[item])
                                    for item in partitions
                                },
                                "bootstrap_fingerprint": bootstrap_fingerprint,
                            }),
                            metadata={
                                "partition": int(getattr(message, "partition", partition.partition)),
                                "offset": int(message.offset),
                            },
                            configuration=configuration,
                        )
                if all(consumer.position(partition) >= end_offsets[partition] for partition in partitions):
                    break
        except IncrementalSourceError:
            raise
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), "Kafka 消费失败，请检查 Broker、Topic 和消费权限", stage="consume") from None
        finally:
            with self._lock:
                self._consumers.pop(thread_id, None)
            try:
                consumer.close(autocommit=False)
            except Exception:
                pass

    def commit(
        self,
        configuration: Mapping[str, str],
        cursor: SourceCursor,
    ) -> None:
        offsets = _cursor_offsets(cursor)
        if not offsets:
            return
        consumer = self._active_consumer()
        temporary = consumer is None
        try:
            if temporary:
                consumer = self._new_consumer(configuration)
            topic = self._topic(configuration)
            partitions = {
                TopicPartition(topic, int(partition)): _offset_metadata(int(offset))
                for partition, offset in offsets.items()
            }
            if temporary:
                consumer.assign(tuple(partitions))
            consumer.commit(offsets=partitions)
        except IncrementalSourceError:
            raise
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), "Kafka Checkpoint 提交失败，请检查 Consumer Group 权限", stage="group_commit") from None
        finally:
            if temporary and consumer is not None:
                try:
                    consumer.close(autocommit=False)
                except Exception:
                    pass

    def _new_consumer(self, configuration: Mapping[str, str], *, managed: bool = False) -> Any:
        bootstrap_servers, _ = self._bootstrap_servers(configuration)
        group = str(configuration.get("consumer_group") or "").strip()
        if not group:
            raise IncrementalSourceError("Kafka Consumer Group 不能为空")
        factory = self._consumer_factory
        if factory is None:
            try:
                from kafka import KafkaConsumer
            except ImportError as exc:
                raise IncrementalSourceError("未安装 kafka-python，请先安装项目依赖") from exc
            factory = KafkaConsumer
        try:
            options = {
                "auto_offset_reset": "none", "allow_auto_create_topics": False,
                "isolation_level": "read_uncommitted", "session_timeout_ms": 6000,
                "request_timeout_ms": REQUEST_TIMEOUT_MS, "api_version_auto_timeout_ms": REQUEST_TIMEOUT_MS,
            } if managed else {
                "auto_offset_reset": "earliest", "session_timeout_ms": 10000, "request_timeout_ms": 30000,
            }
            return factory(
                bootstrap_servers=bootstrap_servers,
                group_id=group,
                enable_auto_commit=False,
                max_poll_records=_MAX_POLL_RECORDS,
                **options,
            )
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), "Kafka Consumer 初始化失败，请检查 Broker 和 Consumer Group") from None

    def _managed_offsets(self, source: Any, *, stage: str = "source_check") -> Any:
        try:
            snapshot = self.offset_reader.read(source.configuration, require_beginning=True)
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage=stage) from None
        if snapshot.identity != source.batch["identity"]:
            raise KafkaBoundaryError("topic_identity_changed")
        if set(snapshot.partitions) != set(offset_vector(source.batch["target"])):
            raise KafkaBoundaryError("partition_set_changed")
        return snapshot

    @staticmethod
    def _check_bounds(snapshot: Any, positions: Mapping[int, int], target: Mapping[int, int]) -> None:
        if set(positions) != set(target) or any(
            not snapshot.beginning[key] <= position <= target[key] <= snapshot.end[key]
            for key, position in positions.items()
        ):
            raise KafkaBoundaryError("offset_out_of_range")

    def read_bounded(self, source: Any, cursor: SourceCursor) -> Iterator[SourceRecord]:
        configuration = source.configuration
        target = offset_vector(source.batch["target"])
        positions = offset_vector(cursor.value.get("partitions")) or offset_vector(source.batch["start"])
        snapshot = self._managed_offsets(source, stage="consume")
        self._check_bounds(snapshot, positions, target)
        if dict(snapshot.group) != source.group_frontier:
            raise KafkaBoundaryError("group_frontier_changed")
        source.check_active()
        topic = configuration["topic"]

        def checkpoint() -> SourceCursor:
            return SourceCursor("kafka", {
                "partitions": {str(key): value for key, value in positions.items()},
                "high_water": {str(key): value for key, value in target.items()},
                "topic_identity": source.batch["identity"],
            })

        bounds = KafkaCursorBounds(MappingProxyType(dict(positions)), MappingProxyType(target), source.batch["identity"])
        if positions == target:
            source.final_cursor = checkpoint()
            return
        try:
            consumer = self._new_consumer(configuration, managed=True)
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage="consume") from None
        thread_id = threading.get_ident()
        with self._lock:
            self._consumers[thread_id] = consumer
        try:
            partitions = tuple(
                TopicPartition(topic, partition) for partition in sorted(target) if positions[partition] < target[partition]
            )
            consumer.assign(partitions)
            for partition in partitions:
                consumer.seek(partition, positions[partition.partition])
            stalled_at = time.monotonic()
            furthest = dict(positions)
            while True:
                source.check_active()
                records = consumer.poll(timeout_ms=500, max_records=_MAX_POLL_RECORDS)
                if source.observe is not None:
                    source.observe("poll")
                for partition, messages in records.items():
                    number = partition.partition
                    for message in messages:
                        if positions[number] <= message.offset < target[number]:
                            positions[number] = message.offset + 1
                            yield _source_record(
                                message.value, KafkaCursorDelta(bounds, number, message.offset + 1),
                                {"partition": number, "offset": message.offset}, configuration,
                            )
                scanned = {**positions, **{
                    partition.partition: min(consumer.position(partition), target[partition.partition])
                    for partition in partitions
                }}
                if scanned == target:
                    positions.update(scanned)
                    source.final_cursor = checkpoint()
                    return
                if any(scanned[key] > furthest[key] for key in scanned):
                    furthest = scanned
                    stalled_at = time.monotonic()
                elif time.monotonic() - stalled_at >= 30:
                    raise KafkaBoundaryError("timeout")
        except IncrementalSourceError:
            raise
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage="consume") from None
        finally:
            with self._lock:
                self._consumers.pop(thread_id, None)
            try:
                consumer.close(autocommit=False)
            except Exception:
                pass

    def commit_bounded(self, source: Any, cursor: SourceCursor) -> None:
        configuration = source.configuration
        durable = offset_vector(cursor.value.get("partitions"))
        snapshot = self._managed_offsets(source, stage="group_commit")
        self._check_bounds(snapshot, durable, offset_vector(source.batch["target"]))
        expected = source.group_frontier
        if any(snapshot.group[key] not in {expected.get(key), durable[key]} for key in durable):
            raise KafkaBoundaryError("group_frontier_changed")
        source.check_active()
        if dict(snapshot.group) != durable:
            consumer = self._active_consumer()
            temporary = consumer is None
            try:
                if temporary:
                    consumer = self._new_consumer(configuration, managed=True)
                topic = configuration["topic"]
                offsets = {TopicPartition(topic, key): _offset_metadata(value) for key, value in durable.items()}
                source.check_active()
                source.external_commit(lambda: consumer.commit(offsets=offsets, timeout_ms=REQUEST_TIMEOUT_MS))
            except KafkaBoundaryError as exc:
                exc.stage = exc.stage or "group_commit"
                raise
            except Exception as exc:
                raise KafkaBoundaryError(classify(exc), stage="group_commit") from None
            finally:
                if temporary and consumer is not None:
                    try:
                        consumer.close(autocommit=False)
                    except Exception:
                        pass
        source.group_frontier = durable

    def validate_bounded_checkpoint(self, source: Any, cursor: SourceCursor) -> None:
        snapshot = self._managed_offsets(source)
        self._check_bounds(snapshot, offset_vector(cursor.value.get("partitions")), offset_vector(source.batch["target"]))
        if dict(snapshot.group) != source.group_frontier:
            raise KafkaBoundaryError("group_frontier_changed")

    def _bootstrap_servers(self, configuration: Mapping[str, str]) -> tuple[list[str], str]:
        bootstrap_env = str(configuration.get("bootstrap_env") or "").strip()
        if not _ENV_NAME.fullmatch(bootstrap_env):
            raise IncrementalSourceError("Kafka Bootstrap 环境变量名无效")
        bootstrap = os.getenv(bootstrap_env, "").strip()
        servers = [item.strip() for item in bootstrap.split(",") if item.strip()]
        if not servers:
            raise IncrementalSourceError("Kafka Bootstrap 环境变量未设置")
        return servers, hashlib.sha256(bootstrap.encode("utf-8")).hexdigest()

    def _bootstrap_fingerprint(self, configuration: Mapping[str, str]) -> str:
        return self._bootstrap_servers(configuration)[1]

    def _active_consumer(self) -> Any | None:
        with self._lock:
            return self._consumers.get(threading.get_ident())

    def _topic(self, configuration: Mapping[str, str]) -> str:
        topic = str(configuration.get("topic") or "").strip()
        if not topic:
            raise IncrementalSourceError("Kafka Topic 不能为空")
        return topic


def _cursor_offsets(cursor: SourceCursor) -> dict[str, int]:
    values = cursor.value or {}
    raw_offsets = values.get("partitions")
    if isinstance(raw_offsets, Mapping):
        offsets = raw_offsets
    elif "partition" in values and "offset" in values:
        offsets = {str(values["partition"]): values["offset"]}
    else:
        return {}
    return _offset_map(offsets, "Kafka Checkpoint 无效")


def _offset_map(value: Any, error_message: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, int] = {}
    for partition, offset in value.items():
        try:
            partition_id = int(partition)
            next_offset = int(offset)
        except (TypeError, ValueError) as exc:
            raise IncrementalSourceError(error_message) from exc
        if partition_id < 0 or next_offset < 0:
            raise IncrementalSourceError(error_message)
        result[str(partition_id)] = next_offset
    return result


def _offset_metadata(offset: int) -> OffsetAndMetadata:
    try:
        return OffsetAndMetadata(offset, "", -1)
    except TypeError:
        return OffsetAndMetadata(offset, "")


try:
    from kafka import TopicPartition
    from kafka.structs import OffsetAndMetadata
except ImportError:
    class TopicPartition:
        def __init__(self, topic: str, partition: int) -> None:
            self.topic = topic
            self.partition = partition

        def __hash__(self) -> int:
            return hash((self.topic, self.partition))

        def __eq__(self, other: object) -> bool:
            return isinstance(other, TopicPartition) and (self.topic, self.partition) == (other.topic, other.partition)

    class OffsetAndMetadata:
        def __init__(self, offset: int, metadata: str) -> None:
            self.offset = offset
            self.metadata = metadata
