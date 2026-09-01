from __future__ import annotations

import json
import hashlib
import os
import re
import threading
from collections.abc import Iterator, Mapping
from typing import Any, Callable

from logrisk.incremental_sources import IncrementalSourceError, SourceCursor, SourceRecord


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


class KafkaPythonConsumerAdapter:
    """Read a finite Kafka high-water mark with database-first checkpointing."""

    adapter_id = KAFKA_PYTHON_ADAPTER_ID

    def __init__(self, consumer_factory: Callable[..., Any] | None = None) -> None:
        self._consumer_factory = consumer_factory
        self._consumers: dict[int, Any] = {}
        self._lock = threading.Lock()

    def read(
        self,
        configuration: Mapping[str, str],
        cursor: SourceCursor,
    ) -> Iterator[SourceRecord]:
        consumer = self._new_consumer(configuration)
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
                for partition in sorted(records, key=lambda item: (item.topic, item.partition)):
                    for message in records[partition]:
                        if message.offset >= end_offsets[partition]:
                            continue
                        current[str(partition.partition)] = int(message.offset) + 1
                        yield SourceRecord(
                            decode_kafka_value(message.value),
                            SourceCursor("kafka", {
                                "partitions": dict(current),
                                "high_water": {
                                    str(item.partition): int(end_offsets[item])
                                    for item in partitions
                                },
                                "bootstrap_fingerprint": bootstrap_fingerprint,
                            }),
                        )
                if all(consumer.position(partition) >= end_offsets[partition] for partition in partitions):
                    break
        except IncrementalSourceError:
            raise
        except Exception as exc:
            raise IncrementalSourceError("Kafka 消费失败，请检查 Broker、Topic 和消费权限") from exc
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
        if temporary:
            consumer = self._new_consumer(configuration)
        try:
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
            raise IncrementalSourceError("Kafka Checkpoint 提交失败，请检查 Consumer Group 权限") from exc
        finally:
            if temporary:
                consumer.close(autocommit=False)

    def _new_consumer(self, configuration: Mapping[str, str]) -> Any:
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
            return factory(
                bootstrap_servers=bootstrap_servers,
                group_id=group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=_MAX_POLL_RECORDS,
                session_timeout_ms=10000,
                request_timeout_ms=30000,
            )
        except Exception as exc:
            raise IncrementalSourceError("Kafka Consumer 初始化失败，请检查 Broker 和 Consumer Group") from exc

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
