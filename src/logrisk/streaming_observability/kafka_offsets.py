from __future__ import annotations

import os
import re
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from logrisk.database import utc_now
from logrisk.incremental_sources import IncrementalSourceError
from logrisk.streaming_observability.contracts import MAX_PARTITIONS, offset
from logrisk.streaming_observability.repository import digest


REQUEST_TIMEOUT_MS = 8000


class KafkaBoundaryError(IncrementalSourceError):
    def __init__(
        self, code: str, message: str = "Kafka 来源读数或检查点无法安全验证", *, stage: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.stage = stage


def classify(exc: Exception) -> str:
    if isinstance(exc, KafkaBoundaryError):
        return exc.code
    names = {cls.__name__.lower() for cls in type(exc).__mro__}
    for fragment, code in (
        ("authorization", "authorization"), ("authentication", "authentication"),
        ("timeout", "timeout"), ("offsetoutofrange", "offset_out_of_range"),
        ("unknowntopic", "topic_unavailable"), ("unsupportedversion", "metadata_unsupported"),
        ("nobrokersavailable", "broker_unavailable"), ("kafkaconnection", "broker_unavailable"),
    ):
        if any(fragment in name for name in names):
            return code
    return "consumer_failed"


def bootstrap_servers(configuration: Mapping[str, str]) -> list[str]:
    name = configuration.get("bootstrap_env", "")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name):
        raise KafkaBoundaryError("config_unavailable")
    servers = [value.strip() for value in os.getenv(name, "").split(",") if value.strip()]
    if not servers:
        raise KafkaBoundaryError("config_unavailable")
    return servers


def _metadata_request(topic: str) -> Any:
    # Kafka Metadata v10 adds the stable topic UUID, using flexible message headers.
    # Wire schema: Apache Kafka clients/src/main/resources/common/message/Metadata{Request,Response}.json.
    from kafka.protocol.api import Request, Response
    from kafka.protocol.types import AbstractType, Boolean, CompactArray, CompactString, Int16, Int32, Schema, TaggedFields

    class TopicUuid(AbstractType):
        @classmethod
        def encode(cls, value: bytes) -> bytes:
            return value

        @classmethod
        def decode(cls, stream: Any) -> bytes:
            return stream.read(16)

    class TopicMetadataResponse(Response):
        API_KEY = 3
        API_VERSION = 10
        FLEXIBLE_VERSION = True
        SCHEMA = Schema(
            ("throttle_ms", Int32),
            ("brokers", CompactArray(
                ("node_id", Int32), ("host", CompactString("utf-8")), ("port", Int32),
                ("rack", CompactString("utf-8")), ("tags", TaggedFields),
            )),
            ("cluster_id", CompactString("utf-8")),
            ("controller_id", Int32),
            ("topics", CompactArray(
                ("error", Int16), ("name", CompactString("utf-8")), ("topic_id", TopicUuid), ("internal", Boolean),
                ("partitions", CompactArray(
                    ("error", Int16), ("partition", Int32), ("leader", Int32), ("leader_epoch", Int32),
                    ("replicas", CompactArray(Int32)), ("isr", CompactArray(Int32)),
                    ("offline_replicas", CompactArray(Int32)), ("tags", TaggedFields),
                )),
                ("topic_operations", Int32), ("tags", TaggedFields),
            )),
            ("cluster_operations", Int32), ("tags", TaggedFields),
        )

    class TopicMetadataRequest(Request):
        API_KEY = 3
        API_VERSION = 10
        FLEXIBLE_VERSION = True
        RESPONSE_TYPE = TopicMetadataResponse
        SCHEMA = Schema(
            ("topics", CompactArray(("topic_id", TopicUuid), ("name", CompactString("utf-8")), ("tags", TaggedFields))),
            ("allow_auto_create", Boolean), ("cluster_operations", Boolean),
            ("topic_operations", Boolean), ("tags", TaggedFields),
        )

    return TopicMetadataRequest([(b"\x00" * 16, topic, {})], False, False, False, {})


def topic_metadata(admin: Any, topic: str) -> tuple[str, set[int]]:
    versions = admin._client.get_api_versions() or {}
    minimum, maximum = versions.get(3, (0, 0))
    if not minimum <= 10 <= maximum:
        if minimum <= 8 and maximum >= 4:
            rows = admin.describe_topics([topic])
            if len(rows) != 1 or rows[0]["topic"] != topic:
                raise KafkaBoundaryError("topic_unavailable")
            from kafka.errors import for_code

            if rows[0]["error_code"]:
                raise for_code(rows[0]["error_code"])()
            partitions = {row["partition"] for row in rows[0]["partitions"]}
            if not partitions:
                raise KafkaBoundaryError("topic_unavailable")
            return "", partitions
        raise KafkaBoundaryError("metadata_unsupported")
    response = admin.send_request(_metadata_request(topic)).to_object()
    if len(response["topics"]) != 1 or response["topics"][0]["name"] != topic:
        raise KafkaBoundaryError("topic_unavailable")
    item = response["topics"][0]
    if item["error"] != 0:
        from kafka.errors import for_code

        raise for_code(item["error"])()
    cluster_id, topic_id = response["cluster_id"], item["topic_id"]
    if not cluster_id or len(topic_id) != 16 or not any(topic_id):
        raise KafkaBoundaryError("topic_identity_unavailable")
    if not item["partitions"]:
        raise KafkaBoundaryError("topic_unavailable")
    partitions = set()
    for partition in item["partitions"]:
        if partition["error"] != 0:
            from kafka.errors import for_code

            raise for_code(partition["error"])()
        partitions.add(partition["partition"])
    return digest([cluster_id, topic_id.hex()]), partitions


@dataclass(frozen=True)
class KafkaOffsets:
    identity: str
    identity_verified: bool
    partitions: tuple[int, ...]
    partition_count: int
    beginning: Mapping[int, int]
    end: Mapping[int, int]
    group: Mapping[int, int | None]
    end_at: str | None
    group_at: str | None
    end_error: str | None = None
    group_error: str | None = None


class KafkaOffsetReader:
    """Read selected topic and group metadata without assignment or record fetching."""

    def __init__(
        self, *, consumer_factory: Callable[..., Any] | None = None,
        admin_factory: Callable[..., Any] | None = None,
        metadata_reader: Callable[..., tuple[str, set[int]]] = topic_metadata,
        clock: Callable[[], str] = utc_now,
    ) -> None:
        self.consumer_factory = consumer_factory
        self.admin_factory = admin_factory
        self.metadata_reader = metadata_reader
        self.clock = clock

    def _new_admin(self, servers: list[str]) -> Any:
        from kafka.admin import KafkaAdminClient

        class OffsetAdminClient(KafkaAdminClient):
            def _refresh_controller_id(self, *_: Any, **__: Any) -> None:
                # OffsetFetch and targeted Metadata only need the group's coordinator
                # and a broker, respectively. Skip the controller's discovery request.
                return None

        kafka_admin = self.admin_factory or OffsetAdminClient
        return kafka_admin(bootstrap_servers=servers, request_timeout_ms=REQUEST_TIMEOUT_MS,
                           api_version_auto_timeout_ms=REQUEST_TIMEOUT_MS)

    def read_identity(self, configuration: Mapping[str, str]) -> str:
        try:
            with ExitStack() as closing:
                admin = self._new_admin(bootstrap_servers(configuration))
                closing.callback(admin.close)
                identity, _ = self.metadata_reader(admin, configuration["topic"])
                if not identity:
                    raise KafkaBoundaryError("topic_identity_unavailable")
                return identity
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage="source_check") from None

    def read(self, configuration: Mapping[str, str], *, require_beginning: bool = False) -> KafkaOffsets:
        try:
            return self._read(configuration, require_beginning=require_beginning)
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage="source_check") from None

    def _read(self, configuration: Mapping[str, str], *, require_beginning: bool) -> KafkaOffsets:
        from kafka import KafkaConsumer, TopicPartition

        kafka_consumer = self.consumer_factory or KafkaConsumer
        servers = bootstrap_servers(configuration)
        topic = configuration["topic"]
        with ExitStack() as closing:
            admin = self._new_admin(servers)
            closing.callback(admin.close)
            identity, all_partitions = self.metadata_reader(admin, topic)
            if require_beginning and not identity:
                raise KafkaBoundaryError("topic_identity_unavailable")
            count = len(all_partitions)
            if require_beginning and count > MAX_PARTITIONS:
                raise KafkaBoundaryError("partition_limit")
            numbers = tuple(sorted(all_partitions)[:MAX_PARTITIONS])
            partitions = tuple(TopicPartition(topic, partition) for partition in numbers)
            end_at = group_at = end_error = group_error = None
            beginning: dict[int, int] = {}
            end: dict[int, int] = {}
            group: dict[int, int | None] = {}
            try:
                consumer = kafka_consumer(
                    bootstrap_servers=servers, group_id=None, enable_auto_commit=False,
                    allow_auto_create_topics=False, isolation_level="read_uncommitted",
                    request_timeout_ms=REQUEST_TIMEOUT_MS, api_version_auto_timeout_ms=REQUEST_TIMEOUT_MS,
                    session_timeout_ms=6000,
                )
                closing.callback(consumer.close, autocommit=False)
                if require_beginning:
                    beginning = self._offsets(consumer.beginning_offsets(partitions), partitions)
                end = self._offsets(consumer.end_offsets(partitions), partitions)
                end_at = self.clock()
            except Exception as exc:
                if require_beginning:
                    raise KafkaBoundaryError(classify(exc)) from None
                end_error = classify(exc)
            try:
                positions = admin.list_consumer_group_offsets(configuration["consumer_group"], partitions=list(partitions))
                for partition in partitions:
                    item = positions.get(partition)
                    raw = getattr(item, "offset", None)
                    group[partition.partition] = offset(raw)
                group_at = self.clock()
            except Exception as exc:
                if require_beginning:
                    raise KafkaBoundaryError(classify(exc)) from None
                group_error = classify(exc)
            latest_identity, latest_partitions = self.metadata_reader(admin, topic)
            if latest_identity != identity:
                raise KafkaBoundaryError("topic_identity_changed")
            if latest_partitions != all_partitions:
                raise KafkaBoundaryError("partition_set_changed")
        if require_beginning and (set(beginning) != set(numbers) or set(end) != set(numbers)):
            raise KafkaBoundaryError("missing_partition")
        return KafkaOffsets(
            identity or uuid.uuid4().hex, bool(identity), numbers, count, beginning, end,
            group, end_at, group_at, end_error, group_error,
        )

    @staticmethod
    def _offsets(values: Mapping[Any, Any], partitions: tuple[Any, ...]) -> dict[int, int]:
        return {partition.partition: value for partition in partitions if (value := offset(values.get(partition))) is not None}
