from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol

from logrisk.kafka_records import KafkaCursorDelta
from logrisk.stream_input_parser import iter_file_records


class IncrementalSourceError(ValueError):
    """A source error that is safe to show in the Dashboard."""


class RecomputeSourceIdentityError(IncrementalSourceError):
    code = "RECOMPUTE_SOURCE_IDENTITY_UNVERIFIABLE"


@dataclass(frozen=True)
class SourceCursor:
    kind: str
    value: dict[str, Any]

    @classmethod
    def empty(cls) -> "SourceCursor":
        return cls(kind="", value={})

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "value": dict(self.value)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | None) -> "SourceCursor":
        value = value or {}
        return cls(kind=str(value.get("kind") or ""), value=dict(value.get("value") or {}))


@dataclass(frozen=True)
class SourceDescriptor:
    kind: str
    identity: dict[str, Any]
    configuration: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "identity": dict(self.identity),
            "configuration": dict(self.configuration),
        }


@dataclass(frozen=True)
class SourceRecord:
    record: dict[str, Any]
    next_cursor: SourceCursor | KafkaCursorDelta
    # Optional, transient position metadata.  Existing two-argument callers
    # remain valid; ingestion accounting never guesses from a high-water mark.
    metadata: dict[str, Any] | None = None
    rejection_code: str | None = None

    @property
    def position(self) -> dict[str, Any] | None:
        """Compatibility alias for adapters that call this ``position``."""

        return dict(self.metadata) if isinstance(self.metadata, Mapping) else None


class IncrementalSource(Protocol):
    def descriptor(self) -> SourceDescriptor:
        raise NotImplementedError

    def validate_descriptor(self, descriptor: Mapping[str, Any]) -> None:
        raise NotImplementedError

    def read(self, cursor: SourceCursor) -> Iterator[SourceRecord]:
        raise NotImplementedError

    def commit(self, cursor: SourceCursor) -> None:
        raise NotImplementedError


class KafkaConsumerAdapter(Protocol):
    """Internal-only adapter contract; implementations own Broker communication."""

    adapter_id: str

    def read(
        self,
        configuration: Mapping[str, str],
        cursor: SourceCursor,
    ) -> Iterator[SourceRecord]:
        raise NotImplementedError

    def commit(
        self,
        configuration: Mapping[str, str],
        cursor: SourceCursor,
    ) -> None:
        raise NotImplementedError


_KAFKA_ADAPTERS: dict[str, KafkaConsumerAdapter] = {}


def parse_kafka_enabled(value: Any) -> bool:
    """Parse the opt-in flag consistently without truthy string coercion."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "0", "false", "no", "off"}:
            return False
        if normalized in {"1", "true", "yes", "on"}:
            return True
    raise IncrementalSourceError("kafka_enabled 必须是布尔值或 true/false、1/0、yes/no、on/off")


def register_kafka_consumer_adapter(adapter: KafkaConsumerAdapter) -> None:
    """Register a reviewed in-process adapter without importing Kafka libraries here."""

    adapter_id = str(getattr(adapter, "adapter_id", "") or "").strip()
    if not adapter_id:
        raise IncrementalSourceError("Kafka 适配器缺少 adapter_id")
    if not callable(getattr(adapter, "read", None)):
        raise IncrementalSourceError("Kafka 适配器缺少 read 方法")
    if not callable(getattr(adapter, "commit", None)):
        raise IncrementalSourceError("Kafka 适配器缺少 commit 方法")
    _KAFKA_ADAPTERS[adapter_id] = adapter


def unregister_kafka_consumer_adapter(adapter_id: str) -> None:
    _KAFKA_ADAPTERS.pop(str(adapter_id), None)


class FileIncrementalSource:
    _FINGERPRINT_BYTES = 65536

    def __init__(
        self,
        path: str | Path,
        *,
        filename: str | None = None,
        max_decompressed_bytes: int | None = None,
        max_compression_ratio: float | None = None,
        max_line_bytes: int | None = None,
        environment: str = "production",
        scope_key: str = "default",
        immutable_identity: bool = False,
        expected_content_digest: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.filename = filename or self.path.name
        self.max_decompressed_bytes = max_decompressed_bytes
        self.max_compression_ratio = max_compression_ratio
        self.max_line_bytes = max_line_bytes
        self.environment = str(environment or "production")
        self.scope_key = str(scope_key or "default")
        self.immutable_identity = bool(immutable_identity or expected_content_digest)
        self.expected_content_digest = expected_content_digest
        self._descriptor_cache: tuple[tuple[int, ...], bool, dict[str, Any]] | None = None
        self._digest_cache: tuple[tuple[int, ...], str] | None = None
        self._validated_snapshot: tuple[int, ...] | None = None

    def descriptor(self) -> SourceDescriptor:
        return SourceDescriptor(
            kind="file",
            identity=self._identity(),
            configuration={"filename": self.filename},
        )

    def validate_descriptor(self, descriptor: Mapping[str, Any]) -> None:
        if str(descriptor.get("kind") or "") != "file":
            raise IncrementalSourceError("恢复来源类型不匹配")
        expected_configuration = dict(descriptor.get("configuration") or {})
        if expected_configuration.get("filename") != self.filename:
            raise IncrementalSourceError("输入文件解析配置已变化，不能继续恢复")
        expected = dict(descriptor.get("identity") or {})
        current = self._identity()
        for field in ("environment", "scope_key"):
            if field in expected and expected.get(field) != current.get(field):
                raise IncrementalSourceError("输入文件作用域已变化，不能继续恢复")
        for field in ("path", "device", "inode"):
            if expected.get(field) != current.get(field):
                raise IncrementalSourceError("输入文件身份已变化，不能继续恢复")
        expected_digest = expected.get("identity_digest")
        if expected_digest and not self.immutable_identity:
            self.immutable_identity = True
            current = self._identity()
        if expected_digest and expected_digest != current.get("identity_digest"):
            raise IncrementalSourceError("输入文件完整内容已变化，不能继续恢复")
        previous_size = int(expected.get("size_bytes") or 0)
        if current["size_bytes"] < previous_size:
            raise IncrementalSourceError("输入文件长度已缩短，不能继续恢复")
        if current["size_bytes"] == previous_size and (
            expected.get("head_sha256") != current.get("head_sha256")
            or expected.get("tail_sha256") != current.get("tail_sha256")
        ):
            raise IncrementalSourceError("输入文件内容已变化，不能继续恢复")
        if current["size_bytes"] > previous_size:
            # The stored head digest covers the whole file when it is smaller
            # than the fingerprint window.  Re-read only that original prefix
            # so an ordinary append remains resumable while a rewrite is
            # rejected.
            original_head = self._digest_segment(0, min(previous_size, self._FINGERPRINT_BYTES))
            if expected.get("head_sha256") != original_head:
                raise IncrementalSourceError("输入文件开头已变化，不能继续恢复")
        assert self._descriptor_cache is not None
        self._validated_snapshot = self._descriptor_cache[0]

    def read(self, cursor: SourceCursor) -> Iterator[SourceRecord]:
        if cursor.kind not in {"", "file"}:
            raise IncrementalSourceError("文件来源不能使用其他来源的 Checkpoint")
        offset = int(cursor.value.get("offset") or 0)
        line = int(cursor.value.get("line") or 1)
        before = self._validated_snapshot or self._stat_snapshot()
        self._ensure_unchanged(before)
        if offset and self.filename.lower().endswith(".gz") and not self.immutable_identity:
            raise IncrementalSourceError("压缩日志恢复需要已验证的完整来源 SHA-256")
        try:
            for item in iter_file_records(
                self.path,
                filename=self.filename,
                start_offset=offset,
                start_line=line,
                replay_prefix=bool(offset and cursor.value.get("line_provenance") != "physical_v2"),
                max_decompressed_bytes=self.max_decompressed_bytes,
                max_compression_ratio=self.max_compression_ratio,
                max_line_bytes=self.max_line_bytes,
            ):
                line_no = item.physical_line
                yield SourceRecord(
                    record=item.record,
                    next_cursor=SourceCursor("file", {
                        "offset": item.byte_offset_end, "line": line_no + 1, "line_provenance": "physical_v2",
                    }),
                    metadata={
                        "partition_key": "",
                        "start": line_no,
                        "end": line_no + 1,
                    },
                )
            self._ensure_unchanged(before)
        except ValueError as exc:
            raise IncrementalSourceError(str(exc)) from exc

    def commit(self, cursor: SourceCursor) -> None:
        if cursor.kind not in {"", "file"}:
            raise IncrementalSourceError("文件来源不能提交其他来源的 Checkpoint")

    def before_durable_checkpoint(self, cursor: SourceCursor) -> None:
        if self._validated_snapshot is not None:
            self._ensure_unchanged(self._validated_snapshot)

    def _identity(self) -> dict[str, Any]:
        before = self._stat_snapshot()
        if self._descriptor_cache is not None:
            snapshot, immutable, identity = self._descriptor_cache
            if before == snapshot and immutable == self.immutable_identity:
                return dict(identity)
        device, inode, size_bytes, mtime_ns, _ = before
        identity = {
            "path": str(self.path.resolve()),
            "device": device,
            "inode": inode,
            "size_bytes": size_bytes,
            "mtime_ns": mtime_ns,
            "head_sha256": self._digest_segment(0, self._FINGERPRINT_BYTES),
            "tail_sha256": self._digest_segment(max(0, size_bytes - self._FINGERPRINT_BYTES), self._FINGERPRINT_BYTES),
        }
        if self.immutable_identity:
            identity.update({
                "environment": self.environment,
                "scope_key": self.scope_key,
                "identity_digest": self.content_digest(),
            })
        self._ensure_unchanged(before)
        self._descriptor_cache = before, self.immutable_identity, identity
        return dict(identity)

    def content_digest(self) -> str:
        """Hash the complete immutable file snapshot in bounded chunks."""

        before = self._stat_snapshot()
        if self._digest_cache is not None and self._digest_cache[0] == before:
            return self._digest_cache[1]
        digest = hashlib.sha256()
        try:
            with self.path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        except OSError as exc:
            raise IncrementalSourceError("输入文件不可读取") from exc
        self._ensure_unchanged(before)
        actual = digest.hexdigest()
        if self.expected_content_digest is not None and self.expected_content_digest != actual:
            raise IncrementalSourceError("输入文件与已验证上传 SHA-256 不一致")
        self._digest_cache = before, actual
        return actual

    def _stat_snapshot(self) -> tuple[int, ...]:
        try:
            stat = self.path.stat()
        except OSError as exc:
            raise IncrementalSourceError("输入文件不可读取") from exc
        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)

    def _ensure_unchanged(self, before: tuple[int, ...]) -> None:
        after = self._stat_snapshot()
        if after != before:
            raise IncrementalSourceError("输入文件在读取期间发生变化")

    def _digest_segment(self, offset: int, length: int) -> str:
        digest = hashlib.sha256()
        with self.path.open("rb") as handle:
            handle.seek(offset)
            digest.update(handle.read(length))
        return digest.hexdigest()


class KafkaIncrementalSource:
    """Incremental Kafka source backed by an explicitly registered adapter."""

    def __init__(
        self,
        configuration: Mapping[str, Any],
        *,
        adapters: Mapping[str, KafkaConsumerAdapter] | None = None,
        environment: str = "production",
        scope_key: str = "default",
    ) -> None:
        # Legacy internal callers may still register explicitly; runtimes always inject their own registry.
        self.adapters = _KAFKA_ADAPTERS if adapters is None else adapters
        self.configuration = {
            "adapter_id": str(configuration.get("adapter_id") or ""),
            "topic": str(configuration.get("topic") or ""),
            "consumer_group": str(configuration.get("consumer_group") or ""),
            "bootstrap_env": str(configuration.get("bootstrap_env") or ""),
        }
        if "cluster_id" in configuration:
            cluster_id = configuration["cluster_id"]
            if not isinstance(cluster_id, str) or not cluster_id.strip() or len(cluster_id) > 256 or any(ord(char) < 32 for char in cluster_id):
                raise IncrementalSourceError("Kafka cluster_id 必须是有效的非空标识")
            self.configuration["cluster_id"] = cluster_id.strip()
        from logrisk.normalizer import validate_source_profile
        profile = validate_source_profile(configuration.get("source_profile"))
        if "source_profile" in configuration and profile is None:
            raise IncrementalSourceError("Kafka source_profile 必须使用登记的解析profile")
        if profile:
            self.configuration["source_profile"] = profile
        self.environment = str(environment or "production")
        self.scope_key = str(scope_key or "default")
        self.observe: Callable[..., None] | None = None
        self.batch: Mapping[str, Any] | None = None
        self.final_cursor: SourceCursor | None = None
        self.group_frontier: dict[int, int | None] = {}
        self.control_epoch: int | None = None
        self.external_commit: Callable[[Callable[[], None]], None] = lambda operation: operation()
        self.check_active: Callable[[], None] = lambda: None

    def descriptor(self) -> SourceDescriptor:
        cluster_id = str(self.configuration.get("cluster_id") or "")
        topic = str(self.configuration.get("topic") or "")
        identity: dict[str, Any] = {
            "environment": self.environment,
            "scope_key": self.scope_key,
        }
        if cluster_id and topic:
            identity["identity_digest"] = hashlib.sha256(
                json.dumps(
                    {"cluster_id": cluster_id, "topic": topic},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
        return SourceDescriptor(kind="kafka", identity=identity, configuration=dict(self.configuration))

    def validate_descriptor(self, descriptor: Mapping[str, Any]) -> None:
        if str(descriptor.get("kind") or "") != "kafka":
            raise IncrementalSourceError("恢复来源类型不匹配")
        if dict(descriptor.get("configuration") or {}) != self.configuration:
            raise IncrementalSourceError("Kafka 来源配置已变化，不能继续恢复")

    def read(self, cursor: SourceCursor) -> Iterator[SourceRecord]:
        adapter_id = self.configuration["adapter_id"]
        adapter = self.adapters.get(adapter_id)
        if adapter is None:
            raise IncrementalSourceError("Kafka 消费适配器未注册，无法启动消费")
        if cursor.kind not in {"", "kafka"}:
            raise IncrementalSourceError("Kafka 来源不能使用其他来源的 Checkpoint")
        self.check_active()

        def adapter_records() -> Iterator[SourceRecord]:
            from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError, classify

            try:
                if self.batch is not None:
                    yield from adapter.read_bounded(self, cursor)
                elif self.observe is not None and callable(getattr(adapter, "read_observed", None)):
                    yield from adapter.read_observed(self.configuration, cursor, self.observe)
                else:
                    yield from adapter.read(self.configuration, cursor)
            except KafkaBoundaryError:
                raise
            except Exception as exc:
                raise KafkaBoundaryError(classify(exc), stage="consume") from None

        def records() -> Iterator[SourceRecord]:
            from logrisk.normalizer import ConfiguredSourceText

            for item in adapter_records():
                # These internal normalization hints belong to registered source
                # configuration. JSON producer fields cannot activate or replace them.
                record = {key: value for key, value in item.record.items() if key not in {"_source_profile", "_source_cluster"}}
                if self.configuration.get("source_profile"):
                    record["_source_profile"] = ConfiguredSourceText(self.configuration["source_profile"])
                if self.configuration.get("cluster_id"):
                    record["_source_cluster"] = ConfiguredSourceText(self.configuration["cluster_id"])
                if self.observe is not None:
                    metadata = item.metadata or {}
                    position = metadata.get("offset")
                    if type(position) is int:
                        self.observe("yield", {"partition": metadata.get("partition"), "next_offset": position + 1})
                yield SourceRecord(record, item.next_cursor, item.metadata, item.rejection_code)
        return iter(records())

    def before_durable_checkpoint(self, cursor: SourceCursor) -> None:
        self.check_active()
        if self.batch is not None:
            adapter = self.adapters[self.configuration["adapter_id"]]
            adapter.validate_bounded_checkpoint(self, cursor)
            self.check_active()

    def commit(self, cursor: SourceCursor) -> None:
        from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError, classify

        adapter_id = self.configuration["adapter_id"]
        adapter = self.adapters.get(adapter_id)
        if adapter is None:
            raise IncrementalSourceError("Kafka 消费适配器未注册，无法提交消费位点")
        if cursor.kind not in {"", "kafka"}:
            raise IncrementalSourceError("Kafka 来源不能提交其他来源的 Checkpoint")
        self.check_active()
        try:
            if self.batch is not None:
                adapter.commit_bounded(self, cursor)
                return
            commit = getattr(adapter, "commit", None)
            if not callable(commit):
                raise KafkaBoundaryError("consumer_failed", "Kafka 消费适配器缺少 commit 方法", stage="group_commit")
            commit(self.configuration, cursor)
        except KafkaBoundaryError as exc:
            exc.stage = exc.stage or "group_commit"
            raise
        except Exception as exc:
            raise KafkaBoundaryError(classify(exc), stage="group_commit") from None


def source_capabilities(
    adapters: Mapping[str, KafkaConsumerAdapter] | None = None,
    *,
    enabled: bool | None = None,
    active_tasks: int = 0,
) -> dict[str, dict[str, Any]]:
    adapters = _KAFKA_ADAPTERS if adapters is None else adapters
    registered = sorted(adapters)
    kafka_enabled = bool(registered) if enabled is None else enabled
    return {
        "file": {"enabled": True, "resume_supported": True},
        "kafka": {
            "configured": bool(registered),
            "enabled": kafka_enabled,
            "dependency_ready": bool(registered) and all(
                getattr(adapter, "dependency_ready", True) is True for adapter in adapters.values()
            ),
            "connection_check": "not_checked",
            "active_tasks": active_tasks,
            "consumption_mode": "bounded_high_water",
            "resume_supported": True,
            "reason": "Kafka 消费适配器未注册" if not registered else "Kafka 适配器已注册；Broker 连接未检查；读取本批高水位后结束",
            "required_fields": ["adapter_id", "topic", "consumer_group", "bootstrap_env"],
            "registered_adapter_ids": registered,
        },
    }
