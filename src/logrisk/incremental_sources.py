from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol

from logrisk.stream_input_parser import iter_log_records_from_file


class IncrementalSourceError(ValueError):
    """A source error that is safe to show in the Dashboard."""


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
    next_cursor: SourceCursor


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
    ) -> None:
        self.path = Path(path)
        self.filename = filename or self.path.name
        self.max_decompressed_bytes = max_decompressed_bytes
        self.max_compression_ratio = max_compression_ratio
        self.max_line_bytes = max_line_bytes

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
        for field in ("path", "device", "inode"):
            if expected.get(field) != current.get(field):
                raise IncrementalSourceError("输入文件身份已变化，不能继续恢复")
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

    def read(self, cursor: SourceCursor) -> Iterator[SourceRecord]:
        if cursor.kind not in {"", "file"}:
            raise IncrementalSourceError("文件来源不能使用其他来源的 Checkpoint")
        offset = int(cursor.value.get("offset") or 0)
        line = int(cursor.value.get("line") or 1)
        try:
            for record in iter_log_records_from_file(
                self.path,
                filename=self.filename,
                start_offset=offset,
                start_line=line,
                max_decompressed_bytes=self.max_decompressed_bytes,
                max_compression_ratio=self.max_compression_ratio,
                max_line_bytes=self.max_line_bytes,
            ):
                next_offset = int(record.pop("_byte_offset_end"))
                next_line = int(record.get("_line_no") or line) + 1
                yield SourceRecord(
                    record=record,
                    next_cursor=SourceCursor("file", {"offset": next_offset, "line": next_line}),
                )
        except ValueError as exc:
            raise IncrementalSourceError(str(exc)) from exc

    def commit(self, cursor: SourceCursor) -> None:
        if cursor.kind not in {"", "file"}:
            raise IncrementalSourceError("文件来源不能提交其他来源的 Checkpoint")

    def _identity(self) -> dict[str, Any]:
        try:
            stat = self.path.stat()
        except OSError as exc:
            raise IncrementalSourceError("输入文件不可读取") from exc
        return {
            "path": str(self.path.resolve()),
            "device": int(stat.st_dev),
            "inode": int(stat.st_ino),
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "head_sha256": self._digest_segment(0, self._FINGERPRINT_BYTES),
            "tail_sha256": self._digest_segment(max(0, stat.st_size - self._FINGERPRINT_BYTES), self._FINGERPRINT_BYTES),
        }

    def _digest_segment(self, offset: int, length: int) -> str:
        digest = hashlib.sha256()
        with self.path.open("rb") as handle:
            handle.seek(offset)
            digest.update(handle.read(length))
        return digest.hexdigest()


class KafkaIncrementalSource:
    """Incremental Kafka source backed by an explicitly registered adapter."""

    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.configuration = {
            "adapter_id": str(configuration.get("adapter_id") or ""),
            "topic": str(configuration.get("topic") or ""),
            "consumer_group": str(configuration.get("consumer_group") or ""),
            "bootstrap_env": str(configuration.get("bootstrap_env") or ""),
        }

    def descriptor(self) -> SourceDescriptor:
        return SourceDescriptor(kind="kafka", identity={}, configuration=dict(self.configuration))

    def validate_descriptor(self, descriptor: Mapping[str, Any]) -> None:
        if str(descriptor.get("kind") or "") != "kafka":
            raise IncrementalSourceError("恢复来源类型不匹配")
        if dict(descriptor.get("configuration") or {}) != self.configuration:
            raise IncrementalSourceError("Kafka 来源配置已变化，不能继续恢复")

    def read(self, cursor: SourceCursor) -> Iterator[SourceRecord]:
        adapter_id = self.configuration["adapter_id"]
        adapter = _KAFKA_ADAPTERS.get(adapter_id)
        if adapter is None:
            raise IncrementalSourceError("Kafka 消费适配器未注册，无法启动消费")
        if cursor.kind not in {"", "kafka"}:
            raise IncrementalSourceError("Kafka 来源不能使用其他来源的 Checkpoint")
        return iter(adapter.read(self.configuration, cursor))

    def commit(self, cursor: SourceCursor) -> None:
        adapter_id = self.configuration["adapter_id"]
        adapter = _KAFKA_ADAPTERS.get(adapter_id)
        if adapter is None:
            raise IncrementalSourceError("Kafka 消费适配器未注册，无法提交消费位点")
        if cursor.kind not in {"", "kafka"}:
            raise IncrementalSourceError("Kafka 来源不能提交其他来源的 Checkpoint")
        commit = getattr(adapter, "commit", None)
        if not callable(commit):
            raise IncrementalSourceError("Kafka 消费适配器缺少 commit 方法")
        commit(self.configuration, cursor)


def source_capabilities() -> dict[str, dict[str, Any]]:
    registered = sorted(_KAFKA_ADAPTERS)
    return {
        "file": {"enabled": True, "resume_supported": True},
        "kafka": {
            "enabled": bool(registered),
            "resume_supported": True,
            "reason": "Kafka 消费适配器未注册" if not registered else "Kafka 适配器已注册；仅允许内部受控任务启动器调用",
            "required_fields": ["adapter_id", "topic", "consumer_group", "bootstrap_env"],
            "registered_adapter_ids": registered,
        },
    }
