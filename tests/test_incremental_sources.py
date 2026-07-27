from __future__ import annotations

import pytest

from logrisk.incremental_sources import (
    FileIncrementalSource,
    IncrementalSourceError,
    KafkaIncrementalSource,
    SourceCursor,
    SourceRecord,
    register_kafka_consumer_adapter,
    unregister_kafka_consumer_adapter,
)


def test_file_source_resumes_at_last_committed_offset(tmp_path):
    path = tmp_path / "messages"
    path.write_text("first\nsecond\n", encoding="utf-8")

    source = FileIncrementalSource(path, filename="messages")

    records = list(source.read(SourceCursor.empty()))
    resumed = list(source.read(records[0].next_cursor))

    assert [item.record["message"] for item in resumed] == ["second"]


def test_kafka_source_refuses_without_registered_adapter():
    source = KafkaIncrementalSource(
        {
            "adapter_id": "internal-kafka",
            "topic": "logs",
            "consumer_group": "logrisk",
            "bootstrap_env": "LOGRISK_KAFKA_BOOTSTRAP",
        }
    )

    with pytest.raises(IncrementalSourceError, match="Kafka 消费适配器未注册"):
        source.read(SourceCursor.empty())


def test_file_source_keeps_line_size_limit(tmp_path):
    path = tmp_path / "messages"
    path.write_text("this line is too long\n", encoding="utf-8")
    source = FileIncrementalSource(path, filename="messages", max_line_bytes=4)

    with pytest.raises(IncrementalSourceError, match="line_too_large"):
        list(source.read(SourceCursor.empty()))


def test_file_source_allows_an_append_but_rejects_a_rewrite(tmp_path):
    path = tmp_path / "messages"
    path.write_text("first\n", encoding="utf-8")
    source = FileIncrementalSource(path, filename="messages")
    descriptor = source.descriptor().to_dict()

    path.write_text("first\nsecond\n", encoding="utf-8")
    source.validate_descriptor(descriptor)

    path.write_text("other\nsecond\n", encoding="utf-8")
    with pytest.raises(IncrementalSourceError, match="输入文件"):
        source.validate_descriptor(descriptor)


def test_kafka_source_delegates_only_to_an_explicitly_registered_adapter():
    class FakeAdapter:
        adapter_id = "fake-kafka"

        def read(self, configuration, cursor):
            assert configuration["topic"] == "logs"
            assert cursor.kind == ""
            yield SourceRecord({"message": "sanitized event"}, SourceCursor("kafka", {"partition": 0, "offset": 1}))

    register_kafka_consumer_adapter(FakeAdapter())
    try:
        source = KafkaIncrementalSource({
            "adapter_id": "fake-kafka",
            "topic": "logs",
            "consumer_group": "logrisk",
            "bootstrap_env": "LOGRISK_KAFKA_BOOTSTRAP",
        })
        records = list(source.read(SourceCursor.empty()))
    finally:
        unregister_kafka_consumer_adapter("fake-kafka")

    assert records[0].next_cursor.value == {"partition": 0, "offset": 1}
