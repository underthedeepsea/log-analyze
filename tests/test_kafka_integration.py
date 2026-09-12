from __future__ import annotations

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import (
    IncrementalSourceError,
    KafkaIncrementalSource,
    SourceCursor,
    SourceDescriptor,
    SourceRecord,
    parse_kafka_enabled,
    register_kafka_consumer_adapter,
    source_capabilities,
    unregister_kafka_consumer_adapter,
)
from logrisk.kafka_adapter import KafkaPythonConsumerAdapter, decode_kafka_value
from logrisk.large_file_pipeline import run_incremental_pipeline
from logrisk.streaming_state import StreamingStateRepository


@pytest.mark.parametrize("value", [None, False, 0, "", "false", "FALSE", " false ", "0", "no", "off"])
def test_kafka_opt_in_parses_disabled_values(value):
    assert parse_kafka_enabled(value) is False


@pytest.mark.parametrize("value", [True, 1, "true", " TRUE ", "1", "yes", "on"])
def test_kafka_opt_in_parses_enabled_values(value):
    assert parse_kafka_enabled(value) is True


@pytest.mark.parametrize("value", ["maybe", 2, 0.0, [], {}])
def test_kafka_opt_in_rejects_ambiguous_values(value):
    with pytest.raises(IncrementalSourceError, match="kafka_enabled"):
        parse_kafka_enabled(value)


def test_explicit_source_registry_never_uses_legacy_global_adapter():
    adapter = KafkaPythonConsumerAdapter(lambda **kwargs: pytest.fail("Consumer must not start"))
    register_kafka_consumer_adapter(adapter)
    try:
        isolated = KafkaIncrementalSource({"adapter_id": adapter.adapter_id}, adapters={})
        with pytest.raises(IncrementalSourceError, match="未注册"):
            isolated.read(SourceCursor.empty())
        with pytest.raises(IncrementalSourceError, match="未注册"):
            isolated.commit(SourceCursor.empty())
        assert source_capabilities({})["kafka"]["enabled"] is False
    finally:
        unregister_kafka_consumer_adapter(adapter.adapter_id)


def test_kafka_status_reports_local_dependency_without_broker_probe(monkeypatch):
    monkeypatch.setattr("logrisk.kafka_adapter.find_spec", lambda name: None)
    adapter = KafkaPythonConsumerAdapter()
    monkeypatch.setattr(adapter, "_new_consumer", lambda *args: pytest.fail("Consumer must not start"))
    status = source_capabilities({adapter.adapter_id: adapter}, enabled=True)["kafka"]

    assert status["configured"] is True
    assert status["enabled"] is True
    assert status["dependency_ready"] is False
    assert status["connection_check"] == "not_checked"
    assert status["active_tasks"] == 0
    assert status["consumption_mode"] == "bounded_high_water"
    assert "读取本批高水位后结束" in status["reason"]
    assert "online" not in status


class FakeIncrementalSource:
    def __init__(self):
        self.commits = []
        self.records = [
            SourceRecord(
                {"timestamp": "2026-09-01T00:00:00+00:00", "node": "node-a", "component": "kernel", "message": "error one"},
                SourceCursor("kafka", {"partition": 0, "offset": 1}),
            ),
            SourceRecord(
                {"timestamp": "2026-09-01T00:01:00+00:00", "node": "node-a", "component": "kernel", "message": "error one"},
                SourceCursor("kafka", {"partition": 0, "offset": 2}),
            ),
        ]

    def descriptor(self):
        return SourceDescriptor(
            kind="kafka",
            identity={},
            configuration={
                "adapter_id": "fake",
                "topic": "logs",
                "consumer_group": "logrisk-test",
                "bootstrap_env": "LOGRISK_KAFKA_BOOTSTRAP",
            },
        )

    def validate_descriptor(self, descriptor):
        assert descriptor == self.descriptor().to_dict()

    def read(self, cursor):
        return iter(self.records)

    def commit(self, cursor):
        self.commits.append(cursor.to_dict())


def test_kafka_value_decodes_json_objects_and_keeps_plain_text_as_message():
    assert decode_kafka_value(b'{"timestamp":"2026-09-01T00:00:00Z","message":"failure"}') == {
        "timestamp": "2026-09-01T00:00:00Z",
        "message": "failure",
    }
    assert decode_kafka_value(b"plain failure") == {"message": "plain failure"}


def test_kafka_python_adapter_reads_to_high_water_mark_and_commits_next_offset(monkeypatch):
    consumers = []

    class Message:
        def __init__(self, offset, value):
            self.offset = offset
            self.value = value

    class FakeConsumer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.partition = None
            self.next_offset = 0
            self.polled = False
            self.committed = None
            consumers.append(self)

        def partitions_for_topic(self, topic):
            return {0}

        def assign(self, partitions):
            self.partition = tuple(partitions)[0]

        def seek_to_beginning(self, partition):
            self.next_offset = 0

        def seek(self, partition, offset):
            self.next_offset = offset

        def end_offsets(self, partitions):
            return {self.partition: 2}

        def poll(self, **kwargs):
            if self.polled:
                return {}
            self.polled = True
            self.next_offset = 3
            return {self.partition: [
                Message(0, b'{"message":"one"}'), Message(1, b"two"), Message(2, b"beyond high water"),
            ]}

        def position(self, partition):
            return self.next_offset

        def commit(self, offsets):
            self.committed = offsets

        def close(self, **kwargs):
            return None

    monkeypatch.setenv("LOGRISK_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    adapter = KafkaPythonConsumerAdapter(FakeConsumer)
    configuration = {
        "topic": "logs",
        "consumer_group": "logrisk-test",
        "bootstrap_env": "LOGRISK_KAFKA_BOOTSTRAP",
    }

    records = adapter.read(configuration, SourceCursor.empty())
    first = next(records)
    adapter.commit(configuration, first.next_cursor)
    remaining = list(records)

    assert first.record == {"message": "one"}
    assert [item.record for item in remaining] == [{"message": "two"}]
    assert first.next_cursor.value["high_water"] == {"0": 2}
    assert len(first.next_cursor.value["bootstrap_fingerprint"]) == 64
    assert consumers[0].kwargs["request_timeout_ms"] > consumers[0].kwargs["session_timeout_ms"]
    assert consumers[0].committed
    assert next(iter(consumers[0].committed.values())).offset == 1


def test_incremental_pipeline_commits_each_bounded_batch_after_state_commit(tmp_path):
    source = FakeIncrementalSource()
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))

    result = run_incremental_pipeline(
        input_job_id="kafka_input_test",
        source=source,
        source_name="kafka://logs",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        streaming_repository=repository,
        stream_batch_records=1,
    )

    assert result["summary"]["total_raw_logs"] == 2
    assert len(result["risk_entities"][0]["top_templates"]) == 1
    assert result["risk_entities"][0]["top_templates"][0]["count"] == 2
    assert source.commits == [
        {"kind": "kafka", "value": {"partition": 0, "offset": 1}},
        {"kind": "kafka", "value": {"partition": 0, "offset": 2}},
    ]
    assert repository.get_task(result["summary"]["streaming_task_id"])["status"] == "completed"


def test_kafka_checkpoint_retries_external_commit_before_reading_next_batch(tmp_path):
    class CommitOnceFailSource(FakeIncrementalSource):
        def __init__(self):
            super().__init__()
            self.fail_next_commit = True

        def read(self, cursor):
            offset = int((cursor.value or {}).get("offset") or 0)
            return iter(self.records[offset:])

        def commit(self, cursor):
            if self.fail_next_commit:
                self.fail_next_commit = False
                raise RuntimeError("temporary Kafka commit failure")
            super().commit(cursor)

    source = CommitOnceFailSource()
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    arguments = {
        "input_job_id": "kafka_commit_retry",
        "source": source,
        "source_name": "kafka://logs",
        "config_path": "configs/drain3_recommended.ini",
        "rules_path": "configs/risk_rules.yaml",
        "state_dir": tmp_path / "state",
        "worker_count": 1,
        "streaming_repository": repository,
        "stream_batch_records": 1,
    }

    try:
        run_incremental_pipeline(**arguments)
    except RuntimeError as exc:
        assert str(exc) == "temporary Kafka commit failure"
    else:
        raise AssertionError("expected the first Kafka commit to fail")

    task_id = repository.list_tasks()[0]["task_id"]
    failed = repository.get_task(task_id)
    assert failed["status"] == "failed"
    assert failed["cursor"]["value"]["offset"] == 1
    assert failed["pending_external_commit"]["value"]["offset"] == 1

    resumed = run_incremental_pipeline(**arguments, resume_task_id=task_id)

    assert resumed["summary"]["total_raw_logs"] == 1
    assert source.commits == [
        {"kind": "kafka", "value": {"partition": 0, "offset": 1}},
        {"kind": "kafka", "value": {"partition": 0, "offset": 2}},
    ]
    completed = repository.get_task(task_id)
    assert completed["status"] == "completed"
    assert completed["pending_external_commit"] is None
    assert completed["records_processed"] == 2
