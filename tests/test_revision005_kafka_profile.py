from __future__ import annotations

import json

import pytest

from logrisk.incremental_sources import FileIncrementalSource, KafkaIncrementalSource, SourceCursor, SourceRecord
from logrisk.normalizer import normalize_record
from logrisk.partition_spool import spool_normalized_records

PROFILE = "log-cloud-shanghai-v1"


class FakeAdapter:
    adapter_id = "profile-fixture"

    def __init__(self, records):
        self.records = records
        self.read_configurations = []
        self.commits = []

    def read(self, configuration, cursor):
        self.read_configurations.append(dict(configuration))
        for index, record in enumerate(self.records):
            yield SourceRecord(record, SourceCursor("kafka", {"partition": 0, "offset": index + 1}), {"partition": 0, "offset": index})

    def commit(self, configuration, cursor):
        self.commits.append((dict(configuration), cursor))


def configuration(**changes):
    return {"adapter_id": "profile-fixture", "topic": "isolated-fixture", "consumer_group": "isolated-fixture-group", "bootstrap_env": "FIXTURE_KAFKA_BOOTSTRAP", "source_profile": PROFILE, "cluster_id": "source-cluster", **changes}


def test_kafka_profile_cri_host_and_outer_beijing_date_reach_native_spool(tmp_path):
    record = {"timestamp": "2026-08-31 00:00:01", "message": "1999-01-01T00:00:00Z stdout F Aug 31 00:00:01 node-a systemd[1]: worker.service: Failed with result 'exit-code'."}
    adapter = FakeAdapter([record])
    source = KafkaIncrementalSource(configuration(), adapters={adapter.adapter_id: adapter})
    items = list(source.read(SourceCursor.empty()))
    assert items[0].next_cursor == SourceCursor("kafka", {"partition": 0, "offset": 1})
    assert items[0].metadata == {"partition": 0, "offset": 0}
    manifest = spool_normalized_records([item.record for item in items], spool_dir=tmp_path / "spool")
    normalized = json.loads((tmp_path / "spool" / manifest["partitions"][0]["path"]).read_text())
    assert normalized["node"] == "node-a" and normalized["component"] == "systemd"
    assert normalized["cluster"] == "source-cluster"
    assert normalized["timestamp"] == "2026-08-30T16:00:01+00:00"
    assert normalized["message_core"] == "worker.service: Failed with result 'exit-code'."
    assert source.descriptor().configuration["source_profile"] == PROFILE
    source.commit(items[0].next_cursor)
    assert adapter.commits[0][1] == items[0].next_cursor
    assert record == {"timestamp": "2026-08-31 00:00:01", "message": "1999-01-01T00:00:00Z stdout F Aug 31 00:00:01 node-a systemd[1]: worker.service: Failed with result 'exit-code'."}


def test_kafka_profile_explicit_producer_cluster_and_host_take_precedence():
    adapter = FakeAdapter([{"cluster": "producer-cluster", "host": "producer-node", "timestamp": "2026-08-31T10:00:00+08:00", "message": "2026-08-31T10:00:00Z stderr F Aug 31 10:00:00 body-node systemd: fixture failure"}])
    source = KafkaIncrementalSource(configuration(), adapters={adapter.adapter_id: adapter})
    normalized = normalize_record(next(source.read(SourceCursor.empty())).record)
    assert normalized["cluster"] == "producer-cluster" and normalized["node"] == "producer-node"
    assert normalized["timestamp"] == "2026-08-31T02:00:00+00:00"


def test_kafka_profile_metadata_identity_is_explicit_and_peer_address_is_not_a_host():
    normalized = normalize_record({"_source_profile": PROFILE, "source_metadata": {"node_id": "metadata-node"}, "message": "dial tcp 10.0.0.44 failed"}, source_profile=PROFILE)
    assert normalized["node"] == "metadata-node"
    unidentified = normalize_record({"_source_profile": PROFILE, "message": "dial tcp 10.0.0.44 failed"}, source_profile=PROFILE)
    assert unidentified["node"] is None


def test_kafka_profile_missing_invalid_outer_time_does_not_borrow_body_time():
    raw = "2026-08-31T00:00:00Z stdout F Aug 31 10:00:00 node-a systemd: fixture failure"
    assert normalize_record({"_source_profile": PROFILE, "message": raw}, source_profile=PROFILE)["timestamp"] is None
    assert normalize_record({"_source_profile": PROFILE, "timestamp": "invalid-outer", "message": raw}, source_profile=PROFILE)["timestamp"] == "invalid-outer"


@pytest.mark.parametrize("changes", [{"source_profile": "unknown"}, {"source_profile": ""}, {"source_profile": {}}, {"cluster_id": {}}, {"cluster_id": True}, {"cluster_id": " "}, {"cluster_id": "bad\ncluster"}])
def test_kafka_profile_invalid_configuration_is_rejected_before_consumption(changes):
    adapter = FakeAdapter([])
    with pytest.raises(ValueError):
        KafkaIncrementalSource(configuration(**changes), adapters={adapter.adapter_id: adapter})
    assert not adapter.read_configurations
    assert KafkaIncrementalSource(configuration(), adapters={adapter.adapter_id: adapter}).descriptor().configuration["cluster_id"] == "source-cluster"


def test_kafka_profile_producer_cannot_override_configured_profile_or_default_scope():
    adapter = FakeAdapter([{"_source_profile": "unknown", "_source_cluster": "untrusted", "message": "Aug 31 10:00:00 node-a systemd: fixture failure"}])
    source = KafkaIncrementalSource(configuration(), adapters={adapter.adapter_id: adapter})
    normalized = normalize_record(next(source.read(SourceCursor.empty())).record)
    assert normalized["cluster"] == "source-cluster" and normalized["node"] == "node-a"
    plain = KafkaIncrementalSource({"adapter_id": adapter.adapter_id, "topic": "topic", "consumer_group": "group", "bootstrap_env": "FIXTURE_KAFKA_BOOTSTRAP"}, adapters={adapter.adapter_id: adapter})
    normalized = normalize_record(next(plain.read(SourceCursor.empty())).record)
    assert normalized["cluster"] == "default"


def test_kafka_profile_source_scope_does_not_relabel_invalid_producer_cluster():
    adapter = FakeAdapter([{"cluster": {"untrusted": "cluster"}, "message": "fixture"}])
    source = KafkaIncrementalSource(configuration(), adapters={adapter.adapter_id: adapter})
    with pytest.raises(ValueError):
        normalize_record(next(source.read(SourceCursor.empty())).record)


@pytest.mark.parametrize("hint", ["producer-custom", PROFILE, {}, False])
def test_kafka_profile_unconfigured_file_json_preserves_legal_producer_metadata(tmp_path, hint):
    row = {"_source_profile": hint, "timestamp": "2026-08-31 00:00:01", "message": "2026-08-31T00:00:01Z stdout F Aug 31 00:00:01 node-a systemd: fixture"}
    path = tmp_path / "producer.jsonl"
    path.write_text(json.dumps(row) + "\n")
    record = next(FileIncrementalSource(path).read(SourceCursor.empty())).record
    normalized = normalize_record(record)
    # The original normalizer accepts arbitrary extra producer JSON fields.
    assert normalized["node"] is None
    assert normalized["timestamp"] == row["timestamp"]
    assert normalized["cluster"] == "default"
    # An explicitly configured caller retains the registered parser behavior.
    configured = normalize_record(record, source_profile=PROFILE)
    assert configured["node"] == "node-a" and configured["timestamp"] == "2026-08-30T16:00:01+00:00"


@pytest.mark.parametrize("hint", ["producer-cluster", {}, True])
def test_kafka_profile_unconfigured_producer_source_cluster_cannot_replace_caller_default(hint):
    normalized = normalize_record({"_source_cluster": hint, "message": "fixture"}, default_cluster="caller-default")
    assert normalized["cluster"] == "caller-default"
    assert normalize_record({"_source_cluster": hint, "cluster": "producer-explicit", "message": "fixture"}, default_cluster="caller-default")["cluster"] == "producer-explicit"
