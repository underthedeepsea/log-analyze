from __future__ import annotations

from logrisk.drain_miner import mine_template_events
from logrisk.template_identity import canonical_template, template_fingerprint


def _record(index: int, node: str, message: str) -> dict[str, object]:
    return {
        "raw_log_id": str(index),
        "timestamp": "2026-07-10T10:00:00+00:00",
        "cluster": "prod-a",
        "node": node,
        "source_type": "syslog",
        "component": "kernel",
        "severity": "ERROR",
        "message_core": message,
        "raw_log": message,
    }


def test_parallel_node_partitions_preserve_input_event_order(tmp_path):
    events, metadata = mine_template_events(
        [
            _record(0, "node-a", "kernel error alpha"),
            _record(1, "node-b", "kernel error beta"),
            _record(2, "node-a", "kernel error gamma"),
        ],
        config_path="configs/drain3_recommended.ini",
        state_dir=tmp_path,
        worker_count=2,
        partition_by_node=True,
        return_metadata=True,
    )

    assert [event["event_id"] for event in events] == ["0", "1", "2"]
    assert metadata == {
        "partition_count": 2,
        "worker_count": 2,
        "parallel": True,
        "process_start_method": "spawn",
    }
    assert (tmp_path / "prod-a__node-a__syslog__kernel.bin").exists()
    assert (tmp_path / "prod-a__node-b__syslog__kernel.bin").exists()


def test_single_partition_limits_worker_count_to_one(tmp_path):
    _, metadata = mine_template_events(
        [_record(0, "node-a", "kernel error")],
        config_path="configs/drain3_recommended.ini",
        state_dir=tmp_path,
        worker_count=8,
        partition_by_node=True,
        return_metadata=True,
    )

    assert metadata == {
        "partition_count": 1,
        "worker_count": 1,
        "parallel": False,
        "process_start_method": "spawn",
    }


def test_template_identity_is_stable_across_clusters_and_nodes(tmp_path):
    first = _record(0, "node-a", "Out of memory: Killed process 42")
    second = _record(1, "node-b", "Out of memory: Killed process 99")
    second["cluster"] = "prod-b"

    events = mine_template_events(
        [first, second],
        config_path="configs/drain3_recommended.ini",
        state_dir=tmp_path,
        partition_by_node=True,
    )

    assert events[0]["template_fingerprint"] == events[1]["template_fingerprint"]
    assert events[0]["template_instance_hash"] != events[1]["template_instance_hash"]
    assert all(event["hash_version"] == "v2" for event in events)


def test_canonical_template_normalizes_spacing_and_drain_placeholders():
    assert canonical_template("  Failed  for <NUM> at <IP> ") == "Failed for <*> at <*>"
    assert template_fingerprint("syslog", "kernel", "Failed for <NUM>") == template_fingerprint(
        "syslog", "kernel", "Failed  for <IP>"
    )


def test_parameter_extraction_is_off_by_default_and_available_in_all_mode(tmp_path):
    record = _record(0, "node-a", "connection from 10.0.0.1")
    default = mine_template_events([record], "configs/drain3_recommended.ini", tmp_path / "off")
    enabled = mine_template_events(
        [record],
        "configs/drain3_recommended.ini",
        tmp_path / "all",
        parameter_extraction_mode="all",
    )

    assert default[0]["parameters"] == []
    assert isinstance(enabled[0]["parameters"], list)


def test_worker_count_respects_configured_cap_and_reserved_cpu(tmp_path, monkeypatch):
    monkeypatch.setattr("logrisk.drain_miner.os.cpu_count", lambda: 8)
    _, metadata = mine_template_events(
        [_record(index, f"node-{index}", f"error {index}") for index in range(6)],
        "configs/drain3_recommended.ini",
        tmp_path,
        worker_count=8,
        max_workers=3,
        reserve_cpu_cores=2,
        partition_by_node=True,
        return_metadata=True,
    )

    assert metadata["worker_count"] == 3
    assert metadata["process_start_method"] == "spawn"
