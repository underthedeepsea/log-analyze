from __future__ import annotations

from logrisk.drain_miner import mine_template_events


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
    }
