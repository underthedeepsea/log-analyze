from __future__ import annotations

from pathlib import Path

from logrisk.database import SQLiteDatabase
from logrisk.node_risk import NodeRiskService
from logrisk.risk_semantics import RiskSemanticService


def services(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    semantics = RiskSemanticService(database, Path("configs/risk_semantics/builtin.yaml"))
    risks = NodeRiskService(database, Path("configs/node_risk.yaml"), clock=lambda: "2026-07-19T12:00:00+00:00")
    return semantics, risks


def record(message: str, *, line_id: str, timestamp: str = "2026-07-19T10:00:00+00:00") -> dict:
    return {
        "raw_log_id": line_id,
        "message_core": message,
        "source_type": "syslog",
        "component": "kernel",
        "cluster": "prod-a",
        "node": "gpu-node-01",
        "timestamp": timestamp,
    }


def test_repeated_xid_is_one_event_with_separate_occurrence_count_and_replay_is_idempotent(tmp_path):
    semantics, subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="line-1")
    event = semantics.match(source)

    subject.ingest(event, source_record=source, source_job_id="job-1", occurrence_count=1000)
    subject.ingest(event, source_record=source, source_job_id="job-1", occurrence_count=1000)
    detail = subject.get_node("prod-a", "gpu-node-01")

    assert detail["statistics"]["event_count_24h"] == 1
    assert detail["statistics"]["occurrence_count_24h"] == 1000
    assert detail["statistics"]["distinct_risk_types_7d"] == 1


def test_same_gpu_different_xid_and_same_xid_different_gpu_do_not_merge(tmp_path):
    semantics, subject = services(tmp_path)
    rows = [
        record("NVRM: Xid (0000:65:00): 35, Video processor exception", line_id="line-35"),
        record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="line-79a"),
        record("NVRM: Xid (0000:66:00): 79, GPU has fallen off the bus", line_id="line-79b"),
    ]
    for row in rows:
        subject.ingest(semantics.match(row), source_record=row, source_job_id="job-xid")

    events = subject.list_events("prod-a", "gpu-node-01")["items"]
    assert len(events) == 3
    assert {event["risk_type"] for event in events} == {
        "gpu.video_processor_exception", "gpu.fallen_off_bus"
    }
    assert {event["semantic_fields"]["pci_bdf"] for event in events} == {"0000:65:00", "0000:66:00"}


def test_active_xid_79_forces_explainable_critical_snapshot(tmp_path):
    semantics, subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="line-critical")
    subject.ingest(semantics.match(source), source_record=source, source_job_id="job-critical")

    snapshot = subject.get_node("prod-a", "gpu-node-01")["snapshot"]

    assert snapshot["overall_level"] == "critical"
    assert snapshot["overall_score"] >= 95
    assert snapshot["active_event_count"] == 1
    assert snapshot["score_breakdown"]["final_score"] == snapshot["overall_score"]
    assert any("fallen off bus" in reason.lower() for reason in snapshot["assessment_reasons"])


def test_cross_cluster_nodes_are_isolated_and_recovery_changes_active_count(tmp_path):
    semantics, subject = services(tmp_path)
    first = record("NVRM: Xid (0000:65:00): 35, Video processor exception", line_id="a")
    second = dict(first, raw_log_id="b", cluster="prod-b")
    event_a = subject.ingest(semantics.match(first), source_record=first, source_job_id="job-a")
    subject.ingest(semantics.match(second), source_record=second, source_job_id="job-b")
    subject.recover_event(event_a["event_id"], operator="qa", reason="设备恢复")

    assert subject.get_node("prod-a", "gpu-node-01")["snapshot"]["active_event_count"] == 0
    assert subject.get_node("prod-b", "gpu-node-01")["snapshot"]["active_event_count"] == 1
