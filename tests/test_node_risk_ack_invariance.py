from __future__ import annotations

from tests.test_node_risk import record, services


def test_acknowledging_unrecovered_critical_event_does_not_lower_floor(tmp_path):
    semantics, subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="critical")
    event = subject.ingest(semantics.match(source), source_record=source, source_job_id="job")
    before = subject.get_node("prod-a", "gpu-node-01")["snapshot"]
    subject.acknowledge_event(event["event_id"], operator="qa", reason="seen")
    after = subject.get_node("prod-a", "gpu-node-01")["snapshot"]

    assert after["overall_level"] == "critical"
    assert after["overall_score"] >= before["overall_score"]
