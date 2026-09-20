from __future__ import annotations

from tests.test_node_risk import record, services


def test_new_batch_reopens_recovered_event_and_old_open_risk_remains_current(tmp_path):
    semantics, subject = services(tmp_path)
    old = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="old", timestamp="2026-05-01T10:00:00+00:00")
    event = semantics.match(old)
    created = subject.ingest(event, source_record=dict(old, source_batch_id="one"), source_job_id="job")
    subject.recover_event(created["event_id"], operator="qa", reason="fixed")
    subject.ingest(event, source_record=dict(old, source_batch_id="two", timestamp="2026-07-19T10:00:00+00:00"), source_job_id="job")

    detail = subject.get_node("prod-a", "gpu-node-01")
    assert detail["snapshot"]["active_event_count"] == 1
    assert detail["events"][0]["status"] == "active"
