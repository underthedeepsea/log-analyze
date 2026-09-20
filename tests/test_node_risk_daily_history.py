from __future__ import annotations

from tests.test_node_risk import record, services


def test_daily_peak_does_not_decrease_after_later_lower_score(tmp_path):
    semantics, subject = services(tmp_path)
    critical = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="critical")
    created = subject.ingest(semantics.match(critical), source_record=critical, source_job_id="job")
    peak = subject.daily("prod-a", "gpu-node-01")[0]["max_overall_score"]
    subject.recover_event(created["event_id"], operator="qa", reason="fixed")
    assert subject.daily("prod-a", "gpu-node-01")[0]["max_overall_score"] == peak
