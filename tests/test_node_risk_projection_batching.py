from __future__ import annotations

from tests.test_node_risk import record, services


def test_batch_refreshes_each_node_once(tmp_path, monkeypatch):
    semantics, subject = services(tmp_path)
    calls = []
    original = subject.recalculate
    monkeypatch.setattr(subject, "recalculate", lambda cluster, node: calls.append((cluster, node)) or original(cluster, node))
    contributions = []
    for index in range(5):
        source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id=str(index))
        source["source_batch_id"] = f"batch-{index}"
        contributions.append({"semantic_event": semantics.match(source), "source_record": source, "source_job_id": "job"})
    subject.ingest_batch(contributions)
    assert calls == [("prod-a", "gpu-node-01")]
    subject.ingest_batch(contributions)
    assert calls == [("prod-a", "gpu-node-01")]


def test_dirty_projection_survives_worker_stop_and_read_refreshes_critical(tmp_path):
    semantics, subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="crash")
    subject.ingest(semantics.match(source),source_record=source,_recalculate=False)
    with subject.database.connect() as connection:
        row = connection.execute("SELECT revision,projected_revision FROM node_risk_projection_revisions").fetchone()
        assert tuple(row) == (1,0)
    assert subject.get_node("prod-a","gpu-node-01")["snapshot"]["overall_level"] == "critical"


def test_projection_cas_rejects_concurrent_fact_change(tmp_path,monkeypatch):
    import pytest
    from logrisk.node_risk import NodeRiskError
    semantics, subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus",line_id="first")
    subject.ingest(semantics.match(source),source_record=source)
    original = subject._score
    def concurrent(events,statistics,now):
        with subject.database.transaction() as connection:
            subject._mark_dirty(connection,"prod-a","gpu-node-01",subject.clock())
        return original(events,statistics,now)
    monkeypatch.setattr(subject,"_score",concurrent)
    with pytest.raises(NodeRiskError,match="版本"):
        subject.recalculate("prod-a","gpu-node-01")
