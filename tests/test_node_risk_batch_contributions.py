from __future__ import annotations

from tests.test_node_risk import record, services


def test_distinct_source_batches_accumulate_and_replay_is_idempotent(tmp_path):
    semantics, subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus", line_id="aggregate")
    event = semantics.match(source)
    first = dict(source, source_batch_id="batch-1")
    second = dict(source, source_batch_id="batch-2")
    subject.ingest(event, source_record=first, source_job_id="job", occurrence_count=10)
    subject.ingest(event, source_record=second, source_job_id="job", occurrence_count=20)
    subject.ingest(event, source_record=second, source_job_id="job", occurrence_count=20)

    assert subject.get_node("prod-a", "gpu-node-01")["statistics"]["occurrence_count_24h"] == 30


def test_physical_contribution_revision_replaces_count_and_old_replay_stays_superseded(tmp_path):
    import pytest
    from logrisk.node_risk import NodeRiskError
    semantics,subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus",line_id="item")
    source.update(source_namespace="immutable-file",source_batch_id="one",semantic_revision="v1")
    event = semantics.match(source)
    subject.ingest(event,source_record=source,source_job_id="analysis-1",occurrence_count=10)
    other = dict(source,source_batch_id="two")
    subject.ingest(event,source_record=other,source_job_id="analysis-1",occurrence_count=20)
    changed = dict(source,semantic_revision="v2",expected_current_revision="v1")
    subject.ingest(event,source_record=changed,source_job_id="analysis-2",occurrence_count=12)
    subject.ingest(event,source_record=source,source_job_id="analysis-3",occurrence_count=10)
    assert subject.get_node("prod-a","gpu-node-01")["statistics"]["occurrence_count_24h"] == 32
    with pytest.raises(NodeRiskError,match="修订"):
        subject.ingest(event,source_record=dict(source,semantic_revision="v3",expected_current_revision="v1"),occurrence_count=99)
    assert subject.get_node("prod-a","gpu-node-01")["statistics"]["occurrence_count_24h"] == 32


def test_revision_changes_risk_type_without_leaving_old_current_risk(tmp_path):
    semantics,subject = services(tmp_path)
    source = record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus",line_id="item")
    source.update(source_namespace="source",source_batch_id="one",semantic_revision="v1")
    event = semantics.match(source)
    subject.ingest(event,source_record=source,occurrence_count=10)
    changed_event = dict(event,risk_type="gpu.reclassified",severity="medium",base_score=40)
    subject.ingest(changed_event,source_record=dict(source,semantic_revision="v2",expected_current_revision="v1"),occurrence_count=10)
    details = subject.get_node("prod-a","gpu-node-01")
    assert details["statistics"]["occurrence_count_24h"] == 10
    assert details["statistics"]["active_event_count"] == 1
    assert any(item["status"] == "superseded" for item in details["events"])
