import pytest

from logrisk.feature_jobs import FeatureJobError, FeatureJobManager, validate_result_document


def entity(entity_id, score):
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": entity_id,
        "risk_score": score,
        "risk_level": "high" if score >= 70 else "medium",
        "top_templates": [],
        "affected_entities": [],
    }


def candidate(source, title="候选特征"):
    return {
        "candidate_id": f"feature-{source['entity_id']}",
        "status": "pending",
        "reviewer_note": "",
        "approved_at": None,
        "cluster": source["cluster"],
        "entity": {"type": source["entity_type"], "id": source["entity_id"]},
        "window_start": source["window_start"],
        "window_end": source["window_end"],
        "risk_score": source["risk_score"],
        "risk_level": source["risk_level"],
        "feature_type": "log_pattern",
        "title": title,
        "summary": "摘要",
        "importance": "high",
        "template_hashes": ["hash"],
        "components": ["kernel"],
        "tags": ["oom"],
        "selection_reason": "重要异常模板",
        "occurrence_count": 2,
        "time_range": {"first_seen": source["window_start"], "last_seen": source["window_end"]},
        "affected_entities": [],
        "source_templates": [{"template_hash": "hash", "template": "OOM", "count": 2}],
        "provider": "ollama",
        "model": "qwen3:1.7b",
    }


def document():
    return {
        "summary": {"total_raw_logs": 10, "total_risk_entities": 3},
        "risk_entities": [entity("node-low", 20), entity("node-high", 90), entity("node-mid", 50)],
    }


def test_validate_result_document_requires_risk_entities():
    with pytest.raises(FeatureJobError, match="risk_entities"):
        validate_result_document({"summary": {}})


def test_job_processes_eligible_entities_serially_by_score_and_continues_failure():
    calls = []

    def extractor(source, **kwargs):
        calls.append(source["entity_id"])
        if source["entity_id"] == "node-high":
            raise RuntimeError("model failed")
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(document(), model="qwen3:1.7b", min_score=40)

    manager.run_job(job_id)
    snapshot = manager.get_job(job_id)

    assert calls == ["node-high", "node-mid"]
    assert snapshot["status"] == "completed_with_errors"
    assert snapshot["progress"] == {"total": 2, "completed": 1, "failed": 1, "percent": 100}
    states = {item["entity_id"]: item["status"] for item in snapshot["entities"]}
    assert states == {"node-high": "failed", "node-mid": "completed", "node-low": "skipped"}
    assert snapshot["features"][0]["entity"]["id"] == "node-mid"
    events, cursor = manager.wait_for_events(job_id, 0, timeout=0)
    assert cursor == len(events)
    assert [event["type"] for event in events][-1] == "job_completed"


def test_failed_entity_can_be_retried():
    attempts = {"count": 0}

    def extractor(source, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary")
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    result = {"summary": {}, "risk_entities": [entity("node-a", 90)]}
    job_id = manager.create_job(result, model="qwen3:1.7b")
    manager.run_job(job_id)

    manager.retry_entity(job_id, "node-a", start=False)
    manager.run_job(job_id, only_entity_id="node-a")

    snapshot = manager.get_job(job_id)
    assert snapshot["entities"][0]["status"] == "completed"
    assert snapshot["features"][0]["candidate_id"] == "feature-node-a"


def test_review_edit_and_export_only_approved_features():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [candidate(source)], auto_start=False)
    result = {"summary": {"total_raw_logs": 1}, "risk_entities": [entity("node-a", 90)]}
    job_id = manager.create_job(result, model="qwen3:1.7b")
    manager.run_job(job_id)

    updated = manager.update_feature(job_id, "feature-node-a", {
        "title": "人工修订标题",
        "summary": "人工确认后的摘要",
        "importance": "critical",
        "tags": ["oom", "approved"],
        "reviewer_note": "已核对模板",
        "status": "approved",
    })
    package = manager.export_approved(job_id)

    assert updated["status"] == "approved"
    assert updated["approved_at"]
    assert package["schema_version"] == "1.0"
    assert package["review_statistics"] == {"total": 1, "approved": 1, "rejected": 0, "pending": 0}
    assert package["approved_features"][0]["title"] == "人工修订标题"
    assert package["approved_features"][0]["reviewer_note"] == "已核对模板"
    assert "root_cause_candidate" not in package["approved_features"][0]


def test_export_requires_an_approved_feature():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [candidate(source)], auto_start=False)
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    with pytest.raises(FeatureJobError, match="至少批准"):
        manager.export_approved(job_id)
