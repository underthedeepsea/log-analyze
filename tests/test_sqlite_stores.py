from __future__ import annotations

from datetime import date

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.sqlite_stores import (
    SQLiteAICache,
    SQLiteAITraceLogger,
    SQLiteApprovalGroupStore,
    SQLiteApprovedRuleStore,
    SQLiteFeatureJobStore,
    SQLiteProcessingMetricsStore,
    SQLiteInputJobStore,
    SQLiteUploadSessionStore,
)
from logrisk.input_jobs import InputJobConfig
from logrisk.upload_sessions import UploadConfig


def test_sqlite_feature_jobs_round_trip_entities_candidates_and_events(tmp_path):
    store = SQLiteFeatureJobStore(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    job = {
        "job_id": "job-1",
        "status": "running",
        "model_profile_id": None,
        "created_at": "2026-07-16T00:00:00+00:00",
        "completed_at": None,
        "entities": [{"entity_id": "node-a", "status": "completed", "risk_score": 70}],
        "features": {"candidate-1": {"candidate_id": "candidate-1", "entity": {"id": "node-a"}, "status": "pending"}},
        "events": [{"sequence": 0, "type": "job_created", "timestamp": "2026-07-16T00:00:00+00:00"}],
    }

    store.save(job)
    loaded = store.load()[0]

    assert loaded["entities"][0]["entity_id"] == "node-a"
    assert loaded["features"]["candidate-1"]["status"] == "pending"
    assert loaded["events"][0]["type"] == "job_created"


def test_sqlite_feature_job_replace_preserves_continuous_learning_feedback(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    store = SQLiteFeatureJobStore(database)
    job = {
        "job_id": "job-feedback",
        "status": "completed",
        "model_profile_id": None,
        "created_at": "2026-07-16T00:00:00+00:00",
        "completed_at": "2026-07-16T00:01:00+00:00",
        "entities": [],
        "features": {"candidate-feedback": {"candidate_id": "candidate-feedback", "status": "pending"}},
        "events": [],
    }
    store.save(job)
    from logrisk.continuous_learning import ContinuousLearningRepository

    repository = ContinuousLearningRepository(database)
    repository.append_feedback(
        candidate_id="candidate-feedback",
        job_id="job-feedback",
        outcome="rejected",
        reason_code="false_positive",
        note="kept history",
        actor="reviewer-a",
        request_id="request-1",
        idempotency_key="feedback-1",
    )

    job["features"]["candidate-feedback"]["status"] = "rejected"
    store.save(job)

    history = repository.list_feedback(candidate_id="candidate-feedback")
    assert len(history) == 1
    assert history[0]["outcome"] == "rejected"


def test_sqlite_feature_job_store_rejects_reparenting_feedback_candidate(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    store = SQLiteFeatureJobStore(database)
    first_job = {
        "job_id": "job-lineage-a",
        "status": "completed",
        "model_profile_id": None,
        "created_at": "2026-07-16T00:00:00+00:00",
        "completed_at": "2026-07-16T00:01:00+00:00",
        "entities": [],
        "features": {"candidate-stable": {"candidate_id": "candidate-stable", "status": "pending"}},
        "events": [],
    }
    store.save(first_job)

    from logrisk.continuous_learning import ContinuousLearningRepository

    repository = ContinuousLearningRepository(database)
    repository.append_feedback(
        candidate_id="candidate-stable",
        job_id="job-lineage-a",
        outcome="rejected",
        reason_code="false_positive",
        note="lineage is fixed",
        actor="reviewer-a",
        request_id="request-lineage-a",
        idempotency_key="feedback-lineage-a",
    )

    second_job = {**first_job, "job_id": "job-lineage-b"}
    with pytest.raises(ValueError, match="feedback history"):
        store.save(second_job)

    with database.connect() as connection:
        candidate = connection.execute(
            "SELECT job_id FROM feature_candidates WHERE candidate_id=?", ("candidate-stable",)
        ).fetchone()
        feedback = connection.execute(
            "SELECT job_id FROM feature_candidate_feedback WHERE candidate_id=?", ("candidate-stable",)
        ).fetchone()
        assert candidate["job_id"] == "job-lineage-a"
        assert feedback["job_id"] == "job-lineage-a"


def test_sqlite_trace_cache_metrics_and_rules_survive_new_store_instances(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    traces = SQLiteAITraceLogger(database)
    traces.append({"trace_id": "trace-1", "job_id": "job-1", "provider": "ollama", "model": "qwen", "status": "success", "created_at": "2026-07-16T00:00:00+00:00"})
    assert SQLiteAITraceLogger(database).get_trace("trace-1")["provider"] == "ollama"

    cache = SQLiteAICache(database)
    cache.set("sig", {"features": []})
    assert SQLiteAICache(database).get("sig") == {"features": []}

    metrics = SQLiteProcessingMetricsStore(database, today=lambda: date(2026, 7, 16))
    assert metrics.add_llm_logs(3) == 3
    assert SQLiteProcessingMetricsStore(database, today=lambda: date(2026, 7, 16)).today_llm_logs() == 3

    rules = SQLiteApprovedRuleStore(database)
    feature = {
        "feature_type": "kernel_error",
        "title": "内核错误",
        "summary": "检测到内核错误",
        "importance": "high",
        "tags": ["内核"],
        "components": ["kernel"],
        "source_templates": [{"template_hash": "hash-1", "category": "kernel"}],
    }
    saved = rules.upsert_feature(feature)
    persisted = SQLiteApprovedRuleStore(database).list_rules()[0]

    assert persisted["rule_id"] == saved["rule_id"]
    assert persisted["schema_version"] == "approved_rule_v2"
    assert persisted["status"] == "active"
    assert persisted["current_version"] == 1
    with database.connect() as connection:
        version = connection.execute(
            "SELECT version, change_type FROM rule_versions WHERE rule_id=?",
            (saved["rule_id"],),
        ).fetchone()
    assert tuple(version) == (1, "rule_created")


def test_sqlite_rule_store_only_matches_active_rules(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    rules = SQLiteApprovedRuleStore(database)
    feature = {
        "feature_type": "kernel_error",
        "title": "内核错误",
        "summary": "检测到内核错误",
        "importance": "high",
        "tags": ["内核"],
        "components": ["kernel"],
        "source_templates": [{"template_hash": "hash-1", "category": "kernel"}],
    }
    saved = rules.upsert_feature(feature)
    entity = {
        "entity_id": "node-a",
        "cluster": "prod-a",
        "top_templates": [{"template_hash": "hash-1", "category": "kernel"}],
    }

    assert rules.match_entity(entity)[0]["rule_id"] == saved["rule_id"]
    with database.transaction() as connection:
        connection.execute(
            "UPDATE approved_rules SET status='disabled' WHERE rule_id=?",
            (saved["rule_id"],),
        )

    assert rules.match_entity(entity) == []


def test_sqlite_approval_group_round_trip_and_candidate_uniqueness(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    groups = SQLiteApprovalGroupStore(database)
    group = groups.save({
        "approval_group_id": "group-1",
        "approval_key": "appr-1",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "feature_type": "network_failure",
        "title": "CNI 网络配置失败",
        "summary": "摘要",
        "importance": "high",
        "status": "pending",
        "rule_id": None,
        "first_seen": "2026-07-16T00:00:00+00:00",
        "last_seen": "2026-07-16T00:05:00+00:00",
        "occurrence_count": 3,
        "affected_entity_count": 1,
        "candidate_count": 1,
        "candidate_ids": ["candidate-1"],
        "created_at": "2026-07-16T00:00:00+00:00",
        "updated_at": "2026-07-16T00:00:00+00:00",
    })
    groups.attach_candidate(group["approval_group_id"], "candidate-1", job_id="job-1", entity_id="node-a")

    restored = SQLiteApprovalGroupStore(database).get_by_key("appr-1")

    assert restored["approval_group_id"] == "group-1"
    assert restored["problem_code"] == "kubernetes.cni.ip_exhaustion"
    assert SQLiteApprovalGroupStore(database).has_candidate("candidate-1")
    assert SQLiteApprovalGroupStore(database).candidate_group_id("candidate-1") == "group-1"


def test_sqlite_upload_and_input_job_keep_metadata_in_database(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    uploads = SQLiteUploadSessionStore(UploadConfig(tmp_path / "uploads", chunk_size_bytes=4), database)
    manifest = uploads.create(filename="messages", size_bytes=5)
    uploads.append_chunk(upload_id=manifest["upload_id"], index=0, data=b"erro")
    uploads.append_chunk(upload_id=manifest["upload_id"], index=1, data=b"r")
    completed = uploads.complete(upload_id=manifest["upload_id"])

    jobs = SQLiteInputJobStore(InputJobConfig(tmp_path / "output"), database)
    job = jobs.create(
        upload_id=manifest["upload_id"],
        filename="messages",
        source_path=str(uploads.source_path(manifest["upload_id"])),
    )
    jobs.write_progress(job["input_job_id"], {"status": "running", "stage": "drain3", "progress": 0.5})
    jobs.write_result(job["input_job_id"], {"risk_entities": []})

    assert completed["status"] == "completed"
    assert not (tmp_path / "uploads" / manifest["upload_id"] / "manifest.json").exists()
    assert SQLiteInputJobStore(InputJobConfig(tmp_path / "output"), database).get_result(job["input_job_id"]) == {"risk_entities": []}


def test_sqlite_upload_metadata_uses_relative_shared_artifact_paths(tmp_path):
    from logrisk.artifact_storage import SharedArtifactStore

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    shared = SharedArtifactStore(tmp_path / "shared")
    uploads = SQLiteUploadSessionStore(
        UploadConfig(tmp_path / "work" / "uploads", chunk_size_bytes=4, artifact_store=shared),
        database,
    )
    manifest = uploads.create(filename="messages", size_bytes=4)
    uploads.append_chunk(upload_id=manifest["upload_id"], index=0, data=b"safe")
    uploads.complete(upload_id=manifest["upload_id"])

    jobs = SQLiteInputJobStore(InputJobConfig(tmp_path / "output", artifact_store=shared), database)
    job = jobs.create(
        upload_id=manifest["upload_id"],
        filename="messages",
        source_path=str(uploads.source_path(manifest["upload_id"])),
    )
    with database.connect() as connection:
        source_path = connection.execute(
            "SELECT source_path FROM upload_sessions WHERE upload_id=?", (manifest["upload_id"],)
        ).fetchone()[0]
        job_json = connection.execute(
            "SELECT job_json FROM input_jobs WHERE input_job_id=?", (job["input_job_id"],)
        ).fetchone()[0]

    expected = f"uploads/{manifest['upload_id']}/source.log"
    assert source_path == expected
    assert expected in job_json
    assert str(shared.root) not in job_json
