from __future__ import annotations

from datetime import date

from logrisk.database import SQLiteDatabase
from logrisk.sqlite_stores import (
    SQLiteAICache,
    SQLiteAITraceLogger,
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
    assert SQLiteApprovedRuleStore(database).list_rules()[0]["rule_id"] == saved["rule_id"]


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
