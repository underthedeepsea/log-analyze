from __future__ import annotations

import json
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.runtime.config import RuntimeConfig
from logrisk.runtime.repository import RuntimeRepository
from logrisk.runtime.service import RuntimeQuotaError, RuntimeService


@pytest.fixture
def database(tmp_path):
    return SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")


@pytest.fixture
def runtime_service(tmp_path, database):
    state_root = tmp_path / "state"
    output_root = tmp_path / "output"
    return RuntimeService(
        database,
        state_root=state_root,
        output_root=output_root,
        config=RuntimeConfig.from_mapping(
            {"retention": {"enabled": True, "completed_days": 1}, "quota": {"soft_limit_bytes": 100, "hard_limit_bytes": 100}}
        ),
    )


def _seed_task_rows(database) -> None:
    now = "2026-07-29T00:00:00+00:00"
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, model_profile_id, connection_snapshot_json, profile_snapshot_json, job_json, created_at, completed_at, updated_at) VALUES (?, ?, NULL, NULL, NULL, ?, ?, NULL, ?)",
            ("job-a", "running", json.dumps({"progress": 0.5}), now, now),
        )
        connection.execute(
            "INSERT INTO streaming_tasks(task_id, source_kind, source_identity_json, config_hash, status, stage, cursor_json, task_json, created_at, updated_at) VALUES (?, 'file', '{}', 'hash', 'completed', 'COMPLETED', '{}', '{}', ?, ?)",
            ("stream-a", now, now),
        )
        connection.execute(
            "INSERT INTO benchmark_suites(suite_id, name, source_type, case_count, suite_json, version, created_at, updated_at, schema_version) VALUES (?, ?, 'custom', 0, '{}', 1, ?, ?, 'benchmark_suite_v1')",
            ("suite-a", "suite", now, now),
        )
        connection.execute(
            "INSERT INTO benchmark_runs(run_id, suite_id, mode, status, idempotency_key, snapshot_json, metrics_json, progress_completed, progress_total, error, version, created_at, updated_at, schema_version) VALUES (?, ?, 'fake', 'completed', ?, '{}', '{}', 1, 1, NULL, 1, ?, ?, 'benchmark_run_v1')",
            ("benchmark-a", "suite-a", "key-a", now, now),
        )
        connection.execute(
            "INSERT INTO ai_traces(trace_id, job_id, provider, model, status, prompt_id, prompt_hash, latency_ms, trace_json, created_at) VALUES (?, NULL, 'mock', 'model', 'success', 'prompt', 'hash', 1, '{}', ?)",
            ("trace-a", now),
        )
        connection.execute(
            "INSERT INTO replay_runs(replay_id, observation_id, source_trace_id, mode, status, idempotency_key, snapshot_json, result_json, error_code, error_message, schema_version, created_at, updated_at, completed_at) VALUES (?, NULL, ?, 'historical', 'completed', ?, '{}', '{}', NULL, NULL, 'replay_v2', ?, ?, ?)",
            ("replay-a", "trace-a", "replay-key-a", now, now, now),
        )


def test_runtime_catalog_combines_existing_task_types(runtime_service, database) -> None:
    _seed_task_rows(database)

    items = runtime_service.list_tasks(page=1, page_size=20)["items"]

    assert {item["kind"] for item in items} >= {
        "feature_job",
        "streaming",
        "benchmark",
        "replay",
    }


def test_retention_dry_run_never_deletes_artifact(runtime_service, tmp_path) -> None:
    artifact = tmp_path / "output" / "uploads" / "expired.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("metadata only", encoding="utf-8")
    with runtime_service.database.transaction() as connection:
        connection.execute(
            "INSERT INTO artifacts(artifact_id, owner_type, owner_id, artifact_type, path, size_bytes, sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
            ("artifact-expired", "input_job", "input-a", "result", str(artifact), artifact.stat().st_size, "2020-01-01T00:00:00+00:00"),
        )

    report = runtime_service.preview_retention()

    assert str(artifact) in report["candidate_paths"]
    assert artifact.exists()


def test_retention_does_not_select_export_or_raw_source_artifacts(runtime_service, tmp_path) -> None:
    for artifact_type in ("export", "raw_source"):
        artifact = tmp_path / "output" / (artifact_type + ".json")
        artifact.write_text(artifact_type, encoding="utf-8")
        with runtime_service.database.transaction() as connection:
            connection.execute(
                "INSERT INTO artifacts(artifact_id, owner_type, owner_id, artifact_type, path, size_bytes, sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                ("artifact-" + artifact_type, "input_job", "input-a", artifact_type, str(artifact), artifact.stat().st_size, "2020-01-01T00:00:00+00:00"),
            )
    database_file = runtime_service.state_root / "logrisk.sqlite3"
    with runtime_service.database.transaction() as connection:
        connection.execute(
            "INSERT INTO artifacts(artifact_id, owner_type, owner_id, artifact_type, path, size_bytes, sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
            ("artifact-runtime-db", "input_job", "input-a", "result", str(database_file), database_file.stat().st_size, "2020-01-01T00:00:00+00:00"),
        )

    report = runtime_service.preview_retention()

    assert report["candidate_paths"] == []


def test_retention_execute_removes_expired_artifact_and_its_metadata(runtime_service, tmp_path) -> None:
    artifact = tmp_path / "output" / "uploads" / "expired-execute.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("metadata only", encoding="utf-8")
    with runtime_service.database.transaction() as connection:
        connection.execute(
            "INSERT INTO artifacts(artifact_id, owner_type, owner_id, artifact_type, path, size_bytes, sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
            ("artifact-expired-execute", "input_job", "input-a", "result", str(artifact), artifact.stat().st_size, "2020-01-01T00:00:00+00:00"),
        )

    completed = runtime_service.run_retention(actor="alice", request_id="runtime-retention-1", execute=True)

    assert completed["status"] == "completed"
    assert completed["summary"]["deleted_files"] == 1
    assert completed["summary"]["deleted_metadata"] == 1
    assert not artifact.exists()
    with runtime_service.database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM artifacts WHERE artifact_id=?", ("artifact-expired-execute",)).fetchone()[0] == 0


def test_quota_exceeded_rejects_new_capacity_consuming_operation(runtime_service) -> None:
    runtime_service.repository.record_quota_snapshot(
        {"total_bytes": 0, "hard_limit_bytes": 100}
    )
    oversized = runtime_service.output_root / "quota-filler.log"
    oversized.write_bytes(b"x" * 101)

    with pytest.raises(RuntimeQuotaError, match="存储配额"):
        runtime_service.require_capacity("upload")


def test_readiness_does_not_require_optional_model_connection(runtime_service) -> None:
    service = RuntimeService(
        runtime_service.database,
        state_root=runtime_service.state_root,
        output_root=runtime_service.output_root,
        config=RuntimeConfig.from_mapping({}),
    )
    readiness = service.readiness()

    assert readiness["ready"] is True
    assert readiness["dependencies"]["model_connections"]["status"] == "unknown"
    assert readiness["checks"]["migrations"] is True
