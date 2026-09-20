from __future__ import annotations

import json
from pathlib import Path

from logrisk.database import SQLiteDatabase
from scripts.backfill_operational_ledgers import run_backfill


def _legacy_rows(database: SQLiteDatabase) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, completed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                "legacy-job",
                "completed",
                json.dumps({"input_job_id": "legacy-input", "summary": {"input_count": 7}}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:03:00+00:00",
                "2026-01-01T00:03:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, completed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                "failed-job",
                "failed",
                json.dumps({"summary": {"input_count": 5}}),
                "2026-01-01T00:00:00+00:00",
                None,
                "2026-01-01T00:02:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO input_jobs(input_job_id, status, stage, job_json, progress_json, result_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-input",
                "completed",
                "done",
                "{}",
                "{}",
                json.dumps({"summary": {"input_count": 7}}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:03:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO ai_traces(trace_id, job_id, provider, model, status, trace_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-trace",
                "legacy-job",
                "fake",
                "model",
                "success",
                json.dumps(
                    {
                        "physical_call_id": "legacy-call",
                        "logical_call_id": "legacy-logical",
                        "attempt_index": 0,
                        "usage": {"input_tokens": 3, "output_tokens": 4},
                    }
                ),
                "2026-01-01T00:02:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO ai_traces(trace_id, job_id, provider, model, status, trace_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "cache-trace",
                "legacy-job",
                "fake",
                "model",
                "cache_hit",
                "{}",
                "2026-01-01T00:02:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO ai_traces(trace_id, job_id, provider, model, status, trace_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "ambiguous-trace",
                "legacy-job",
                "fake",
                "model",
                "success",
                "{}",
                "2026-01-01T00:02:00+00:00",
            ),
        )


def test_backfill_is_dry_run_then_public_idempotent_apply(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "backfill.sqlite3")
    _legacy_rows(database)
    dry_run = run_backfill(database)
    assert dry_run["mode"] == "dry-run"
    assert dry_run["planned"] == 3
    assert dry_run["reasons"]["cache_excluded"] == 1
    assert dry_run["reasons"]["ambiguous_trace_attempt"] == 1
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM operational_analysis_runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM operational_backfill_items").fetchone()[0] == 0

    applied = run_backfill(database, apply=True)
    assert applied["applied"] == 3  # two roots and one confirmed physical trace
    with database.connect() as connection:
        root = connection.execute(
            "SELECT status, provenance, input_count, reported_input_count FROM operational_analysis_runs WHERE input_job_id=?",
            ("legacy-input",),
        ).fetchone()
        assert tuple(root) == ("completed", "reported", None, 7)
        failed = connection.execute(
            "SELECT status FROM operational_analysis_runs WHERE input_job_id IS NULL"
        ).fetchone()
        assert failed[0] == "partial"
        member = connection.execute(
            "SELECT status FROM operational_analysis_members WHERE feature_job_id=?",
            ("failed-job",),
        ).fetchone()
        assert member[0] == "failed"
        assert connection.execute("SELECT COUNT(*) FROM operational_physical_calls").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM operational_backfill_items WHERE status='skipped'").fetchone()[0] == 3

    repeated = run_backfill(database, apply=True)
    assert repeated["applied"] == 0
    assert repeated["already_done"] == 6


def test_backfill_changed_legacy_source_is_reported_as_conflict(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "backfill.sqlite3")
    _legacy_rows(database)
    run_backfill(database, apply=True)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE feature_jobs SET job_json=? WHERE job_id=?",
            (json.dumps({"summary": {"input_count": 999}}), "legacy-job"),
        )
    result = run_backfill(database)
    assert result["conflicts"] == 2
    assert result["reasons"]["source_changed"] == 1
    assert result["reasons"]["backfill_receipt_conflict"] == 1
