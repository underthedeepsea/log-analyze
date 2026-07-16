from __future__ import annotations

import json

from logrisk.database import SQLiteDatabase
from logrisk.legacy_import import LegacyStateImporter


def test_legacy_importer_imports_core_files_once(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "approved_rules.json").write_text(json.dumps({
        "schema_version": "1.0",
        "rules": [{
            "rule_id": "rule-1", "signature": "sig", "feature_type": "kernel_error",
            "approved_at": "2026-07-01T00:00:00+00:00", "updated_at": "2026-07-01T00:00:00+00:00",
        }],
    }), encoding="utf-8")
    (state / "ai_traces.jsonl").write_text(json.dumps({
        "trace_id": "trace-1", "provider": "ollama", "status": "success", "created_at": "2026-07-01T00:00:00+00:00",
    }) + "\n", encoding="utf-8")
    database = SQLiteDatabase(state / "logrisk.sqlite3")
    importer = LegacyStateImporter(database, state)

    first = importer.run()
    second = importer.run()

    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM approved_rules").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM ai_traces").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM legacy_imports").fetchone()[0] == 2
    assert first["records_imported"] == 2
    assert second["records_imported"] == 0


def test_legacy_importer_keeps_orphan_upload_id_only_in_job_snapshot(tmp_path):
    state = tmp_path / "state"
    jobs = tmp_path / "output" / "uploads"
    job_dir = jobs / "input_job_1"
    state.mkdir()
    job_dir.mkdir(parents=True)
    (job_dir / "job.json").write_text(json.dumps({
        "input_job_id": "input_job_1",
        "upload_id": "missing-upload",
        "status": "completed",
        "stage": "completed",
        "created_at": "2026-07-01T00:00:00+00:00",
    }), encoding="utf-8")
    database = SQLiteDatabase(state / "logrisk.sqlite3")

    result = LegacyStateImporter(database, state, jobs).run()

    with database.connect() as connection:
        row = connection.execute("SELECT upload_id, job_json FROM input_jobs WHERE input_job_id='input_job_1'").fetchone()
    assert result["records_imported"] == 1
    assert row["upload_id"] is None
    assert json.loads(row["job_json"])["upload_id"] == "missing-upload"
