from __future__ import annotations

import json
from copy import deepcopy

from logrisk.approved_rules import ApprovedRuleStore
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
        imported_rule = connection.execute(
            "SELECT status, current_version, schema_version FROM approved_rules WHERE rule_id='rule-1'"
        ).fetchone()
        assert tuple(imported_rule) == ("active", 1, "approved_rule_v1")
        assert connection.execute(
            "SELECT change_type FROM rule_versions WHERE rule_id='rule-1' AND version=1"
        ).fetchone()[0] == "legacy_import"
        assert connection.execute("SELECT COUNT(*) FROM ai_traces").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM legacy_imports").fetchone()[0] == 2
    assert first["records_imported"] == 2
    assert second["records_imported"] == 0


def test_legacy_importer_preserves_v1_valid_v2_and_skips_malformed_v2(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    seed = ApprovedRuleStore(tmp_path / "seed.json")
    valid_v2 = seed.upsert_feature({
        "feature_type": "kernel_error",
        "title": "内核错误",
        "summary": "检测到内核错误",
        "components": ["kernel"],
        "source_templates": [{"template_hash": "hash-kernel", "category": "kernel"}],
    })
    malformed_v2 = deepcopy(valid_v2)
    malformed_v2.update({"rule_id": "rule-malformed", "signature": "sig-malformed", "approval_key": "appr-corrupt"})
    explicit_v1 = {
        "rule_id": "rule-v1",
        "signature": "sig-v1",
        "feature_type": "kernel_error",
        "schema_version": "approved_rule_v1",
        "approved_at": "2026-07-01T00:00:00+00:00",
        "updated_at": "2026-07-01T00:00:00+00:00",
    }
    legacy = {
        "rule_id": "rule-legacy",
        "signature": "sig-legacy",
        "feature_type": "kernel_error",
        "approved_at": "2026-07-01T00:00:00+00:00",
        "updated_at": "2026-07-01T00:00:00+00:00",
    }
    (state / "approved_rules.json").write_text(json.dumps({
        "schema_version": "1.0",
        "rules": [legacy, explicit_v1, valid_v2, malformed_v2],
    }), encoding="utf-8")
    database = SQLiteDatabase(state / "logrisk.sqlite3")

    result = LegacyStateImporter(database, state).run()

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT rule_id, schema_version, problem_code, approval_key FROM approved_rules ORDER BY rule_id"
        ).fetchall()
    assert result["records_imported"] == 3
    assert {row[0]: tuple(row[1:]) for row in rows} == {
        "rule-legacy": ("approved_rule_v1", None, None),
        "rule-v1": ("approved_rule_v1", None, None),
        valid_v2["rule_id"]: ("approved_rule_v2", valid_v2["problem_code"], valid_v2["approval_key"]),
    }
    assert not any(row[0] == "rule-malformed" for row in rows)


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
