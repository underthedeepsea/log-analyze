from __future__ import annotations

from pathlib import Path
import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import pytest

from logrisk.continuous_learning import ContinuousLearningRepository
from logrisk.database import PostgresDatabase, SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.streaming_state import StreamingStateRepository
from pipeline.database_migrate import SQLiteToPostgresMigration, _digest_rows, build_migration_preview, parse_args


def test_sqlite_to_postgres_preview_orders_parent_tables_and_reports_missing_artifacts(tmp_path):
    source = tmp_path / "source.sqlite3"
    database = SQLiteDatabase(source)
    missing_path = tmp_path / "not-copied.log"
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, created_at, updated_at) "
            "VALUES ('ollama-local', 'Ollama', 'ollama', 'http://127.0.0.1:11434', '2026-07-23T00:00:00+00:00', '2026-07-23T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO model_profiles(profile_id, connection_id, model, display_name, profile_json, created_at, updated_at) "
            "VALUES ('profile-a', 'ollama-local', 'qwen', 'Qwen', '{}', '2026-07-23T00:00:00+00:00', '2026-07-23T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO artifacts(artifact_id, owner_type, owner_id, artifact_type, path, created_at) "
            "VALUES ('artifact-a', 'job', 'job-a', 'result', ?, '2026-07-23T00:00:00+00:00')",
            (str(missing_path),),
        )

    preview = build_migration_preview(source)

    assert preview["mode"] == "metadata_only"
    assert preview["tables"].index("provider_connections") < preview["tables"].index("model_profiles")
    assert preview["counts"]["provider_connections"] == 1
    assert preview["artifact_paths"]["checked"] == 1
    assert preview["artifact_paths"]["missing"] == [str(missing_path)]
    assert "schema_migrations" not in preview["tables"]


def test_postgres_migration_cli_requires_one_explicit_mode(tmp_path):
    args = parse_args(["--source-sqlite", str(tmp_path / "source.sqlite3"), "--dry-run"])

    assert args.dry_run is True
    assert args.execute is False
    assert args.verify is False


def test_migration_digest_normalizes_json_and_boolean_values_across_providers():
    sqlite_rows = [{"enabled": True, "payload": '{"nested":true}', "created_at": "2026-07-23T00:00:00Z"}]
    postgres_rows = [{"enabled": True, "payload": {"nested": True}, "created_at": "2026-07-23T00:00:00+00:00"}]

    assert _digest_rows(sqlite_rows) == _digest_rows(postgres_rows)


def test_continuous_learning_migrations_define_the_same_metadata_contract():
    sqlite_sql = Path("database/migrations/0018_continuous_learning.sql").read_text(encoding="utf-8")
    postgres_sql = Path("database/postgres/migrations/0018_continuous_learning.sql").read_text(encoding="utf-8")

    for sql in (sqlite_sql, postgres_sql):
        assert "feature_candidate_feedback" in sql
        assert "dataset_family_id" in sql
        assert "revision_number" in sql
        assert "content_sha256" in sql
        assert "lifecycle_status" in sql
        assert "dataset_content_sha256" in sql
        assert "record_count" in sql
        assert "continuous_learning_feedback_v1" in sql


def test_continuous_learning_sqlite_migration_backfills_dataset_family_metadata(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    source = Path("database/migrations/0001_initial.sql")
    (migrations / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    database_path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(database_path, migrations_dir=migrations)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO drain_datasets(dataset_id, name, version, dataset_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "legacy-dataset",
                "Legacy",
                "1.0.0",
                '{"records": [{"record_id": "record-1"}]}',
                "2026-08-23T00:00:00+00:00",
                "2026-08-23T00:00:00+00:00",
            ),
        )

    migration = Path("database/migrations/0018_continuous_learning.sql")
    (migrations / migration.name).write_text(migration.read_text(encoding="utf-8"), encoding="utf-8")

    # Re-applying the already installed migration must leave the row intact.
    SQLiteDatabase(database_path, migrations_dir=migrations)
    with database.connect() as connection:
        row = connection.execute(
            "SELECT dataset_family_id, revision_number, content_sha256, record_count, lifecycle_status, schema_version "
            "FROM drain_datasets WHERE dataset_id=?",
            ("legacy-dataset",),
        ).fetchone()

    assert dict(row) == {
        "dataset_family_id": "legacy-dataset",
        "revision_number": 1,
        "content_sha256": hashlib.sha256(
            json.dumps([{"record_id": "record-1"}], ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "record_count": 1,
        "lifecycle_status": "approved",
        "schema_version": "drain_dataset_revision_v1",
    }


def test_candidate_feedback_lineage_migrations_have_matching_composite_contract():
    sqlite_sql = Path("database/migrations/0019_candidate_feedback_lineage.sql").read_text(encoding="utf-8")
    postgres_sql = Path("database/postgres/migrations/0019_candidate_feedback_lineage.sql").read_text(encoding="utf-8")

    for sql in (sqlite_sql, postgres_sql):
        normalized = " ".join(sql.split()).lower()
        assert "unique (candidate_id, job_id)" in normalized or "unique index" in normalized
        assert "foreign key (candidate_id, job_id)" in normalized
        assert "references feature_candidates(candidate_id, job_id)" in normalized
        assert "feature_candidate_feedback" in normalized

    assert "feature_candidate_feedback_legacy" in sqlite_sql
    assert "drop constraint" in postgres_sql.lower()


def test_sqlite_lineage_migration_preserves_legacy_feedback_and_same_job_idempotency(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for source in sorted(Path("database/migrations").glob("*.sql")):
        if source.name.startswith("0019_"):
            continue
        shutil.copy(source, migrations / source.name)

    path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(path, migrations_dir=migrations)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            ("legacy-job", "completed", "{}", "2026-08-23T00:00:00+00:00", "2026-08-23T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO feature_candidates(candidate_id, job_id, entity_id, status, candidate_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-candidate",
                "legacy-job",
                "entity-1",
                "pending",
                '{"candidate_id":"legacy-candidate"}',
                "2026-08-23T00:00:00+00:00",
                "2026-08-23T00:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO feature_candidate_feedback("
            "feedback_id, candidate_id, job_id, outcome, reason_code, note, actor, request_id, idempotency_key, created_at, schema_version"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-feedback",
                "legacy-candidate",
                "legacy-job",
                "rejected",
                "false_positive",
                "legacy decision",
                "reviewer-a",
                "request-legacy",
                "key-legacy",
                "2026-08-23T00:00:00+00:00",
                "continuous_learning_feedback_v1",
            ),
        )

    shutil.copy("database/migrations/0019_candidate_feedback_lineage.sql", migrations / "0019_candidate_feedback_lineage.sql")
    upgraded = SQLiteDatabase(path, migrations_dir=migrations)

    with upgraded.connect() as connection:
        row = connection.execute(
            "SELECT feedback_id, candidate_id, job_id, idempotency_key FROM feature_candidate_feedback"
        ).fetchone()
        foreign_keys = connection.execute("PRAGMA foreign_key_list(feature_candidate_feedback)").fetchall()
    assert dict(row) == {
        "feedback_id": "legacy-feedback",
        "candidate_id": "legacy-candidate",
        "job_id": "legacy-job",
        "idempotency_key": "key-legacy",
    }
    assert len({item[0] for item in foreign_keys if item[2] == "feature_candidates"}) == 1

    repeated = ContinuousLearningRepository(upgraded).append_feedback(
        candidate_id="legacy-candidate",
        job_id="legacy-job",
        outcome="approved",
        reason_code="validated_reuse",
        note="must remain idempotent",
        actor="reviewer-b",
        request_id="request-retry",
        idempotency_key="key-legacy",
    )
    assert repeated["feedback_id"] == "legacy-feedback"


def test_sqlite_lineage_migration_rolls_back_failed_copy_and_can_retry(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for source in sorted(Path("database/migrations").glob("*.sql")):
        if source.name.startswith("0019_"):
            continue
        shutil.copy(source, migrations / source.name)

    path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(path, migrations_dir=migrations)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            ("legacy-job-a", "completed", "{}", "2026-08-23T00:00:00+00:00", "2026-08-23T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            ("legacy-job-b", "completed", "{}", "2026-08-23T00:00:00+00:00", "2026-08-23T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO feature_candidates(candidate_id, job_id, entity_id, status, candidate_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-candidate-race",
                "legacy-job-a",
                "entity-1",
                "pending",
                '{"candidate_id":"legacy-candidate-race"}',
                "2026-08-23T00:00:00+00:00",
                "2026-08-23T00:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO feature_candidate_feedback("
            "feedback_id, candidate_id, job_id, outcome, reason_code, note, actor, request_id, idempotency_key, created_at, schema_version"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-feedback-race",
                "legacy-candidate-race",
                "legacy-job-b",
                "rejected",
                "false_positive",
                "legacy mismatched row",
                "reviewer-a",
                "request-race",
                "key-race",
                "2026-08-23T00:00:00+00:00",
                "continuous_learning_feedback_v1",
            ),
        )

    shutil.copy("database/migrations/0019_candidate_feedback_lineage.sql", migrations / "0019_candidate_feedback_lineage.sql")
    with pytest.raises(sqlite3.IntegrityError):
        SQLiteDatabase(path, migrations_dir=migrations)

    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        feedback = connection.execute(
            "SELECT feedback_id, candidate_id, job_id FROM feature_candidate_feedback"
        ).fetchone()
        applied = {row[0] for row in connection.execute("SELECT version FROM schema_migrations")}
    assert "feature_candidate_feedback" in tables
    assert "feature_candidate_feedback_legacy" not in tables
    assert feedback == ("legacy-feedback-race", "legacy-candidate-race", "legacy-job-b")
    assert "0019" not in applied

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE feature_candidate_feedback SET job_id=? WHERE feedback_id=?",
            ("legacy-job-a", "legacy-feedback-race"),
        )

    upgraded = SQLiteDatabase(path, migrations_dir=migrations)
    with upgraded.connect() as connection:
        feedback = connection.execute(
            "SELECT feedback_id, candidate_id, job_id FROM feature_candidate_feedback"
        ).fetchone()
        applied = {row[0] for row in connection.execute("SELECT version FROM schema_migrations")}
    assert tuple(feedback) == ("legacy-feedback-race", "legacy-candidate-race", "legacy-job-a")
    assert "0019" in applied


@pytest.mark.skipif(not os.getenv("LOGRISK_TEST_POSTGRES_URL"), reason="未设置 LOGRISK_TEST_POSTGRES_URL")
def test_optional_postgres_integration_migrates_json_and_foreign_keys(tmp_path):
    psycopg = pytest.importorskip("psycopg")
    source = tmp_path / "source.sqlite3"
    database = SQLiteDatabase(source)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, created_at, updated_at) "
            "VALUES ('ollama-local', 'Ollama', 'ollama', 'http://127.0.0.1:11434', '2026-07-23T00:00:00+00:00', '2026-07-23T00:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO model_profiles(profile_id, connection_id, model, display_name, profile_json, created_at, updated_at) "
            "VALUES ('profile-a', 'ollama-local', 'qwen', 'Qwen', '{\"nested\":true}', '2026-07-23T00:00:00+00:00', '2026-07-23T00:00:00+00:00')"
        )
    base_url = os.environ["LOGRISK_TEST_POSTGRES_URL"]
    schema = "logrisk_test_" + uuid.uuid4().hex[:12]
    parts = urlsplit(base_url)
    query = parse_qsl(parts.query, keep_blank_values=True) + [("options", "-c search_path=" + schema)]
    target_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment))
    with psycopg.connect(base_url, autocommit=True) as connection:
        connection.execute('CREATE SCHEMA "' + schema + '"')
    try:
        result = SQLiteToPostgresMigration(source, target_url, state_root=tmp_path / "state").execute()
        assert result["verification"]["valid"] is True
        assert result["verification"]["tables"]["model_profiles"]["primary_key_match"] is True
    finally:
        with psycopg.connect(base_url, autocommit=True) as connection:
            connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')


@pytest.mark.skipif(not os.getenv("LOGRISK_TEST_POSTGRES_URL"), reason="未设置 LOGRISK_TEST_POSTGRES_URL")
def test_optional_postgres_streaming_checkpoint_uses_jsonb_and_foreign_keys(tmp_path):
    psycopg = pytest.importorskip("psycopg")
    base_url = os.environ["LOGRISK_TEST_POSTGRES_URL"]
    schema = "logrisk_stream_" + uuid.uuid4().hex[:12]
    parts = urlsplit(base_url)
    query = parse_qsl(parts.query, keep_blank_values=True) + [("options", "-c search_path=" + schema)]
    target_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment))
    with psycopg.connect(base_url, autocommit=True) as connection:
        connection.execute('CREATE SCHEMA "' + schema + '"')
    try:
        database = PostgresDatabase(target_url, state_root=tmp_path / "state")
        path = tmp_path / "messages"
        path.write_text("error one\n", encoding="utf-8")
        repository = StreamingStateRepository(database)
        task = repository.create_or_load(
            descriptor=FileIncrementalSource(path, filename="messages").descriptor(),
            config_hash="d" * 64,
        )
        repository.commit_window(
            task["task_id"],
            window_id="file-offset:10",
            cursor=SourceCursor("file", {"offset": 10, "line": 2}),
            templates=[{
                "template_hash": "abc",
                "component": "kernel",
                "template": "error <*>",
                "count": 1,
                "window_start": "2026-07-27T00:00:00+00:00",
            }],
        )

        assert repository.get_task(task["task_id"])["cursor"]["value"]["offset"] == 10
        assert repository.list_unknown_templates(task_id=task["task_id"])[0]["template"]["template_hash"] == "abc"
    finally:
        with psycopg.connect(base_url, autocommit=True) as connection:
            connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
