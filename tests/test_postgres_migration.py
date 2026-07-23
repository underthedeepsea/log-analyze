from __future__ import annotations

from pathlib import Path
import os
import uuid
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import pytest

from logrisk.database import SQLiteDatabase
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
