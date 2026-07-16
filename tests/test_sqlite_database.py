from __future__ import annotations

import sqlite3
import shutil

from logrisk.database import SQLiteDatabase


def test_database_applies_migrations_and_enables_safety_pragmas(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")

    with database.connect() as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]

    assert {
        "schema_migrations",
        "legacy_imports",
        "provider_connections",
        "model_profiles",
        "prompt_templates",
        "feature_jobs",
        "approved_rules",
        "ai_traces",
        "upload_sessions",
        "drain_config_versions",
        "semantic_dictionary_versions",
    } <= tables
    assert foreign_keys == 1
    assert journal_mode == "wal"


def test_database_migration_is_idempotent(tmp_path):
    path = tmp_path / "logrisk.sqlite3"
    SQLiteDatabase(path)
    SQLiteDatabase(path)

    with sqlite3.connect(path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]

    assert count == 2


def test_output_budget_migration_updates_old_builtin_profile_and_removes_derived_options(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    shutil.copy("database/migrations/0001_initial.sql", migrations / "0001_initial.sql")
    path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(path, migrations_dir=migrations)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, created_at, updated_at) "
            "VALUES ('ollama-local', 'Ollama', 'ollama', 'http://127.0.0.1:11434', 'now', 'now')"
        )
        connection.execute(
            "INSERT INTO model_profiles(profile_id, connection_id, model, display_name, structured_output_mode, profile_json, created_at, updated_at) "
            "VALUES ('qwen3_5_4b_mlx', 'ollama-local', 'qwen3.5:4b-mlx', '4B', 'json_schema', ?, 'now', 'now')",
            ('{"profile_id":"qwen3_5_4b_mlx","provider":"ollama","connection_id":"ollama-local","model":"qwen3.5:4b-mlx","max_output_tokens":900,"default_prompt_id":"p","options":{"temperature":0,"num_predict":900,"think":false,"structured_output_mode":"json_schema"}}',),
        )
    shutil.copy("database/migrations/0002_profile_output_budget.sql", migrations / "0002_profile_output_budget.sql")

    SQLiteDatabase(path, migrations_dir=migrations)

    with sqlite3.connect(path) as connection:
        profile_json = connection.execute(
            "SELECT profile_json FROM model_profiles WHERE profile_id='qwen3_5_4b_mlx'"
        ).fetchone()[0]
        max_output, num_predict = connection.execute(
            "SELECT json_extract(?, '$.max_output_tokens'), json_extract(?, '$.options.num_predict')",
            (profile_json, profile_json),
        ).fetchone()
    assert max_output == 1600
    assert num_predict is None
