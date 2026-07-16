from __future__ import annotations

import sqlite3

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

    assert count == 1
