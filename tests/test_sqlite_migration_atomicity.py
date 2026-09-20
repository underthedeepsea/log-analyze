from __future__ import annotations

import sqlite3

import pytest

from logrisk.database import SQLiteDatabase


def test_failed_migration_rolls_back_ddl_rows_and_version(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001_bad.sql").write_text(
        "CREATE TABLE partial_state(id INTEGER PRIMARY KEY, note TEXT);\n"
        "INSERT INTO partial_state VALUES (7, 'semi;colon');\n"
        "THIS IS NOT SQL;\n",
        encoding="utf-8",
    )
    path = tmp_path / "db.sqlite3"

    with pytest.raises(sqlite3.DatabaseError):
        SQLiteDatabase(path, migrations_dir=migrations)

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='partial_state'"
        ).fetchone() is None
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 0


def test_trigger_and_quoted_semicolon_are_complete_statements(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001_trigger.sql").write_text(
        "CREATE TABLE source(id INTEGER PRIMARY KEY, note TEXT);\n"
        "CREATE TABLE audit(note TEXT);\n"
        "CREATE TRIGGER source_insert AFTER INSERT ON source BEGIN\n"
        "  INSERT INTO audit(note) VALUES (NEW.note || ';seen');\n"
        "END;\n"
        "INSERT INTO source(note) VALUES ('a;b');\n",
        encoding="utf-8",
    )

    database = SQLiteDatabase(tmp_path / "db.sqlite3", migrations_dir=migrations)
    with database.connect() as connection:
        assert connection.execute("SELECT note FROM audit").fetchone()[0] == "a;b;seen"
