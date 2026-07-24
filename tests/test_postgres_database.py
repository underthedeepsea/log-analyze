from __future__ import annotations

import os
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase, create_database, qmark_to_pyformat
from logrisk.database_config import DatabaseConnectionSettings, resolve_database_runtime


def test_qmark_converter_preserves_question_marks_in_sql_literals_and_comments():
    sql = "SELECT '?' AS literal, note FROM items WHERE id=? -- ?\nAND label='why?' /* ? */"

    assert qmark_to_pyformat(sql) == "SELECT '?' AS literal, note FROM items WHERE id=%s -- ?\nAND label='why?' /* ? */"


def test_database_factory_keeps_sqlite_as_default_runtime(tmp_path):
    database = create_database(provider="sqlite", sqlite_path=tmp_path / "logrisk.sqlite3", state_root=tmp_path)

    assert isinstance(database, SQLiteDatabase)
    assert database.provider == "sqlite"


def test_database_runtime_prefers_explicit_arguments_over_environment_and_saved_candidate(tmp_path, monkeypatch):
    candidate = DatabaseConnectionSettings(tmp_path / "database_connection.json")
    candidate.save({
        "provider": "postgres",
        "host": "candidate-db",
        "port": 5432,
        "database": "logrisk",
        "user": "logrisk",
        "sslmode": "require",
        "password_env": "CANDIDATE_PASSWORD",
    })
    monkeypatch.setenv("LOGRISK_DATABASE_PROVIDER", "postgres")
    monkeypatch.setenv("LOGRISK_DATABASE_URL", "postgresql://env-user:env-secret@env-db/logrisk")

    runtime = resolve_database_runtime(
        provider="sqlite",
        database_path=tmp_path / "explicit.sqlite3",
        settings=candidate,
    )

    assert runtime.provider == "sqlite"
    assert runtime.sqlite_path == tmp_path / "explicit.sqlite3"
    assert runtime.database_url is None


def test_database_url_cli_argument_selects_postgres_unless_cli_provider_overrides_it(tmp_path):
    runtime = resolve_database_runtime(
        database_url="postgresql://user:secret@db.example/logrisk",
        database_path=tmp_path / "local.sqlite3",
        environ={"LOGRISK_DATABASE_PROVIDER": "sqlite"},
    )

    assert runtime.provider == "postgres"
    assert runtime.source == "cli"


def test_database_settings_never_persist_or_expose_password(tmp_path, monkeypatch):
    settings = DatabaseConnectionSettings(tmp_path / "database_connection.json")
    monkeypatch.setenv("LOGRISK_POSTGRES_PASSWORD", "not-for-storage")

    saved = settings.save({
        "provider": "postgres",
        "host": "db.internal",
        "port": 5432,
        "database": "logrisk",
        "user": "app_user",
        "sslmode": "verify-full",
        "password_env": "LOGRISK_POSTGRES_PASSWORD",
        "password": "not-for-storage",
    })

    persisted = settings.path.read_text(encoding="utf-8")
    assert "not-for-storage" not in persisted
    assert "not-for-storage" not in repr(saved)
    assert saved["password_configured"] is True
    assert saved["host"] == "db.internal"


def test_postgres_candidate_requires_password_environment_value(tmp_path):
    settings = DatabaseConnectionSettings(tmp_path / "database_connection.json")
    settings.save({
        "provider": "postgres",
        "host": "db.internal",
        "database": "logrisk",
        "user": "app_user",
        "password_env": "MISSING_PG_PASSWORD",
    })

    with pytest.raises(ValueError, match="MISSING_PG_PASSWORD"):
        resolve_database_runtime(settings=settings, environ={})


def test_postgres_migrations_track_each_sqlite_schema_version_without_sqlite_only_syntax():
    sqlite_versions = [path.name for path in sorted(Path("database/migrations").glob("*.sql"))]
    postgres_root = Path("database/postgres/migrations")
    postgres_versions = [path.name for path in sorted(postgres_root.glob("*.sql"))]

    assert postgres_versions == sqlite_versions
    combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in postgres_root.glob("*.sql"))
    assert "autoincrement" not in combined
    assert "insert or ignore" not in combined
    assert "strftime(" not in combined
    assert " json_extract(" not in combined
    assert "jsonb" in combined
    assert "timestamptz" in combined
    extension_migration = (postgres_root / "0007_extension_model_provider.sql").read_text(encoding="utf-8")
    assert "extension" in extension_migration
    assert "credential_envs_json JSONB" in extension_migration


def test_postgres_dependency_is_optional_and_kept_out_of_default_requirements():
    default_requirements = Path("requirements.txt").read_text(encoding="utf-8")
    optional_requirements = Path("requirements-postgres.txt").read_text(encoding="utf-8")

    assert "psycopg" not in default_requirements
    assert "psycopg[binary]" in optional_requirements


def test_runtime_stores_do_not_use_sqlite_only_insert_or_or_rowid_ordering():
    runtime_files = [
        Path("src/logrisk/sqlite_stores.py"),
        Path("src/logrisk/legacy_import.py"),
        Path("src/logrisk/risk_semantics.py"),
        Path("src/logrisk/node_risk.py"),
        Path("src/logrisk/rule_governance.py"),
        Path("src/logrisk/ai_harness/model_profile.py"),
        Path("src/logrisk/benchmark_center/repository.py"),
    ]
    combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in runtime_files)

    assert "insert or ignore" not in combined
    assert "insert or replace" not in combined
    assert "order by rowid" not in combined
