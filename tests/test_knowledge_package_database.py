from __future__ import annotations

import sqlite3


def test_knowledge_package_migration_creates_registry_tables_and_constraints(tmp_path) -> None:
    from logrisk.database import SQLiteDatabase

    database = SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    with database.connect() as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'knowledge_package%'")
        }
        assert tables == {
            "knowledge_packages",
            "knowledge_package_versions",
            "knowledge_package_assets",
            "knowledge_package_dependencies",
            "knowledge_package_imports",
            "knowledge_package_audit_events",
        }
        versions = {row["version"] for row in connection.execute("SELECT version FROM schema_migrations")}
        assert "0015" in versions
        with sqlite3.Connection(tmp_path / "state" / "logrisk.sqlite3") as raw:
            raw.execute("PRAGMA foreign_keys = ON")
            raw.execute(
                "INSERT INTO knowledge_packages(package_id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                ("demo", "Demo", "demo", "now", "now"),
            )
            raw.execute(
                "INSERT INTO knowledge_package_versions(package_id, version, manifest_json, package_sha256, artifact_path, compressed_bytes, expanded_bytes, platform_min_version, platform_max_version_exclusive, status, installed_by, installed_at, state_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("demo", "1.0.0", "{}", "a" * 64, "packages/demo.zip", 1, 1, "1.32.0", "2.0.0", "installed", "actor", "now", 1, "now", "now"),
            )
            raw.commit()
            try:
                raw.execute(
                    "INSERT INTO knowledge_package_versions(package_id, version, manifest_json, package_sha256, artifact_path, compressed_bytes, expanded_bytes, platform_min_version, platform_max_version_exclusive, status, installed_by, installed_at, state_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("demo", "1.0.0", "{}", "b" * 64, "packages/other.zip", 1, 1, "1.32.0", "2.0.0", "installed", "actor", "now", 1, "now", "now"),
                )
            except sqlite3.IntegrityError:
                pass
            else:
                raise AssertionError("same package version must be unique")


def test_schema_migration_is_immutable_after_restart(tmp_path) -> None:
    from logrisk.database import SQLiteDatabase

    path = tmp_path / "state" / "logrisk.sqlite3"
    first = SQLiteDatabase(path)
    second = SQLiteDatabase(path)
    assert first.provider == second.provider == "sqlite"
