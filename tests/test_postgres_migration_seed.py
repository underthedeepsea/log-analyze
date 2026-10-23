from __future__ import annotations

import os
import json
import sqlite3
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest

from logrisk.database import PostgresDatabase, SQLiteDatabase
from pipeline.database_migrate import MigrationError, SQLiteToPostgresMigration


@pytest.fixture
def migration_pair(tmp_path):
    base_url = os.getenv("LOGRISK_TEST_POSTGRES_URL")
    if not base_url:
        pytest.skip("LOGRISK_TEST_POSTGRES_URL required")
    psycopg = pytest.importorskip("psycopg")
    schema = "seed_" + uuid.uuid4().hex
    with psycopg.connect(base_url, autocommit=True) as connection:
        connection.execute('CREATE SCHEMA "' + schema + '"')
    parts = urlsplit(base_url)
    query = parse_qsl(parts.query)
    query.append(("options", "-csearch_path=" + schema))
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    source = tmp_path / "source.sqlite3"
    SQLiteDatabase(source)
    target = PostgresDatabase(url, state_root=tmp_path / "target")
    migration = SQLiteToPostgresMigration(source, url, state_root=tmp_path / "target")
    with sqlite3.connect(source) as connection:
        source_initial = connection.execute("SELECT singleton,generation FROM approval_projection_state").fetchall()
    target_initial = seed_rows(target)
    probe = {"source_initial": source_initial, "target_initial": target_initial}
    try:
        yield source, target, migration
    finally:
        probe["target_final"] = seed_rows(target)
        probe_dir = os.getenv("LOGRISK_TEST_MIGRATION_PROBES")
        if probe_dir:
            from pathlib import Path
            Path(probe_dir).mkdir(parents=True, exist_ok=True)
            Path(probe_dir, schema + ".json").write_text(json.dumps(probe, indent=2) + "\n")
        with psycopg.connect(base_url, autocommit=True) as connection:
            connection.execute('DROP SCHEMA "' + schema + '" CASCADE')


def seed_rows(target):
    with target.connect() as connection:
        return [dict(row) for row in connection.execute("SELECT * FROM approval_projection_state ORDER BY singleton").fetchall()]


def add_business(connection):
    connection.execute("INSERT INTO provider_connections(connection_id,display_name,provider,base_url,created_at,updated_at) "
                       "VALUES ('seed-probe','probe','ollama','http://127.0.0.1:1','2026-10-23T00:00:00Z','2026-10-23T00:00:00Z')")


@pytest.mark.parametrize("generation", [None, "canonical-v1", "custom-legitimate-generation"])
def test_fresh_seed_preserves_exact_source_state(migration_pair, generation):
    source, target, migration = migration_pair
    with sqlite3.connect(source) as connection:
        original = connection.execute("SELECT generation FROM approval_projection_state WHERE singleton=1").fetchone()[0]
        if generation is not None:
            connection.execute("UPDATE approval_projection_state SET generation=? WHERE singleton=1", (generation,))
    expected = generation or original
    if generation is None:
        assert uuid.UUID(expected).hex == expected
        assert uuid.UUID(expected).version == 4
    report = migration.execute()
    probe_dir = os.getenv("LOGRISK_TEST_MIGRATION_PROBES")
    if probe_dir:
        from pathlib import Path
        Path(probe_dir).mkdir(parents=True, exist_ok=True)
        Path(probe_dir, "verification-" + str(generation) + ".json").write_text(json.dumps(report["verification"], indent=2) + "\n")
    check = report["verification"]["tables"]["approval_projection_state"]
    assert check["valid"] and check["primary_key_match"]
    assert check["source_count"] == check["target_count"] == 1
    assert check["source_digest"] == check["target_digest"]
    assert seed_rows(target) == [{"singleton": 1, "generation": expected}]
    assert migration.verify()["verification"]["valid"]


@pytest.mark.parametrize("existing", ["business", "custom-state", "extra-state"])
def test_nonempty_target_is_rejected_without_mutation(migration_pair, existing):
    source, target, migration = migration_pair
    with target.transaction() as connection:
        if existing == "business":
            add_business(connection)
        elif existing == "custom-state":
            connection.execute("UPDATE approval_projection_state SET generation='do-not-replace'")
        else:
            connection.execute("INSERT INTO approval_projection_state VALUES (2,'canonical-v1')")
    before = seed_rows(target)
    with pytest.raises(MigrationError, match="目标非空"):
        migration.execute()
    assert seed_rows(target) == before
    with target.connect() as connection:
        assert connection.execute("SELECT COUNT(*) AS n FROM provider_connections").fetchone()["n"] == (1 if existing == "business" else 0)


@pytest.mark.parametrize("failure", ["copy", "verification", "foreign-key"])
def test_seed_cleanup_and_copied_rows_roll_back(migration_pair, monkeypatch, failure):
    source, target, migration = migration_pair
    before = seed_rows(target)
    assert len(before) == 1 and before[0]["singleton"] == 1
    assert uuid.UUID(before[0]["generation"]).hex == before[0]["generation"]
    with sqlite3.connect(source) as connection:
        add_business(connection)
        if failure == "foreign-key":
            connection.execute("INSERT INTO model_profiles(profile_id,connection_id,model,display_name,profile_json,created_at,updated_at) "
                               "VALUES ('broken','missing-parent','model','probe','{}','2026-10-23T00:00:00Z','2026-10-23T00:00:00Z')")
    if failure == "copy":
        original = migration._copy_table
        observed_cleanup = []
        def fail_copy(source_connection, target_connection, table):
            if not observed_cleanup:
                assert not seed_rows_in_transaction(target_connection)
                observed_cleanup.append(True)
            if table == "provider_connections":
                original(source_connection, target_connection, table)
                raise RuntimeError("injected copy failure after write")
            return original(source_connection, target_connection, table)
        monkeypatch.setattr(migration, "_copy_table", fail_copy)
    elif failure == "verification":
        original_verify = migration._verify
        def fail_verify(*args):
            result = original_verify(*args)
            assert result["valid"]
            return {**result, "valid": False, "failed_tables": ["injected-verification"]}
        monkeypatch.setattr(migration, "_verify", fail_verify)
    psycopg = pytest.importorskip("psycopg")
    expected = {"copy": RuntimeError, "verification": MigrationError, "foreign-key": psycopg.errors.ForeignKeyViolation}[failure]
    with pytest.raises(expected):
        migration.execute()
    assert seed_rows(target) == before
    if failure == "copy":
        assert observed_cleanup == [True]
    with target.connect() as connection:
        assert connection.execute("SELECT COUNT(*) AS n FROM provider_connections").fetchone()["n"] == 0
        assert connection.execute("SELECT COUNT(*) AS n FROM model_profiles").fetchone()["n"] == 0


def seed_rows_in_transaction(connection):
    return connection.execute("SELECT * FROM approval_projection_state").fetchall()
