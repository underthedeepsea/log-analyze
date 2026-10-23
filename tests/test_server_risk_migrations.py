from __future__ import annotations

import shutil
from pathlib import Path
import pytest
from logrisk.database import SQLiteDatabase
from logrisk.ai_harness.model_profile import ModelProfileRegistry
from logrisk.server_risk.contracts import LING_PROFILE, LING_MODEL, digest


def old_database(tmp_path):
    migrations = tmp_path / "migrations"; migrations.mkdir()
    for source in Path("database/migrations").glob("*.sql"):
        if source.name < "0032": shutil.copyfile(source, migrations / source.name)
    db = SQLiteDatabase(tmp_path / "old.sqlite3", migrations_dir=migrations)
    return db, migrations


def test_empty_and_old_upgrade_preserve_seed_default_and_user_profile(tmp_path):
    from logrisk.ai_harness.connections import ConnectionStore
    db, migrations = old_database(tmp_path)
    connections = ConnectionStore(db); connections.seed_defaults("http://127.0.0.1:11434")
    profiles = ModelProfileRegistry(Path("configs/model_profiles.yaml"), database=db)
    # Existing explicit same-ID user choice must never be replaced by migration.
    with db.transaction() as conn:
        conn.execute("UPDATE model_profiles SET model='user-custom-ling',profile_json=json_set(profile_json,'$.model','user-custom-ling') WHERE profile_id=?", (LING_PROFILE,))
    with db.connect() as conn:
        before = digest([dict(r) for r in conn.execute("SELECT * FROM model_profiles ORDER BY profile_id")])
        old_tables = {t: digest([dict(r) for r in conn.execute("SELECT * FROM " + t)]) for t in ("node_risk_events", "node_risk_ingestions", "node_risk_score_samples")}
    shutil.copyfile(Path("database/migrations/0032_server_risk_intelligence.sql"), migrations / "0032_server_risk_intelligence.sql")
    db.migrate(); db.migrate()
    with db.connect() as conn:
        assert digest([dict(r) for r in conn.execute("SELECT * FROM model_profiles ORDER BY profile_id")]) == before
        assert {t: digest([dict(r) for r in conn.execute("SELECT * FROM " + t)]) for t in old_tables} == old_tables
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version='0032'").fetchone()[0] == 1
    empty = SQLiteDatabase(tmp_path / "empty.sqlite3")
    with empty.connect() as conn:
        assert len(conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'server_risk_%' AND type='table'").fetchall()) == 5


def test_old_database_missing_ling_gets_nondefault_seed(tmp_path):
    from logrisk.ai_harness.connections import ConnectionStore
    db, migrations = old_database(tmp_path)
    ConnectionStore(db).seed_defaults("http://127.0.0.1:11434"); ModelProfileRegistry(Path("configs/model_profiles.yaml"), database=db)
    with db.transaction() as conn:
        conn.execute("DELETE FROM model_profiles WHERE profile_id=?", (LING_PROFILE,))
    with db.connect() as conn:
        defaults = [dict(r) for r in conn.execute("SELECT * FROM app_settings")]
    shutil.copyfile(Path("database/migrations/0032_server_risk_intelligence.sql"), migrations / "0032_server_risk_intelligence.sql")
    db.migrate()
    with db.connect() as conn:
        assert conn.execute("SELECT model FROM model_profiles WHERE profile_id=?", (LING_PROFILE,)).fetchone()[0] == LING_MODEL
        assert [dict(r) for r in conn.execute("SELECT * FROM app_settings")] == defaults


def test_dictionary_contains_new_tables_and_both_provider_constraints():
    import yaml
    schema = yaml.safe_load(Path("database/schema.yaml").read_text())
    tables = ("server_risk_observations", "server_risk_projection_state", "server_risk_episodes", "server_risk_decisions", "server_risk_reports")
    assert all(table in schema["tables"] for table in tables)
    for provider in ("migrations", "postgres/migrations"):
        sql = Path("database/" + provider + "/0032_server_risk_intelligence.sql").read_text()
        assert all("CREATE TABLE " + table in sql for table in tables)
        assert "model_calls BETWEEN 0 AND 1" in sql and "WHERE lifecycle='open'" in sql
