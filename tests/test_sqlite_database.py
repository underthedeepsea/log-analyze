from __future__ import annotations

import sqlite3
import shutil
import json
from pathlib import Path

from logrisk.approval_dedup import build_approval_key
from logrisk.approved_rules import RuleFormat, classify_rule
from logrisk.database import SQLiteDatabase
from logrisk.sqlite_stores import SQLiteApprovedRuleStore


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
        "approval_groups",
        "approval_group_candidates",
        "rule_versions",
        "rule_feedback",
        "rule_audit_events",
        "ai_traces",
        "upload_sessions",
        "drain_config_versions",
        "semantic_dictionary_versions",
        "risk_semantic_rules",
        "risk_semantic_rule_versions",
        "node_risk_events",
        "node_risk_daily",
        "node_risk_snapshots",
        "agent_runs",
        "agent_run_steps",
        "agent_tool_calls",
        "agent_artifacts",
        "agent_run_events",
        "agent_workflows",
        "agent_workflow_runs",
        "agent_workflow_nodes",
        "agent_workflow_events",
    } <= tables
    assert foreign_keys == 1
    assert journal_mode == "wal"


def test_database_migration_is_idempotent(tmp_path):
    path = tmp_path / "logrisk.sqlite3"
    SQLiteDatabase(path)
    SQLiteDatabase(path)

    with sqlite3.connect(path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]

    assert count == 21


def test_extension_provider_migration_upgrades_existing_connection_and_profile(tmp_path):
    migration = Path("database/migrations/0007_extension_model_provider.sql")
    assert migration.exists()
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for source in sorted(Path("database/migrations").glob("000[1-6]_*.sql")):
        shutil.copy(source, migrations / source.name)
    path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(path, migrations_dir=migrations)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, created_at, updated_at) "
            "VALUES ('ollama-local', 'Ollama', 'ollama', 'http://127.0.0.1:11434', 'now', 'now')"
        )
        connection.execute(
            "INSERT INTO model_profiles(profile_id, connection_id, model, display_name, profile_json, created_at, updated_at) "
            "VALUES ('profile-old', 'ollama-local', 'qwen', '旧画像', '{}', 'now', 'now')"
        )
    shutil.copy(migration, migrations / migration.name)

    SQLiteDatabase(path, migrations_dir=migrations)

    with sqlite3.connect(path) as connection:
        old_profile = connection.execute(
            "SELECT connection_id FROM model_profiles WHERE profile_id='profile-old'"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO provider_connections(connection_id, display_name, provider, base_url, adapter_id, credential_envs_json, extension_config_json, created_at, updated_at) "
            "VALUES ('internal-token', '内部', 'extension', 'https://internal.example', 'token_auth_template', '{}', '{}', 'now', 'now')"
        )
        fields = {row[1] for row in connection.execute("PRAGMA table_info(provider_connections)")}

    assert old_profile == "ollama-local"
    assert {"adapter_id", "credential_envs_json", "extension_config_json"} <= fields


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


def test_qwen_9b_profile_migration_seeds_existing_database_without_changing_default(tmp_path):
    migration = Path("database/migrations/0003_seed_qwen3_5_9b_mlx.sql")
    assert migration.exists()
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
            "INSERT INTO app_settings(setting_key, value_json, updated_at) "
            "VALUES ('default_model_profile_id', '\"qwen3_1_7b_fast\"', 'now')"
        )
    shutil.copy(migration, migrations / migration.name)

    SQLiteDatabase(path, migrations_dir=migrations)

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT model, profile_json FROM model_profiles WHERE profile_id='qwen3_5_9b_mlx'"
        ).fetchone()
        default_profile = connection.execute(
            "SELECT value_json FROM app_settings WHERE setting_key='default_model_profile_id'"
        ).fetchone()[0]
    assert row is not None
    assert row[0] == "qwen3.5:9b-mlx"
    assert '"context_window_tokens":262144' in row[1]
    assert '"max_output_tokens":2000' in row[1]
    assert default_profile == '"qwen3_1_7b_fast"'


def test_schema_dictionary_describes_rule_lifecycle_tables():
    schema = Path("database/schema.yaml").read_text(encoding="utf-8")

    assert "schema_version: 21" in schema
    assert 'candidate_job: "feature_candidates.(candidate_id, job_id) ON DELETE RESTRICT"' in schema
    assert "uq_feature_candidates_candidate_job(candidate_id, job_id)" in schema
    assert "rule_versions:" in schema
    assert "rule_feedback:" in schema
    assert "rule_audit_events:" in schema
    assert "current_version: 当前规则版本" in schema
    assert "adapter_id: 扩展适配器标识" in schema
    assert "streaming_tasks:" in schema
    assert "unknown_template_queue:" in schema
    assert "runtime_policies:" in schema
    assert "runtime_audit_events:" in schema
    assert "multi_source_observations:" in schema
    assert "multi_source_correlations:" in schema
    assert "release_validations:" in schema
    assert "orchestration_runs:" in schema
    assert "input_orchestration_runs:" in schema
    assert "agent_workflows:" in schema
    assert "agent_workflow_runs:" in schema


def test_rule_governance_migration_versions_existing_rules_with_lifecycle_defaults(tmp_path):
    migration = Path("database/migrations/0004_rule_lifecycle_governance.sql")
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    shutil.copy("database/migrations/0001_initial.sql", migrations / "0001_initial.sql")
    path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(path, migrations_dir=migrations)
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO approved_rules(rule_id, signature, feature_type, rule_json, approved_at, updated_at) "
            "VALUES ('rule-old', 'sig-old', 'kernel_error', ?, '2026-06-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00')",
            ('{"rule_id":"rule-old","signature":"sig-old","feature_type":"kernel_error"}',),
        )
    shutil.copy(migration, migrations / migration.name)

    SQLiteDatabase(path, migrations_dir=migrations)

    with sqlite3.connect(path) as connection:
        status, version, snapshot = connection.execute(
            "SELECT r.status, v.version, v.rule_json FROM approved_rules r JOIN rule_versions v USING(rule_id) "
            "WHERE r.rule_id='rule-old'"
        ).fetchone()
    assert status == "active"
    assert version == 1
    assert '"schema_version":"approved_rule_v2"' in snapshot
    assert '"status":"active"' in snapshot


def test_approved_rule_classification_migration_normalizes_only_unmarked_legacy_rows(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for source in sorted(Path("database/migrations").glob("*.sql")):
        if source.name != "0021_approved_rule_classification.sql":
            shutil.copy(source, migrations / source.name)
    path = tmp_path / "logrisk.sqlite3"
    database = SQLiteDatabase(path, migrations_dir=migrations)
    legacy = {
        "rule_id": "legacy-a",
        "signature": "sig-a",
        "feature_type": "kernel_error",
        "components": ["kernel"],
        "template_signatures": [{"template_hash": "hash-a", "category": "kernel"}],
    }
    valid_v2 = {
        "rule_id": "valid-b",
        "signature": "sig-b",
        "feature_type": "kernel_error",
        "components": ["kernel"],
        "template_signatures": [{"template_hash": "hash-b", "category": "kernel"}],
        "problem_code": "linux.memory.oom",
        "approval_key": build_approval_key("kernel_error", "linux.memory.oom", ["kernel"], []),
        "match_mode": "semantic",
        "status": "active",
        "current_version": 1,
        "schema_version": "approved_rule_v2",
    }
    malformed_v2 = {
        **valid_v2,
        "rule_id": "malformed-c",
        "signature": "sig-c",
        "approval_key": "appr-c",
        "match_mode": "unsupported",
        "schema_version": "approved_rule_v2",
    }
    with database.transaction() as connection:
        for rule in (legacy, valid_v2, malformed_v2):
            connection.execute(
                "INSERT INTO approved_rules(rule_id, signature, feature_type, rule_json, approved_at, updated_at, status, current_version, next_review_at, schema_version, problem_code, approval_key) "
                "VALUES (?, ?, ?, ?, ?, ?, 'active', 1, NULL, 'approved_rule_v2', ?, ?)",
                (
                    rule["rule_id"], rule["signature"], rule["feature_type"], json.dumps(rule),
                    "2026-07-01T00:00:00+00:00", "2026-07-01T00:00:00+00:00",
                    rule.get("problem_code"), rule.get("approval_key"),
                ),
            )
    shutil.copy("database/migrations/0021_approved_rule_classification.sql", migrations / "0021_approved_rule_classification.sql")

    upgraded = SQLiteDatabase(path, migrations_dir=migrations)

    with upgraded.connect() as connection:
        rows = connection.execute(
            "SELECT rule_id, schema_version, problem_code, approval_key, rule_json FROM approved_rules ORDER BY rule_id"
        ).fetchall()
    values = {row[0]: row for row in rows}
    assert tuple(values["legacy-a"][1:4]) == ("approved_rule_v1", None, None)
    assert json.loads(values["legacy-a"][4])["schema_version"] == "approved_rule_v1"
    assert tuple(values["valid-b"][1:4]) == ("approved_rule_v2", "linux.memory.oom", valid_v2["approval_key"])
    assert tuple(values["malformed-c"][1:4]) == ("approved_rule_v2", "linux.memory.oom", "appr-c")

    store = SQLiteApprovedRuleStore(upgraded)
    assert [item["rule_id"] for item in store.match_feature({
        **legacy,
        "schema_version": "approved_rule_v1",
    })] == ["legacy-a"]
    assert [item["rule_id"] for item in store.match_feature({
        "feature_type": "kernel_error",
        "problem_code": "linux.memory.oom",
        "components": ["kernel"],
        "source_templates": [{"template_hash": "hash-b", "category": "kernel"}],
    })] == ["valid-b"]
    classifications = {
        rule["rule_id"]: classify_rule(rule).kind
        for rule in store._read_locked()
    }
    assert classifications == {
        "legacy-a": RuleFormat.LEGACY_V1,
        "valid-b": RuleFormat.VALID_V2,
        "malformed-c": RuleFormat.MALFORMED_V2,
    }
