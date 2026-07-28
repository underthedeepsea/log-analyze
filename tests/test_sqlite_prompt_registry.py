from __future__ import annotations

import hashlib

import pytest

from logrisk.ai_harness.prompt_registry import SQLitePromptRegistry
from logrisk.database import SQLiteDatabase


def valid_feature_prompt(label: str) -> str:
    return (
        f"{label}\nReturn raw JSON only. feature_type must be lowercase_snake_case. "
        "Every feature must contain exactly: feature_type, title, summary, importance, "
        "template_hashes, components, tags, selection_reason."
    )


def test_sqlite_prompt_registry_seeds_files_and_keeps_version_history(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    first_prompt = valid_feature_prompt("first prompt")
    second_prompt = valid_feature_prompt("second prompt")
    (prompt_dir / "feature.md").write_text(first_prompt, encoding="utf-8")
    config = tmp_path / "ai.yaml"
    config.write_text(
        "defaults:\n  feature_extract: feature\nprompts:\n  - prompt_id: feature\n    display_name: Feature\n    analysis_type: feature_extract\n    is_default: true\n",
        encoding="utf-8",
    )
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    registry = SQLitePromptRegistry(database, prompt_dir, config)

    assert registry.get_default("feature_extract").content == first_prompt
    updated = registry.update("feature", second_prompt, "优化格式")

    assert updated.content == second_prompt
    assert registry.history("feature")[0]["content"] == first_prompt
    assert (prompt_dir / "feature.md").read_text(encoding="utf-8") == first_prompt
    assert SQLitePromptRegistry(database, prompt_dir, config).load("feature").content == second_prompt


def test_sqlite_registry_upgrades_legacy_prompt_contract_and_preserves_history(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    fixed_prompt = valid_feature_prompt("fixed prompt")
    (prompt_dir / "feature.md").write_text(fixed_prompt, encoding="utf-8")
    config = tmp_path / "ai.yaml"
    config.write_text(
        "defaults:\n  feature_extract: feature\nprompts:\n  - prompt_id: feature\n    analysis_type: feature_extract\n    is_default: true\n",
        encoding="utf-8",
    )
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    legacy = "feature_type title summary importance template_hashes components"
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO prompt_templates(prompt_id, analysis_type, status, is_default, current_version, created_at, updated_at) "
            "VALUES ('feature', 'feature_extract', 'active', 1, 1, 'now', 'now')"
        )
        connection.execute(
            "INSERT INTO prompt_versions(prompt_id, version, content, content_sha256, note, created_at) "
            "VALUES ('feature', 1, ?, ?, 'legacy', 'now')",
            (legacy, hashlib.sha256(legacy.encode()).hexdigest()),
        )

    registry = SQLitePromptRegistry(database, prompt_dir, config)

    assert registry.load("feature").content == fixed_prompt
    assert registry.load("feature").version == "v2"
    assert registry.history("feature")[0]["content"] == legacy
    with database.connect() as connection:
        note = connection.execute(
            "SELECT note FROM prompt_versions WHERE prompt_id='feature' AND version=2"
        ).fetchone()[0]
    assert note == "系统修复：补齐 8 字段输出契约"


def test_sqlite_registry_rejects_feature_prompt_missing_required_fields(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature.md").write_text(valid_feature_prompt("seed"), encoding="utf-8")
    config = tmp_path / "ai.yaml"
    config.write_text(
        "prompts:\n  - prompt_id: feature\n    analysis_type: feature_extract\n",
        encoding="utf-8",
    )
    registry = SQLitePromptRegistry(SQLiteDatabase(tmp_path / "logrisk.sqlite3"), prompt_dir, config)

    with pytest.raises(ValueError, match="tags.*selection_reason"):
        registry.update(
            "feature",
            "feature_type title summary importance template_hashes components",
        )


def test_sqlite_registry_loads_exact_historical_hash(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    content = valid_feature_prompt("first")
    (prompt_dir / "feature.md").write_text(content, encoding="utf-8")
    registry = SQLitePromptRegistry(
        SQLiteDatabase(tmp_path / "logrisk.sqlite3"),
        prompt_dir,
    )
    first = registry.load("feature")
    registry.update("feature", valid_feature_prompt("second"), "second")

    resolved = registry.load_by_hash("feature", first.sha256)

    assert resolved.content == content
    assert resolved.sha256 == first.sha256
