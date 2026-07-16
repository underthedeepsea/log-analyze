from __future__ import annotations

from logrisk.ai_harness.prompt_registry import SQLitePromptRegistry
from logrisk.database import SQLiteDatabase


def test_sqlite_prompt_registry_seeds_files_and_keeps_version_history(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature.md").write_text("first prompt", encoding="utf-8")
    config = tmp_path / "ai.yaml"
    config.write_text(
        "defaults:\n  feature_extract: feature\nprompts:\n  - prompt_id: feature\n    display_name: Feature\n    analysis_type: feature_extract\n    is_default: true\n",
        encoding="utf-8",
    )
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    registry = SQLitePromptRegistry(database, prompt_dir, config)

    assert registry.get_default("feature_extract").content == "first prompt"
    updated = registry.update("feature", "second prompt", "优化格式")

    assert updated.content == "second prompt"
    assert registry.history("feature")[0]["content"] == "first prompt"
    assert (prompt_dir / "feature.md").read_text(encoding="utf-8") == "first prompt"
    assert SQLitePromptRegistry(database, prompt_dir, config).load("feature").content == "second prompt"
