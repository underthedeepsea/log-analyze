import hashlib

import pytest

from logrisk.ai_harness.prompt_registry import PromptRegistry, validate_feature_prompt_contract


def test_load_prompt_template_with_hash(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    prompt = prompt_dir / "feature_extract_v1.md"
    prompt.write_text("system prompt\n", encoding="utf-8")

    template = PromptRegistry(prompt_dir).load("feature_extract_v1")

    assert template.prompt_id == "feature_extract_v1"
    assert template.content == "system prompt\n"
    assert template.sha256 == hashlib.sha256(b"system prompt\n").hexdigest()
    assert template.path == str(prompt)


def test_missing_prompt_raises_file_not_found(tmp_path):
    try:
        PromptRegistry(tmp_path).load("missing")
    except FileNotFoundError as exc:
        assert "missing" in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_list_prompts_and_get_default_from_config(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v1.md").write_text("system prompt\n", encoding="utf-8")
    config = tmp_path / "ai_harness.yaml"
    config.write_text(
        """
defaults:
  feature_extract: feature_extract_v1
prompts:
  - prompt_id: feature_extract_v1
    display_name: 日志特征识别 v1
    description: 外置 Prompt
    analysis_type: feature_extract
    status: active
    is_default: true
    version: v1
""".strip(),
        encoding="utf-8",
    )

    registry = PromptRegistry(prompt_dir, config)
    prompts = registry.list_prompts()
    default = registry.get_default("feature_extract")

    assert prompts[0].display_name == "日志特征识别 v1"
    assert prompts[0].analysis_type == "feature_extract"
    assert prompts[0].is_default is True
    assert default.prompt_id == "feature_extract_v1"


def test_repo_default_uses_compact_strict_json_v3_prompt():
    registry = PromptRegistry("prompts", "configs/ai_harness.yaml")

    default = registry.get_default("feature_extract")
    prompts = {prompt.prompt_id: prompt for prompt in registry.list_prompts()}

    assert default.prompt_id == "feature_extract_v3_compact_strict_json_en"
    assert prompts["feature_extract_v3_compact_strict_json_en"].description == "compact strict JSON for 小参数模型"
    assert prompts["feature_extract_v2_compact_en"].description == "compact for 小参数模型"
    assert prompts["feature_extract_v2_strict_en"].description == "for 大参数模型"


def test_feature_prompt_contract_requires_single_coherent_anomaly_constraint():
    content = (
        "feature_type title summary importance template_hashes components tags selection_reason "
        "lowercase_snake_case"
    )

    with pytest.raises(ValueError, match="coherent abnormal pattern"):
        validate_feature_prompt_contract(content)


def test_all_repo_feature_prompts_declare_the_complete_output_contract():
    registry = PromptRegistry("prompts", "configs/ai_harness.yaml")
    required_fields = (
        "feature_type",
        "title",
        "summary",
        "importance",
        "template_hashes",
        "components",
        "tags",
        "selection_reason",
    )

    for prompt in registry.list_prompts():
        if prompt.analysis_type != "feature_extract":
            continue
        assert all(field in prompt.content for field in required_fields), prompt.prompt_id
        assert "lowercase_snake_case" in prompt.content, prompt.prompt_id
        assert "one coherent abnormal pattern" in prompt.content.lower(), prompt.prompt_id
        assert "different failure semantics" in prompt.content.lower(), prompt.prompt_id
        assert not (
            prompt.content.strip().startswith("```")
            and prompt.content.strip().endswith("```")
        ), prompt.prompt_id


def test_update_prompt_records_version_history(tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    old_prompt = (
        "old prompt lowercase_snake_case feature_type title summary importance "
        "template_hashes components tags selection_reason one coherent abnormal pattern "
        "different failure semantics"
    )
    new_prompt = (
        "new prompt lowercase_snake_case feature_type title summary importance "
        "template_hashes components tags selection_reason one coherent abnormal pattern "
        "different failure semantics"
    )
    (prompt_dir / "feature_extract_v1.md").write_text(old_prompt, encoding="utf-8")
    history = tmp_path / "state" / "prompt_versions.json"
    registry = PromptRegistry(prompt_dir, history_path=history)

    updated = registry.update("feature_extract_v1", new_prompt, note="人工编辑")
    versions = registry.history("feature_extract_v1")

    assert updated.content == new_prompt
    assert versions[0]["content"] == old_prompt
    assert versions[0]["note"] == "人工编辑"
    assert versions[0]["sha256"] == hashlib.sha256(old_prompt.encode()).hexdigest()
