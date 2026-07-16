import pytest

from logrisk.ai_harness.connections import ConnectionStore
from logrisk.ai_harness.model_profile import ModelProfileRegistry
from logrisk.database import SQLiteDatabase


def test_model_profile_registry_loads_default_and_options(tmp_path):
    config = tmp_path / "model_profiles.yaml"
    config.write_text(
        """
default_profile_id: qwen3_1_7b_fast
profiles:
  qwen3_1_7b_fast:
    provider: ollama
    model: qwen3:1.7b
    display_name: Qwen3 1.7B
    parameter_size: 1.7b
    context_window_tokens: 8192
    recommended_input_tokens: 4500
    max_output_tokens: 1200
    default_prompt_id: feature_extract_v2_compact_en
    thinking:
      enabled: false
    evidence_budget:
      max_templates: 6
      max_template_chars: 220
      max_affected_entities: 20
      max_evidence_chars: 8000
    options:
      temperature: 0
  disabled:
    enabled: false
    provider: ollama
    model: qwen3:7b
    default_prompt_id: feature_extract_v2_strict_en
""",
        encoding="utf-8",
    )

    registry = ModelProfileRegistry(config)
    profile = registry.get()

    assert registry.default_profile_id == "qwen3_1_7b_fast"
    assert profile.model == "qwen3:1.7b"
    assert profile.evidence_budget.max_templates == 6
    assert profile.build_model_options()["think"] is False
    assert profile.build_model_options()["num_predict"] == 1200
    with pytest.raises(ValueError, match="disabled"):
        registry.get("disabled")


def test_project_model_profiles_are_loadable():
    registry = ModelProfileRegistry("configs/model_profiles.yaml")

    assert registry.get().profile_id == "qwen3_1_7b_fast"
    profiles = {item.profile_id: item for item in registry.list_enabled()}
    assert set(profiles) >= {
        "qwen3_5_4b_mlx",
        "qwen3_1_7b_fast",
        "qwen3_6_35b_a3b",
        "deepseek_v4_flash",
    }
    assert profiles["qwen3_5_4b_mlx"].model == "qwen3.5:4b-mlx"
    assert profiles["qwen3_5_4b_mlx"].build_model_options()["num_predict"] == 900
    assert profiles["deepseek_v4_flash"].model == "deepseek-v4:flash"


def test_model_profile_registry_can_save_new_profile(tmp_path):
    config = tmp_path / "model_profiles.yaml"
    config.write_text("default_profile_id: base\nprofiles:\n  base:\n    provider: ollama\n    model: qwen3:1.7b\n    default_prompt_id: feature_extract_v2_compact_en\n", encoding="utf-8")
    registry = ModelProfileRegistry(config)

    saved = registry.save({
        "profile_id": "custom_profile",
        "provider": "ollama",
        "model": "qwen3.5:4b-mlx",
        "display_name": "Custom",
        "default_prompt_id": "feature_extract_v2_compact_en",
        "thinking_enabled": False,
        "evidence_budget": {"max_templates": 5},
    })

    assert saved.profile_id == "custom_profile"
    assert registry.get("custom_profile").model == "qwen3.5:4b-mlx"


def test_sqlite_profile_registry_binds_connection_and_structured_mode(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    connections = ConnectionStore(database)
    connections.seed_defaults("http://127.0.0.1:11434")
    registry = ModelProfileRegistry("configs/model_profiles.yaml", database=database)

    saved = registry.save({
        "profile_id": "remote_profile",
        "connection_id": "ollama-local",
        "model": "qwen3:1.7b",
        "display_name": "测试 Profile",
        "default_prompt_id": "feature_extract_v3_compact_strict_json_en",
        "structured_output_mode": "json_object",
    })

    reloaded = ModelProfileRegistry("configs/model_profiles.yaml", database=database).get("remote_profile")
    assert saved.connection_id == "ollama-local"
    assert reloaded.structured_output_mode == "json_object"
    assert reloaded.build_model_options()["structured_output_mode"] == "json_object"
