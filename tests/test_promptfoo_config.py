import json
from pathlib import Path

import yaml


def test_promptfoo_config_uses_local_ollama_and_feature_extract_v1():
    config = yaml.safe_load(Path("promptfoo.yaml").read_text(encoding="utf-8"))
    prompt_source = Path("eval_cases/promptfoo/feature_extract_v1.js").read_text(encoding="utf-8")

    assert config["prompts"] == ["file://eval_cases/promptfoo/feature_extract_v1.js"]
    assert config["providers"][0]["id"] == "ollama:chat:qwen3:1.7b"
    assert "prompts/feature_extract_v1.md" in prompt_source
    assert "evidence_json" in prompt_source


def test_promptfoo_suite_has_five_cases_and_required_assertions():
    config = yaml.safe_load(Path("promptfoo.yaml").read_text(encoding="utf-8"))
    cases = json.loads(Path("eval_cases/promptfoo/cases.json").read_text(encoding="utf-8"))
    assertions = "\n".join(item["value"] for item in config["defaultTest"]["assert"])

    assert len(cases) >= 5
    assert "validJson" in assertions
    assert "knownHashes" in assertions
    assert "forbiddenClaims" in assertions
    assert all(isinstance(case["vars"]["allowed_hashes_json"], str) for case in cases)
    assert all(isinstance(case["vars"]["forbidden_json"], str) for case in cases)
    assert all(not any(isinstance(value, list) for value in case["vars"].values()) for case in cases)
