import json
import subprocess
from pathlib import Path

import yaml


def test_promptfoo_config_loads_production_default_prompt():
    config = yaml.safe_load(Path("promptfoo.yaml").read_text(encoding="utf-8"))
    loader = Path("eval_cases/promptfoo/load_prompt.js").read_text(encoding="utf-8")

    assert config["prompts"] == ["file://eval_cases/promptfoo/load_prompt.js"]
    assert config["providers"][0]["id"] == "ollama:chat:qwen3:1.7b"
    assert "configs/ai_harness.yaml" in loader
    assert "LOGRISK_EVAL_PROMPT_ID" in loader


def test_promptfoo_generated_cases_are_deterministic_and_assertions_are_strong():
    path = Path("eval_cases/promptfoo/generated_cases.json")
    before = path.read_text(encoding="utf-8")
    subprocess.run([".venv/bin/python", "scripts/generate_promptfoo_cases.py"], check=True)
    config = yaml.safe_load(Path("promptfoo.yaml").read_text(encoding="utf-8"))
    cases = json.loads(path.read_text(encoding="utf-8"))
    assertions = "\n".join(item["value"] for item in config["defaultTest"]["assert"])

    assert path.read_text(encoding="utf-8") == before
    assert len(cases) >= 5
    for name in ("validJson", "schemaValid", "knownHashes", "knownComponents", "expectedFeatureTypes", "expectedEmptyFeatures", "forbiddenClaims", "noRawLogLeak"):
        assert name in assertions
    assert all(isinstance(case["vars"]["expected_json"], str) for case in cases)
