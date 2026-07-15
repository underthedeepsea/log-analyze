from __future__ import annotations

import json
from pathlib import Path

import pytest

from logrisk.semantic.schema import SemanticValidationError
from logrisk.semantic.store import SemanticDictionaryStore


BUILTINS = Path("configs/semantic_dictionary").resolve()


def store(tmp_path) -> SemanticDictionaryStore:
    return SemanticDictionaryStore(tmp_path / "semantic", BUILTINS)


def custom_rule(rule_id: str = "linux-custom-io-error") -> dict:
    return {
        "rule_id": rule_id,
        "field": "errno_name",
        "pattern": r"error_name=(?P<value>[A-Z0-9_]+)",
        "group": "value",
        "value_type": "string",
        "typed_mask": "ERRNO",
        "tags": ["系统错误", "错误名称"],
        "priority": 90,
        "source_types": [],
        "components": [],
    }


def test_lists_four_independent_read_only_baselines(tmp_path):
    items = store(tmp_path).list_dictionaries()

    assert {item["dictionary_id"] for item in items} == {"linux", "kubernetes", "nvidia", "container_runtime"}
    assert all(item["active_version"] == 1 for item in items)
    assert all(item["builtin_read_only"] is True for item in items)
    assert all(item["content_hash"] for item in items)


def test_candidate_versions_are_append_only_and_reject_stale_save(tmp_path):
    subject = store(tmp_path)
    candidate = subject.create_candidate("linux", {"operator": "qa"})
    saved = subject.save_version("linux", {
        "expected_version": candidate["version"],
        "custom_rules": [custom_rule()],
        "operator": "qa",
    })

    assert candidate["version"] == 2
    assert saved["version"] == 3
    assert subject.get_version("linux", 2)["custom_rules"] == []
    assert subject.get_version("linux", 3)["custom_rules"][0]["rule_id"] == "linux-custom-io-error"
    with pytest.raises(SemanticValidationError, match="版本冲突"):
        subject.save_version("linux", {"expected_version": 2, "custom_rules": []})
    with pytest.raises(SemanticValidationError, match="只读"):
        subject.save_version("linux", {"expected_version": 1, "custom_rules": []})


def test_publish_requires_validation_and_confirmation_then_pins_hash(tmp_path):
    subject = store(tmp_path)
    candidate = subject.create_candidate("linux", {})
    version = candidate["version"]

    with pytest.raises(SemanticValidationError, match="校验"):
        subject.publish("linux", version, {"confirmed": True})
    validation = subject.validate_version("linux", version)
    with pytest.raises(SemanticValidationError, match="人工确认"):
        subject.publish("linux", version, {"confirmed": False})
    published = subject.publish("linux", version, {"confirmed": True, "operator": "qa"})

    assert validation["valid"] is True
    assert published["status"] == "published"
    assert subject.active_snapshot()["versions"]["linux"]["content_hash"] == candidate["content_hash"]


def test_failed_candidate_does_not_change_active_pointer(tmp_path):
    subject = store(tmp_path)
    before = subject.active_snapshot()["versions"]["linux"]
    candidate = subject.create_candidate("linux", {})
    broken = custom_rule("broken-conflict")
    broken.update({"field": "errno", "priority": 100})

    with pytest.raises(SemanticValidationError, match="优先级冲突"):
        subject.save_version("linux", {
            "expected_version": candidate["version"],
            "custom_rules": [broken],
        })

    assert subject.active_snapshot()["versions"]["linux"] == before


def test_rollback_is_confirmed_and_audited(tmp_path):
    subject = store(tmp_path)
    candidate = subject.create_candidate("nvidia", {})
    subject.validate_version("nvidia", candidate["version"])
    subject.publish("nvidia", candidate["version"], {"confirmed": True, "operator": "qa"})

    with pytest.raises(SemanticValidationError, match="人工确认"):
        subject.rollback("nvidia", 1, {"confirmed": False})
    rolled_back = subject.rollback("nvidia", 1, {"confirmed": True, "operator": "qa"})

    assert rolled_back["active_version"] == 1
    events = [json.loads(line) for line in (tmp_path / "semantic" / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [event["action"] for event in events][-2:] == ["publish", "rollback"]


def test_active_snapshot_test_returns_typed_result(tmp_path):
    result = store(tmp_path).test_snapshot({
        "message_core": "NVRM: Xid 79, GPU has fallen off the bus",
        "source_type": "syslog",
        "component": "kernel",
    })

    assert result["semantic_fields"]["xid_code"] == 79
    assert result["matched_rule_ids"] == ["nvidia-xid-code"]
    assert result["dictionary_versions"]["nvidia"]["version"] == 1
