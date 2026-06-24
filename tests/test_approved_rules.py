import json

import pytest

from logrisk.approved_rules import ApprovedRuleError, ApprovedRuleStore


def feature(title="内存压力"):
    return {
        "feature_type": "resource_pressure",
        "title": title,
        "summary": "节点发生内存不足",
        "importance": "critical",
        "tags": ["oom", "memory"],
        "selection_reason": "高风险模板",
        "source_templates": [
            {
                "template_hash": "hash-oom",
                "category": "memory",
                "template": "Out of memory: Killed process <*> ",
                "component": "kernel",
                "count": 3,
            }
        ],
    }


def entity(cluster="prod-a", entity_id="node-a"):
    return {
        "cluster": cluster,
        "entity_type": "node",
        "entity_id": entity_id,
        "top_templates": [
            {
                "template_hash": "hash-oom",
                "category": "memory",
                "template": "Out of memory: Killed process 42",
                "component": "kernel",
                "count": 8,
            }
        ],
    }


def test_approved_rule_persists_and_matches_globally(tmp_path):
    path = tmp_path / "approved_rules.json"
    stored = ApprovedRuleStore(path).upsert_feature(feature())

    reloaded = ApprovedRuleStore(path)
    matches = reloaded.match_entity(entity(cluster="another", entity_id="other"))

    assert matches[0]["rule_id"] == stored["rule_id"]
    assert matches[0]["title"] == "内存压力"
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == "1.0"


def test_duplicate_signature_updates_instead_of_appending(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    first = store.upsert_feature(feature(title="旧标题"))
    second = store.upsert_feature(feature(title="新标题"))

    rules = store.list_rules()

    assert len(rules) == 1
    assert rules[0]["title"] == "新标题"
    assert second["rule_id"] == first["rule_id"]


def test_rule_requires_all_template_category_pairs(tmp_path):
    combined = feature()
    combined["source_templates"].append({
        "template_hash": "hash-eviction",
        "category": "eviction",
        "template": "eviction manager",
    })
    store = ApprovedRuleStore(tmp_path / "rules.json")
    store.upsert_feature(combined)

    assert store.match_entity(entity()) == []


def test_malformed_existing_rule_file_is_rejected(tmp_path):
    path = tmp_path / "rules.json"
    path.write_text("not json", encoding="utf-8")

    with pytest.raises(ApprovedRuleError, match="规则库"):
        ApprovedRuleStore(path).list_rules()


def test_rule_store_excludes_historical_entity_and_raw_log_fields(tmp_path):
    value = feature()
    value.update({
        "entity": {"type": "node", "id": "secret-node"},
        "cluster": "secret-cluster",
        "samples": ["raw secret"],
        "raw_sample": "raw secret",
    })
    store = ApprovedRuleStore(tmp_path / "rules.json")

    stored = store.upsert_feature(value)

    serialized = json.dumps(stored, ensure_ascii=False)
    assert "secret-node" not in serialized
    assert "secret-cluster" not in serialized
    assert "raw secret" not in serialized


def test_record_reuse_increments_usage_metadata(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    stored = store.upsert_feature(feature())

    updated = store.record_reuse(stored["rule_id"])

    assert updated["reuse_count"] == 1
    assert updated["last_reused_at"]
