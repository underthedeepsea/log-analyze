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


def cni_feature(feature_type="cni_network_failure", problem_code="kubernetes.cni.ip_exhaustion", component="kubelet"):
    return {
        "feature_type": feature_type,
        "problem_code": problem_code,
        "title": "CNI IP 地址耗尽",
        "summary": "CNI 地址池没有可用 IP。",
        "importance": "high",
        "tags": ["cni"],
        "selection_reason": "CNI 模板命中地址耗尽语义。",
        "source_templates": [{
            "template_hash": "hash-cni-" + component,
            "template_fingerprint": "fingerprint-cni",
            "category": "network",
            "component": component,
            "template": "CNI failed: no enough ips",
            "count": 2,
        }],
    }


def test_approved_rule_persists_and_matches_globally(tmp_path):
    path = tmp_path / "approved_rules.json"
    stored = ApprovedRuleStore(path).upsert_feature(feature())

    reloaded = ApprovedRuleStore(path)
    matches = reloaded.match_entity(entity(cluster="another", entity_id="other"))

    assert matches[0]["rule_id"] == stored["rule_id"]
    assert matches[0]["title"] == "内存压力"
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == "1.0"


def test_fingerprint_rule_matches_different_cluster_instance_hash(tmp_path):
    value = feature()
    value["source_templates"][0].update({
        "template_fingerprint": "fingerprint-oom",
        "template_instance_hash": "instance-a",
        "hash_version": "v2",
    })
    target = entity(cluster="prod-b", entity_id="node-b")
    target["top_templates"][0].update({
        "template_hash": "different-legacy-hash",
        "template_fingerprint": "fingerprint-oom",
        "template_instance_hash": "instance-b",
        "hash_version": "v2",
    })
    store = ApprovedRuleStore(tmp_path / "rules.json")

    stored = store.upsert_feature(value)

    assert store.match_entity(target)[0]["rule_id"] == stored["rule_id"]
    assert stored["template_signatures"][0]["template_fingerprint"] == "fingerprint-oom"


def test_duplicate_signature_updates_instead_of_appending(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    first = store.upsert_feature(feature(title="旧标题"))
    second = store.upsert_feature(feature(title="新标题"))

    rules = store.list_rules()

    assert len(rules) == 1
    assert rules[0]["title"] == "新标题"
    assert second["rule_id"] == first["rule_id"]


def test_rule_store_persists_lineage_without_affecting_match(tmp_path):
    value = feature()
    value.update({
        "job_id": "job-1",
        "candidate_id": "candidate-1",
        "trace_id": "trace-1",
        "prompt_id": "feature_extract_v3",
        "prompt_hash": "prompt-sha",
        "provider": "ollama",
        "model": "qwen3:1.7b",
        "evidence_hash": "evidence-sha",
    })
    store = ApprovedRuleStore(tmp_path / "rules.json")

    stored = store.upsert_feature(value)
    matches = store.match_entity(entity())

    assert stored["lineage"] == {
        "job_id": "job-1",
        "candidate_id": "candidate-1",
        "trace_id": "trace-1",
        "prompt_id": "feature_extract_v3",
        "prompt_hash": "prompt-sha",
        "provider": "ollama",
        "model": "qwen3:1.7b",
        "evidence_hash": "evidence-sha",
    }
    assert matches[0]["lineage"]["trace_id"] == "trace-1"


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


def test_semantic_rule_matches_new_wrapper_without_template_identity(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    stored = store.upsert_feature(cni_feature())
    target = cni_feature(
        feature_type="pod_sandbox_network_failure",
        problem_code="runtime_sandbox_create_failed",
        component="containerd",
    )

    matches = store.match_feature(target)

    assert [item["rule_id"] for item in matches] == [stored["rule_id"]]


def test_v1_wrapper_rule_stays_strict_against_new_semantic_identity(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    stored = store.upsert_feature(cni_feature())
    legacy = store.list_rules()[0]
    legacy["approval_key"] = "appr-v1-wrapper"
    legacy["problem_code"] = "kubernetes.cni.plugin_failure"
    store._write_locked([legacy])

    wrapper = cni_feature(
        feature_type="pod_sandbox_network_failure",
        problem_code="runtime_sandbox_create_failed",
        component="containerd",
    )
    assert store.match_feature(wrapper) == []

    replacement = store.upsert_feature(wrapper)

    assert replacement["rule_id"] != stored["rule_id"]
    rules = {item["rule_id"]: item for item in store.list_rules()}
    assert len(rules) == 2
    assert rules[stored["rule_id"]]["approval_key"] == "appr-v1-wrapper"


def test_semantic_entity_matching_does_not_require_original_component(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    stored = store.upsert_feature(cni_feature(component="kubelet"))
    target = {
        "entity_type": "node",
        "entity_id": "node-b",
        "cluster": "prod-b",
        "top_templates": [{
            "template_hash": "hash-cni-containerd",
            "template_fingerprint": "another-wrapper",
            "category": "network",
            "component": "containerd",
            "template": "CreatePodSandbox failed: cni no enough ips",
            "count": 3,
        }],
    }

    matches = store.match_entity(target)

    assert [item["rule_id"] for item in matches] == [stored["rule_id"]]


def test_canonical_rule_matching_ignores_jobs_nodes_and_wrapper_fields(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    stored = store.upsert_feature(cni_feature())
    wrapper = cni_feature(
        feature_type="pod_sandbox_network_failure",
        problem_code="runtime_sandbox_create_failed",
        component="containerd",
    )
    wrapper["job_id"] = "job-wrapper"
    wrapper["candidate_id"] = "candidate-wrapper"
    wrapper["source_templates"][0]["template_fingerprint"] = "wrapper-containerd"

    assert [item["rule_id"] for item in store.match_feature(wrapper)] == [stored["rule_id"]]
    assert [item["rule_id"] for item in store.match_entity({
        "cluster": "prod-b",
        "entity_type": "node",
        "entity_id": "node-b",
        "top_templates": [{
            "template_hash": "hash-wrapper",
            "template_fingerprint": "wrapper-containerd",
            "category": "network",
            "component": "containerd",
            "template": "CreatePodSandbox failed: cni no enough ips",
        }],
    })] == [stored["rule_id"]]


def test_v1_rule_matching_stays_on_strict_legacy_identity(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    legacy = {
        "rule_id": "legacy-v1-rule",
        "signature": "legacy-signature",
        "feature_type": "cni_network_failure",
        "title": "历史 CNI 规则",
        "summary": "历史物理规则",
        "importance": "high",
        "components": ["kubelet"],
        "template_signatures": [{"template_hash": "legacy-hash", "category": "network"}],
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "approval_key": "appr-v1-physical",
        "anchor_signatures": ["legacy-hash|network"],
        "match_mode": "semantic",
        "schema_version": "approved_rule_v1",
        "status": "active",
        "approved_at": "2026-09-01T00:00:00+00:00",
        "created_at": "2026-09-01T00:00:00+00:00",
        "updated_at": "2026-09-01T00:00:00+00:00",
        "current_version": 1,
        "next_review_at": "2026-10-01T00:00:00+00:00",
    }
    store._write_locked([legacy])
    wrapper = cni_feature(
        feature_type="pod_sandbox_network_failure",
        problem_code="runtime_sandbox_create_failed",
        component="containerd",
    )

    assert store.match_feature(wrapper) == []
    assert store.match_entity({
        "entity_type": "node",
        "entity_id": "node-b",
        "top_templates": [{
            "template_hash": "wrapper-hash",
            "template_fingerprint": "wrapper-containerd",
            "category": "network",
            "component": "containerd",
            "template": "CreatePodSandbox failed: cni no enough ips",
        }],
    }) == []

    physical = cni_feature(component="kubelet")
    physical["approval_key"] = "appr-v1-physical"
    physical["source_templates"][0].pop("template_fingerprint")
    physical["source_templates"][0]["template_hash"] = "legacy-hash"
    assert [item["rule_id"] for item in store.match_feature(physical)] == [legacy["rule_id"]]
    assert [item["rule_id"] for item in store.match_entity({
        "entity_type": "node",
        "entity_id": "node-a",
        "top_templates": [{
            "template_hash": "legacy-hash",
            "category": "network",
            "component": "kubelet",
        }],
    })] == [legacy["rule_id"]]


def test_inactive_rule_gets_distinct_active_replacement_with_lineage(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    predecessor = store.upsert_feature(cni_feature())
    disabled = store.list_rules()[0]
    disabled["status"] = "disabled"
    store._write_locked([disabled])
    replacement_input = cni_feature(
        feature_type="pod_sandbox_network_failure",
        problem_code="runtime_sandbox_create_failed",
        component="containerd",
    )
    replacement_input.update({"job_id": "job-replacement", "candidate_id": "candidate-replacement"})
    replacement_entity = {
        "entity_type": "node",
        "entity_id": "node-replacement",
        "top_templates": [dict(replacement_input["source_templates"][0])],
    }

    assert store.match_feature(replacement_input) == []
    assert store.match_entity(replacement_entity) == []
    replacement = store.upsert_feature(replacement_input)
    repeated = store.upsert_feature({**replacement_input, "title": "更新后的替换规则"})
    rules = {item["rule_id"]: item for item in store.list_rules()}

    assert replacement["rule_id"] != predecessor["rule_id"]
    assert replacement["status"] == "active"
    assert replacement["lineage"]["predecessor_rule_id"] == predecessor["rule_id"]
    assert replacement["predecessor_rule_id"] == predecessor["rule_id"]
    assert repeated["rule_id"] == replacement["rule_id"]
    assert rules[predecessor["rule_id"]]["status"] == "disabled"
    assert rules[replacement["rule_id"]]["status"] == "active"
