from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor

from logrisk.approval_dedup import (
    approval_identity,
    build_approval_key,
    collect_problem_codes,
    derive_problem_code,
    is_canonical_problem_code,
    normalize_problem_code,
    same_approval_identity,
    select_primary_problem_code,
)
from logrisk.approved_rules import ApprovedRuleStore
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager
from logrisk.sqlite_stores import SQLiteApprovalGroupStore, SQLiteApprovedRuleStore, SQLiteFeatureJobStore


def test_problem_code_normalization_keeps_distinct_causes_distinct():
    assert normalize_problem_code("CNI no enough IP") == "kubernetes.cni.ip_exhaustion"
    assert normalize_problem_code("cni_ip_exhaustion") == "kubernetes.cni.ip_exhaustion"
    assert normalize_problem_code("CNI no enough IP 10.1.2.3") == "kubernetes.cni.ip_exhaustion"
    assert build_approval_key("network_failure", "CNI no enough IP", ["kubelet"], ["anchor"]) == build_approval_key(
        "network_failure", "kubernetes.cni.ip_exhaustion", ["kubelet"], ["anchor"]
    )
    assert build_approval_key("network_failure", "CNI config syntax error", ["kubelet"], ["anchor"]) != build_approval_key(
        "network_failure", "CNI no enough IP", ["kubelet"], ["anchor"]
    )


def test_same_approval_key_reuses_rule_for_different_wrapper_sets(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    first_source = entity("node-a", "2026-06-22T10:00:00+08:00")
    second_source = entity("node-b", "2026-06-22T11:00:00+08:00")
    first = candidate(first_source, "candidate-a")
    second = candidate(second_source, "candidate-b")
    second["source_templates"].append({
        "template_fingerprint": "supporting-wrapper",
        "category": "runtime",
        "component": "kubelet",
        "template": "Error syncing pod",
        "count": 1,
    })

    first_rule = rules.upsert_feature(first)
    second_rule = rules.upsert_feature(second)

    assert second_rule["rule_id"] == first_rule["rule_id"]
    assert len(rules.list_rules()) == 1
    assert rules.match_entity(second_source)[0]["rule_id"] == first_rule["rule_id"]


def test_model_candidates_without_explicit_anchor_ignore_wrapper_fingerprint():
    first = candidate(entity("node-a", "2026-06-22T10:00:00+08:00"), "candidate-a")
    second = candidate(entity("node-b", "2026-06-22T11:00:00+08:00"), "candidate-b")
    first.pop("problem_code")
    first.pop("anchor_signatures")
    second.pop("problem_code")
    second.pop("anchor_signatures")
    second["source_templates"][0]["template_fingerprint"] = "different-wrapper-fingerprint"

    assert approval_identity(first)["approval_key"] == approval_identity(second)["approval_key"]


def test_cni_wrappers_choose_the_same_concrete_root_cause():
    candidates = [
        {
            "candidate_id": "candidate-a",
            "job_id": "job-a",
            "feature_type": "cni_network_failure",
            "problem_code": "runtime_cni_setup_failed",
            "components": ["kubelet"],
            "source_templates": [
                {"component": "kubelet", "template": "NetworkPlugin cni failed: no enough ips"},
            ],
        },
        {
            "candidate_id": "candidate-b",
            "job_id": "job-b",
            "feature_type": "pod_sandbox_network_failure",
            "problem_code": "runtime_sandbox_create_failed",
            "components": ["containerd"],
            "source_templates": [
                {"component": "containerd", "template": "CreatePodSandbox failed: cni no enough ips"},
            ],
        },
    ]

    assert [derive_problem_code(candidate) for candidate in candidates] == [
        "kubernetes.cni.ip_exhaustion",
        "kubernetes.cni.ip_exhaustion",
    ]
    assert approval_identity(candidates[0])["approval_key"] == approval_identity(candidates[1])["approval_key"]


def test_canonical_approval_identity_ignores_wrapper_shape():
    first = {
        "feature_type": "cni_network_failure",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "components": ["kubelet"],
        "anchor_signatures": ["wrapper-a"],
    }
    second = {
        "feature_type": "pod_sandbox_network_failure",
        "problem_code": "CNI no enough IP",
        "components": ["containerd"],
        "anchor_signatures": ["wrapper-b"],
    }

    assert approval_identity(first)["approval_key"] == approval_identity(second)["approval_key"]


def test_conflicting_concrete_cni_causes_use_strict_fallback():
    feature = {
        "feature_type": "cni_network_failure",
        "problem_code": "runtime_cni_setup_failed",
        "semantic_fields": {
            "risk_type": "kubernetes.cni.config_error",
        },
        "source_templates": [
            {"template": "CNI config syntax error and no enough ips"},
        ],
    }

    assert derive_problem_code(feature).startswith("logrisk.cni_network_failure.")


def test_same_approval_identity_supports_legacy_and_v2_semantic_candidates():
    legacy = {
        "feature_type": "cni_network_failure",
        "problem_code": "runtime_cni_setup_failed",
        "approval_key": "appr-legacy-wrapper",
        "components": ["kubelet"],
        "source_templates": [{"template": "NetworkPlugin cni failed: no enough ips"}],
    }
    current = {
        "feature_type": "pod_sandbox_network_failure",
        "problem_code": "runtime_sandbox_create_failed",
        "approval_key": approval_identity({"problem_code": "kubernetes.cni.ip_exhaustion"})["approval_key"],
        "components": ["containerd"],
        "source_templates": [{"template": "CreatePodSandbox failed: cni no enough ips"}],
    }

    assert same_approval_identity(legacy, current)


def test_collect_problem_codes_keeps_concrete_and_wrapper_evidence():
    feature = {
        "feature_type": "cni_network_failure",
        "problem_code": "runtime_cni_setup_failed",
        "semantic_fields": {"risk_type": "kubernetes.cni.config_error"},
        "source_templates": [{
            "template_fingerprint": "fingerprint-cni",
            "template": "NetworkPlugin cni failed: no enough ips",
        }],
    }

    assert set(collect_problem_codes(feature)) == {
        "kubernetes.cni.plugin_failure",
        "kubernetes.cni.config_error",
        "kubernetes.cni.ip_exhaustion",
    }


def test_collect_problem_codes_keeps_independent_keyword_hits_from_one_text():
    feature = {
        "feature_type": "cni_network_failure",
        "source_templates": [{
            "template_fingerprint": "fingerprint-conflict",
            "template": "CNI config syntax error: no enough ips",
        }],
    }

    assert set(collect_problem_codes(feature)) >= {
        "kubernetes.cni.config_error",
        "kubernetes.cni.ip_exhaustion",
    }
    assert derive_problem_code(feature).startswith("logrisk.cni_network_failure.")


def test_recursive_semantic_fields_collect_lists_and_top_level_cause():
    feature = {
        "feature_type": "cni_network_failure",
        "cause": "kubernetes.cni.config_error",
        "semantic_fields": [{
            "risk_semantic": ["kubernetes.cni.ip_exhaustion"],
        }],
    }

    assert set(collect_problem_codes(feature)) >= {
        "kubernetes.cni.config_error",
        "kubernetes.cni.ip_exhaustion",
    }


def test_generic_semantic_code_precedes_runtime_wrapper_code():
    assert select_primary_problem_code([
        "runtime_sandbox_create_failed",
        "kubernetes.cni.network_failure",
    ]) == "kubernetes.cni.network_failure"


def test_conflicting_concrete_codes_use_the_strict_fallback_even_with_explicit_code():
    codes = [
        "kubernetes.cni.ip_exhaustion",
        "kubernetes.cni.config_error",
    ]
    left = {
        "feature_type": "cni_network_failure",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "semantic_fields": {"risk_type": "kubernetes.cni.config_error"},
        "components": ["kubelet"],
        "anchor_signatures": ["shared-anchor"],
        "source_templates": [{
            "template_fingerprint": "shared-anchor",
            "category": "network",
            "template": "CNI config syntax error",
        }],
    }
    right = {
        "feature_type": "cni_network_failure",
        "problem_code": "kubernetes.cni.config_error",
        "semantic_fields": {"risk_type": "kubernetes.cni.ip_exhaustion"},
        "components": ["kubelet"],
        "anchor_signatures": ["shared-anchor"],
        "source_templates": [{
            "template_fingerprint": "shared-anchor",
            "category": "network",
            "template": "CNI no enough ips",
        }],
    }

    assert select_primary_problem_code(codes, explicit_code=codes[0]) is None
    assert derive_problem_code(left).startswith("logrisk.cni_network_failure.")
    assert derive_problem_code(right).startswith("logrisk.cni_network_failure.")
    assert approval_identity(left)["approval_key"] == approval_identity(right)["approval_key"]


def test_unknown_and_unclassified_codes_use_strict_fallback_identity():
    assert not is_canonical_problem_code("unknown")
    assert not is_canonical_problem_code("unknown.cause")
    assert not is_canonical_problem_code("unclassified")
    assert not is_canonical_problem_code("unclassified.cause")
    assert not is_canonical_problem_code("logrisk.cni_network_failure.deadbeef")

    left = {
        "feature_type": "network_failure",
        "problem_code": "unknown",
        "components": ["kubelet"],
        "anchor_signatures": ["anchor-a"],
    }
    right = {
        "feature_type": "pod_sandbox_failure",
        "problem_code": "unknown",
        "components": ["containerd"],
        "anchor_signatures": ["anchor-b"],
    }

    assert approval_identity(left)["approval_key"] != approval_identity(right)["approval_key"]
    assert not same_approval_identity(left, right)


def test_unknown_namespaces_are_not_canonical_anywhere_in_the_code():
    assert not is_canonical_problem_code("vendor.unknown")
    assert not is_canonical_problem_code("vendor.unknown.cause")
    assert not is_canonical_problem_code("unknown.vendor.cause")
    assert not is_canonical_problem_code("vendor.unclassified_problem")


def test_unknown_fallback_hashes_all_evidence_and_preserves_source_anchors():
    base = {
        "feature_type": "network_failure",
        "problem_code": "unknown",
        "cause": "unknown_cause",
        "source_templates": [{
            "template_fingerprint": "anchor-a",
            "category": "network",
        }],
    }
    unknown_only = dict(base)
    unknown_only.pop("cause")
    different_anchor = dict(base)
    different_anchor["source_templates"] = [{
        "template_fingerprint": "anchor-b",
        "category": "network",
    }]

    assert derive_problem_code(base).startswith("logrisk.network_failure.")
    assert derive_problem_code(unknown_only).startswith("logrisk.network_failure.")
    assert derive_problem_code(base) != derive_problem_code(unknown_only)
    assert approval_identity(base)["approval_key"] != approval_identity(different_anchor)["approval_key"]


def test_logrisk_fallback_preserves_distinct_source_anchors():
    left = {
        "feature_type": "network_failure",
        "problem_code": "logrisk.network_failure.old",
        "source_templates": [{"template_fingerprint": "anchor-a"}],
    }
    right = {
        "feature_type": "network_failure",
        "problem_code": "logrisk.network_failure.old",
        "source_templates": [{"template_fingerprint": "anchor-b"}],
    }

    assert approval_identity(left)["approval_key"] != approval_identity(right)["approval_key"]


def test_all_cni_ip_exhaustion_wrappers_share_one_v2_identity():
    candidates = [
        {
            "feature_type": "kubelet_network_signal",
            "title": "kubelet title",
            "summary": "kubelet summary",
            "components": ["kubelet"],
            "source_templates": [{
                "template_fingerprint": "wrapper-kubelet",
                "template_hash": "hash-kubelet",
                "template": "NetworkPlugin cni failed: no enough ips",
            }],
        },
        {
            "feature_type": "containerd_sandbox_signal",
            "title": "containerd title",
            "summary": "containerd summary",
            "components": ["containerd"],
            "source_templates": [{
                "template_fingerprint": "wrapper-containerd",
                "template_hash": "hash-containerd",
                "template": "CreatePodSandbox failed: cni no enough ips",
            }],
        },
        {
            "feature_type": "runtime_cni_setup_failure",
            "title": "runtime setup title",
            "summary": "runtime setup summary",
            "problem_code": "runtime_cni_setup_failed",
            "components": ["runtime-wrapper"],
            "source_templates": [{
                "template_fingerprint": "wrapper-setup",
                "template_hash": "hash-setup",
                "template": "runtime CNI setup failed: no enough ips",
            }],
        },
        {
            "feature_type": "runtime_sandbox_create_failure",
            "title": "sandbox title",
            "summary": "sandbox summary",
            "problem_code": "runtime_sandbox_create_failed",
            "components": ["sandbox-wrapper"],
            "source_templates": [{
                "template_fingerprint": "wrapper-sandbox",
                "template_hash": "hash-sandbox",
                "template": "runtime sandbox create failed: cni no enough ips",
            }],
        },
    ]

    assert {derive_problem_code(candidate) for candidate in candidates} == {
        "kubernetes.cni.ip_exhaustion",
    }
    assert len({approval_identity(candidate)["approval_key"] for candidate in candidates}) == 1


def test_runtime_sandbox_wrapper_without_cni_token_finds_ip_exhaustion():
    feature = {
        "feature_type": "runtime_network_failure",
        "problem_code": "runtime_sandbox_create_failed",
        "summary": "failed to setup network for sandbox: no enough ips",
    }

    assert derive_problem_code(feature) == "kubernetes.cni.ip_exhaustion"


def test_generic_network_and_plain_pod_sandbox_text_do_not_become_cni_ip_exhaustion():
    generic_network = {
        "feature_type": "network_failure",
        "summary": "network reports no enough ips",
    }
    plain_pod_sandbox = {
        "feature_type": "pod_sandbox_network_failure",
        "summary": "CreatePodSandbox failed: no enough ips",
    }

    assert derive_problem_code(generic_network) != "kubernetes.cni.ip_exhaustion"
    assert derive_problem_code(plain_pod_sandbox) == "kubernetes.runtime.pod_sandbox_failure"


def test_canonical_identity_ignores_presentation_and_operational_fields():
    left = {
        "job_id": "job-a",
        "feature_type": "network_failure",
        "title": "Old title",
        "summary": "Old summary",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "components": ["kubelet"],
        "cluster": "prod-a",
        "entity": {"type": "node", "id": "node-a"},
        "window_start": "2026-09-01T10:00:00+00:00",
        "window_end": "2026-09-01T10:05:00+00:00",
        "source_templates": [{
            "template_hash": "hash-a",
            "template_fingerprint": "fingerprint-a",
            "category": "network",
            "component": "kubelet",
        }],
    }
    right = {
        "job_id": "job-b",
        "feature_type": "pod_sandbox_network_failure",
        "title": "New title",
        "summary": "New summary",
        "problem_code": "CNI no enough ips",
        "components": ["containerd"],
        "cluster": "prod-b",
        "entity": {"type": "node", "id": "node-b"},
        "window_start": "2026-09-02T11:00:00+00:00",
        "window_end": "2026-09-02T11:05:00+00:00",
        "source_templates": [{
            "template_hash": "hash-b",
            "template_fingerprint": "fingerprint-b",
            "category": "runtime",
            "component": "containerd",
        }],
    }

    assert same_approval_identity(left, right)
    assert approval_identity(left)["approval_key"] == approval_identity(right)["approval_key"]


def test_historical_v1_keys_still_compare_by_their_physical_key():
    left = {
        "feature_type": "cni_network_failure",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "components": ["kubelet"],
        "anchor_signatures": ["legacy-anchor"],
        "approval_key": "appr-v1-left",
    }
    right = dict(left)
    right["approval_key"] = "appr-v1-right"

    assert not same_approval_identity(left, right)
    right["approval_key"] = left["approval_key"]
    assert same_approval_identity(left, right)


def test_empty_fallback_keeps_the_historical_v1_digest_material():
    feature = {"feature_type": "network_failure"}
    expected_digest = hashlib.sha256(b"[]").hexdigest()[:16]

    assert derive_problem_code(feature) == f"logrisk.network_failure.{expected_digest}"


def test_explicit_unknown_code_is_hashed_as_strict_fallback_evidence():
    assert derive_problem_code({
        "feature_type": "network_failure",
        "problem_code": "unknown",
    }).startswith("logrisk.network_failure.")


def test_pod_sandbox_oom_is_not_classified_as_cni_plugin_failure():
    feature = {
        "feature_type": "runtime_sandbox_failure",
        "summary": "pod sandbox failed: out of memory",
    }

    assert derive_problem_code(feature) == "linux.memory.oom"


def test_distinct_problem_codes_do_not_merge_on_same_template_signature(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    source = entity("node-a", "2026-06-22T10:00:00+08:00")
    first = candidate(source, "candidate-a")
    second = candidate(source, "candidate-b")
    second["problem_code"] = "CNI config syntax error"

    first_rule = rules.upsert_feature(first)
    second_rule = rules.upsert_feature(second)

    assert second_rule["rule_id"] != first_rule["rule_id"]
    assert len(rules.list_rules()) == 2


def test_concurrent_sqlite_approvals_keep_one_rule_per_approval_key(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    first = candidate(entity("node-a", "2026-06-22T10:00:00+08:00"), "candidate-a")
    second = candidate(entity("node-b", "2026-06-22T11:00:00+08:00"), "candidate-b")
    second["source_templates"].append({"template_hash": "wrapper", "category": "runtime", "count": 1})

    def approve(feature):
        return SQLiteApprovedRuleStore(database).upsert_feature(feature)

    with ThreadPoolExecutor(max_workers=2) as pool:
        saved = list(pool.map(approve, (first, second)))

    assert saved[0]["rule_id"] == saved[1]["rule_id"]
    assert len(SQLiteApprovedRuleStore(database).list_rules()) == 1


def entity(entity_id: str, window_start: str) -> dict:
    window_end = window_start.replace("10:00", "10:05").replace("11:00", "11:05")
    template = {
        "template_hash": f"hash-cni-{window_start}",
        "template_fingerprint": "fingerprint-cni",
        "category": "network",
        "component": "kubelet",
        "template": "CNI no enough IPs while creating pod sandbox",
        "count": 3,
        "first_seen": window_start,
        "last_seen": window_end,
    }
    return {
        "window_start": window_start,
        "window_end": window_end,
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": entity_id,
        "risk_score": 90,
        "risk_level": "critical",
        "top_templates": [template],
        "affected_entities": [],
    }


def candidate(source: dict, candidate_id: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "status": "pending",
        "reviewer_note": "",
        "approved_at": None,
        "cluster": source["cluster"],
        "entity": {"type": source["entity_type"], "id": source["entity_id"]},
        "window_start": source["window_start"],
        "window_end": source["window_end"],
        "risk_score": source["risk_score"],
        "risk_level": source["risk_level"],
        "feature_type": "network_failure",
        "problem_code": "CNI no enough IP",
        "anchor_signatures": ["fingerprint-cni"],
        "title": "CNI 网络配置失败",
        "summary": "检测到 CNI 网络配置失败日志",
        "importance": "critical",
        "template_hashes": [source["top_templates"][0]["template_hash"]],
        "components": ["kubelet"],
        "tags": ["CNI", "网络"],
        "selection_reason": "该模板记录了 CNI 网络配置失败。",
        "occurrence_count": 3,
        "time_range": {"first_seen": source["window_start"], "last_seen": source["window_end"]},
        "affected_entities": [],
        "source_templates": [dict(source["top_templates"][0])],
        "provider": "ollama",
        "model": "qwen3:1.7b",
    }


def test_same_risk_across_nodes_and_windows_uses_one_approval_group(tmp_path):
    sources = [entity("node-a", "2026-06-22T10:00:00+08:00"), entity("node-b", "2026-06-22T11:00:00+08:00")]
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
        rule_store=ApprovedRuleStore(tmp_path / "rules.json"),
        auto_start=False,
    )
    job_id = manager.create_job({"summary": {}, "risk_entities": sources}, model="qwen3:1.7b")

    manager.run_job(job_id)
    features = manager.get_job(job_id)["features"]

    assert {feature["problem_code"] for feature in features} == {"kubernetes.cni.ip_exhaustion"}
    assert len({feature["approval_key"] for feature in features}) == 1
    groups = manager.list_approval_groups()
    assert len(groups) == 1
    assert groups[0]["candidate_count"] == 2
    assert groups[0]["occurrence_count"] == 6


def test_rule_approved_after_job_creation_is_reused_before_model_call(tmp_path):
    calls = []
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    source = entity("node-a", "2026-06-22T10:00:00+08:00")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: calls.append(source) or [],
        rule_store=rules,
        auto_start=False,
    )
    job_id = manager.create_job({"summary": {}, "risk_entities": [source]}, model="qwen3:1.7b")

    rules.upsert_feature(candidate(source, "seed-candidate"))
    manager.run_job(job_id)

    snapshot = manager.get_job(job_id)
    assert calls == []
    assert snapshot["entities"][0]["status"] == "rule_matched"


def test_candidate_created_after_rule_approval_is_auto_resolved(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    source = entity("node-a", "2026-06-22T10:00:00+08:00")
    created = {"value": False}

    def extractor(current, **kwargs):
        if not created["value"]:
            rules.upsert_feature(candidate(current, "approved-seed"))
            created["value"] = True
        return [candidate(current, "candidate-a")]

    manager = FeatureJobManager(extractor=extractor, rule_store=rules, auto_start=False)
    job_id = manager.create_job({"summary": {}, "risk_entities": [source]}, model="qwen3:1.7b")
    manager.run_job(job_id)

    feature = manager.get_job(job_id)["features"][0]
    assert feature["status"] == "approved"
    assert feature["resolution_type"] == "group_matched"


def test_approval_reconciles_pending_duplicates_across_jobs(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
        rule_store=rules,
        auto_start=False,
    )
    first = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    second = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-b", "2026-06-22T11:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    manager.run_job(first)
    manager.run_job(second)

    approved = manager.update_feature(first, "candidate-node-a", {"status": "approved"})
    resolved = manager.get_job(second)["features"][0]

    assert approved["status"] == "approved"
    assert approved["auto_resolved_count"] == 1
    assert resolved["status"] == "approved"
    assert resolved["resolution_type"] == "group_matched"
    assert resolved["resolved_rule_id"] == approved["rule_id"]
    assert resolved["duplicate_of"] == "candidate-node-a"


def test_approval_groups_and_candidate_identity_survive_sqlite_restart(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    persistence = SQLiteFeatureJobStore(database)
    groups = SQLiteApprovalGroupStore(database)
    rules = SQLiteApprovedRuleStore(database)
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
        rule_store=rules,
        approval_group_store=groups,
        persistence=persistence,
        auto_start=False,
        interrupt_on_restore=False,
    )
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    restored = FeatureJobManager(
        extractor=lambda source, **kwargs: [],
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=SQLiteApprovalGroupStore(database),
        persistence=SQLiteFeatureJobStore(database),
        auto_start=False,
        interrupt_on_restore=False,
    )

    assert restored.list_approval_groups()[0]["candidate_count"] == 1
    assert restored.get_job(job_id)["features"][0]["approval_key"].startswith("appr_")


def test_restart_backfills_pending_candidate_against_existing_rule(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    source = entity("node-a", "2026-06-22T10:00:00+08:00")
    persistence = SQLiteFeatureJobStore(database)
    groups = SQLiteApprovalGroupStore(database)
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, "candidate-a")],
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=groups,
        persistence=persistence,
        auto_start=False,
        interrupt_on_restore=False,
    )
    job_id = manager.create_job({"summary": {}, "risk_entities": [source]}, model="qwen3:1.7b")
    manager.run_job(job_id)

    SQLiteApprovedRuleStore(database).upsert_feature(candidate(source, "approved-seed"))

    restored = FeatureJobManager(
        extractor=lambda source, **kwargs: [],
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=SQLiteApprovalGroupStore(database),
        persistence=SQLiteFeatureJobStore(database),
        auto_start=False,
        interrupt_on_restore=False,
    )

    feature = restored.get_job(job_id)["features"][0]
    assert feature["status"] == "approved"
    assert feature["resolution_type"] == "group_matched"
