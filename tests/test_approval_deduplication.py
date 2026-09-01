from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from logrisk.approval_dedup import (
    approval_identity,
    build_approval_key,
    derive_problem_code,
    normalize_problem_code,
    same_approval_identity,
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
