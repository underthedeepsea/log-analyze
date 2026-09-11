from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest

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
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
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


def test_historical_physical_keys_do_not_override_matching_logical_identity():
    left = {
        "feature_type": "cni_network_failure",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "components": ["kubelet"],
        "anchor_signatures": ["legacy-anchor"],
        "approval_key": "appr-v1-left",
    }
    right = dict(left)
    right["approval_key"] = "appr-v1-right"

    assert same_approval_identity(left, right)
    right["approval_key"] = left["approval_key"]
    assert same_approval_identity(left, right)


def test_stale_physical_key_cannot_bridge_safe_and_unsafe_logical_identities():
    safe = {
        "feature_type": "kubelet_container_stats_failure",
        "approval_key": "appr-old-stats",
        "source_templates": [{
            "template_hash": "stats",
            "component": "kubelet",
            "template": "Failed to get system container stats",
        }],
    }
    unsafe = {
        "feature_type": "mixed_runtime_failure",
        "approval_key": "appr-old-stats",
        "source_templates": [
            dict(safe["source_templates"][0]),
            {
                "template_hash": "opaque",
                "component": "kubelet",
                "template": "opaque vendor cleanup failure",
            },
        ],
    }

    assert approval_identity(safe)["semantic_safe"] is True
    assert approval_identity(unsafe)["semantic_safe"] is False
    assert not same_approval_identity(safe, unsafe)


def test_bodyless_v1_exact_template_identity_survives_semantic_safety_difference():
    rule = {
        "schema_version": "approved_rule_v1",
        "feature_type": "network_failure",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "components": ["kubelet"],
        "template_signatures": [{"template_hash": "legacy-hash", "category": "network"}],
        "anchor_signatures": ["legacy-hash|network"],
        "approval_key": "appr-old-physical",
    }
    candidate = {
        "schema_version": "approved_rule_v1",
        "feature_type": "network_failure",
        "template_hashes": ["legacy-hash"],
        "components": ["kubelet"],
        "source_templates": [{"template_hash": "legacy-hash", "category": "network"}],
        "anchor_signatures": ["legacy-hash|network"],
        "approval_key": "appr-different-physical",
    }

    assert approval_identity(rule)["semantic_safe"] is True
    assert approval_identity(candidate)["semantic_safe"] is False
    assert same_approval_identity(rule, candidate)


def _version_boundary_identity(schema_version=None, *, evaluator_passed=None, candidate_id=None):
    value = {
        "feature_type": "network_failure",
        "problem_code": "kubernetes.cni.ip_exhaustion",
        "components": ["kubelet"],
        "template_hashes": ["shared-template"],
        "source_templates": [{
            "template_hash": "shared-template",
            "category": "network",
            "component": "kubelet",
            "template": "CNI failed: no enough ips",
            "count": 1,
        }],
        "anchor_signatures": ["shared-template|network"],
    }
    if schema_version is not None:
        value["schema_version"] = schema_version
    if evaluator_passed is not None:
        value["evaluator_result"] = {"passed": evaluator_passed}
    if candidate_id is not None:
        value.update({
            "candidate_id": candidate_id,
            "status": "pending",
            "entity": {"type": "node", "id": candidate_id},
        })
    return value


def _version_boundary_rule():
    rule = _version_boundary_identity("approved_rule_v1")
    rule.update({
        "rule_id": "legacy-version-boundary",
        "signature": "legacy-version-boundary-signature",
        "template_signatures": rule.pop("source_templates"),
        "status": "active",
        "approved_at": "2026-06-22T00:00:00+00:00",
        "created_at": "2026-06-22T00:00:00+00:00",
        "updated_at": "2026-06-22T00:00:00+00:00",
        "current_version": 1,
        "next_review_at": "2026-07-22T00:00:00+00:00",
    })
    return rule


def _version_boundary_candidate(source, schema_version, candidate_id, *, evaluator_passed=None):
    value = _version_boundary_identity(
        schema_version,
        evaluator_passed=evaluator_passed,
        candidate_id=candidate_id,
    )
    value.update({
        "cluster": source["cluster"],
        "window_start": source["window_start"],
        "window_end": source["window_end"],
        "risk_score": source["risk_score"],
        "risk_level": source["risk_level"],
        "title": "CNI 网络异常",
        "summary": "CNI 地址池没有可用 IP。",
        "importance": "high",
        "tags": ["cni"],
        "selection_reason": "模板直接记录 CNI 地址耗尽。",
        "occurrence_count": 1,
    })
    return value


def test_shared_identity_rejects_explicit_v1_v2_boundary_even_for_safe_and_failed_candidates():
    rule = _version_boundary_rule()
    safe_v2 = _version_boundary_identity("approved_rule_v2", evaluator_passed=True)
    failed_v2 = _version_boundary_identity("approved_rule_v2", evaluator_passed=False)

    assert not same_approval_identity(rule, safe_v2)
    assert not same_approval_identity(rule, failed_v2)

    unversioned_legacy = _version_boundary_identity()
    assert same_approval_identity(rule, unversioned_legacy)


@pytest.mark.parametrize("missing_side", ["left", "right", "both"])
def test_shared_v1_identity_requires_nonempty_raw_feature_types(missing_side):
    left = _version_boundary_identity("approved_rule_v1")
    right = _version_boundary_identity("approved_rule_v1")
    if missing_side in {"left", "both"}:
        left.pop("feature_type")
    if missing_side in {"right", "both"}:
        right.pop("feature_type")

    assert not same_approval_identity(left, right)


def test_reconcile_does_not_auto_approve_failed_explicit_v2_candidate_for_v1_rule(tmp_path):
    rule = _version_boundary_rule()
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    rules._write_locked([rule])
    source = entity("node-v2", "2026-06-22T10:00:00+08:00")
    manager = FeatureJobManager(
        extractor=lambda current, **kwargs: [_version_boundary_candidate(
            current, "approved_rule_v2", "candidate-v2", evaluator_passed=False,
        )],
        rule_store=rules,
        auto_start=False,
    )
    job_id = manager.create_job({"summary": {}, "risk_entities": [source]}, model="qwen3:1.7b")
    manager.run_job(job_id)

    result = manager.reconcile_pending_candidates(rule)

    assert result["auto_resolved_candidates"] == 0
    assert manager.get_job(job_id)["features"][0]["status"] == "pending"


def test_group_rejection_does_not_reject_failed_explicit_v2_candidate_for_v1_candidate(tmp_path):
    sources = [
        entity("node-v1", "2026-06-22T10:00:00+08:00"),
        entity("node-v2", "2026-06-22T11:00:00+08:00"),
    ]

    def extractor(source, **kwargs):
        schema = "approved_rule_v1" if source["entity_id"] == "node-v1" else "approved_rule_v2"
        return [_version_boundary_candidate(
            source,
            schema,
            f"candidate-{source['entity_id']}",
            evaluator_passed=False if schema == "approved_rule_v2" else None,
        )]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_ids = [
        manager.create_job({"summary": {}, "risk_entities": [source]}, model="qwen3:1.7b")
        for source in sources
    ]
    for job_id in job_ids:
        manager.run_job(job_id)

    manager.update_feature(
        job_ids[0],
        "candidate-node-v1",
        {"status": "rejected", "review_scope": "approval_identity"},
    )

    assert manager.get_job(job_ids[1])["features"][0]["status"] == "pending"


def test_restore_does_not_inherit_approved_v1_group_into_failed_explicit_v2_candidate(tmp_path):
    database = SQLiteDatabase(tmp_path / "state.sqlite3")
    persistence = SQLiteFeatureJobStore(database)
    groups = SQLiteApprovalGroupStore(database)
    rules = SQLiteApprovedRuleStore(database)
    rules._write_locked([_version_boundary_rule()])

    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [_version_boundary_candidate(
            source,
            "approved_rule_v1",
            "candidate-v1",
        )],
        rule_store=rules,
        approval_group_store=groups,
        persistence=persistence,
        auto_start=False,
        interrupt_on_restore=False,
    )
    first_source = entity("node-v1", "2026-06-22T10:00:00+08:00")
    first_source["top_templates"] = [dict(_version_boundary_candidate(
        first_source,
        "approved_rule_v1",
        "source-template",
    )["source_templates"][0])]
    first_job = manager.create_job(
        {"summary": {}, "risk_entities": [first_source]},
        model="qwen3:1.7b",
    )
    manager.run_job(first_job)
    approved = manager.get_job(first_job)["features"][0]
    assert approved["status"] == "approved"

    second_job = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-v2", "2026-06-22T11:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    pending = _version_boundary_candidate(
        entity("node-v2", "2026-06-22T11:00:00+08:00"),
        "approved_rule_v2",
        "candidate-v2",
        evaluator_passed=False,
    )
    pending.update({
        "job_id": second_job,
        "approval_key": approved["approval_key"],
        "approval_group_id": approved["approval_group_id"],
    })
    with manager._lock:
        job = manager._job(second_job)
        job["status"] = "completed"
        job["entities"][0]["status"] = "completed"
        job["entities"][0]["feature_ids"] = ["candidate-v2"]
        job["features"]["candidate-v2"] = pending
        manager.persistence.save(job)

    restored = FeatureJobManager(
        extractor=lambda source, **kwargs: [],
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=SQLiteApprovalGroupStore(database),
        persistence=SQLiteFeatureJobStore(database),
        auto_start=False,
        interrupt_on_restore=False,
    )

    updated = restored.update_feature(second_job, "candidate-v2", {"reviewer_note": "restored"})

    assert updated["status"] == "pending"


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


def stats_candidate(source: dict, candidate_id: str, *, mixed: bool = False) -> dict:
    templates = [{
        "template_hash": "stats-template",
        "category": "runtime",
        "component": "kubelet",
        "template": "Failed to get system container stats",
        "count": 1,
    }]
    if mixed:
        templates.append({
            "template_hash": "opaque-template",
            "category": "runtime",
            "component": "kubelet",
            "template": "opaque vendor cleanup failure",
            "count": 1,
        })
    return {
        **candidate(source, candidate_id),
        "feature_type": "mixed_runtime_failure" if mixed else "kubelet_container_stats_failure",
        "problem_code": "kubernetes.runtime.container_stats_failure",
        "template_hashes": [item["template_hash"] for item in templates],
        "source_templates": templates,
        "components": ["kubelet"],
        "anchor_signatures": ["stats-template|runtime"],
    }


def _stale_stats_manager(tmp_path):
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [stats_candidate(
            source,
            f"candidate-{source['entity_id']}",
            mixed=source["entity_id"] == "unsafe",
        )],
        rule_store=ApprovedRuleStore(tmp_path / "rules.json"),
        auto_start=False,
    )
    safe_job = manager.create_job(
        {"summary": {}, "risk_entities": [entity("safe", "2026-06-22T10:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    unsafe_job = manager.create_job(
        {"summary": {}, "risk_entities": [entity("unsafe", "2026-06-22T11:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    manager.run_job(safe_job)
    manager.run_job(unsafe_job)
    safe = manager._jobs[safe_job]["features"]["candidate-safe"]
    unsafe = manager._jobs[unsafe_job]["features"]["candidate-unsafe"]
    unsafe["approval_key"] = safe["approval_key"]
    return manager, safe_job, unsafe_job


def test_approval_reconciliation_ignores_stale_physical_key_on_unsafe_candidate(tmp_path):
    manager, safe_job, unsafe_job = _stale_stats_manager(tmp_path)

    manager.update_feature(safe_job, "candidate-safe", {"status": "approved"})

    assert manager.get_job(unsafe_job)["features"][0]["status"] == "pending"


def test_group_rejection_ignores_stale_physical_key_on_unsafe_candidate(tmp_path):
    manager, safe_job, unsafe_job = _stale_stats_manager(tmp_path)

    manager.update_feature(
        safe_job,
        "candidate-safe",
        {"status": "rejected", "review_scope": "approval_identity"},
    )

    assert manager.get_job(unsafe_job)["features"][0]["status"] == "pending"


def test_restored_physical_group_cannot_auto_approve_logically_unsafe_candidate(tmp_path):
    manager, safe_job, unsafe_job = _stale_stats_manager(tmp_path)
    approved = manager.update_feature(safe_job, "candidate-safe", {"status": "approved"})
    unsafe = manager._jobs[unsafe_job]["features"]["candidate-unsafe"]
    unsafe.update({
        "status": "pending",
        "approval_key": approved["approval_key"],
        "approval_group_id": approved["approval_group_id"],
    })

    updated = manager.update_feature(
        unsafe_job,
        "candidate-unsafe",
        {"reviewer_note": "restored historical locator"},
    )

    assert updated["status"] == "pending"
    assert updated["resolution_type"] == "manual"


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


def test_approval_reconciliation_persists_every_sibling_and_event_after_restart(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")

    def build_manager():
        return FeatureJobManager(
            extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
            rule_store=SQLiteApprovedRuleStore(database),
            approval_group_store=SQLiteApprovalGroupStore(database),
            persistence=SQLiteFeatureJobStore(database),
            auto_start=False,
            interrupt_on_restore=False,
        )

    manager = build_manager()
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

    approved = manager.update_feature(
        first,
        "candidate-node-a",
        {"status": "approved", "review_scope": "approval_identity"},
    )

    sibling = SQLiteFeatureJobStore(database).load_candidate("candidate-node-b")
    assert sibling["status"] == "approved"
    assert sibling["resolution_type"] == "group_matched"
    assert sibling["resolved_rule_id"] == approved["rule_id"]
    assert any(
        event["type"] == "pending_candidate_reconciled"
        for event in SQLiteFeatureJobStore(database).load_job(second)["events"]
    )

    restored = build_manager()
    assert restored.get_job(second)["features"][0]["status"] == "approved"
    assert restored.list_persisted_candidates(status="pending") == []


def test_duplicate_approval_is_idempotent_without_duplicate_events_or_reuse(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, "candidate-node-a")],
        rule_store=rules,
        auto_start=False,
    )
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    first = manager.update_feature(job_id, "candidate-node-a", {"status": "approved"})
    events_after_first = manager.list_events(job_id)
    second = manager.update_feature(job_id, "candidate-node-a", {"status": "approved"})

    assert second["rule_id"] == first["rule_id"]
    assert len(rules.list_rules()) == 1
    assert rules.list_rules()[0]["reuse_count"] == 0
    assert len(manager.list_events(job_id)) == len(events_after_first)
    assert sum(event["type"] == "feature_updated" for event in manager.list_events(job_id)) == 1


def test_group_reject_leaves_terminal_candidate_unchanged_and_creates_no_rule(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    persistence = SQLiteFeatureJobStore(database)
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=SQLiteApprovalGroupStore(database),
        persistence=persistence,
        auto_start=False,
        interrupt_on_restore=False,
    )
    job_ids = [
        manager.create_job(
            {"summary": {}, "risk_entities": [entity(entity_id, window)]},
            model="qwen3:1.7b",
        )
        for entity_id, window in (
            ("node-a", "2026-06-22T10:00:00+08:00"),
            ("node-b", "2026-06-22T11:00:00+08:00"),
            ("node-c", "2026-06-22T12:00:00+08:00"),
        )
    ]
    for job_id in job_ids:
        manager.run_job(job_id)

    terminal = manager.update_feature(
        job_ids[0], "candidate-node-a", {"status": "rejected", "reviewer_note": "已确认误报"}
    )
    terminal_updated_at = persistence.load_candidate("candidate-node-a")["updated_at"]

    manager.update_feature(
        job_ids[1],
        "candidate-node-b",
        {"status": "rejected", "review_scope": "approval_identity"},
    )

    assert persistence.load_candidate("candidate-node-a")["status"] == "rejected"
    assert persistence.load_candidate("candidate-node-a")["reviewer_note"] == terminal["reviewer_note"]
    assert persistence.load_candidate("candidate-node-a")["updated_at"] == terminal_updated_at
    assert all(
        persistence.load_candidate(f"candidate-node-{node}")["status"] == "rejected"
        for node in ("a", "b", "c")
    )
    assert SQLiteApprovedRuleStore(database).list_rules() == []


def test_candidate_level_reject_keeps_other_pending_candidates_reviewable(tmp_path):
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
        rule_store=ApprovedRuleStore(tmp_path / "rules.json"),
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

    manager.update_feature(first, "candidate-node-a", {"status": "rejected"})

    assert manager.get_job(second)["features"][0]["status"] == "pending"
    assert manager.list_approval_groups()[0]["status"] == "pending"


def test_concurrent_duplicate_approvals_create_one_rule_and_one_event(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    persistence = SQLiteFeatureJobStore(database)
    setup = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, "candidate-node-a")],
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=SQLiteApprovalGroupStore(database),
        persistence=SQLiteFeatureJobStore(database),
        auto_start=False,
        interrupt_on_restore=False,
    )
    job_id = setup.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")]},
        model="qwen3:1.7b",
    )
    setup.run_job(job_id)
    managers = [
        FeatureJobManager(
            extractor=lambda source, **kwargs: [],
            rule_store=SQLiteApprovedRuleStore(database),
            approval_group_store=SQLiteApprovalGroupStore(database),
            persistence=SQLiteFeatureJobStore(database),
            auto_start=False,
            interrupt_on_restore=False,
        )
        for _ in range(2)
    ]

    def approve(manager):
        try:
            return manager.update_feature(job_id, "candidate-node-a", {"status": "approved"})
        except FeatureJobError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(approve, managers))

    assert all(
        not isinstance(result, FeatureJobError)
        or (result.code, result.status_code) == ("candidate_state_conflict", 409)
        for result in results
    )
    assert SQLiteApprovedRuleStore(database).list_rules()[0]["reuse_count"] == 0
    assert len(SQLiteApprovedRuleStore(database).list_rules()) == 1
    assert persistence.load_candidate("candidate-node-a")["status"] == "approved"
    assert sum(
        event["type"] == "feature_updated"
        for event in persistence.load_job(job_id)["events"]
    ) == 1


def test_candidate_and_group_statistics_are_separate_and_aggregate_members():
    from logrisk.approval_queue import build_review_groups

    first = candidate(entity("node-a", "2026-06-22T10:00:00+08:00"), "candidate-a")
    second = candidate(entity("node-b", "2026-06-22T11:00:00+08:00"), "candidate-b")
    second["occurrence_count"] = 4
    other = candidate(entity("node-c", "2026-06-22T12:00:00+08:00"), "candidate-c")
    other["problem_code"] = "OOM"
    other["feature_type"] = "memory_pressure"
    other["anchor_signatures"] = ["oom-anchor"]
    other["template_hashes"] = ["hash-oom"]
    other["source_templates"] = [{"template_hash": "hash-oom", "category": "memory", "template": "Out of memory", "count": 5}]
    groups = build_review_groups([first, second, other])

    cni_group = next(item for item in groups if item["problem_code"] == "kubernetes.cni.ip_exhaustion")
    assert sum(item["candidate_count"] for item in groups) == 3
    assert cni_group["candidate_count"] == 2
    assert cni_group["occurrence_count"] == 7
    assert cni_group["affected_entity_count"] == 2
    assert cni_group["first_seen"] == "2026-06-22T10:00:00+08:00"
    assert cni_group["last_seen"] == "2026-06-22T11:05:00+08:00"


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

    crashed_snapshot = persistence.load_job(job_id)
    crashed_snapshot["entities"][0]["feature_ids"] = []
    persistence.save(crashed_snapshot)
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


def test_unresolved_identity_is_stable_after_persistence_and_repeated_reads():
    from logrisk.approval_dedup import approval_identity

    feature = {
        "feature_type": "unresolved_template_evidence",
        "title": "未解析证据", "components": ["kubelet"],
        "template_hashes": ["unknown-a"],
        "source_templates": [{"template_hash": "unknown-a", "component": "kubelet",
                              "template": "Unclassified runtime operation failed"}],
    }
    original = approval_identity(feature)
    assert original["semantic_safe"] is False
    for _ in range(4):
        feature.update(approval_identity(feature))
        assert feature["approval_key"] == original["approval_key"]
        assert feature["problem_code"] == original["problem_code"]
    different = {**feature, "template_hashes": ["unknown-b"], "anchor_signatures": [],
                 "source_templates": [{"template_hash": "unknown-b", "component": "kubelet",
                                       "template": "different unclassified condition"}]}
    assert approval_identity(different)["approval_key"] != original["approval_key"]


def test_conflicting_identity_is_stable_without_losing_conflict_gate():
    from logrisk.approval_dedup import approval_identity

    feature = {
        "feature_type": "unresolved_template_evidence", "components": ["kubelet"],
        "template_hashes": ["conflict"],
        "source_templates": [{"template_hash": "conflict", "component": "kubelet",
                              "template": "failed to pull image: unauthorized and manifest unknown"}],
    }
    original = approval_identity(feature)
    for _ in range(3):
        feature.update(approval_identity(feature))
        assert feature["approval_key"] == original["approval_key"]
        assert feature["semantic_safe"] is False
        assert feature["ambiguity"] is True
        assert set(feature["supporting_codes"]) == {
            "kubernetes.image.pull_unauthorized", "kubernetes.image.pull_not_found",
        }
