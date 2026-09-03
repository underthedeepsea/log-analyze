from __future__ import annotations

from dataclasses import replace
import importlib
from pathlib import Path
from types import SimpleNamespace

from logrisk.approval_dedup import approval_identity


def test_persisted_candidates_from_different_jobs_form_one_semantic_review_group():
    build_review_groups = importlib.import_module("logrisk.approval_queue").build_review_groups
    candidates = [
        {
            "candidate_id": "candidate-a",
            "job_id": "job-a",
            "feature_type": "cni_network_failure",
            "problem_code": "runtime_cni_setup_failed",
            "components": ["kubelet"],
            "status": "pending",
            "risk_score": 80,
            "importance": "high",
            "entity": {"type": "node", "id": "node-a"},
            "cluster": "prod-a",
            "source_templates": [
                {"component": "kubelet", "template": "NetworkPlugin cni failed: no enough ips", "count": 3},
            ],
        },
        {
            "candidate_id": "candidate-b",
            "job_id": "job-b",
            "feature_type": "pod_sandbox_network_failure",
            "problem_code": "runtime_sandbox_create_failed",
            "components": ["containerd"],
            "status": "pending",
            "risk_score": 70,
            "importance": "high",
            "entity": {"type": "node", "id": "node-b"},
            "cluster": "prod-a",
            "source_templates": [
                {"component": "containerd", "template": "CreatePodSandbox failed: cni no enough ips", "count": 4},
            ],
        },
    ]

    groups = build_review_groups(candidates)

    assert len(groups) == 1
    assert groups[0]["candidate_count"] == 2
    assert groups[0]["occurrence_count"] == 7
    assert groups[0]["affected_entity_count"] == 2
    assert groups[0]["review_key"] == "semantic:kubernetes.cni.ip_exhaustion"


def test_facade_restores_pending_queue_after_application_rebuild(tmp_path):
    from logrisk.application import ApplicationConfig, build_application_container
    from logrisk.application.api import ApiFacade

    project_root = Path(__file__).resolve().parents[1]
    config = replace(
        ApplicationConfig.for_test(project_root=project_root, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
        interrupt_feature_jobs=False,
    )
    first = build_application_container(config)

    def source(entity_id, component, template, problem_code, feature_type):
        return {
            "window_start": "2026-09-01T00:00:00+00:00",
            "window_end": "2026-09-01T00:05:00+00:00",
            "cluster": "prod-a",
            "entity_type": "node",
            "entity_id": entity_id,
            "risk_score": 90,
            "risk_level": "high",
            "top_templates": [{
                "template_hash": "hash-" + entity_id,
                "template_fingerprint": "fingerprint-" + entity_id,
                "category": "network",
                "component": component,
                "template": template,
                "count": 2,
            }],
            "affected_entities": [],
            "test_problem_code": problem_code,
            "test_feature_type": feature_type,
        }

    def extractor(current, **kwargs):
        template = current["top_templates"][0]
        return [{
            "candidate_id": "candidate-" + current["entity_id"],
            "status": "pending",
            "cluster": current["cluster"],
            "entity": {"type": "node", "id": current["entity_id"]},
            "window_start": current["window_start"],
            "window_end": current["window_end"],
            "risk_score": current["risk_score"],
            "feature_type": current["test_feature_type"],
            "problem_code": current["test_problem_code"],
            "title": "CNI 网络风险",
            "summary": "CNI 地址池没有可用 IP。",
            "importance": "high",
            "components": [template["component"]],
            "source_templates": [dict(template)],
            "occurrence_count": 2,
        }]

    first.feature_jobs.extractor = extractor
    for item in (
        source("node-a", "kubelet", "NetworkPlugin cni failed: no enough ips", "runtime_cni_setup_failed", "cni_network_failure"),
        source("node-b", "containerd", "CreatePodSandbox failed: cni no enough ips", "runtime_sandbox_create_failed", "pod_sandbox_network_failure"),
    ):
        job_id = first.feature_jobs.create_job({"summary": {}, "risk_entities": [item]}, model="qwen3.5:4b-mlx")
        first.feature_jobs.run_job(job_id)

    rebuilt = build_application_container(config)
    response = ApiFacade(rebuilt, version="1.36.1").dispatch_read(
        "/api/feature-approvals",
        {"status": "pending", "page_size": "100"},
    )

    assert response.status == 200
    assert response.body["total_groups"] == 1
    assert response.body["total_candidates"] == 2
    assert response.body["items"][0]["review_key"] == "semantic:kubernetes.cni.ip_exhaustion"
    assert response.body["items"][0]["representative"]["model"] == "qwen3.5:4b-mlx"


def test_v1_and_fallback_candidates_keep_strict_logical_identity():
    build_review_groups = importlib.import_module("logrisk.approval_queue").build_review_groups
    candidates = [
        {
            "candidate_id": "legacy-a",
            "job_id": "job-a",
            "approval_group_id": "physical-group-a",
            "approval_key": "legacy-approval-a",
            "schema_version": "approved_rule_v1",
            "feature_type": "network_failure",
            "problem_code": "unknown_problem_code",
            "components": ["kubelet"],
            "anchor_signatures": ["network-anchor-a"],
            "status": "pending",
            "entity": {"type": "node", "id": "node-a"},
        },
        {
            "candidate_id": "legacy-b",
            "job_id": "job-b",
            "approval_group_id": "physical-group-b",
            "approval_key": "legacy-approval-b",
            "schema_version": "approved_rule_v1",
            "feature_type": "network_failure",
            "problem_code": "unknown_problem_code",
            "components": ["kubelet"],
            "anchor_signatures": ["network-anchor-b"],
            "status": "pending",
            "entity": {"type": "node", "id": "node-b"},
        },
    ]

    groups = build_review_groups(candidates)

    assert [group["candidate_ids"] for group in groups] == [["legacy-a"], ["legacy-b"]]
    assert {group["representative"]["approval_group_id"] for group in groups} == {
        "physical-group-a",
        "physical-group-b",
    }


def test_queue_paginates_groups_after_full_candidate_enumeration():
    class CandidateSource:
        def __init__(self, candidates):
            self.candidates = candidates
            self.limits = []

        def list_persisted_candidates(self, *, status, limit):
            self.limits.append(limit)
            return self.candidates

    candidates = [
        {
            "candidate_id": f"candidate-{index:03d}",
            "job_id": f"job-{index:03d}",
            "problem_code": f"service.failure.{index:03d}",
            "feature_type": "service_failure",
            "status": "pending",
            "created_at": f"2026-09-01T00:{index // 60:02d}:{index % 60:02d}+00:00",
            "risk_score": index,
            "importance": "medium",
            "entity": {"type": "node", "id": f"node-{index:03d}"},
        }
        for index in range(101)
    ]
    source = CandidateSource(candidates)
    from logrisk.application.api import ApiFacade

    facade = ApiFacade(SimpleNamespace(feature_jobs=source), version="1.36.1")

    first = facade.feature_approvals({"status": "pending", "page_size": "2"})
    second = facade.feature_approvals({"status": "pending", "page_size": "2", "cursor": "2"})

    assert source.limits == [None, None]
    assert first.body["total_groups"] == 101
    assert first.body["total_candidates"] == 101
    assert len(first.body["items"]) == 2
    assert first.body["next_cursor"] == "2"
    expected_keys = sorted(
        f"approval:{approval_identity(candidate)['approval_key']}"
        for candidate in candidates
    )
    assert [item["review_key"] for item in first.body["items"]] == expected_keys[:2]
    assert [item["review_key"] for item in second.body["items"]] == expected_keys[2:4]
    assert second.body["next_cursor"] == "4"
    assert first.body["metrics"]["logrisk_approval_candidates_total"] == 101
    assert first.body["metrics"]["logrisk_approval_fallback_candidates"] == 101


def test_queue_rejects_invalid_cursor_and_selects_deterministic_representative():
    build_review_groups = importlib.import_module("logrisk.approval_queue").build_review_groups
    candidates = [
        {
            "candidate_id": "later-critical",
            "job_id": "job-2",
            "problem_code": "kubernetes.cni.ip_exhaustion",
            "feature_type": "service_failure",
            "status": "pending",
            "importance": "critical",
            "risk_score": 99,
            "created_at": "2026-09-01T00:01:00+00:00",
            "entity": {"type": "node", "id": "node-2"},
            "source_templates": [{"template_hash": "hash-2", "count": 2}],
            "raw_logs": ["secret raw line"],
        },
        {
            "candidate_id": "earlier-high",
            "job_id": "job-1",
            "problem_code": "kubernetes.cni.ip_exhaustion",
            "feature_type": "service_failure",
            "status": "pending",
            "importance": "high",
            "risk_score": 80,
            "created_at": "2026-09-01T00:00:00+00:00",
            "entity": {"type": "node", "id": "node-1"},
            "source_templates": [{"template_hash": "hash-1", "count": 3}],
        },
    ]

    groups = build_review_groups(candidates)

    assert groups[0]["representative"]["candidate_id"] == "later-critical"
    assert groups[0]["candidate_count"] == 2
    assert groups[0]["occurrence_count"] == 5
    assert "samples" not in groups[0]["representative"]
    assert "raw_sample" not in groups[0]["representative"]
    assert "raw_logs" not in groups[0]["representative"]

    from logrisk.application.api import ApiFacade

    facade = ApiFacade(
        SimpleNamespace(feature_jobs=SimpleNamespace(list_persisted_candidates=lambda **_: candidates)),
        version="1.36.1",
    )
    invalid = facade.feature_approvals({"cursor": "not-a-cursor"})

    assert invalid.status == 422
    assert invalid.body["code"] == "invalid_cursor"


def test_queue_keeps_generic_wrapper_in_template_set_group():
    build_review_groups = importlib.import_module("logrisk.approval_queue").build_review_groups
    candidate = {
        "candidate_id": "generic-wrapper",
        "job_id": "job-generic",
        "feature_type": "cni_network_failure",
        "problem_code": "kubernetes.cni.plugin_failure",
        "components": ["kubelet"],
        "status": "pending",
        "source_templates": [{
            "template_hash": "generic-wrapper-hash",
            "category": "network",
            "component": "kubelet",
            "template": "CNI plugin failed",
        }],
    }

    groups = build_review_groups([candidate])

    assert groups[0]["problem_code"] == "kubernetes.cni.plugin_failure"
    assert groups[0]["match_mode"] == "template_set"
    assert groups[0]["review_key"] == f"approval:{approval_identity(candidate)['approval_key']}"
    assert groups[0]["resolution_confidence"] == "high"
    assert groups[0]["resolution_source"] == "selected_template_pattern"
    assert groups[0]["semantic_safe"] is False
    assert groups[0]["ambiguity"] is False
    assert groups[0]["representative"]["problem_resolution"] == {
        "confidence": "high",
        "semantic_safe": False,
        "ambiguity": False,
        "evidence_source": "selected_template_pattern",
        "matched_rule": "cni_plugin_failure_wrapper_v1",
        "supporting_codes": ["kubernetes.cni.plugin_failure"],
        "subtype": None,
    }


def test_approval_metrics_report_semantic_safe_and_fallback_candidates():
    from logrisk.approval_queue import approval_metrics, build_review_groups

    candidates = [
        {
            "candidate_id": "oom",
            "status": "pending",
            "feature_type": "runtime_failure",
            "source_templates": [{"template": "Out of memory: killed process <*>"}],
        },
        {
            "candidate_id": "generic",
            "status": "pending",
            "feature_type": "cni_network_failure",
            "source_templates": [{"template": "CNI plugin failed"}],
        },
    ]

    groups = build_review_groups(candidates)
    metrics = approval_metrics(candidates, groups)

    assert metrics == {
        "logrisk_approval_candidates_total": 2,
        "logrisk_approval_review_groups": 2,
        "logrisk_approval_canonical_candidates": 1,
        "logrisk_approval_fallback_candidates": 1,
        "logrisk_approval_ambiguous_candidates": 0,
        "logrisk_approval_semantic_safe_candidates": 1,
        "canonical_problem_code_coverage": 0.5,
        "fallback_problem_code_ratio": 0.5,
        "approval_compression_ratio": 0.0,
        "semantic_ambiguity_ratio": 0.0,
    }


def test_queue_preserves_orphaned_volume_subtype_for_reviewers():
    from logrisk.approval_queue import build_review_groups

    groups = build_review_groups([{
        "candidate_id": "orphaned-subpath",
        "status": "pending",
        "feature_type": "pod_cleanup_failure",
        "source_templates": [{
            "template_fingerprint": "fixture-orphaned-subpath",
            "category": "volume",
            "component": "kubelet",
            "template": "volume subpaths still present",
        }],
    }])

    assert groups[0]["resolution_subtype"] == "volume_subpath"
    assert groups[0]["representative"]["problem_resolution"]["subtype"] == "volume_subpath"


def test_queue_subtype_describes_the_selected_representative():
    groups = importlib.import_module("logrisk.approval_queue").build_review_groups([
        {
            "candidate_id": "directory",
            "status": "pending",
            "importance": "critical",
            "created_at": "2026-09-01T00:01:00+00:00",
            "feature_type": "pod_cleanup_failure",
            "source_templates": [{
                "template_fingerprint": "fixture-directory",
                "category": "volume",
                "component": "kubelet",
                "template": "orphaned pod directory not empty",
            }],
        },
        {
            "candidate_id": "path",
            "status": "pending",
            "importance": "low",
            "created_at": "2026-09-01T00:00:00+00:00",
            "feature_type": "pod_cleanup_failure",
            "source_templates": [{
                "template_fingerprint": "fixture-path",
                "category": "volume",
                "component": "kubelet",
                "template": "orphaned pod volume paths still present",
            }],
        },
    ])

    assert groups[0]["representative"]["candidate_id"] == "directory"
    assert groups[0]["resolution_subtype"] == "directory_not_empty"
