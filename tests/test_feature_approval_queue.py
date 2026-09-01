from __future__ import annotations

from dataclasses import replace
import importlib
from pathlib import Path


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
