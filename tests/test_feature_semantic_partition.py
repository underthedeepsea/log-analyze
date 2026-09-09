from __future__ import annotations

import copy

from logrisk.approval_dedup import approval_identity
from logrisk.feature_extractor_ollama import _attach_source_facts, _candidate_id
from logrisk.feature_semantic_partition import (
    ProblemPresentation,
    partition_feature_by_semantics,
    problem_presentation,
)
from logrisk.problem_resolver import concrete_problem_codes


def test_same_selected_evidence_ignores_model_naming_after_partition():
    templates = [
        {"template_hash": "timeout", "component": "kubelet", "template": "PullImage failed: i/o timeout"},
        {"template_hash": "wrapper", "component": "kubelet", "template": "ImagePullBackOff"},
    ]
    identities = []
    for name in ("kubelet_image_pull_failure", "kubelet_image_pull_timeout"):
        children = partition_feature_by_semantics({"top_templates": templates}, {
            "feature_type": name, "template_hashes": ["timeout", "wrapper"], "importance": "high",
        })
        identities.append([
            approval_identity({**child, "source_templates": [
                t for t in templates if t["template_hash"] in child["template_hashes"]
            ]}) for child in children
        ])
    assert [i["approval_key"] for i in identities[0]] == [i["approval_key"] for i in identities[1]]
    assert [i["semantic_safe"] for i in identities[0]] == [True, False]


def test_partition_preserves_missing_evidence_and_rollback(monkeypatch):
    feature = {"feature_type": "mixed", "template_hashes": ["known", "missing"]}
    entity = {"top_templates": [{"template_hash": "known", "template": "CrashLoopBackOff"}]}
    original = copy.deepcopy(feature)
    assert partition_feature_by_semantics(entity, feature) == [original]
    feature["template_hashes"] = ["known"]
    monkeypatch.setenv("LOGRISK_SEMANTIC_RESOLVER_ENABLED", "false")
    assert partition_feature_by_semantics(entity, feature) == [feature]


def test_inseparable_conflict_stays_in_one_conservative_residual():
    child, = partition_feature_by_semantics({"top_templates": [{
        "template_hash": "conflict", "component": "kubelet",
        "template": "failed to pull image: unauthorized and manifest unknown",
    }]}, {"template_hashes": ["conflict"], "summary": "节点资源受限"})
    assert child["template_hashes"] == ["conflict"]
    assert child["feature_type"] == "unresolved_template_evidence"
    assert "节点资源受限" not in child["summary"]


def test_image_pull_candidate_does_not_count_orphaned_pod_evidence():
    entity = {"entity_id": "node-a", "entity_type": "node", "top_templates": [
        {"template_hash": "orphan", "component": "kubelet", "count": 1800,
         "template": "Orphaned pod <UUID> found, but volume subpaths are still present on disk"},
        {"template_hash": "backoff", "component": "kubelet", "count": 265,
         "template": "ImagePullBackOff"},
        {"template_hash": "transport", "component": "kubelet", "count": 36,
         "template": "PullImage failed Get https //registry.example/v2/ EOF"},
    ]}
    children = [_attach_source_facts(entity, child, "fake", "ollama") for child in
                partition_feature_by_semantics(entity, {
                    "feature_type": "kubelet_image_pull_failure", "importance": "high",
                    "template_hashes": ["orphan", "backoff", "transport"],
                })]
    assert [child["occurrence_count"] for child in children] == [1800, 265, 36]
    assert len({h for c in children for h in c["template_hashes"]}) == 3
    assert children[2]["problem_code"] == "kubernetes.image.pull_transport_failure"
    assert children[1]["semantic_safe"] is False


def test_every_concrete_code_has_presentation():
    for problem_code in concrete_problem_codes():
        presentation = problem_presentation(problem_code)
        assert presentation is not None
        assert isinstance(presentation, ProblemPresentation)


def test_partition_splits_two_high_confidence_semantics():
    entity = {
        "top_templates": [
            {
                "template_hash": "hash-crash",
                "component": "kubelet",
                "template": "CrashLoopBackOff",
            },
            {
                "template_hash": "hash-stats",
                "component": "kubelet",
                "template": "Failed to get system container stats",
            },
        ],
    }
    feature = {
        "feature_type": "mixed_kubelet_failure",
        "title": "混合异常",
        "summary": "检测到多个异常。",
        "importance": "high",
        "template_hashes": ["hash-crash", "hash-stats"],
        "components": ["kubelet"],
        "tags": ["Kubelet"],
        "selection_reason": "模型选择了两个异常模板。",
    }

    children = partition_feature_by_semantics(entity, feature)

    assert len(children) == 2
    assert {tuple(child["template_hashes"]) for child in children} == {
        ("hash-crash",),
        ("hash-stats",),
    }
    assert {child["feature_type"] for child in children} == {
        "kubelet_pod_crash_loop",
        "kubelet_container_stats_failure",
    }


def test_partition_isolates_unresolved_evidence_without_losing_hashes():
    entity = {
        "top_templates": [
            {
                "template_hash": "known",
                "component": "kubelet",
                "template": "CrashLoopBackOff",
            },
            {
                "template_hash": "unknown",
                "component": "kubelet",
                "template": "opaque vendor runtime failure",
            },
        ],
    }
    feature = {
        "feature_type": "mixed_runtime_failure",
        "title": "运行时异常",
        "summary": "检测到运行时异常。",
        "importance": "high",
        "template_hashes": ["known", "unknown"],
        "components": ["kubelet"],
        "tags": ["运行时"],
        "selection_reason": "模型选择异常模板。",
    }

    children = partition_feature_by_semantics(entity, feature)

    assert [child["template_hashes"] for child in children] == [["known"], ["unknown"]]
    assert children[1]["feature_type"] == "unresolved_template_evidence"
    assert "待复核" in children[1]["title"]
    assert feature["template_hashes"] == ["known", "unknown"]


def test_candidate_id_differs_for_mixed_feature_and_each_semantic_child():
    entity = {
        "cluster": "kubernetes",
        "entity_type": "node",
        "entity_id": "node-a",
        "window_start": "2026-09-04T00:00:00Z",
        "top_templates": [
            {
                "template_hash": "hash-crash",
                "component": "kubelet",
                "template": "CrashLoopBackOff",
            },
            {
                "template_hash": "hash-stats",
                "component": "kubelet",
                "template": "Failed to get system container stats",
            },
        ],
    }
    feature = {
        "feature_type": "mixed_kubelet_failure",
        "template_hashes": ["hash-crash", "hash-stats"],
    }

    children = partition_feature_by_semantics(entity, feature)
    original_id = _candidate_id(entity, feature)
    child_ids = [_candidate_id(entity, child) for child in children]

    assert all(child_id != original_id for child_id in child_ids)
    assert len(set(child_ids)) == len(children)


def test_cache_variants_converge_and_subpath_evidence_splits():
    identities = []
    for index in range(5):
        templates = [{
            "template_hash": f"cache-{index}", "component": "kubelet", "count": 2,
            "template": 'Partial failure issuing cadvisor.ContainerInfoV2 partial failures '
                        f'["/sanitized-pod-{index}.slice" RecentStats unable to find data in memory cache]',
        }]
        if index == 2:
            templates.append({
                "template_hash": "subpath", "component": "kubelet", "count": 3,
                "template": "error cleaning subPath mounts for volume <*> "
                            "could not get consistent content of /proc/mounts after <NUM> attempts",
            })
        children = partition_feature_by_semantics({"top_templates": templates}, {
            "feature_type": "unresolved_template_evidence", "importance": "medium",
            "template_hashes": [t["template_hash"] for t in templates],
        })
        assert [h for child in children for h in child["template_hashes"]] == [
            t["template_hash"] for t in templates
        ]
        assert len(children) == len(templates)
        for child in children:
            child["source_templates"] = [
                t for t in templates if t["template_hash"] in child["template_hashes"]
            ]
            identity = approval_identity(child)
            assert identity["semantic_safe"] is True
            if child["feature_type"] == "runtime_cadvisor_cache_miss":
                identities.append(identity["approval_key"])
            else:
                assert child["feature_type"] == "volume_subpath_cleanup_failure"
    assert len(identities) == 5
    assert len(set(identities)) == 1
