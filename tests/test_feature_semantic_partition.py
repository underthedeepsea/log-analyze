from __future__ import annotations

from logrisk.feature_semantic_partition import partition_feature_by_semantics


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


def test_partition_keeps_feature_when_any_selected_template_is_unresolved():
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

    assert children == [feature]
