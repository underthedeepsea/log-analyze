from __future__ import annotations

import hashlib

import pytest

from logrisk.approval_dedup import approval_identity, build_approval_key, derive_problem_code
from logrisk.problem_resolver import ProblemResolution, resolve_problem


def selected(template: str, *, category: str = "runtime", component: str = "kubelet") -> dict:
    return {
        "template_hash": f"selected-{hashlib.sha256(template.encode('utf-8')).hexdigest()[:8]}",
        "template": template,
        "category": category,
        "component": component,
    }


def test_problem_resolution_is_frozen_metadata():
    resolution = ProblemResolution(
        problem_code="kubernetes.pod.crash_loop",
        confidence="high",
        semantic_safe=True,
        ambiguity=False,
        evidence_source="selected_template_pattern",
        matched_rule="crash_loop_v1",
        supporting_codes=("kubernetes.pod.crash_loop",),
    )

    with pytest.raises(AttributeError):
        resolution.problem_code = "changed"


def test_t1_same_container_stats_semantics_ignore_wrapper_and_presentation():
    left = {
        "feature_type": "container_runtime_signal",
        "title": "kubelet observation",
        "summary": "system container statistics unavailable",
        "source_templates": [selected("Failed to get system container stats")],
    }
    right = {
        "feature_type": "runtime_health_failure",
        "title": "containerd observation",
        "summary": "different wording",
        "source_templates": [selected("failed to get container info")],
    }

    assert resolve_problem(left).problem_code == "kubernetes.runtime.container_stats_failure"
    assert resolve_problem(right).problem_code == "kubernetes.runtime.container_stats_failure"
    assert resolve_problem(left).semantic_safe is True
    assert approval_identity(left)["approval_key"] == approval_identity(right)["approval_key"]


def test_container_stats_operation_keeps_stats_as_primary_and_not_found_as_subtype():
    feature = {
        "feature_type": "kubelet_container_stats_error",
        "source_templates": [selected(
            "Failed to get system container stats: "
            "failed to get container info: unknown container"
        )],
    }

    resolution = resolve_problem(feature)

    assert resolution.problem_code == "kubernetes.runtime.container_stats_failure"
    assert resolution.subtype == "container_not_found"
    assert resolution.semantic_safe is True
    assert resolution.ambiguity is False


def test_standalone_no_such_container_remains_container_not_found():
    feature = {
        "feature_type": "kubelet_container_runtime_error",
        "source_templates": [selected(
            'ContainerStatus "<id>" from runtime service failed: '
            "rpc error: code = Unknown desc = No such container"
        )],
    }

    resolution = resolve_problem(feature)

    assert resolution.problem_code == "kubernetes.runtime.container_not_found"
    assert resolution.subtype is None
    assert resolution.semantic_safe is True


def test_t2_selected_ip_evidence_wins_over_summary_pollution():
    feature = {
        "feature_type": "cni_network_failure",
        "summary": "CNI configuration error",
        "source_templates": [selected("NetworkPlugin cni failed: no enough ips", category="network")],
    }

    resolution = resolve_problem(feature)

    assert resolution.problem_code == "kubernetes.cni.ip_exhaustion"
    assert resolution.semantic_safe is True
    assert resolution.confidence == "high"


def test_t3_entity_context_cannot_pollute_selected_feature_identity():
    feature = {
        "feature_type": "container_runtime_signal",
        "source_templates": [selected("Failed to get system container stats")],
    }
    entity = {
        "top_templates": [selected("CNI teardown failed: WorkloadEndpoint not found", category="network")],
    }

    resolution = resolve_problem(feature, entity)

    assert resolution.problem_code == "kubernetes.runtime.container_stats_failure"
    assert "kubernetes.cni.workload_endpoint_not_found" not in resolution.supporting_codes
    assert approval_identity(feature, entity)["problem_code"] == "kubernetes.runtime.container_stats_failure"


def test_t4_generic_cni_wrapper_is_not_semantic_safe():
    feature = {
        "feature_type": "cni_network_failure",
        "source_templates": [selected("CNI plugin failed", category="network")],
    }

    resolution = resolve_problem(feature)
    identity = approval_identity(feature)

    assert resolution.problem_code == "kubernetes.cni.plugin_failure"
    assert resolution.semantic_safe is False
    assert identity["match_mode"] == "template_set"


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ("CNI no enough ips", "kubernetes.cni.ip_exhaustion"),
        ("CNI config syntax error", "kubernetes.cni.config_error"),
        ("CNI WorkloadEndpoint not found", "kubernetes.cni.workload_endpoint_not_found"),
        ("CNI delete not supported", "kubernetes.cni.delete_not_supported"),
    ],
)
def test_t5_cni_concrete_causes_are_distinct(template: str, expected: str):
    resolution = resolve_problem({"source_templates": [selected(template, category="network")]})

    assert resolution.problem_code == expected
    assert resolution.semantic_safe is True


def test_t6_crashloop_wrappers_share_one_semantic_identity():
    left = resolve_problem({"source_templates": [selected("CrashLoopBackOff", category="pod")]})
    right = resolve_problem({"source_templates": [selected("Back-off restarting failed container", category="pod")]})

    assert left.problem_code == right.problem_code == "kubernetes.pod.crash_loop"
    assert left.semantic_safe is True and right.semantic_safe is True


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ("failed to pull image: connection reset by peer", "kubernetes.image.pull_transport_failure"),
        ("failed to pull image: unauthorized", "kubernetes.image.pull_unauthorized"),
        ("failed to pull image: manifest unknown", "kubernetes.image.pull_not_found"),
    ],
)
def test_t7_image_pull_root_causes_do_not_merge(template: str, expected: str):
    resolution = resolve_problem({"source_templates": [selected(template, category="image")]})

    assert resolution.problem_code == expected
    assert resolution.semantic_safe is True


def test_t8_mixed_selected_semantics_are_ambiguous_and_fallback():
    feature = {
        "feature_type": "mixed_runtime_failure",
        "source_templates": [
            selected("unknown container", category="runtime"),
            selected("unable to find data in memory cache", category="runtime"),
        ],
    }

    resolution = resolve_problem(feature)
    identity = approval_identity(feature)

    assert resolution.problem_code is None
    assert set(resolution.supporting_codes) == {
        "kubernetes.runtime.container_not_found",
        "kubernetes.runtime.cadvisor_cache_miss",
    }
    assert resolution.ambiguity is True
    assert resolution.semantic_safe is False
    assert identity["problem_code"].startswith("logrisk.mixed_runtime_failure.")
    assert identity["match_mode"] == "template_set"


def test_t9_unknown_selected_pattern_uses_strict_fallback():
    feature = {
        "feature_type": "runtime_failure",
        "source_templates": [selected("unclassified runtime condition")],
    }

    resolution = resolve_problem(feature)
    identity = approval_identity(feature)

    assert resolution.problem_code is None
    assert resolution.confidence == "low"
    assert resolution.evidence_source == "fallback"
    assert resolution.semantic_safe is False
    assert identity["problem_code"].startswith("logrisk.runtime_failure.")
    assert identity["match_mode"] == "template_set"


def test_unregistered_dotted_structured_code_uses_strict_fallback():
    feature = {
        "feature_type": "runtime_failure",
        "problem_code": "vendor.some_issue",
        "components": ["kubelet"],
        "source_templates": [selected("opaque vendor condition")],
    }

    resolution = resolve_problem(feature)
    identity = approval_identity(feature)

    assert resolution.problem_code is None
    assert resolution.semantic_safe is False
    assert resolution.supporting_codes == ()
    assert identity["problem_code"].startswith("logrisk.runtime_failure.")
    assert identity["match_mode"] == "template_set"


def test_legacy_node_memory_pressure_alias_remains_resolvable():
    feature = {
        "feature_type": "resource_pressure",
        "problem_code": "k8s_node_memory_pressure",
    }

    resolution = resolve_problem(feature)

    assert resolution.problem_code == "kubernetes.node.memory_pressure"
    assert resolution.semantic_safe is True
    assert derive_problem_code(feature) == "kubernetes.node.memory_pressure"


def test_selected_oom_uses_resolver_safety_for_approval_key():
    feature = {
        "feature_type": "runtime_sandbox_failure",
        "components": ["kubelet"],
        "source_templates": [selected("pod sandbox failed: out of memory")],
    }

    resolution = resolve_problem(feature)
    identity = approval_identity(feature)

    assert resolution.problem_code == "linux.memory.oom"
    assert resolution.semantic_safe is True
    assert identity["match_mode"] == "semantic"
    assert build_approval_key(
        "runtime_sandbox_failure", "linux.memory.oom", ["kubelet"], ["anchor"], semantic_safe=False,
    ) != build_approval_key(
        "runtime_sandbox_failure", "linux.memory.oom", ["kubelet"], ["anchor"], semantic_safe=True,
    )


def test_semantic_resolver_switch_restores_strict_rollback_identity(monkeypatch):
    feature = {
        "feature_type": "runtime_failure",
        "source_templates": [selected("Out of memory: Killed process <*>")],
    }
    enabled_identity = approval_identity(feature)

    monkeypatch.setenv("LOGRISK_SEMANTIC_RESOLVER_ENABLED", "false")
    disabled_identity = approval_identity(feature)

    assert enabled_identity["match_mode"] == "semantic"
    assert disabled_identity["problem_code"] == "linux.memory.oom"
    assert disabled_identity["match_mode"] == "template_set"
    assert disabled_identity["semantic_safe"] is False
    assert disabled_identity["resolution_source"] == "rollback_legacy"
    assert disabled_identity["approval_key"] != enabled_identity["approval_key"]
