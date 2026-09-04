from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Mapping

from logrisk.problem_resolver import concrete_problem_codes, resolve_selected_template


@dataclass(frozen=True)
class ProblemPresentation:
    feature_type: str
    title: str
    tags: tuple[str, ...]


_PROBLEM_PRESENTATIONS = {
    "kubernetes.cni.config_error": ProblemPresentation(
        feature_type="cni_config_error",
        title="CNI 网络配置错误日志",
        tags=("CNI", "配置错误"),
    ),
    "kubernetes.cni.delete_not_supported": ProblemPresentation(
        feature_type="cni_delete_not_supported",
        title="CNI 删除操作不支持日志",
        tags=("CNI", "删除不支持"),
    ),
    "kubernetes.cni.ip_exhaustion": ProblemPresentation(
        feature_type="cni_ip_exhaustion",
        title="CNI IP 地址耗尽日志",
        tags=("CNI", "IP 耗尽"),
    ),
    "kubernetes.cni.workload_endpoint_not_found": ProblemPresentation(
        feature_type="cni_workload_endpoint_not_found",
        title="CNI 工作负载端点不存在日志",
        tags=("CNI", "端点不存在"),
    ),
    "kubernetes.image.gc_failure": ProblemPresentation(
        feature_type="image_gc_failure",
        title="容器镜像垃圾回收失败日志",
        tags=("容器镜像", "垃圾回收失败"),
    ),
    "kubernetes.image.pull_not_found": ProblemPresentation(
        feature_type="image_pull_not_found",
        title="容器镜像不存在日志",
        tags=("容器镜像", "镜像不存在"),
    ),
    "kubernetes.image.pull_transport_failure": ProblemPresentation(
        feature_type="image_pull_transport_failure",
        title="容器镜像传输失败日志",
        tags=("容器镜像", "传输失败"),
    ),
    "kubernetes.image.pull_unauthorized": ProblemPresentation(
        feature_type="image_pull_unauthorized",
        title="容器镜像拉取未授权日志",
        tags=("容器镜像", "未授权"),
    ),
    "kubernetes.kubelet.checkpoint_resource_not_found": ProblemPresentation(
        feature_type="kubelet_checkpoint_resource_not_found",
        title="Kubelet 检查点资源不存在日志",
        tags=("Kubelet", "检查点资源不存在"),
    ),
    "kubernetes.kubelet.orphaned_pod_residual": ProblemPresentation(
        feature_type="kubelet_orphaned_pod_residual",
        title="Kubelet 孤立 Pod 残留日志",
        tags=("Kubelet", "孤立 Pod"),
    ),
    "kubernetes.node.memory_pressure": ProblemPresentation(
        feature_type="node_memory_pressure",
        title="Kubernetes 节点内存压力日志",
        tags=("Kubernetes", "内存压力"),
    ),
    "kubernetes.pod.crash_loop": ProblemPresentation(
        feature_type="kubelet_pod_crash_loop",
        title="容器反复启动失败日志",
        tags=("Kubernetes", "CrashLoop"),
    ),
    "kubernetes.runtime.cadvisor_cache_miss": ProblemPresentation(
        feature_type="runtime_cadvisor_cache_miss",
        title="容器运行时监控缓存未命中日志",
        tags=("容器运行时", "监控缓存"),
    ),
    "kubernetes.runtime.container_not_found": ProblemPresentation(
        feature_type="kubelet_container_not_found",
        title="容器运行时目标容器不存在日志",
        tags=("容器运行时", "容器不存在"),
    ),
    "kubernetes.runtime.container_stats_failure": ProblemPresentation(
        feature_type="kubelet_container_stats_failure",
        title="容器统计信息获取失败日志",
        tags=("容器运行时", "统计获取失败"),
    ),
    "kubernetes.runtime.exec_process_still_running": ProblemPresentation(
        feature_type="runtime_exec_process_still_running",
        title="容器执行进程仍在运行日志",
        tags=("容器运行时", "执行进程"),
    ),
    "kubernetes.runtime.filesystem_stats_path_missing": ProblemPresentation(
        feature_type="runtime_filesystem_stats_path_missing",
        title="容器文件系统统计路径缺失日志",
        tags=("容器运行时", "文件系统统计"),
    ),
    "kubernetes.volume.subpath_cleanup_failure": ProblemPresentation(
        feature_type="volume_subpath_cleanup_failure",
        title="卷子路径清理失败日志",
        tags=("Kubernetes", "卷清理失败"),
    ),
    "kubernetes.volume.unmount_failure": ProblemPresentation(
        feature_type="volume_unmount_failure",
        title="卷卸载失败日志",
        tags=("Kubernetes", "卷卸载失败"),
    ),
    "linux.memory.oom": ProblemPresentation(
        feature_type="linux_memory_oom",
        title="Linux 内存耗尽日志",
        tags=("Linux", "OOM"),
    ),
}

_SELECTION_REASON = "该候选由语义解析器根据模型选中的脱敏证据模板按单一异常语义拆分生成。"


def problem_presentation(problem_code: str) -> ProblemPresentation | None:
    return _PROBLEM_PRESENTATIONS.get(problem_code)


def _fallback(feature: Any) -> list[dict]:
    if isinstance(feature, Mapping):
        return [copy.deepcopy(dict(feature))]
    return [copy.deepcopy(feature)]


def partition_feature_by_semantics(entity: Any, feature: Any) -> list[dict]:
    if not isinstance(entity, Mapping) or not isinstance(feature, Mapping):
        return _fallback(feature)

    top_templates = entity.get("top_templates") or []
    template_hashes = feature.get("template_hashes") or []
    if not isinstance(top_templates, (list, tuple)) or not isinstance(template_hashes, (list, tuple)):
        return _fallback(feature)

    by_hash = {
        str(item.get("template_hash") or ""): item
        for item in top_templates
        if isinstance(item, Mapping)
    }
    hashes = [str(item) for item in template_hashes]
    sources = [by_hash[item] for item in hashes if item in by_hash]
    if len(sources) != len(hashes) or not hashes:
        return _fallback(feature)

    groups: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for item_hash, source in zip(hashes, sources):
        resolution = resolve_selected_template(source)
        if (
            not resolution.problem_code
            or resolution.confidence != "high"
            or not resolution.semantic_safe
            or resolution.ambiguity
        ):
            return _fallback(feature)
        if resolution.problem_code not in concrete_problem_codes():
            return _fallback(feature)
        presentation = problem_presentation(resolution.problem_code)
        if presentation is None:
            return _fallback(feature)
        groups.setdefault(resolution.problem_code, []).append((item_hash, source))

    if len(groups) <= 1:
        return _fallback(feature)

    children: list[dict] = []
    for problem_code, selected in groups.items():
        presentation = problem_presentation(problem_code)
        if presentation is None:
            return _fallback(feature)
        child_hashes = [item_hash for item_hash, _ in selected]
        templates = [source for _, source in selected]
        child_components = sorted({
            str(item.get("component") or "").strip()
            for item in templates
            if str(item.get("component") or "").strip()
        })
        component_text = "、".join(child_components) or "相关组件"
        children.append({
            "feature_type": presentation.feature_type,
            "title": presentation.title,
            "summary": (
                f"检测到 {component_text} 中与“{presentation.title}”一致的异常证据，"
                f"当前候选引用 {len(child_hashes)} 个脱敏模板。"
            ),
            "importance": copy.deepcopy(feature.get("importance")),
            "template_hashes": child_hashes,
            "components": child_components,
            "tags": list(presentation.tags),
            "selection_reason": _SELECTION_REASON,
        })
    return children
