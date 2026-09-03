from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class ProblemResolution:
    """Deterministic, sanitized metadata for one approval-identity decision."""

    problem_code: str | None
    confidence: str
    semantic_safe: bool
    ambiguity: bool
    evidence_source: str
    matched_rule: str | None
    supporting_codes: tuple[str, ...]
    subtype: str | None = None


_UNKNOWN_CODES = frozenset({
    "unknown",
    "unknown_cause",
    "unknown_problem",
    "unknown_problem_code",
    "unknown_problem_code",
    "unknown.problem",
    "unclassified",
    "unclassified_cause",
    "unclassified_problem",
    "unclassified_problem_code",
})

_GENERIC_CODES = frozenset({
    "kubernetes.cni.network_failure",
    "kubernetes.cni.plugin_failure",
    "kubernetes.image.pull_failure",
    "kubernetes.runtime.generic_failure",
    "kubernetes.runtime.pod_sandbox_failure",
    "runtime.failure",
    "runtime.sandbox_failure",
})

_ALIASES = {
    "cni_no_ip": "kubernetes.cni.ip_exhaustion",
    "cni_no_enough_ip": "kubernetes.cni.ip_exhaustion",
    "cni_no_enough_ips": "kubernetes.cni.ip_exhaustion",
    "cni_ip_exhaustion": "kubernetes.cni.ip_exhaustion",
    "cni_ip_address_exhaustion": "kubernetes.cni.ip_exhaustion",
    "cni_address_exhaustion": "kubernetes.cni.ip_exhaustion",
    "kubernetes_cni_no_enough_ips": "kubernetes.cni.ip_exhaustion",
    "kubernetes_cni_ip_exhaustion": "kubernetes.cni.ip_exhaustion",
    "runtime_cni_ip_exhaustion": "kubernetes.cni.ip_exhaustion",
    "cni_config_syntax_error": "kubernetes.cni.config_error",
    "cni_network_config_error": "kubernetes.cni.config_error",
    "cni_network_failure": "kubernetes.cni.plugin_failure",
    "runtime_cni_setup_failed": "kubernetes.cni.plugin_failure",
    "kubernetes_cni_plugin_failure": "kubernetes.cni.plugin_failure",
    "runtime_sandbox_create_failed": "kubernetes.runtime.pod_sandbox_failure",
    "k8s_node_memory_pressure": "kubernetes.node.memory_pressure",
    "runtime_sandbox_failure": "kubernetes.runtime.pod_sandbox_failure",
    "pod_sandbox_failure": "kubernetes.runtime.pod_sandbox_failure",
    "container_stats_failure": "kubernetes.runtime.container_stats_failure",
    "container_not_found": "kubernetes.runtime.container_not_found",
    "crash_loop": "kubernetes.pod.crash_loop",
    "crashloopbackoff": "kubernetes.pod.crash_loop",
    "orphaned_pod_residual": "kubernetes.kubelet.orphaned_pod_residual",
    "volume_subpath_cleanup_failure": "kubernetes.volume.subpath_cleanup_failure",
    "volume_unmount_failure": "kubernetes.volume.unmount_failure",
    "image_pull_transport_failure": "kubernetes.image.pull_transport_failure",
    "image_pull_unauthorized": "kubernetes.image.pull_unauthorized",
    "image_pull_not_found": "kubernetes.image.pull_not_found",
    "image_gc_failure": "kubernetes.image.gc_failure",
    "cni_workload_endpoint_not_found": "kubernetes.cni.workload_endpoint_not_found",
    "cni_delete_not_supported": "kubernetes.cni.delete_not_supported",
    "checkpoint_resource_not_found": "kubernetes.kubelet.checkpoint_resource_not_found",
    "exec_process_still_running": "kubernetes.runtime.exec_process_still_running",
    "cadvisor_cache_miss": "kubernetes.runtime.cadvisor_cache_miss",
    "filesystem_stats_path_missing": "kubernetes.runtime.filesystem_stats_path_missing",
    "linux_oom": "linux.memory.oom",
}

_CONCRETE_CODES = frozenset({
    "kubernetes.cni.config_error",
    "kubernetes.cni.delete_not_supported",
    "kubernetes.cni.ip_exhaustion",
    "kubernetes.cni.workload_endpoint_not_found",
    "kubernetes.image.gc_failure",
    "kubernetes.image.pull_not_found",
    "kubernetes.image.pull_transport_failure",
    "kubernetes.image.pull_unauthorized",
    "kubernetes.kubelet.checkpoint_resource_not_found",
    "kubernetes.kubelet.orphaned_pod_residual",
    "kubernetes.node.memory_pressure",
    "kubernetes.pod.crash_loop",
    "kubernetes.runtime.cadvisor_cache_miss",
    "kubernetes.runtime.container_not_found",
    "kubernetes.runtime.container_stats_failure",
    "kubernetes.runtime.exec_process_still_running",
    "kubernetes.runtime.filesystem_stats_path_missing",
    "kubernetes.volume.subpath_cleanup_failure",
    "kubernetes.volume.unmount_failure",
    "linux.memory.oom",
})

_STATS_OPERATION_PATTERN = re.compile(
    r"failed\s+to\s+get\s+(?:system\s+)?container\s+(?:stats|statistics|info)"
    r"|system\s+container\s+stats"
    r"|container\s+stats"
    r"|recentstats",
    re.I,
)

_CONTAINER_NOT_FOUND_PATTERN = re.compile(
    r"(?:unknown|no such|does not exist|not found)\s+container"
    r"|container\s+(?:is\s+)?(?:unknown|not found|does not exist)",
    re.I,
)


@dataclass(frozen=True)
class _Match:
    code: str
    source: str
    confidence: str
    matched_rule: str | None
    subtype: str | None = None


def _alias_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _remove_ip_literals(value: str) -> str:
    patterns = (
        r"(?<![0-9A-Za-z])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9A-Za-z])",
        r"(?<![0-9A-Fa-f])(?:[0-9A-Fa-f]{0,4}:){2,}[0-9A-Fa-f]{0,4}(?![0-9A-Fa-f])",
    )
    result = value
    for pattern in patterns:
        result = re.sub(pattern, "", result)
    return result


def _normalize_code(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = _remove_ip_literals(unicodedata.normalize("NFKC", value).strip().lower())
    alias = _ALIASES.get(_alias_key(text))
    if alias:
        return alias
    parts = []
    for segment in re.split(r"[./:]+", text):
        tokens = re.findall(r"[a-z0-9]+", segment)
        if tokens:
            parts.append("_".join(tokens))
    return ".".join(parts) or None


def _is_unknown(code: str) -> bool:
    return (
        code in _UNKNOWN_CODES
        or code.startswith(("unknown.", "unclassified."))
        or any(
            segment in {"unknown", "unclassified"}
            or segment.startswith(("unknown_", "unclassified_"))
            for segment in code.split(".")
        )
    )


def _is_generic(code: str) -> bool:
    return code in _GENERIC_CODES


def _is_concrete(code: str) -> bool:
    return bool(
        code
        and "." in code
        and code in _CONCRETE_CODES
        and not _is_unknown(code)
        and not code.startswith("logrisk.")
        and not _is_generic(code)
    )


def _iter_values(value: Any) -> Iterable[Any]:
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _iter_values(item)
    else:
        yield value


def _structured_codes(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        code = _normalize_code(value)
        if code and ("." in code or code in _ALIASES.values()):
            yield code
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _structured_codes(item)
        return
    if not isinstance(value, Mapping):
        return
    for key in (
        "problem_code",
        "problemCode",
        "problem_codes",
        "risk_type",
        "cause",
        "root_cause",
        "root_cause_code",
        "risk_semantic",
        "semantic_fields",
    ):
        if key in value:
            yield from _structured_codes(value[key])


def _selected_templates(
    feature: Mapping[str, Any], entity: Mapping[str, Any] | None,
) -> list[Mapping[str, Any]]:
    for field in ("source_templates", "template_signatures"):
        value = feature.get(field)
        if isinstance(value, list) and value:
            return [item for item in value if isinstance(item, Mapping)]

    if feature.get("top_templates") and entity is feature:
        return [item for item in feature["top_templates"] if isinstance(item, Mapping)]

    selected_hashes = {
        str(item).strip()
        for item in _iter_values(feature.get("template_hashes"))
        if str(item).strip()
    }
    if not selected_hashes or not entity:
        return []
    templates = entity.get("top_templates")
    if not isinstance(templates, list):
        return []
    return [
        item for item in templates
        if isinstance(item, Mapping)
        and bool({
            str(item.get("template_hash") or "").strip(),
            str(item.get("template_fingerprint") or "").strip(),
        } & selected_hashes)
    ]


def _structured_matches(
    feature: Mapping[str, Any], sources: Iterable[Mapping[str, Any]],
) -> list[_Match]:
    values: list[Any] = [
        feature.get("problem_code"),
        feature.get("problemCode"),
        feature.get("risk_type"),
        feature.get("cause"),
        feature.get("risk_semantic"),
        feature.get("semantic_fields"),
    ]
    for source in sources:
        values.extend((
            source.get("problem_code"),
            source.get("problemCode"),
            source.get("risk_type"),
            source.get("cause"),
            source.get("risk_semantic"),
            source.get("semantic_fields"),
        ))
    matches: list[_Match] = []
    for value in values:
        for code in _structured_codes(value):
            if not (_is_concrete(code) or _is_generic(code)):
                continue
            matches.append(_Match(
                code=code,
                source="structured_semantic",
                confidence="high",
                matched_rule="structured_problem_code",
            ))
    return _unique_matches(matches)


def _combined_template_text(source: Mapping[str, Any]) -> str:
    return " ".join(
        str(source.get(field) or "")
        for field in ("template", "pattern", "category", "component")
        if source.get(field) is not None
    )


def _cni_context(text: str) -> bool:
    return bool(re.search(
        r"\bcni(?:\b|_)|network[-\s]+plugin|networkplugin|"
        r"network\s+(?:config(?:uration)?|setup)|"
        r"network\s+(?:for|in|on)\s+(?:the\s+)?(?:pod\s+)?sandbox|"
        r"(?:pod\s+)?sandbox[-\s]+(?:network|cni)|"
        r"workloadendpoint|网络配置|网络插件|网络.{0,20}沙箱|沙箱.{0,20}网络",
        text,
    ))


def _template_matches(source: Mapping[str, Any]) -> list[_Match]:
    text = unicodedata.normalize("NFKC", _combined_template_text(source)).lower()
    matches: list[_Match] = []
    cni = _cni_context(text)

    if re.search(r"workload[-_ ]?endpoint\s*(?:was\s+)?(?:not found|does not exist)|"
                 r"(?:no such|missing)\s+workload[-_ ]?endpoint", text):
        matches.append(_Match(
            "kubernetes.cni.workload_endpoint_not_found", "selected_template_pattern", "high",
            "cni_workload_endpoint_not_found_v1",
        ))
    if cni and re.search(r"(?:delete|deletion|del).{0,50}(?:not supported|unsupported|not implemented)|"
                        r"(?:not supported|unsupported|not implemented).{0,50}(?:delete|deletion)", text):
        matches.append(_Match(
            "kubernetes.cni.delete_not_supported", "selected_template_pattern", "high",
            "cni_delete_not_supported_v1",
        ))
    if cni and re.search(r"no[ _-]*(?:(?:enough|free)\s+)?ips?|"
                        r"ip(?:v4)?\s*(?:address|地址)?\s*(?:exhaust|deplet|耗尽)|地址\s*耗尽", text):
        matches.append(_Match(
            "kubernetes.cni.ip_exhaustion", "selected_template_pattern", "high",
            "cni_ip_exhaustion_v1",
        ))
    if cni and re.search(r"(?:config(?:uration)?\s*(?:syntax\s*)?(?:error|invalid|failed)|"
                        r"invalid\s+(?:cni|network)|syntax\s+error|配置.*(?:语法|错误))", text):
        matches.append(_Match(
            "kubernetes.cni.config_error", "selected_template_pattern", "high",
            "cni_config_error_v1",
        ))

    stats_context = bool(_STATS_OPERATION_PATTERN.search(text))
    not_found_context = bool(_CONTAINER_NOT_FOUND_PATTERN.search(text))

    if stats_context:
        matches.append(_Match(
            "kubernetes.runtime.container_stats_failure",
            "selected_template_pattern",
            "high",
            "container_stats_failure_v2",
            "container_not_found" if not_found_context else None,
        ))
    elif not_found_context:
        matches.append(_Match(
            "kubernetes.runtime.container_not_found",
            "selected_template_pattern",
            "high",
            "container_not_found_v1",
        ))

    if re.search(r"(?:unable\s+to\s+find|not found|missing).{0,45}"
                 r"(?:data|stats|statistics).{0,25}(?:memory\s+)?cache|"
                 r"(?:cadvisor|recentstats).{0,60}(?:cache|missing|not found)|"
                 r"(?:cache miss|missing from memory cache)", text):
        matches.append(_Match(
            "kubernetes.runtime.cadvisor_cache_miss", "selected_template_pattern", "high",
            "cadvisor_cache_miss_v1",
        ))

    if re.search(r"crashloopbackoff|crashloop|back[- ]off\s+restarting\s+failed\s+container|"
                 r"restarting\s+failed\s+container", text):
        matches.append(_Match(
            "kubernetes.pod.crash_loop", "selected_template_pattern", "high",
            "crash_loop_v1",
        ))

    if re.search(r"failed\s+to\s+clean(?:\s+up)?\s+(?:the\s+)?(?:volume\s+)?subpath|"
                 r"(?:volume\s+)?subpath.{0,45}(?:cleanup|clean\s*up|remove|delete)", text):
        matches.append(_Match(
            "kubernetes.volume.subpath_cleanup_failure", "selected_template_pattern", "high",
            "volume_subpath_cleanup_v1", "volume_subpath",
        ))
    elif re.search(r"volume\s+subpaths?\s+still\s+present", text):
        matches.append(_Match(
            "kubernetes.kubelet.orphaned_pod_residual", "selected_template_pattern", "high",
            "orphaned_pod_residual_v1", "volume_subpath",
        ))
    elif re.search(r"(?:pod|volume).{0,40}directory\s+not\s+empty", text):
        matches.append(_Match(
            "kubernetes.kubelet.orphaned_pod_residual", "selected_template_pattern", "high",
            "orphaned_pod_residual_v1", "directory_not_empty",
        ))
    elif re.search(r"orphaned\s+pod|volume\s+paths?\s+still\s+present", text):
        matches.append(_Match(
            "kubernetes.kubelet.orphaned_pod_residual", "selected_template_pattern", "high",
            "orphaned_pod_residual_v1", "volume_path",
        ))
    if re.search(r"(?:failed\s+to\s+)?unmount(?:ing)?\s+(?:the\s+)?volume|"
                 r"unmountvolume|volume.{0,30}unmount", text):
        matches.append(_Match(
            "kubernetes.volume.unmount_failure", "selected_template_pattern", "high",
            "volume_unmount_v1",
        ))

    if re.search(r"image\s+(?:garbage\s+collection|gc)|"
                 r"(?:failed|failure)\s+to\s+(?:garbage\s+collect|gc)\s+images?|"
                 r"garbage\s+collect(?:ion)?\s+failed", text):
        matches.append(_Match(
            "kubernetes.image.gc_failure", "selected_template_pattern", "high",
            "image_gc_failure_v1",
        ))

    image_context = bool(re.search(r"image|registry|manifest|pull", text))
    if image_context and re.search(r"unauthori[sz]ed|authentication\s+required|"
                                   r"pull\s+access\s+denied|not\s+authorized|"
                                   r"access\s+denied|\b401\b", text):
        matches.append(_Match(
            "kubernetes.image.pull_unauthorized", "selected_template_pattern", "high",
            "image_pull_unauthorized_v1",
        ))
    if image_context and re.search(r"manifest\s+unknown|repository\s+does\s+not\s+exist|"
                                   r"name\s+unknown|image.{0,40}(?:not found|does not exist)|"
                                   r"(?:not found|does not exist).{0,40}image", text):
        matches.append(_Match(
            "kubernetes.image.pull_not_found", "selected_template_pattern", "high",
            "image_pull_not_found_v1",
        ))
    if image_context and re.search(r"connection\s+reset|connection\s+refused|timed?\s*out|"
                                   r"timeout|transport|tls|dial\s+tcp|no\s+route\s+to\s+host|"
                                   r"network\s+is\s+unreachable|i/o\s+timeout|unexpected\s+eof", text):
        matches.append(_Match(
            "kubernetes.image.pull_transport_failure", "selected_template_pattern", "high",
            "image_pull_transport_v1",
        ))

    if re.search(r"checkpoint.{0,60}(?:resource\s+)?(?:not found|does not exist|missing)|"
                 r"(?:resource|checkpoint).{0,30}no such", text):
        matches.append(_Match(
            "kubernetes.kubelet.checkpoint_resource_not_found", "selected_template_pattern", "high",
            "checkpoint_resource_not_found_v1",
        ))
    if re.search(r"(?:exec|session).{0,80}(?:process|command).{0,40}still\s+running|"
                 r"process.{0,50}still\s+running.{0,50}(?:exec|session)|"
                 r"exec\s+session.{0,50}(?:ended|finished).{0,50}running", text):
        matches.append(_Match(
            "kubernetes.runtime.exec_process_still_running", "selected_template_pattern", "high",
            "exec_process_still_running_v1",
        ))
    if re.search(r"(?:filesystem|file\s+system).{0,70}(?:stat|stats).{0,70}(?:path.{0,20}"
                 r"(?:missing|not found|does not exist)|no such file)|"
                 r"(?:failed\s+to\s+get|get).{0,30}filesystem\s+stats.{0,60}"
                 r"(?:path|no such|not found)", text):
        matches.append(_Match(
            "kubernetes.runtime.filesystem_stats_path_missing", "selected_template_pattern", "high",
            "filesystem_stats_path_missing_v1",
        ))

    if re.search(r"\boom\b|out\s+of\s+memory|memory\s+(?:exhaust|deplet)|内存.*(?:耗尽|不足|溢出)", text):
        matches.append(_Match(
            "linux.memory.oom", "selected_template_pattern", "high", "linux_oom_v1",
        ))

    concrete = {match.code for match in matches if _is_concrete(match.code)}
    if not concrete:
        if cni and re.search(r"(?:plugin|network).{0,40}(?:failed|failure|error)|"
                            r"(?:failed|failure|error).{0,40}(?:plugin|network)", text):
            matches.append(_Match(
                "kubernetes.cni.plugin_failure", "selected_template_pattern", "high",
                "cni_plugin_failure_wrapper_v1",
            ))
        elif re.search(r"pod\s*sandbox|podsandbox|沙箱", text) and re.search(
            r"failed|failure|error|失败|错误", text,
        ):
            matches.append(_Match(
                "kubernetes.runtime.pod_sandbox_failure", "selected_template_pattern", "high",
                "pod_sandbox_failure_wrapper_v1",
            ))
        elif image_context and re.search(r"imagepullbackoff|failed\s+to\s+pull\s+image|"
                                         r"image\s+pull", text):
            matches.append(_Match(
                "kubernetes.image.pull_failure", "selected_template_pattern", "high",
                "image_pull_failure_wrapper_v1",
            ))
    return _unique_matches(matches)


def _unique_matches(matches: Iterable[_Match]) -> list[_Match]:
    result: list[_Match] = []
    seen: set[tuple[str, str, str | None]] = set()
    for match in matches:
        key = (match.code, match.source, match.matched_rule)
        if key not in seen:
            seen.add(key)
            result.append(match)
    return result


def _feature_type_matches(feature: Mapping[str, Any]) -> list[_Match]:
    value = feature.get("feature_type")
    code = _normalize_code(value)
    if code and (_is_concrete(code) or _is_generic(code)):
        return [_Match(code, "feature_type_hint", "medium", "feature_type_hint")]
    if not isinstance(value, str) or not value.strip():
        return []
    matches = []
    for match in _template_matches({"template": value}):
        matches.append(_Match(match.code, "feature_type_hint", "medium", match.matched_rule))
    return _unique_matches(matches)


def _text_hint_matches(feature: Mapping[str, Any]) -> list[_Match]:
    text = " ".join(
        str(value)
        for field in ("title", "summary", "selection_reason")
        for value in _iter_values(feature.get(field))
        if isinstance(value, str) and value.strip()
    )
    tags = " ".join(
        str(value)
        for value in _iter_values(feature.get("tags"))
        if isinstance(value, str) and value.strip()
    )
    if tags:
        text = f"{text} {tags}"
    if not text.strip():
        return []
    return [
        _Match(match.code, "deterministic_keyword", "medium", match.matched_rule)
        for match in _template_matches({"template": text})
    ]


def _codes(matches: Iterable[_Match]) -> set[str]:
    return {match.code for match in matches if not _is_unknown(match.code) and not match.code.startswith("logrisk.")}


def _resolution(
    matches: Iterable[_Match],
    *,
    semantic_safe: bool,
    ambiguity: bool = False,
    default_source: str = "fallback",
    default_confidence: str = "low",
) -> ProblemResolution:
    values = _unique_matches(matches)
    codes = sorted(_codes(values))
    if ambiguity or len({code for code in codes if _is_concrete(code)}) > 1:
        concrete = tuple(sorted(code for code in codes if _is_concrete(code)))
        source = values[0].source if values else default_source
        confidence = values[0].confidence if values else default_confidence
        return ProblemResolution(None, confidence, False, True, source, None, concrete)
    if not codes:
        return ProblemResolution(None, default_confidence, False, False, default_source, None, ())
    concrete = [code for code in codes if _is_concrete(code)]
    code = concrete[0] if concrete else codes[0]
    match = next((item for item in values if item.code == code), values[0])
    safe = semantic_safe and bool(concrete) and not _is_generic(code)
    return ProblemResolution(
        code,
        match.confidence,
        safe,
        False,
        match.source,
        match.matched_rule,
        tuple(codes),
        match.subtype,
    )


def resolve_selected_template(template: Mapping[str, Any]) -> ProblemResolution:
    matches = _template_matches(template)
    concrete = [match for match in matches if _is_concrete(match.code)]
    if not concrete:
        generic = [match for match in matches if _is_generic(match.code)]
        return _resolution(generic, semantic_safe=False) if generic else ProblemResolution(
            None, "low", False, False, "fallback", None, ()
        )

    codes = {match.code for match in concrete}
    if len(codes) > 1:
        return _resolution(concrete, semantic_safe=False, ambiguity=True)
    return _resolution(concrete, semantic_safe=True)


def resolve_problem(
    feature: Mapping[str, Any], entity: Mapping[str, Any] | None = None,
) -> ProblemResolution:
    """Resolve one feature using only selected evidence and deterministic rules."""

    if not isinstance(feature, Mapping):
        return ProblemResolution(None, "low", False, False, "fallback", None, ())

    sources = _selected_templates(feature, entity)
    structured = _structured_matches(feature, sources)
    selected = _unique_matches(
        match
        for source in sources
        for match in _template_matches(source)
    )

    structured_concrete = _codes(structured) & {
        code for code in _codes(structured) if _is_concrete(code)
    }
    selected_concrete = _codes(selected) & {
        code for code in _codes(selected) if _is_concrete(code)
    }
    if structured_concrete and selected_concrete and structured_concrete != selected_concrete:
        return _resolution(
            [
                *[match for match in structured if _is_concrete(match.code)],
                *[match for match in selected if _is_concrete(match.code)],
            ],
            semantic_safe=False,
            ambiguity=True,
        )
    if structured_concrete or selected_concrete:
        matches = structured if structured_concrete else selected
        return _resolution(matches, semantic_safe=True)

    type_hints = _feature_type_matches(feature)
    text_hints = _text_hint_matches(feature)
    for hints in (type_hints, text_hints):
        if any(_is_concrete(match.code) for match in hints):
            return _resolution(hints, semantic_safe=False)
    generic = selected or structured or type_hints or text_hints
    if generic:
        return _resolution(generic, semantic_safe=False)
    return ProblemResolution(None, "low", False, False, "fallback", None, ())


resolve_problem_code = resolve_problem
