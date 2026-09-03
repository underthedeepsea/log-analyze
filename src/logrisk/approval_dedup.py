from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
import unicodedata
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from logrisk.problem_resolver import resolve_problem


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_feature_type(value: Any) -> str:
    """Return the stable, node-independent form used by approval identities."""

    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized or "unknown_feature"


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


_PROBLEM_CODE_ALIASES = {
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
    "runtime_cni_setup_failed": "kubernetes.cni.plugin_failure",
    "kubernetes_cni_plugin_failure": "kubernetes.cni.plugin_failure",
    "runtime_sandbox_create_failed": "kubernetes.runtime.pod_sandbox_failure",
    "k8s_node_memory_pressure": "kubernetes.node.memory_pressure",
    "linux_oom": "linux.memory.oom",
}

_CNI_CONCRETE_PROBLEM_CODES = frozenset({
    "kubernetes.cni.ip_exhaustion",
    "kubernetes.cni.config_error",
})

_RUNTIME_WRAPPER_PROBLEM_CODES = frozenset({
    "kubernetes.cni.plugin_failure",
    "kubernetes.runtime.pod_sandbox_failure",
})

_UNKNOWN_PROBLEM_CODES = frozenset({
    "unknown",
    "unknown_cause",
    "unknown_problem",
    "unknown_problem_code",
    "unknown.problem",
    "unclassified",
    "unclassified_cause",
    "unclassified_problem",
    "unclassified_problem_code",
})

def _is_unknown_problem_code(code: str) -> bool:
    segments = code.split(".")
    has_unknown_segment = any(
        segment in {"unknown", "unclassified"}
        or segment.startswith(("unknown_", "unclassified_"))
        for segment in segments
    )
    return (
        code in _UNKNOWN_PROBLEM_CODES
        or code.startswith(("unknown.", "unclassified."))
        or has_unknown_segment
    )


def normalize_problem_code(value: Any) -> str | None:
    """Normalize a problem code without accepting node, pod, or time identity."""

    if not isinstance(value, str) or not value.strip():
        return None
    text = _remove_ip_literals(unicodedata.normalize("NFKC", value).strip().lower())
    alias = _PROBLEM_CODE_ALIASES.get(_alias_key(text))
    if alias:
        return alias
    parts = []
    for segment in re.split(r"[./:]+", text):
        tokens = re.findall(r"[a-z0-9]+", segment)
        if tokens:
            parts.append("_".join(tokens))
    return ".".join(parts) or None


def _iter_values(value: Any) -> list[Any]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        values: list[Any] = []
        for item in value:
            if isinstance(item, (list, tuple, set)):
                values.extend(_iter_values(item))
            else:
                values.append(item)
        return values
    return []


def _canonical_signature(value: Any) -> str | None:
    if isinstance(value, str):
        return _remove_ip_literals(value.strip()) or None
    if not isinstance(value, Mapping):
        return None
    identity = _remove_ip_literals(str(value.get("template_fingerprint") or value.get("template_hash") or "").strip())
    category = str(value.get("category") or "").strip()
    if identity:
        return f"{identity}|{category}" if category else identity
    signature = value.get("signature")
    if isinstance(signature, str) and signature.strip():
        return _remove_ip_literals(signature.strip()) or None
    safe = {
        key: value.get(key)
        for key in ("semantic_rule_id", "risk_type", "category", "component", "feature_hint")
        if value.get(key) is not None
    }
    return json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":")) if safe else None


def _canonical_signatures(value: Any) -> list[str]:
    items = []
    for item in _iter_values(value):
        normalized = _canonical_signature(item)
        if normalized:
            items.append(normalized)
    return sorted(set(items))


def _source_templates(feature: Mapping[str, Any], entity: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    sources = feature.get("source_templates")
    if isinstance(sources, list) and sources:
        return [item for item in sources if isinstance(item, Mapping)]
    signatures = feature.get("template_signatures")
    if isinstance(signatures, list) and signatures:
        return [item for item in signatures if isinstance(item, Mapping)]
    if entity is feature:
        templates = feature.get("top_templates")
        if isinstance(templates, list):
            return [item for item in templates if isinstance(item, Mapping)]
    if entity:
        templates = entity.get("top_templates")
        hashes = {str(item) for item in _iter_values(feature.get("template_hashes"))}
        if isinstance(templates, list) and hashes:
            return [
                item for item in templates
                if isinstance(item, Mapping)
                and bool({
                    str(item.get("template_hash") or ""),
                    str(item.get("template_fingerprint") or ""),
                } & hashes)
            ]
    return []


def _nested_semantic_values(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        normalized = normalize_problem_code(value)
        return (normalized,) if normalized else ()
    if isinstance(value, (list, tuple, set)):
        values: list[str] = []
        for item in value:
            values.extend(_nested_semantic_values(item))
        return values
    if not isinstance(value, Mapping):
        return ()
    values = []
    for key in (
        "problem_code",
        "problemCode",
        "risk_type",
        "cause",
        "risk_semantic",
        "semantic_fields",
    ):
        values.extend(_nested_semantic_values(value.get(key)))
    return values


def _append_problem_code(codes: list[str], value: Any) -> None:
    normalized = normalize_problem_code(value)
    if normalized:
        normalized = _PROBLEM_CODE_ALIASES.get(_alias_key(normalized), normalized)
        if normalized not in codes:
            codes.append(normalized)


def _keyword_problem_codes(text: str) -> list[str]:
    lowered = text.lower()
    codes: list[str] = []
    if re.search(r"oom|out\s+of\s+memory|内存.*(?:耗尽|不足)|内存溢出", lowered):
        codes.append("linux.memory.oom")

    cni_context = bool(re.search(
        r"\bcni(?:\b|_)|network[-\s]+plugin|networkplugin|"
        r"network\s+(?:config(?:uration)?|setup)|"
        r"network\s+(?:for|in|on)\s+(?:the\s+)?(?:pod\s+)?sandbox|"
        r"(?:pod\s+)?sandbox[-\s]+(?:network|cni)|"
        r"网络配置|网络插件|网络.{0,20}沙箱|沙箱.{0,20}网络",
        lowered,
    ))
    if cni_context and re.search(
        r"no[ _-]*(?:(?:enough|free)\s+)?ips?|ip(?:v4)?\s*(?:address|地址)?\s*(?:exhaust|deplet|耗尽)|地址\s*耗尽",
        lowered,
    ):
        codes.append("kubernetes.cni.ip_exhaustion")
    if cni_context and re.search(
        r"syntax|invalid\s+(?:cni|network)|config(?:uration)?\s*(?:syntax\s*)?(?:error|invalid)|语法|配置.*(?:语法|错误)",
        lowered,
    ):
        codes.append("kubernetes.cni.config_error")
    if cni_context and re.search(r"failed|failure|error|失败|错误", lowered):
        codes.append("kubernetes.cni.plugin_failure")
    if re.search(r"pod\s+sandbox|podsandbox|沙箱", lowered) and re.search(r"failed|failure|error|失败|错误", lowered):
        codes.append("kubernetes.runtime.pod_sandbox_failure")
    return list(dict.fromkeys(codes))


def _feature_text(feature: Mapping[str, Any], sources: Iterable[Mapping[str, Any]]) -> str:
    values = [
        feature.get("feature_type"),
        feature.get("title"),
        feature.get("summary"),
        feature.get("selection_reason"),
        feature.get("problem_code"),
        feature.get("problemCode"),
        feature.get("risk_type"),
        feature.get("cause"),
        *[str(item) for item in _iter_values(feature.get("tags"))],
    ]
    for source in sources:
        values.extend((source.get("template"), source.get("category"), source.get("feature_hint")))
        semantic = source.get("risk_semantic")
        if isinstance(semantic, Mapping):
            values.extend((semantic.get("risk_type"), semantic.get("category"), *[str(item) for item in _iter_values(semantic.get("tags"))]))
        values.append(source.get("cause"))
    return " ".join(str(value) for value in values if value is not None)


def _has_semantic_identity(feature: Mapping[str, Any], sources: Iterable[Mapping[str, Any]]) -> bool:
    if any(normalize_problem_code(feature.get(field)) for field in ("problem_code", "problemCode", "risk_type", "cause")):
        return True
    if any(_nested_semantic_values(value) for value in (feature.get("semantic_fields"), feature.get("risk_semantic"))):
        return True
    return any(
        any(
            _nested_semantic_values(source.get(field))
            for field in (
                "problem_code",
                "problemCode",
                "risk_type",
                "cause",
                "risk_semantic",
                "semantic_fields",
            )
        )
        for source in sources
    )


def collect_problem_codes(
    feature: Mapping[str, Any], entity: Mapping[str, Any] | None = None
) -> list[str]:
    """Collect semantic signals from the feature and its selected templates."""

    sources = _source_templates(feature, entity)
    codes: list[str] = []
    for value in (
        feature.get("problem_code"),
        feature.get("problemCode"),
        feature.get("risk_type"),
        feature.get("cause"),
    ):
        _append_problem_code(codes, value)
    for value in (feature.get("semantic_fields"), feature.get("risk_semantic")):
        for normalized in _nested_semantic_values(value):
            _append_problem_code(codes, normalized)
    for keyword in _keyword_problem_codes(_feature_text(feature, sources)):
        _append_problem_code(codes, keyword)

    for source in sources:
        for value in (
            source.get("problem_code"),
            source.get("problemCode"),
            source.get("risk_type"),
            source.get("cause"),
            source.get("risk_semantic"),
            source.get("semantic_fields"),
        ):
            _append_problem_code(codes, value)
            for normalized in _nested_semantic_values(value):
                _append_problem_code(codes, normalized)
    return codes


def select_primary_problem_code(
    codes: Iterable[Any], explicit_code: Any = None
) -> str | None:
    """Prefer a concrete CNI cause while preserving strict ambiguity fallback."""

    normalized_codes: list[str] = []
    for value in codes:
        _append_problem_code(normalized_codes, value)
    _append_problem_code(normalized_codes, explicit_code)
    concrete = {code for code in normalized_codes if code in _CNI_CONCRETE_PROBLEM_CODES}
    if len(concrete) > 1:
        return None
    if concrete:
        return sorted(concrete)[0]

    usable = [
        code for code in normalized_codes
        if not _is_unknown_problem_code(code) and not code.startswith("logrisk.")
    ]
    if usable:
        return min(usable, key=lambda code: (_problem_code_priority(code), code))
    return None


def _problem_code_priority(code: str) -> int:
    if code in _CNI_CONCRETE_PROBLEM_CODES:
        return 0
    if _is_unknown_problem_code(code) or code.startswith("logrisk."):
        return 3
    if code in _RUNTIME_WRAPPER_PROBLEM_CODES or code.startswith("runtime."):
        return 2
    if is_canonical_problem_code(code):
        return 1
    return 2


def derive_problem_code(feature: Mapping[str, Any], entity: Mapping[str, Any] | None = None) -> str:
    resolution = resolve_problem(feature, entity)
    if resolution.problem_code:
        return resolution.problem_code

    sources = _source_templates(feature, entity)
    codes = collect_problem_codes(feature, entity)

    anchors = _canonical_signatures(feature.get("anchor_signatures"))
    if not anchors:
        anchors = _canonical_signatures(sources) or _canonical_signatures(feature.get("template_hashes"))
    fallback_payload: Any = anchors
    if codes:
        fallback_payload = {
            "problem_codes": sorted(set(codes)),
            "anchor_signatures": anchors,
        }
    digest = hashlib.sha256(json.dumps(fallback_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]
    return f"logrisk.{normalize_feature_type(feature.get('feature_type'))}.{digest}"


def entity_problem_codes(entity: Mapping[str, Any]) -> set[str]:
    return set(collect_problem_codes(entity, entity))


def component_scope(feature: Mapping[str, Any], entity: Mapping[str, Any] | None = None) -> list[str]:
    components = [str(item).strip().lower() for item in _iter_values(feature.get("components")) if str(item).strip()]
    if not components and feature.get("component"):
        components = [str(feature["component"]).strip().lower()]
    sources = _source_templates(feature, entity)
    if not components:
        components = [str(item.get("component")).strip().lower() for item in sources if str(item.get("component") or "").strip()]
    return sorted(set(components))


def anchor_signatures(feature: Mapping[str, Any], entity: Mapping[str, Any] | None = None) -> list[str]:
    explicit = _canonical_signatures(feature.get("anchor_signatures"))
    if explicit:
        return explicit
    sources = _source_templates(feature, entity)
    anchors = _canonical_signatures(sources)
    if not anchors:
        anchors = _canonical_signatures(feature.get("template_hashes"))
    if not anchors:
        return []

    resolution = resolve_problem(feature, entity)
    if resolution.semantic_safe:
        return []
    return anchors[:1]


def build_approval_key(
    feature_type: Any,
    problem_code: Any,
    component_scope: Iterable[Any] | Any = (),
    anchor_signatures: Iterable[Any] | Any = (),
    *,
    anchor_template_fingerprints: Iterable[Any] | Any | None = None,
    semantic_safe: bool | None = None,
) -> str:
    """Build an approval identity from semantic fields only."""

    if anchor_template_fingerprints is not None and not anchor_signatures:
        anchor_signatures = anchor_template_fingerprints
    normalized_problem_code = normalize_problem_code(problem_code) or "unknown.problem"
    if semantic_safe is None:
        semantic_safe = resolve_problem({
            "feature_type": feature_type,
            "problem_code": normalized_problem_code,
        }).semantic_safe
    canonical = bool(semantic_safe)
    if canonical:
        payload = {"problem_code": normalized_problem_code}
    else:
        payload = {
            "feature_type": normalize_feature_type(feature_type),
            "problem_code": normalized_problem_code,
            "component_scope": sorted({
                _remove_ip_literals(str(item).strip().lower())
                for item in _iter_values(component_scope)
                if str(item).strip()
            }),
            "anchor_signatures": _canonical_signatures(anchor_signatures),
        }
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f"appr_{digest}"


def approval_identity(feature: Mapping[str, Any], entity: Mapping[str, Any] | None = None) -> dict[str, Any]:
    resolution = resolve_problem(feature, entity)
    problem_code = resolution.problem_code
    if not problem_code:
        sources = _source_templates(feature, entity)
        codes = collect_problem_codes(feature, entity)
        anchors = _canonical_signatures(feature.get("anchor_signatures"))
        if not anchors:
            anchors = _canonical_signatures(sources) or _canonical_signatures(feature.get("template_hashes"))
        fallback_payload: Any = anchors
        if codes:
            fallback_payload = {
                "problem_codes": sorted(set(codes)),
                "anchor_signatures": anchors,
            }
        digest = hashlib.sha256(
            json.dumps(
                fallback_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:16]
        problem_code = f"logrisk.{normalize_feature_type(feature.get('feature_type'))}.{digest}"
    components = component_scope(feature, entity)
    anchors = anchor_signatures(feature, entity)
    approval_key = build_approval_key(
        feature.get("feature_type"),
        problem_code,
        components,
        anchors,
        semantic_safe=resolution.semantic_safe,
    )
    return {
        "problem_code": problem_code,
        "approval_key": approval_key,
        "component_scope": components,
        "anchor_signatures": anchors,
        "match_mode": "semantic" if resolution.semantic_safe else "template_set",
        "resolution_confidence": resolution.confidence,
        "resolution_source": resolution.evidence_source,
        "semantic_safe": resolution.semantic_safe,
        "ambiguity": resolution.ambiguity,
        "matched_rule": resolution.matched_rule,
        "supporting_codes": list(resolution.supporting_codes),
    }


def is_canonical_problem_code(code: str | None) -> bool:
    normalized = normalize_problem_code(code)
    return bool(normalized) and (
        "." in normalized
        and not _is_unknown_problem_code(normalized)
        and not normalized.startswith("logrisk.")
    )


def same_approval_identity(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_resolution = resolve_problem(left)
    right_resolution = resolve_problem(right)
    left_code = derive_problem_code(left)
    right_code = derive_problem_code(right)
    left_semantic_safe = left_resolution.semantic_safe
    right_semantic_safe = right_resolution.semantic_safe

    left_key = str(left.get("approval_key") or "").strip()
    right_key = str(right.get("approval_key") or "").strip()
    left_v2_key = approval_identity(left)["approval_key"]
    right_v2_key = approval_identity(right)["approval_key"]
    left_legacy = bool(left_key and left_key != left_v2_key)
    right_legacy = bool(right_key and right_key != right_v2_key)

    if left_key and right_key and left_key == right_key:
        return True
    if left_legacy and right_legacy:
        return False
    if left_semantic_safe and right_semantic_safe:
        return left_code == right_code
    if left_semantic_safe != right_semantic_safe:
        return False

    left_key = left_key or left_v2_key
    right_key = right_key or right_v2_key
    return bool(left_key) and left_key == right_key


def group_id_for_key(approval_key: str) -> str:
    return f"approval-group-{hashlib.sha256(str(approval_key).encode('utf-8')).hexdigest()[:20]}"


class InMemoryApprovalGroupStore:
    """Small default store used by standalone and test FeatureJobManager instances."""

    def __init__(self, clock: Any = _now) -> None:
        self.clock = clock
        self._lock = threading.RLock()
        self._groups: dict[str, dict[str, Any]] = {}
        self._by_key: dict[str, str] = {}
        self._candidates: dict[str, str] = {}

    def get_by_key(self, approval_key: str) -> dict[str, Any] | None:
        with self._lock:
            group_id = self._by_key.get(str(approval_key))
            return copy.deepcopy(self._groups[group_id]) if group_id else None

    def get_by_id(self, approval_group_id: str) -> dict[str, Any] | None:
        with self._lock:
            return copy.deepcopy(self._groups.get(str(approval_group_id)))

    def list_groups(self, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            values = list(self._groups.values())
            if status:
                values = [item for item in values if item.get("status") == status]
            return copy.deepcopy(sorted(values, key=lambda item: str(item.get("created_at") or "")))

    def save(self, group: Mapping[str, Any]) -> dict[str, Any]:
        value = copy.deepcopy(dict(group))
        with self._lock:
            existing_id = self._by_key.get(str(value["approval_key"]))
            if existing_id and existing_id != value.get("approval_group_id"):
                value["approval_group_id"] = existing_id
            value.setdefault("updated_at", self.clock())
            self._groups[str(value["approval_group_id"])] = value
            self._by_key[str(value["approval_key"])] = str(value["approval_group_id"])
            return copy.deepcopy(value)

    def has_candidate(self, candidate_id: str) -> bool:
        with self._lock:
            return str(candidate_id) in self._candidates

    def attach_candidate(self, approval_group_id: str, candidate_id: str, **metadata: Any) -> None:
        with self._lock:
            candidate = str(candidate_id)
            group_id = str(approval_group_id)
            if candidate in self._candidates and self._candidates[candidate] != group_id:
                raise ValueError("Candidate 只能归属于一个 Approval Group")
            self._candidates[candidate] = group_id

    def candidate_group_id(self, candidate_id: str) -> str | None:
        with self._lock:
            return self._candidates.get(str(candidate_id))
