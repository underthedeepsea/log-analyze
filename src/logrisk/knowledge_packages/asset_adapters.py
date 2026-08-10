from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Callable

import yaml

from .errors import KnowledgePackageError


class KnowledgeAssetAdapterRegistry:
    """Materialize data-only assets into domain candidate hooks.

    A missing domain hook remains safe: the asset is recorded as a candidate
    resource in the package registry and can be wired by a later domain owner.
    """

    def __init__(self, *, hooks: dict[str, Callable[[dict[str, Any], bytes], dict[str, Any]]] | None = None) -> None:
        self.hooks = dict(hooks or {})

    def materialize(self, asset: dict[str, Any], content: bytes) -> dict[str, Any]:
        asset_type = str(asset["asset_type"])
        hook = self.hooks.get(asset_type)
        if hook:
            result = hook(asset, content)
            if not isinstance(result, dict) or not result.get("resource_id"):
                raise KnowledgePackageError(f"资产适配器未返回资源标识: {asset_type}", code="asset_materialize_invalid")
            return {
                "target_domain": str(result.get("target_domain") or asset_type),
                "resource_id": str(result["resource_id"]),
                "version": str(result.get("version") or "1"),
            }
        # A deterministic candidate reference is useful for UI review and is
        # deliberately not a production activation or a cross-domain publish.
        return {
            "target_domain": asset_type,
            "resource_id": f"candidate-{asset['package_id']}-{asset['asset_id']}",
            "version": hashlib.sha256(content).hexdigest()[:12],
        }


def build_domain_adapter_registry(*, prompt_registry: Any, drain_quality: Any, semantic_dictionaries: Any, risk_semantics: Any) -> KnowledgeAssetAdapterRegistry:
    """Build safe candidate adapters from already-constructed domain services.

    These hooks only create unpublished candidates. They never call a publish,
    approve, activate, or model-execution method. Rule-candidate JSON remains a
    deterministic reference because the approval store requires a real feature
    job lineage and must not be fabricated by a package import.
    """

    def prompt_candidate(asset: dict[str, Any], content: bytes) -> dict[str, Any]:
        text = _decode_text(content, "Prompt")
        prompt_id = f"package-{asset['package_id']}-{asset['asset_id']}"
        prompt = prompt_registry.create_candidate(
            prompt_id,
            text,
            display_name=f"知识包 · {asset['asset_id']}",
            description=f"来自知识包 {asset['package_id']} v{asset['version']} 的候选 Prompt",
        )
        return {"target_domain": "prompt", "resource_id": prompt.prompt_id, "version": prompt.version}

    def drain_candidate(asset: dict[str, Any], content: bytes) -> dict[str, Any]:
        text = _decode_text(content, "Drain3 配置")
        configs = drain_quality.configs
        parser = getattr(configs, "_parse", None)
        if callable(parser):
            parser(text)
        candidate = configs.create_candidate({
            "name": f"知识包 · {asset['asset_id']}",
            "description": f"来自知识包 {asset['package_id']} v{asset['version']}",
            "operator": "knowledge-package",
        })
        version = configs.save_version(candidate["config_id"], {
            "ini_content": text,
            "expected_version": int(candidate["version"]),
            "operator": "knowledge-package",
        })
        return {"target_domain": "drain3_config", "resource_id": version["config_id"], "version": f"v{version['version']}"}

    def semantic_candidate(asset: dict[str, Any], content: bytes) -> dict[str, Any]:
        payload = load_yaml_asset(content)
        if not isinstance(payload, dict) or not isinstance(payload.get("rules"), list):
            raise KnowledgePackageError("语义词典资产必须包含 rules 数组", code="asset_content_invalid")
        dictionary_id = str(payload.get("dictionary_id") or "").strip()
        if not dictionary_id:
            raise KnowledgePackageError("语义词典缺少 dictionary_id", code="asset_content_invalid")
        existing = {str(item.get("dictionary_id")) for item in semantic_dictionaries.list_dictionaries()}
        if dictionary_id not in existing:
            raise KnowledgePackageError("语义词典目标不存在: " + dictionary_id, code="asset_target_not_found")
        builtin_ids = {
            str(rule.get("rule_id"))
            for item in semantic_dictionaries.list_dictionaries()
            if str(item.get("dictionary_id")) == dictionary_id
            for rule in item.get("builtin_rules") or []
        }
        custom_rules = [rule for rule in payload["rules"] if isinstance(rule, dict) and str(rule.get("rule_id")) not in builtin_ids]
        candidate = semantic_dictionaries.create_candidate(dictionary_id, {"operator": "knowledge-package", "custom_rules": custom_rules})
        return {"target_domain": "semantic_dictionary", "resource_id": dictionary_id, "version": str(candidate["version"])}

    def risk_semantic_candidate(asset: dict[str, Any], content: bytes) -> dict[str, Any]:
        payload = load_yaml_asset(content)
        if not isinstance(payload, dict) or not isinstance(payload.get("rules"), list) or not payload["rules"]:
            raise KnowledgePackageError("风险语义资产必须包含非空 rules 数组", code="asset_content_invalid")
        created: list[str] = []
        for rule in payload["rules"]:
            if not isinstance(rule, dict):
                raise KnowledgePackageError("风险语义规则必须是对象", code="asset_content_invalid")
            created.append(str(risk_semantics.create_rule(_normalize_risk_rule(rule), operator="knowledge-package", reason="知识包导入候选")["id"]))
        return {"target_domain": "risk_semantics", "resource_id": ",".join(created), "version": hashlib.sha256(content).hexdigest()[:12]}

    def gold_dataset_candidate(asset: dict[str, Any], content: bytes) -> dict[str, Any]:
        records = []
        for line_number, line in enumerate(_decode_text(content, "Gold Dataset").splitlines(), 1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise KnowledgePackageError(f"Gold Dataset 第 {line_number} 行不是 JSON", code="asset_content_invalid") from exc
            records.append(item)
        if not records:
            raise KnowledgePackageError("Gold Dataset 不能为空", code="asset_content_invalid")
        dataset = drain_quality.datasets.create({
            "dataset_id": f"package-{asset['package_id']}-{asset['asset_id']}",
            "name": f"知识包 · {asset['asset_id']}",
            "description": f"来自知识包 {asset['package_id']} v{asset['version']}",
            "version": asset["version"],
            "records": records,
        })
        return {"target_domain": "gold_dataset", "resource_id": dataset["dataset_id"], "version": str(dataset["version"])}

    return KnowledgeAssetAdapterRegistry(hooks={
        "feature_prompt": prompt_candidate,
        "drain3_profile": drain_candidate,
        "semantic_dictionary": semantic_candidate,
        "risk_semantics": risk_semantic_candidate,
        "gold_dataset": gold_dataset_candidate,
    })


def load_json_asset(content: bytes) -> Any:
    try:
        return json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KnowledgePackageError("JSON 资产内容无效", code="asset_content_invalid") from exc


def _decode_text(content: bytes, label: str) -> str:
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise KnowledgePackageError(label + "必须是 UTF-8 文本", code="asset_content_invalid") from exc


def _normalize_risk_rule(raw: dict[str, Any]) -> dict[str, Any]:
    """Accept the compact package format while sending the domain service its v1 shape."""
    if isinstance(raw.get("match"), dict) and isinstance(raw.get("classification"), dict):
        return dict(raw)
    rule_id = str(raw.get("id") or "").strip()
    risk_type = str(raw.get("risk_type") or "").strip()
    pattern = str(raw.get("pattern") or "").strip()
    if not rule_id or not risk_type or not pattern:
        raise KnowledgePackageError("风险语义规则必须包含 id、risk_type 和 pattern", code="asset_content_invalid")
    severity = str(raw.get("severity") or "medium").lower()
    if severity not in {"low", "medium", "high", "critical"}:
        raise KnowledgePackageError("风险语义 severity 无效", code="asset_content_invalid")
    domain = str(raw.get("domain") or risk_type.split(".", 1)[0])
    category = str(raw.get("category") or risk_type.split(".", 1)[-1])
    sample = str(raw.get("sample") or "feature candidate sample")
    return {
        "id": rule_id,
        "display_name": str(raw.get("display_name") or rule_id),
        "description": str(raw.get("description") or "来自离线知识包的风险语义候选。"),
        "domain": domain,
        "category": category,
        "risk_type": risk_type,
        "risk_subtype": raw.get("risk_subtype"),
        "match": {
            "source_types": list(raw.get("source_types") or ["syslog", "system", "journal", "unknown"]),
            "components": list(raw.get("components") or []),
            "message_regex": [pattern],
        },
        "extract": dict(raw.get("extract") or {}),
        "classification": {
            "default_severity": severity,
            "base_score": float(raw.get("base_score") or {"low": 20, "medium": 45, "high": 75, "critical": 100}[severity]),
            "confidence": float(raw.get("confidence") or 0.9),
        },
        "dedup": {
            "key_fields": list(raw.get("dedup_fields") or ["cluster", "node_id", "risk_type"]),
            "window_seconds": int(raw.get("window_seconds") or 300),
        },
        "lifecycle": dict(raw.get("lifecycle") or {"recovery_mode": "explicit_or_timeout", "recovery_timeout_seconds": 3600}),
        "recommendation": dict(raw.get("recommendation") or {"action_code": "observe", "automation_allowed": False}),
        "test_samples": dict(raw.get("test_samples") or {"positive": [sample], "negative": ["normal operation completed"]}),
        "tags": [str(item) for item in (raw.get("tags") or [domain, category]) if str(item)],
        "priority": int(raw.get("priority") or 100),
        "source": "imported",
        "status": "draft",
        "enabled": True,
        "version": 1,
    }


def load_yaml_asset(content: bytes) -> Any:
    try:
        return yaml.safe_load(content.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise KnowledgePackageError("YAML 资产内容无效", code="asset_content_invalid") from exc
