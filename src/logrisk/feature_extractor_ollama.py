from __future__ import annotations

import hashlib
import json
import socket
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"
IMPORTANCE_LEVELS = {"critical", "high", "medium", "low"}

FEATURE_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "features": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "feature_type": {"type": "string"},
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "importance": {"type": "string", "enum": sorted(IMPORTANCE_LEVELS)},
                    "template_hashes": {"type": "array", "items": {"type": "string"}},
                    "components": {"type": "array", "items": {"type": "string"}},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "selection_reason": {"type": "string"},
                },
                "required": [
                    "feature_type",
                    "title",
                    "summary",
                    "importance",
                    "template_hashes",
                    "components",
                    "tags",
                    "selection_reason",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["features"],
    "additionalProperties": False,
}


class FeatureExtractionError(RuntimeError):
    """Raised when Ollama cannot return valid log feature candidates."""


def _validate_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise FeatureExtractionError("Ollama URL 必须是有效的 http 或 https 地址")
    return normalized


def _sanitized_templates(entity: Dict[str, Any]) -> list[Dict[str, Any]]:
    allowed = (
        "template_hash",
        "component",
        "severity",
        "template",
        "category",
        "count",
        "first_seen",
        "last_seen",
        "feature_hint",
    )
    return [
        {key: template.get(key) for key in allowed}
        for template in (entity.get("top_templates") or [])
    ]


def _evidence_for_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "window_start": entity.get("window_start"),
        "window_end": entity.get("window_end"),
        "cluster": entity.get("cluster"),
        "entity": {"type": entity.get("entity_type"), "id": entity.get("entity_id")},
        "risk_score": entity.get("risk_score"),
        "risk_level": entity.get("risk_level"),
        "affected_entities": entity.get("affected_entities") or [],
        "templates": _sanitized_templates(entity),
    }


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FeatureExtractionError(f"特征字段 {field} 必须是非空字符串")
    return value.strip()


def _string_list(value: Any, field: str, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise FeatureExtractionError(f"特征字段 {field} 必须是字符串数组")
    result = list(dict.fromkeys(item.strip() for item in value))
    if not allow_empty and not result:
        raise FeatureExtractionError(f"特征字段 {field} 不能为空")
    return result


def _validate_model_feature(value: Any, known_hashes: set[str]) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise FeatureExtractionError("候选特征必须是 JSON object")
    required = (
        "feature_type",
        "title",
        "summary",
        "importance",
        "template_hashes",
        "components",
        "tags",
        "selection_reason",
    )
    for field in required:
        if field not in value:
            raise FeatureExtractionError(f"候选特征缺少字段 {field}")

    importance = _string(value["importance"], "importance")
    if importance not in IMPORTANCE_LEVELS:
        raise FeatureExtractionError("特征字段 importance 无效")
    template_hashes = _string_list(value["template_hashes"], "template_hashes", allow_empty=False)
    unknown = set(template_hashes) - known_hashes
    if unknown:
        raise FeatureExtractionError(f"候选特征引用未知 template_hash: {sorted(unknown)}")

    return {
        "feature_type": _string(value["feature_type"], "feature_type"),
        "title": _string(value["title"], "title"),
        "summary": _string(value["summary"], "summary"),
        "importance": importance,
        "template_hashes": template_hashes,
        "components": _string_list(value["components"], "components"),
        "tags": _string_list(value["tags"], "tags"),
        "selection_reason": _string(value["selection_reason"], "selection_reason"),
    }


def _candidate_id(entity: Dict[str, Any], feature: Dict[str, Any]) -> str:
    material = "|".join([
        str(entity.get("cluster") or ""),
        str(entity.get("entity_type") or ""),
        str(entity.get("entity_id") or ""),
        str(entity.get("window_start") or ""),
        feature["feature_type"],
        ",".join(sorted(feature["template_hashes"])),
    ])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:20]


def _attach_source_facts(
    entity: Dict[str, Any],
    feature: Dict[str, Any],
    model: str,
) -> Dict[str, Any]:
    source_by_hash = {
        str(template.get("template_hash")): template
        for template in _sanitized_templates(entity)
    }
    sources = [source_by_hash[template_hash] for template_hash in feature["template_hashes"]]
    first_seen = [source.get("first_seen") for source in sources if source.get("first_seen")]
    last_seen = [source.get("last_seen") for source in sources if source.get("last_seen")]
    return {
        "candidate_id": _candidate_id(entity, feature),
        "status": "pending",
        "reviewer_note": "",
        "approved_at": None,
        "cluster": entity.get("cluster"),
        "entity": {"type": entity.get("entity_type"), "id": entity.get("entity_id")},
        "window_start": entity.get("window_start"),
        "window_end": entity.get("window_end"),
        "risk_score": entity.get("risk_score"),
        "risk_level": entity.get("risk_level"),
        "affected_entities": entity.get("affected_entities") or [],
        **feature,
        "occurrence_count": sum(int(source.get("count") or 0) for source in sources),
        "time_range": {
            "first_seen": min(first_seen) if first_seen else entity.get("window_start"),
            "last_seen": max(last_seen) if last_seen else entity.get("window_end"),
        },
        "source_templates": sources,
        "provider": "ollama",
        "model": model,
    }


def _request_features(
    entity: Dict[str, Any],
    model: str,
    base_url: str,
    timeout: float,
) -> list[Dict[str, Any]]:
    evidence = _evidence_for_entity(entity)
    body = {
        "model": model,
        "stream": False,
        "format": FEATURE_RESPONSE_SCHEMA,
        "options": {"temperature": 0},
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是日志特征识别器，不是 RCA 系统。只识别值得提交给外部 RCA 专家的关键日志特征。"
                    "禁止输出根因、影响评估、处置建议或虚构证据。每个特征必须引用输入中真实存在的 template_hash。"
                    "使用中文并严格输出给定 JSON Schema。"
                ),
            },
            {"role": "user", "content": json.dumps(evidence, ensure_ascii=False, separators=(",", ":"))},
        ],
    }
    request = Request(
        f"{base_url}/api/chat",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw_response = response.read()
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise FeatureExtractionError(f"Ollama HTTP {exc.code}: {details}") from exc
    except (URLError, TimeoutError, socket.timeout) as exc:
        raise FeatureExtractionError(f"无法连接 Ollama: {exc}") from exc

    try:
        payload = json.loads(raw_response)
        content = payload["message"]["content"]
        model_output = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError) as exc:
        raise FeatureExtractionError("Ollama 返回了无效的结构化特征响应") from exc
    if not isinstance(model_output, dict) or not isinstance(model_output.get("features"), list):
        raise FeatureExtractionError("Ollama 特征响应缺少 features 数组")

    known_hashes = {
        str(template.get("template_hash"))
        for template in evidence["templates"]
        if template.get("template_hash")
    }
    return [_validate_model_feature(feature, known_hashes) for feature in model_output["features"]]


def extract_features_for_entity(
    entity: Dict[str, Any],
    model: str,
    base_url: str = DEFAULT_OLLAMA_URL,
    timeout: float = 120,
) -> list[Dict[str, Any]]:
    if not model or not model.strip():
        raise FeatureExtractionError("必须指定 Ollama 模型")
    if timeout <= 0:
        raise FeatureExtractionError("Ollama timeout 必须大于 0")
    normalized_url = _validate_base_url(base_url)
    model_name = model.strip()
    features = _request_features(entity, model_name, normalized_url, timeout)
    return [_attach_source_facts(entity, feature, model_name) for feature in features]


def generate_feature_candidates(
    entities: list[Dict[str, Any]],
    model: str,
    base_url: str = DEFAULT_OLLAMA_URL,
    timeout: float = 120,
    min_score: float = 40,
) -> list[Dict[str, Any]]:
    results = []
    for entity in entities:
        if float(entity.get("risk_score") or 0) < min_score:
            continue
        results.extend(extract_features_for_entity(entity, model, base_url, timeout))
    return results
