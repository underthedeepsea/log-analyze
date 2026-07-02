from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from logrisk.ai_harness.evidence_builder import (
    build_feature_evidence,
    evidence_hash,
    sanitized_templates,
)
from logrisk.ai_harness.evaluator import evaluate_feature_output
from logrisk.ai_harness.model_client import ModelClient, ModelClientError
from logrisk.ai_harness.prompt_registry import PromptRegistry, PromptTemplate
from logrisk.ai_harness.providers.ollama import OllamaModelClient
from logrisk.ai_harness.trace_logger import AITraceLogger


DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"
FEATURE_PROMPT_ID = "feature_extract_v2_compact_en"
ROOT = Path(__file__).resolve().parents[2]
PROMPT_REGISTRY = PromptRegistry(ROOT / "prompts", ROOT / "configs" / "ai_harness.yaml")
TRACE_LOGGER = AITraceLogger()
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
    try:
        return OllamaModelClient._validate_base_url(base_url)
    except ModelClientError as exc:
        raise FeatureExtractionError(str(exc)) from exc


def _write_trace(
    *,
    prompt: PromptTemplate,
    evidence: Dict[str, Any],
    provider: str,
    model: str,
    job_id: str | None,
    raw_output: str,
    parsed_output: Dict[str, Any] | None,
    validation_result: Dict[str, Any],
    evaluator_result: Dict[str, Any] | None = None,
    latency_ms: int,
    status: str,
) -> str | None:
    trace_id = str(uuid.uuid4())
    try:
        TRACE_LOGGER.append({
            "trace_id": trace_id,
            "job_id": job_id,
            "candidate_id": None,
            "entity_type": evidence.get("entity", {}).get("type"),
            "entity_id": evidence.get("entity", {}).get("id"),
            "prompt_id": prompt.prompt_id,
            "prompt_hash": prompt.sha256,
            "prompt_path": prompt.path,
            "provider": provider,
            "model": model,
            "input_evidence_hash": evidence_hash(evidence),
            "input_evidence": evidence,
            "raw_output": raw_output,
            "parsed_output": parsed_output or {},
            "validation_result": validation_result,
            "evaluator_result": evaluator_result or {"passed": False, "errors": [], "warnings": [], "score": 0.0, "rule_results": []},
            "latency_ms": latency_ms,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
        })
        return trace_id
    except Exception:
        return None


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
        for template in sanitized_templates(entity)
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
    model_client: ModelClient | None = None,
    prompt_id: str = FEATURE_PROMPT_ID,
    job_id: str | None = None,
) -> tuple[list[Dict[str, Any]], str | None, Dict[str, Any]]:
    evidence = build_feature_evidence(entity)
    prompt = PROMPT_REGISTRY.load(prompt_id)
    messages = [
        {"role": "system", "content": prompt.content},
        {"role": "user", "content": json.dumps(evidence, ensure_ascii=False, separators=(",", ":"))},
    ]
    client = model_client or OllamaModelClient(base_url)
    start = time.perf_counter()
    try:
        model_output = client.generate_json(
            messages,
            FEATURE_RESPONSE_SCHEMA,
            model=model,
            timeout=timeout,
            options={"temperature": 0},
        )
    except ModelClientError as exc:
        _write_trace(
            prompt=prompt,
            evidence=evidence,
            provider="ollama",
            model=model,
            job_id=job_id,
            raw_output=exc.raw_output,
            parsed_output=None,
            validation_result={"valid": False, "errors": [str(exc)], "warnings": []},
            latency_ms=int((time.perf_counter() - start) * 1000),
            status=exc.status,
        )
        raise FeatureExtractionError(str(exc)) from exc
    if not isinstance(model_output, dict) or not isinstance(model_output.get("features"), list):
        _write_trace(
            prompt=prompt,
            evidence=evidence,
            provider="ollama",
            model=model,
            job_id=job_id,
            raw_output=json.dumps(model_output, ensure_ascii=False),
            parsed_output=model_output if isinstance(model_output, dict) else {},
            validation_result={"valid": False, "errors": ["missing_features"], "warnings": []},
            latency_ms=int((time.perf_counter() - start) * 1000),
            status="validation_failed",
        )
        raise FeatureExtractionError("Ollama 特征响应缺少 features 数组")

    known_hashes = {
        str(template.get("template_hash"))
        for template in evidence["templates"]
        if template.get("template_hash")
    }
    try:
        features = [_validate_model_feature(feature, known_hashes) for feature in model_output["features"]]
    except FeatureExtractionError as exc:
        _write_trace(
            prompt=prompt,
            evidence=evidence,
            provider="ollama",
            model=model,
            job_id=job_id,
            raw_output=json.dumps(model_output, ensure_ascii=False),
            parsed_output=model_output,
            validation_result={"valid": False, "errors": [str(exc)], "warnings": []},
            latency_ms=int((time.perf_counter() - start) * 1000),
            status="validation_failed",
        )
        raise
    evaluator_results = [
        evaluate_feature_output(feature=feature, entity=entity, evidence=evidence)
        for feature in features
    ]
    failed_evaluations = [result for result in evaluator_results if not result.get("passed")]
    evaluator_summary = {
        "passed": not failed_evaluations,
        "errors": [error for result in failed_evaluations for error in result.get("errors", [])],
        "warnings": [warning for result in evaluator_results for warning in result.get("warnings", [])],
        "score": min((float(result.get("score") or 0.0) for result in evaluator_results), default=1.0),
        "rule_results": [rule for result in evaluator_results for rule in result.get("rule_results", [])],
    }
    if failed_evaluations:
        _write_trace(
            prompt=prompt,
            evidence=evidence,
            provider="ollama",
            model=model,
            job_id=job_id,
            raw_output=json.dumps(model_output, ensure_ascii=False),
            parsed_output=model_output,
            validation_result={"valid": True, "errors": [], "warnings": []},
            evaluator_result=evaluator_summary,
            latency_ms=int((time.perf_counter() - start) * 1000),
            status="evaluator_failed",
        )
        raise FeatureExtractionError("Evaluator 拦截模型输出: " + (evaluator_summary["errors"][0] if evaluator_summary["errors"] else "质量门禁未通过"))
    trace_id = _write_trace(
        prompt=prompt,
        evidence=evidence,
        provider="ollama",
        model=model,
        job_id=job_id,
        raw_output=json.dumps(model_output, ensure_ascii=False),
        parsed_output=model_output,
        validation_result={"valid": True, "errors": [], "warnings": []},
        evaluator_result=evaluator_summary,
        latency_ms=int((time.perf_counter() - start) * 1000),
        status="success",
    )
    return features, trace_id, evaluator_summary


def extract_features_for_entity(
    entity: Dict[str, Any],
    model: str,
    base_url: str = DEFAULT_OLLAMA_URL,
    timeout: float = 120,
    model_client: ModelClient | None = None,
    prompt_id: str = FEATURE_PROMPT_ID,
    job_id: str | None = None,
) -> list[Dict[str, Any]]:
    if not model or not model.strip():
        raise FeatureExtractionError("必须指定 Ollama 模型")
    if timeout <= 0:
        raise FeatureExtractionError("Ollama timeout 必须大于 0")
    normalized_url = _validate_base_url(base_url)
    model_name = model.strip()
    selected_prompt = prompt_id or FEATURE_PROMPT_ID
    features, trace_id, evaluator_result = _request_features(entity, model_name, normalized_url, timeout, model_client, selected_prompt, job_id)
    attached = [_attach_source_facts(entity, feature, model_name) for feature in features]
    for feature in attached:
        feature["prompt_id"] = selected_prompt
        feature["trace_id"] = trace_id
        feature["evaluator_result"] = evaluator_result
    return attached


def generate_feature_candidates(
    entities: list[Dict[str, Any]],
    model: str,
    base_url: str = DEFAULT_OLLAMA_URL,
    timeout: float = 120,
    min_score: float = 40,
    model_client: ModelClient | None = None,
    prompt_id: str = FEATURE_PROMPT_ID,
    job_id: str | None = None,
) -> list[Dict[str, Any]]:
    results = []
    for entity in entities:
        if float(entity.get("risk_score") or 0) < min_score:
            continue
        results.extend(extract_features_for_entity(entity, model, base_url, timeout, model_client, prompt_id, job_id))
    return results
