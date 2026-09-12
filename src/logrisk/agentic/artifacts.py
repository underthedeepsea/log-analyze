from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from .errors import AgenticError
from .tool_registry import _reject_sensitive


MAX_ARTIFACT_BYTES = 16 * 1024
MAX_DEPENDENCY_BYTES = 32 * 1024
READ_ARTIFACT_TYPES = {
    "get_sanitized_evidence": "evidence_assessment_v1",
    "find_approved_rules": "rule_match_assessment_v1",
    "inspect_knowledge_assets": "rule_match_assessment_v1",
}


def canonical_fingerprint(value: Any) -> str:
    return hashlib.sha256(_encoded(value)).hexdigest()


def _encoded(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def read_tool_artifact(
    *, run_id: str, source_job_id: str, entity_id: str, tool_name: str,
    tool_call_id: str, evidence_hash: str, output: dict[str, Any],
) -> dict[str, Any]:
    _reject_sensitive(output)
    payload = {
        "schema_version": READ_ARTIFACT_TYPES[tool_name],
        "producer_run_id": run_id,
        "evidence_refs": [{"source_job_id": source_job_id, "entity_id": entity_id,
                           "evidence_hash": evidence_hash, "tool_call_id": tool_call_id}],
        "source_tool": tool_name,
        "safe_payload": copy.deepcopy(output),
        "limitations": [],
        "requires_explicit_read": False,
    }
    if len(_encoded(payload)) > MAX_ARTIFACT_BYTES:
        payload["safe_payload"] = {}
        payload["requires_explicit_read"] = True
        payload["limitations"] = [
            "产物超过 16 KiB；完整脱敏结果保留于 producer_run_id / tool_call_id，必须显式读取后使用。"
        ]
    if len(_encoded(payload)) > MAX_ARTIFACT_BYTES:
        raise AgenticError("Agent 产物引用超过大小限制", code="agent_artifact_too_large")
    return payload


def dependency_artifacts(nodes: list[dict[str, Any]], dependencies: list[str]) -> dict[str, Any]:
    summaries = {
        node["node_id"]: copy.deepcopy(node.get("result_summary") or {})
        for node in nodes if node["node_id"] in dependencies
    }
    _reject_sensitive(summaries)
    if len(_encoded(summaries)) <= MAX_DEPENDENCY_BYTES:
        return summaries
    # Retain every artifact identity; payloads remain available in the producer run.
    for summary in summaries.values():
        summary["artifacts"] = [
            {"artifact_id": item.get("artifact_id"), "artifact_type": item.get("artifact_type"),
             "run_id": item.get("run_id") or summary.get("child_agent_run_id"),
             "requires_explicit_read": True}
            for item in summary.get("artifacts") or []
        ]
        summary["limitations"] = ["依赖摘要超过 32 KiB；必须显式读取引用的产物后使用。"]
    if len(_encoded(summaries)) > MAX_DEPENDENCY_BYTES:
        raise AgenticError("Agent 依赖引用超过大小限制", code="agent_dependency_too_large")
    return summaries
