from __future__ import annotations

import copy
import json
import re
import uuid
from datetime import datetime, timezone

from logrisk.approved_rules import rule_scope_matches
from typing import Any

from .artifacts import canonical_fingerprint
from .errors import AgenticError
from .models import validate_evidence_scope
from .tool_registry import _reject_sensitive, validate_argument_schema

EXPERT_ROLES = frozenset({"evidence_specialist", "rule_specialist", "feature_specialist"})
MAX_EXPERT_CONTEXT_BYTES = 64 * 1024
# Group related uncertainty into evidence/counts/rules/upstream/cause/impact/
# recovery/manual-review categories. Text and conclusions remain uncapped.
MAX_EXPERT_LIMITATIONS = 8
_CREDENTIAL = re.compile(r"(?i)(?:authorization\s*:\s*bearer\s+|(?:api[_-]?key|password|secret|token)\s*[:=]\s*['\"]?)([^<\s'\"]+)")


def reject_provider_secrets(value: Any) -> None:
    _reject_sensitive(value)
    if isinstance(value, str) and _CREDENTIAL.search(value):
        raise AgenticError("专家材料含未脱敏凭据", code="agent_context_sensitive")
    if isinstance(value, dict):
        for item in value.values():
            reject_provider_secrets(item)
    elif isinstance(value, list):
        for item in value:
            reject_provider_secrets(item)


def validate_material(evidence: dict[str, Any], source_job_id: str, entity_id: str, *, expected_cluster: str | None = None) -> None:
    validate_evidence_scope(evidence, source_job_id, entity_id, code="agent_scope_invalid")
    entity = evidence.get("entity") or {}
    if entity.get("id") != entity_id or not isinstance(entity.get("cluster"), str) or not entity["cluster"]:
        raise AgenticError("专家证据缺少规范实体/集群", code="agent_scope_invalid")
    if expected_cluster is not None and entity["cluster"] != expected_cluster:
        raise AgenticError("专家证据跨集群", code="agent_scope_invalid")
    seen: dict[str, tuple[Any, Any]] = {}
    templates = evidence.get("templates")
    if not isinstance(templates, list) or not templates:
        raise AgenticError("专家证据缺少真实模板", code="agent_template_identity_missing")
    for item in templates:
        if not isinstance(item, dict) or not isinstance(item.get("template_hash"), str) or not item["template_hash"].strip() or not isinstance(item.get("template"), str) or not item["template"].strip():
            raise AgenticError("专家证据缺少真实模板标识或内容", code="agent_template_identity_missing")
        for key, expected in (("cluster", entity["cluster"]), ("entity_id", entity_id), ("node", entity_id), ("source_job_id", source_job_id)):
            if key in item and item[key] is not None and item[key] != expected:
                raise AgenticError("模板混入其他实体/来源", code="agent_scope_invalid")
        signature = (item.get("component"), item["template"])
        if item["template_hash"] in seen and seen[item["template_hash"]] != signature:
            raise AgenticError("同一模板标识对应冲突内容", code="agent_template_identity_ambiguous")
        seen[item["template_hash"]] = signature
    reject_provider_secrets(evidence)


RULE_CONTEXT_FIELDS = (
    "rule_id", "title", "feature_type", "status", "current_version", "approved_at",
    "problem_code", "approval_key", "selection_reason", "summary", "match_mode", "components",
    "review_scope", "scope", "cluster", "conditions", "anchor_signatures", "supporting_signatures",
    "next_review_at", "expires_at", "valid_until",
)


def approved_rule_summary(rule: dict[str, Any], cluster: str | None) -> dict[str, Any] | None:
    if rule.get("status") != "active" or not rule_scope_matches(rule, cluster):
        return None
    # next_review_at is a review reminder, not an expiry in the existing domain.
    for field in ("expires_at", "valid_until"):
        if rule.get(field) is not None:
            try:
                expiry = datetime.fromisoformat(str(rule[field]).replace("Z", "+00:00"))
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                if expiry <= datetime.now(timezone.utc):
                    return None
            except (ValueError, TypeError):
                return None
    value = {key: copy.deepcopy(rule[key]) for key in RULE_CONTEXT_FIELDS if key in rule}
    value["template_signatures"] = copy.deepcopy(rule.get("template_signatures") or [])
    value["current_version"] = rule.get("current_version")
    value["applicability"] = "reference_only_requires_evidence_conditions"
    value["canonical_hash"] = canonical_fingerprint(value)
    return value


def expert_rule_bindings(context: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    """Bind all visible rule material, including completed upstream dependencies."""
    values = [{"rule_id": item["rule_id"], "current_version": item.get("current_version"),
               "reference": rule_reference(item), "canonical_hash": item.get("canonical_hash")}
              for item in context["approved_rules"]["items"]]
    for upstream in context["upstream_assessments"]:
        inherited = upstream.get("rule_bindings") or []
        if set(upstream["assessment"]["rule_refs"]) - {item.get("reference") for item in inherited}:
            raise AgenticError("上游规则引用缺少权威快照绑定", code="agent_rule_reference_changed")
        values.extend(copy.deepcopy(inherited))
    result: dict[str, dict[str, Any]] = {}
    for item in values:
        key = str(item.get("rule_id") or "")
        if not key or not item.get("canonical_hash") or (key in result and result[key] != item):
            raise AgenticError("规则规范身份冲突或缺失", code="agent_rule_reference_changed")
        result[key] = item
    return tuple(result[key] for key in sorted(result))


def rule_reference(rule: dict[str, Any]) -> str:
    return f"{rule['rule_id']}@{rule.get('current_version') if rule.get('current_version') is not None else 'unknown'}"


def reference_array_schema(values: Any, *, minimum: int = 0) -> dict[str, Any]:
    identities = sorted(set(values))
    item: dict[str, Any] = {"type": "string", "minLength": 1}
    if identities:
        item["enum"] = identities
    return {"type": "array", "items": item, "minItems": minimum,
            "maxItems": len(identities), "uniqueItems": True}


def evidence_facts(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    """Per-template facts, never distinct objects inferred from log frequency.

    A typed pod UID is an observed identifier, not a complete affected-object
    census. Aggregated templates without that registered typed value stay unknown.
    """
    facts = []
    seen = set()
    for item in evidence["templates"]:
        ref = item["template_hash"]
        if ref in seen:
            raise AgenticError("事实模板标识重复", code="agent_fact_invalid")
        seen.add(ref)
        count = item.get("count")
        if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count < 0):
            raise AgenticError("日志出现次数无效", code="agent_fact_invalid")
        identifiers = []
        fields = item.get("semantic_fields") or {}
        versions = item.get("semantic_dictionary_versions") or {}
        for parameter in item.get("typed_parameters") or []:
            if parameter.get("field") != "pod_uid" or parameter.get("dictionary_id") not in versions or not parameter.get("rule_id"):
                continue
            value = parameter.get("value")
            if not isinstance(value, str) or fields.get("pod_uid") != value:
                continue
            try:
                identifier = str(uuid.UUID(value))
            except (ValueError, AttributeError):
                continue
            if identifier not in identifiers:
                identifiers.append(identifier)
        semantic = item.get("risk_semantic") or {}
        anomaly = semantic.get("risk_type") or "template:" + ref
        if not isinstance(anomaly, str) or not anomaly:
            raise AgenticError("异常类型缺少可核对来源", code="agent_fact_invalid")
        facts.append({"template_ref": ref, "anomaly_type": anomaly,
                      "log_occurrence_count": count if count is not None else "unknown",
                      "proven_object_count": len(identifiers) if identifiers else "unknown",
                      "object_ids": sorted(identifiers)})
    return sorted(facts, key=lambda item: item["template_ref"])



EXPERT_GOALS = {
    "evidence_specialist": "按输入异常分组解释证据所支持的风险、条件后果与具体人工核查；区分日志次数、唯一对象、根因与恢复的未知边界",
    "rule_specialist": "按输入异常分组审阅已提供批准规则的适用条件和证据缺口，说明风险、条件后果与具体人工核查；无可引用规则时明确查询范围边界",
    "feature_specialist": "结合本实体两个已完成专家意见，提取证据支持的待人工审批日志特征，保留不同异常及其原因边界",
}


def expert_goal(role_id: str, goal: Any = None) -> str:
    base = EXPERT_GOALS[role_id]
    supplied = str(goal or "").strip()
    defaults = {"提取可审批日志特征", f"以{role_id}角色处理当前风险实体", base}
    return base if not supplied or supplied in defaults else base + "；补充目标：" + supplied


def expert_input_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Lossless v3 presentation; the context hash still binds the original context.

    Group only by the exact registered anomaly type (otherwise template identity).
    This is presentation, not correlation, deduplication or a causal assertion.
    The original evidence and local assessment contract are never modified.
    """
    value = copy.deepcopy(summary)
    context = value.get("expert_context")
    if not context or context.get("schema_version") != "expert_context_v3":
        return value
    evidence = context["evidence"]
    scope = context["scope"]
    validate_material(evidence, scope["source_job_id"], scope["entity_id"], expected_cluster=scope["cluster"])
    facts = evidence_facts(evidence)
    if context.get("facts") != facts:
        raise AgenticError("输入事实与锁定模板不一致", code="agent_fact_invalid")
    groups: dict[str, list[str]] = {}
    for fact in facts:
        groups.setdefault(fact["anomaly_type"], []).append(fact["template_ref"])
    context.pop("facts")
    # Counts, literals and template content appear once, in the unchanged templates.
    context["object_evidence"] = {
        fact["template_ref"]: {"proven_object_count": fact["proven_object_count"], "object_ids": fact["object_ids"]}
        for fact in facts
    }
    context["evidence_groups"] = [
        {"anomaly_type": anomaly, "template_refs": refs}
        for anomaly, refs in sorted(groups.items())
    ]
    context["presentation_boundary"] = "evidence_groups仅按原异常类型分组，未知类型逐模板独立；不表示同因或同一对象。context_hash绑定原始完整上下文；本视图保留原模板全部字段，仅消除facts中的重复次数/引用/类型。object_evidence不是完整受影响对象普查；实际根因、影响及恢复未知。"
    reject_provider_secrets(value)
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_EXPERT_CONTEXT_BYTES:
        raise AgenticError("完整专家材料超过有界上下文；不能静默遗漏后继续", code="agent_expert_context_too_large")
    return value

def fact_schema(fact: dict[str, Any]) -> dict[str, Any]:
    properties = {}
    for key, value in fact.items():
        if key == "object_ids":
            properties[key] = reference_array_schema(value, minimum=len(value))
        else:
            properties[key] = {"type": "integer" if isinstance(value, int) else "string", "const": value}
    return {"type": "object", "additionalProperties": False,
            "required": list(properties), "properties": properties}


def validate_fact_observations(value: dict[str, Any], evidence: dict[str, Any]) -> None:
    facts_list = evidence_facts(evidence)
    validate_argument_schema(value.get("observations"), {"type": "array", "minItems": len(facts_list), "maxItems": len(facts_list), "uniqueItems": True})
    if value.get("root_cause_status") != "unknown" or value.get("recovery_status") != "unknown":
        raise AgenticError("聚合证据不证明根因或恢复", code="agent_fact_invalid")
    facts = {item["template_ref"]: item for item in evidence_facts(evidence)}
    refs = []
    for observation in value["observations"]:
        if not isinstance(observation, dict) or observation.get("template_ref") not in facts:
            raise AgenticError("事实引用不存在", code="agent_fact_invalid")
        ref = observation["template_ref"]
        validate_argument_schema(observation, fact_schema(facts[ref]))
        if observation != facts[ref]:
            raise AgenticError("日志次数、对象证明或异常类型不符", code="agent_fact_invalid")
        refs.append(ref)
    if len(refs) != len(set(refs)) or set(refs) != set(facts) or set(refs) - set(value["template_refs"]):
        raise AgenticError("事实被合并、遗漏或未引用", code="agent_fact_invalid")


ANALYSIS_KINDS = {"association_hypothesis": "unverified", "cause_hypothesis": "unverified",
                  "conditional_impact": "conditional", "manual_guidance": "guidance"}
_LITERAL = re.compile(r"(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}|\b(?:\d{1,3}\.){3}\d{1,3}\b|\b[A-Z]{2,}[-_][A-Z0-9_-]+\b|\b\d{4}-\d{2}-\d{2}T[\d:.]+(?:Z|[+-]\d{2}:\d{2})?")


def analysis_schema(context: dict[str, Any]) -> dict[str, Any]:
    nonempty = {"type": "string", "minLength": 1}
    hashes = [t["template_hash"] for t in context["evidence"]["templates"]]
    upstream = [a["artifact_id"] for a in context["upstream_assessments"]]
    blocks = []
    for kind, status in ANALYSIS_KINDS.items():
        blocks.append({"type": "object", "additionalProperties": False,
                       "required": ["kind", "status", "fact_refs", "text", "missing_evidence", "manual_checks"],
                       "properties": {"kind": {"type": "string", "const": kind},
                                      "status": {"type": "string", "const": status},
                                      "fact_refs": reference_array_schema(hashes, minimum=2 if kind == "association_hypothesis" else 1),
                                      "text": nonempty,
                                      "missing_evidence": {"type": "array", "items": nonempty, "minItems": 1 if status == "unverified" else 0, "maxItems": 8, "uniqueItems": True},
                                      "manual_checks": {"type": "array", "items": nonempty, "minItems": 1, "maxItems": 8, "uniqueItems": True}}})
    props = {"schema_version": {"type": "string", "const": "expert_analysis_v3"},
             "role_id": {"type": "string", "const": context["role_id"]},
             "context_hash": {"type": "string", "const": context["context_hash"]},
             "template_refs": reference_array_schema(hashes, minimum=1),
             "rule_refs": reference_array_schema(rule_reference(r) for r in context["approved_rules"]["items"]),
             "upstream_refs": reference_array_schema(upstream, minimum=len(upstream) if context["role_id"] == "feature_specialist" else 0),
             "analyses": {"type": "array", "items": {"oneOf": blocks}, "minItems": 1, "maxItems": 8, "uniqueItems": True},
             "limitations": {"type": "array", "items": nonempty, "minItems": 1, "maxItems": 8, "uniqueItems": True}}
    return {"type": "object", "additionalProperties": False, "required": list(props), "properties": props}


def validate_analysis(value: dict[str, Any], evidence: dict[str, Any]) -> None:
    permitted = {t["template_hash"]: t for t in evidence["templates"]}
    referenced = set(value["template_refs"])
    for block in value["analyses"]:
        kind = block.get("kind") if isinstance(block, dict) else None
        if kind not in ANALYSIS_KINDS:
            raise AgenticError("解释类型无效", code="agent_analysis_invalid")
        refs = block.get("fact_refs") or []
        if block.get("status") != ANALYSIS_KINDS[kind] or len(refs) != len(set(refs)) or not refs or set(refs) - referenced or set(refs) - set(permitted) or (kind == "association_hypothesis" and len(refs) < 2):
            raise AgenticError("假设状态或引用越界", code="agent_analysis_invalid")
        # Validate the selected branch locally: oneOf alone is a provider schema.
        branch = next(b for b in analysis_schema({"role_id": value["role_id"], "context_hash": value["context_hash"], "evidence": evidence, "approved_rules": {"items": []}, "upstream_assessments": []})["properties"]["analyses"]["items"]["oneOf"] if b["properties"]["kind"]["const"] == kind)
        validate_argument_schema(block, branch)
        source = json.dumps({"entity": evidence["entity"], "window": evidence.get("window"), "templates": [permitted[ref] for ref in refs]}, ensure_ascii=False)
        source_literals = set(_LITERAL.findall(source))
        texts = [block["text"], *block["missing_evidence"], *block["manual_checks"]]
        for text in texts:
            if any(token not in source_literals for token in _LITERAL.findall(text)):
                raise AgenticError("解释改写或编造事实标识", code="agent_analysis_literal_invalid")
    # This validates references/modality/literals, never general natural-language truth.


def assessment_schema(context: dict[str, Any]) -> dict[str, Any]:
    if context.get("schema_version") == "expert_context_v3":
        return analysis_schema(context)
    nonempty = {"type": "string", "minLength": 1}
    upstream = [item["artifact_id"] for item in context["upstream_assessments"]]
    schema = {
        "type": "object", "additionalProperties": False,
        "required": ["role_id", "context_hash", "conclusion", "template_refs", "rule_refs", "upstream_refs", "limitations"],
        "properties": {
            "role_id": {"type": "string", "const": context["role_id"]},
            "context_hash": {"type": "string", "const": context["context_hash"]},
            "conclusion": {**nonempty, "description": "用中文保留实际风险、直接证据原因、底层根因边界、条件后果和人工建议；实际影响与恢复无证据时保持未知。"},
            "template_refs": reference_array_schema((item["template_hash"] for item in context["evidence"]["templates"]), minimum=1),
            "rule_refs": reference_array_schema(rule_reference(item) for item in context["approved_rules"]["items"]),
            "upstream_refs": reference_array_schema(upstream, minimum=len(upstream) if context["role_id"] == "feature_specialist" else 0),
            "limitations": {"type": "array", "items": {**nonempty, "description": "一项独立的中文不确定性；相关事实可合并，不能重复句子或把配置当恢复事件。"}, "minItems": 1, "maxItems": MAX_EXPERT_LIMITATIONS, "uniqueItems": True,
                            "description": "只列实际存在的1至8项独立限制，不凑满、不循环；完整列完后关闭数组并完成其余必需字段。"},
        },
    }


    if context.get("schema_version") == "expert_context_v2":
        facts = evidence_facts(context["evidence"])
        schema["required"].extend(["observations", "root_cause_status", "recovery_status"])
        schema["properties"].update({
            "observations": {"type": "array", "minItems": len(facts), "maxItems": len(facts),
                             "uniqueItems": True, "items": {"oneOf": [fact_schema(fact) for fact in facts]}},
            "root_cause_status": {"type": "string", "const": "unknown"},
            "recovery_status": {"type": "string", "const": "unknown"},
        })
    return schema


def validate_assessment(value: Any, context: dict[str, Any], steps: Any = ()) -> dict[str, Any]:
    validate_argument_schema(value, assessment_schema(context))
    if value["role_id"] != context["role_id"] or value["context_hash"] != context["context_hash"]:
        raise AgenticError("专家意见未绑定本次真实模型输入", code="agent_assessment_scope_invalid")
    allowed = {
        "template_refs": {item["template_hash"] for item in context["evidence"]["templates"]},
        "rule_refs": {rule_reference(item) for item in context["approved_rules"]["items"]},
        "upstream_refs": {item["artifact_id"] for item in context["upstream_assessments"]},
    }
    for field, permitted in allowed.items():
        if len(value[field]) != len(set(value[field])) or set(value[field]) - permitted:
            raise AgenticError("专家意见引用不存在或已失效材料", code="agent_assessment_reference_invalid")
    if context["role_id"] == "feature_specialist":
        if set(value["upstream_refs"]) != allowed["upstream_refs"]:
            raise AgenticError("特征意见缺少真实上游意见引用", code="agent_assessment_reference_invalid")
        for step in steps:
            feature = step.arguments.get("feature")
            if feature and set(feature.get("template_hashes") or []) - set(value["template_refs"]):
                raise AgenticError("候选未绑定模型实际分析的模板", code="agent_assessment_reference_invalid")
    if context.get("schema_version") == "expert_context_v2":
        validate_fact_observations(value, context["evidence"])
    if context.get("schema_version") == "expert_context_v3":
        validate_analysis(value, context["evidence"])
    reject_provider_secrets(value)
    return copy.deepcopy(value)


def completed_upstream(run: dict[str, Any], repository: Any, evidence: dict[str, Any], evidence_hash: str) -> list[dict[str, Any]]:
    snapshot = run["locked_snapshot"]
    result = []
    for ref in snapshot.get("dependency_artifact_refs") or []:
        child = repository.get_run(str(ref.get("child_agent_run_id") or ""))
        locked = child["locked_snapshot"]
        if child["status"] not in {"awaiting_human", "completed"} or child["source_job_id"] != run["source_job_id"] or child["entity_id"] != run["entity_id"] or locked.get("workflow_run_id") != snapshot.get("workflow_run_id") or locked.get("workflow_node_id") != ref.get("node_id"):
            raise AgenticError("上游意见跨实体/工作流或未完成", code="agent_upstream_scope_invalid")
        opinions = [item for item in child["artifacts"] if item["artifact_type"] == "expert_assessment_v1"]
        if len(opinions) != 1:
            raise AgenticError("上游缺少唯一真实模型意见", code="agent_upstream_assessment_missing")
        artifact = opinions[0];payload = artifact["payload"];scope = payload.get("scope") or {}
        response = payload.get("model_response")
        if not isinstance(response, dict) or canonical_fingerprint(response) != payload.get("model_response_hash") or response.get("assessment") != payload.get("assessment") or payload.get("assessment", {}).get("context_hash") != payload.get("context_hash"):
            raise AgenticError("上游模型返回与已存意见不一致", code="agent_upstream_scope_invalid")
        if payload.get("origin") != "model" or scope.get("evidence_hash") != evidence_hash or scope.get("cluster") != evidence["entity"]["cluster"] or scope.get("source_job_id") != run["source_job_id"] or scope.get("entity_id") != run["entity_id"] or payload.get("assessment", {}).get("role_id") != locked.get("workflow_role_id"):
            raise AgenticError("上游模型意见证据范围已变化", code="agent_upstream_scope_invalid")
        if payload["assessment"].get("schema_version") != "expert_analysis_v3":
            raise AgenticError("新分析不能借用旧或失败上游", code="agent_upstream_scope_invalid")
        validate_analysis(payload["assessment"], evidence)
        result.append({"artifact_id": artifact["artifact_id"], "producer_run_id": child["run_id"], "role_id": locked["workflow_role_id"], "assessment": copy.deepcopy(payload["assessment"]), "model_response_hash": payload["model_response_hash"], "rule_bindings": copy.deepcopy(payload.get("rule_bindings") or [])})
    if snapshot.get("workflow_role_id") == "feature_specialist" and {item["role_id"] for item in result} != {"evidence_specialist", "rule_specialist"}:
        raise AgenticError("特征专家必须读取证据/规则专家的已完成真实意见", code="agent_upstream_assessment_missing")
    return result


def build_expert_context(run: dict[str, Any], evidence: dict[str, Any], rules: dict[str, Any], reads: list[dict[str, Any]], upstream: list[dict[str, Any]], evidence_hash: str) -> dict[str, Any]:
    value = {
        "schema_version": "expert_context_v3", "role_id": run["locked_snapshot"]["workflow_role_id"],
        "scope": {"source_job_id": run["source_job_id"], "entity_id": run["entity_id"], "cluster": evidence["entity"]["cluster"], "evidence_hash": evidence_hash},
        "evidence": copy.deepcopy(evidence), "facts": evidence_facts(evidence), "approved_rules": copy.deepcopy(rules),
        "read_receipts": copy.deepcopy(reads), "upstream_assessments": copy.deepcopy(upstream),
        "boundaries": ["工具材料不是模型意见；只引用本上下文真实标识。", "active规则只是参考；需版本/适用条件及原Evaluator，不自动批准或推断根因。", "未知原因/实际影响/恢复保持未知；候选等待人工审批。"],
    }
    reject_provider_secrets(value)
    value["context_hash"] = canonical_fingerprint(value)
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_EXPERT_CONTEXT_BYTES:
        raise AgenticError("完整专家材料超过有界上下文；不能静默遗漏后继续", code="agent_expert_context_too_large")
    return value
