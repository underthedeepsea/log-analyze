from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from logrisk.ai_harness.evaluator import EVALUATOR_VERSION, evaluate_feature_output

from .artifacts import canonical_fingerprint
from .errors import AgenticError
from .evidence_context import approved_rule_summary, rule_reference
from .tool_registry import AgentToolContext, ToolRegistry, _reject_sensitive


_NONEMPTY_STRING = {"type": "string", "minLength": 1}
_STRING_LIST = {"type": "array", "minItems": 1, "items": _NONEMPTY_STRING}
_OPTIONAL_STRING_LIST = {"type": "array", "items": _NONEMPTY_STRING}
_FEATURE_PROPERTIES = {
    "feature_type": _NONEMPTY_STRING,
    "title": _NONEMPTY_STRING,
    "summary": _NONEMPTY_STRING,
    "importance": {"type": "string", "enum": ["critical", "high", "medium", "low"]},
    "template_hashes": _STRING_LIST,
    "components": _STRING_LIST,
    "tags": _STRING_LIST,
    "selection_reason": _NONEMPTY_STRING,
    "match_mode": {"type": "string", "enum": ["semantic", "template_set"]},
    "problem_resolution": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "confidence": _NONEMPTY_STRING,
            "semantic_safe": {"type": "boolean"},
            "ambiguity": {"type": "boolean"},
            "evidence_source": _NONEMPTY_STRING,
            "matched_rule": {"type": ["string", "null"]},
            "supporting_codes": {"type": "array", "items": _NONEMPTY_STRING},
            "subtype": {"type": ["string", "null"]},
            "missing_selected_ids": {"type": "array", "items": _NONEMPTY_STRING},
            "unresolved_selected_ids": {"type": "array", "items": _NONEMPTY_STRING},
        },
    },
}
_FEATURE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["feature_type", "title", "summary", "importance", "template_hashes", "components", "tags", "selection_reason"],
    "properties": _FEATURE_PROPERTIES,
}


def build_agent_tool_registry(feature_jobs: Any, rule_governance: Any, knowledge_packages: Any) -> ToolRegistry:
    def load_evidence(context: AgentToolContext) -> dict[str, Any]:
        value = feature_jobs.get_agent_evidence(context.source_job_id, context.entity_id)
        _reject_sensitive(value)
        from .models import validate_evidence_scope
        validate_evidence_scope(value, context.source_job_id, context.entity_id, code="tool_scope_violation")
        if context.cluster is not None and value.get("entity", {}).get("cluster") != context.cluster:
            raise AgenticError("Run Evidence 跨集群", code="tool_scope_violation")
        if context.evidence_hash and canonical_fingerprint(value) != context.evidence_hash:
            raise AgenticError("Run Evidence 已变化，需要重新校验", code="agent_evidence_changed")
        return value

    def evidence(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        if str(arguments["job_id"]) != context.source_job_id or str(arguments["entity_id"]) != context.entity_id:
            raise AgenticError("工具请求超出当前 Run 实体边界", code="tool_scope_violation", status_code=403)
        return load_evidence(context)

    def approved_rules(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        components = set(map(str, arguments.get("components") or []))
        hashes = set(map(str, arguments.get("template_hashes") or []))

        cluster = context.cluster
        if cluster is None:
            cluster = load_evidence(context).get("entity", {}).get("cluster")

        def summary(rule: dict[str, Any]) -> dict[str, Any] | None:
            return approved_rule_summary(rule, cluster)

        finder = getattr(rule_governance, "find_active_rules_by_evidence", None)
        if callable(finder):
            rows = finder(hashes, components, scope=cluster, cursor=None, limit=500)
            items = [value for rule in rows if (value := summary(rule)) is not None]
            return {"items": items, "total": len(items), "matched": len(items), "truncated": len(rows) >= 500}
        items = []
        page_number, total, scanned = 1, 0, 0
        while True:
            page = rule_governance.list_rules(status="active", page=page_number, page_size=100)
            rows = page.get("items") or []
            total = int((page.get("pagination") or {}).get("total", scanned + len(rows)))
            for rule in rows:
                signatures = rule.get("template_signatures") or []
                rule_hashes = {str(value) for item in signatures for value in (item.get("template_hash"), item.get("template_fingerprint")) if value}
                rule_components = set(map(str, rule.get("components") or [])) | {str(item.get("component")) for item in signatures if item.get("component")}
                if (not hashes or hashes & rule_hashes) and (not components or components & rule_components):
                    value = summary(rule)
                    if value is not None:
                        items.append(value)
            scanned += len(rows)
            if not rows or scanned >= total:
                break
            page_number += 1
        return {"items": items, "total": total, "matched": len(items), "truncated": scanned < total}

    def validate_rule_bindings(bindings: tuple[dict[str, Any], ...], context: AgentToolContext, *, connection: Any = None) -> None:
        from logrisk.rule_governance import RuleGovernanceRepository
        repository = getattr(rule_governance, "repository", None)
        expected = {item["rule_id"]: item for item in bindings}
        current: dict[str, dict[str, Any]] = {}
        if isinstance(repository, RuleGovernanceRepository):
            manager_database = getattr(getattr(feature_jobs, "persistence", None), "database", None)
            authority = repository.database
            same_database = authority is manager_database
            if manager_database is not None and authority.provider == manager_database.provider == "sqlite":
                same_database = Path(authority.path).resolve() == Path(manager_database.path).resolve()
            if not same_database:
                raise AgenticError("规则权威库与候选库不一致", code="agent_rule_reference_changed")
            if connection is not None and authority.provider == "sqlite":
                main = next(row["file"] for row in connection.execute("PRAGMA database_list") if row["name"] == "main")
                if Path(main).resolve() != Path(authority.path).resolve():
                    raise AgenticError("规则校验事务来自其他数据库", code="agent_rule_reference_changed")
            for rule_id in expected:
                rule = repository.get_rule(rule_id, connection=connection)
                if rule is not None:
                    current[rule_id] = rule
        else:
            # Compatibility path: read the actual source again, never the old tool result.
            page_number, scanned = 1, 0
            while True:
                page = rule_governance.list_rules(status="active", page=page_number, page_size=100)
                rows = page.get("items") or []
                for rule in rows:
                    rule_id = rule.get("rule_id")
                    if rule_id in expected:
                        if rule_id in current:
                            raise AgenticError("权威规则身份重复", code="agent_rule_reference_changed")
                        current[rule_id] = rule
                scanned += len(rows)
                if not rows or scanned >= int((page.get("pagination") or {}).get("total", scanned)):
                    break
                page_number += 1
        cluster = context.cluster or load_evidence(context).get("entity", {}).get("cluster")
        for rule_id, binding in expected.items():
            value = approved_rule_summary(current.get(rule_id, {}), cluster)
            if value is None or rule_reference(value) != binding["reference"] or value["canonical_hash"] != binding["canonical_hash"]:
                raise AgenticError("批准规则状态/有效期/版本/规范内容已变化", code="agent_rule_reference_changed")

    def assets(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        items = []
        for package in knowledge_packages.list_packages():
            detail = knowledge_packages.get_package(str(package.get("package_id") or ""))
            for version in detail.get("versions") or []:
                if version.get("status") != "installed":
                    continue
                for asset in version.get("assets") or []:
                    if asset.get("status") == "materialized":
                        items.append({
                            "package_id": package.get("package_id"), "package_version": version.get("version"),
                            "asset_id": asset.get("asset_id"), "asset_type": asset.get("asset_type"),
                            "target_domain": asset.get("target_domain"), "target_resource_id": asset.get("target_resource_id"),
                        })
        return {"items": items}

    def evaluate(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        evidence_value = load_evidence(context)
        feature = copy.deepcopy(arguments["feature"])
        entity = {
            "entity_id": evidence_value["entity"].get("id"),
            "entity_type": evidence_value["entity"].get("type"),
        }
        result = evaluate_feature_output(feature=feature, entity=entity, evidence=evidence_value)
        # Only Evaluator diagnostics use this alias; arbitrary message fields remain forbidden.
        result["rule_results"] = [
            {("detail" if key == "message" else key): value for key, value in row.items()}
            for row in result["rule_results"]
        ]
        return {**result, "feature": feature, "fingerprint": canonical_fingerprint(feature),
                "evidence_hash": canonical_fingerprint(evidence_value), "evaluator_version": EVALUATOR_VERSION}

    def register_candidate(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        load_evidence(context)
        kwargs: dict[str, Any] = {"run_id": context.run_id}
        if context.rule_bindings:
            kwargs["precondition"] = lambda connection: registry.validate_rule_bindings(context.rule_bindings, context, connection=connection)
        return feature_jobs.register_agent_candidate(
            context.source_job_id, context.entity_id, arguments["feature"], **kwargs
        )

    registry = ToolRegistry(evidence_loader=load_evidence, rule_validator=validate_rule_bindings)
    registry.register(name="get_sanitized_evidence", description="读取风险实体的聚合脱敏 Evidence", required_arguments=("job_id", "entity_id"), argument_schema={"type": "object", "additionalProperties": False, "required": ["job_id", "entity_id"], "properties": {"job_id": _NONEMPTY_STRING, "entity_id": _NONEMPTY_STRING}}, handler=evidence)
    registry.register(name="find_approved_rules", description="查询匹配的已批准规则", required_arguments=(), optional_arguments=("template_hashes", "components"), argument_schema={"type": "object", "additionalProperties": False, "properties": {"template_hashes": _OPTIONAL_STRING_LIST, "components": _OPTIONAL_STRING_LIST}}, handler=approved_rules)
    registry.register(name="inspect_knowledge_assets", description="查询已安装的知识包摘要", required_arguments=(), argument_schema={"type": "object", "additionalProperties": False, "properties": {}}, handler=assets)
    registry.register(name="evaluate_candidate", description="执行确定性候选质量校验", required_arguments=("feature",), argument_schema={"type": "object", "additionalProperties": False, "required": ["feature"], "properties": {"feature": _FEATURE_SCHEMA}}, handler=evaluate)
    registry.register(name="register_feature_candidate", description="登记等待人工审批的候选特征", required_arguments=("feature",), argument_schema={"type": "object", "additionalProperties": False, "required": ["feature"], "properties": {"feature": _FEATURE_SCHEMA}}, handler=register_candidate, writes_candidate=True)
    return registry
