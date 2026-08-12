from __future__ import annotations

from typing import Any

from logrisk.ai_harness.evaluator import evaluate_feature_output

from .tool_registry import AgentToolContext, ToolRegistry


def build_agent_tool_registry(feature_jobs: Any, rule_governance: Any, knowledge_packages: Any) -> ToolRegistry:
    registry = ToolRegistry()

    def evidence(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        if str(arguments["job_id"]) != context.source_job_id or str(arguments["entity_id"]) != context.entity_id:
            from .errors import AgenticError
            raise AgenticError("工具请求超出当前 Run 实体边界", code="tool_scope_violation", status_code=403)
        return feature_jobs.get_agent_evidence(context.source_job_id, context.entity_id)

    def approved_rules(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        page = rule_governance.list_rules(status="active", page=1, page_size=100)
        components = set(map(str, arguments.get("components") or []))
        hashes = set(map(str, arguments.get("template_hashes") or []))
        items = []
        for rule in page.get("items") or []:
            signatures = rule.get("template_signatures") or []
            rule_hashes = {str(item.get("template_hash") or item.get("template_fingerprint") or "") for item in signatures}
            rule_components = {str(item.get("component") or "") for item in signatures}
            if (not hashes or hashes & rule_hashes) and (not components or components & rule_components):
                items.append({
                    "rule_id": rule.get("rule_id"), "title": rule.get("title"), "feature_type": rule.get("feature_type"),
                    "status": rule.get("status"), "template_signatures": signatures,
                })
        return {"items": items}

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
        evidence_value = feature_jobs.get_agent_evidence(context.source_job_id, context.entity_id)
        entity = {
            "entity_id": evidence_value["entity"].get("id"),
            "entity_type": evidence_value["entity"].get("type"),
        }
        return evaluate_feature_output(feature=arguments["feature"], entity=entity, evidence=evidence_value)

    def register_candidate(arguments: dict[str, Any], context: AgentToolContext) -> dict[str, Any]:
        return feature_jobs.register_agent_candidate(
            context.source_job_id, context.entity_id, arguments["feature"], run_id=context.run_id
        )

    registry.register(name="get_sanitized_evidence", description="读取风险实体的聚合脱敏 Evidence", required_arguments=("job_id", "entity_id"), handler=evidence)
    registry.register(name="find_approved_rules", description="查询匹配的已批准规则", required_arguments=(), optional_arguments=("template_hashes", "components"), handler=approved_rules)
    registry.register(name="inspect_knowledge_assets", description="查询已安装的知识包摘要", required_arguments=(), handler=assets)
    registry.register(name="evaluate_candidate", description="执行确定性候选质量校验", required_arguments=("feature",), handler=evaluate)
    registry.register(name="register_feature_candidate", description="登记等待人工审批的候选特征", required_arguments=("feature",), handler=register_candidate, writes_candidate=True)
    return registry
