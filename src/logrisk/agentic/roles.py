from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AgentRole:
    role_id: str
    display_name: str
    description: str
    allowed_tools: tuple[str, ...]
    max_steps: int
    max_tool_calls: int
    timeout_seconds: float


class RoleRegistry:
    def __init__(self, roles: tuple[AgentRole, ...]) -> None:
        self._roles = {role.role_id: role for role in roles}

    def get(self, role_id: str) -> AgentRole | None:
        return self._roles.get(str(role_id))

    def list(self) -> list[AgentRole]:
        return [self._roles[key] for key in sorted(self._roles)]


def build_role_registry(allowed_roles: tuple[str, ...] | None = None) -> RoleRegistry:
    roles = (
        AgentRole("evidence_specialist", "证据专家", "读取当前实体的聚合脱敏 Evidence", ("get_sanitized_evidence",), 3, 4, 120),
        AgentRole("rule_specialist", "规则专家", "查询已批准规则和已安装知识资产摘要", ("find_approved_rules", "inspect_knowledge_assets"), 4, 6, 120),
        AgentRole("feature_specialist", "特征专家", "提取、校验并登记待人工审批 Candidate", ("get_sanitized_evidence", "evaluate_candidate", "register_feature_candidate"), 6, 10, 180),
    )
    if allowed_roles is not None:
        allowed = set(allowed_roles)
        selected = tuple(role for role in roles if role.role_id in allowed)
        if not selected or len(selected) != len(allowed):
            raise ValueError("Agent 工作流 allowed_roles 必须全部来自固定角色注册表")
        roles = selected
    return RoleRegistry(roles)
