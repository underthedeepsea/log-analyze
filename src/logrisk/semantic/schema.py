from __future__ import annotations

import hashlib
import json
import re
from typing import Any


ALLOWED_FIELDS = {
    "http_status",
    "http_status_class",
    "errno",
    "errno_name",
    "exit_code",
    "signal",
    "xid_code",
    "k8s_reason",
    "oom_process",
    "device",
    "filesystem_type",
}
ALLOWED_VALUE_TYPES = {"integer", "string"}
RULE_KEYS = {
    "rule_id",
    "field",
    "pattern",
    "group",
    "value_type",
    "typed_mask",
    "tags",
    "priority",
    "source_types",
    "components",
}


class SemanticValidationError(ValueError):
    pass


def _strings(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise SemanticValidationError(f"{label} 必须是字符串数组")
    return [item.strip() for item in value]


def _validate_rule(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SemanticValidationError(f"规则 {index + 1} 必须是 object")
    unknown = set(raw) - RULE_KEYS
    if unknown:
        raise SemanticValidationError(f"规则 {index + 1} 包含未知字段: {', '.join(sorted(unknown))}")
    rule_id = raw.get("rule_id")
    field = raw.get("field")
    pattern = raw.get("pattern")
    group = raw.get("group")
    if not isinstance(rule_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,63}", rule_id):
        raise SemanticValidationError(f"规则 {index + 1} rule_id 无效")
    if field not in ALLOWED_FIELDS:
        raise SemanticValidationError(f"规则 {rule_id} field 不在白名单")
    if not isinstance(pattern, str) or not pattern:
        raise SemanticValidationError(f"规则 {rule_id} pattern 不能为空")
    try:
        compiled = re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        raise SemanticValidationError(f"规则 {rule_id} 正则无效: {exc}") from exc
    if not isinstance(group, str) or group not in compiled.groupindex:
        raise SemanticValidationError(f"规则 {rule_id} 缺少命名组 {group or 'value'}")
    value_type = raw.get("value_type")
    if value_type not in ALLOWED_VALUE_TYPES:
        raise SemanticValidationError(f"规则 {rule_id} value_type 无效")
    typed_mask = raw.get("typed_mask")
    if not isinstance(typed_mask, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{1,31}", typed_mask):
        raise SemanticValidationError(f"规则 {rule_id} typed_mask 无效")
    tags = _strings(raw.get("tags"), f"规则 {rule_id} tags")
    if not tags:
        raise SemanticValidationError(f"规则 {rule_id} tags 不能为空")
    try:
        priority = int(raw.get("priority"))
    except (TypeError, ValueError) as exc:
        raise SemanticValidationError(f"规则 {rule_id} priority 必须是整数") from exc
    return {
        "rule_id": rule_id,
        "field": field,
        "pattern": pattern,
        "group": group,
        "value_type": value_type,
        "typed_mask": typed_mask,
        "tags": tags[:4],
        "priority": priority,
        "source_types": _strings(raw.get("source_types", []), f"规则 {rule_id} source_types"),
        "components": _strings(raw.get("components", []), f"规则 {rule_id} components"),
    }


def validate_dictionary(payload: Any, *, expected_id: str | None = None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SemanticValidationError("语义词典必须是 object")
    dictionary_id = payload.get("dictionary_id")
    if not isinstance(dictionary_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", dictionary_id):
        raise SemanticValidationError("dictionary_id 无效")
    if expected_id and dictionary_id != expected_id:
        raise SemanticValidationError("dictionary_id 与目标词典不一致")
    try:
        version = int(payload.get("version", 1))
    except (TypeError, ValueError) as exc:
        raise SemanticValidationError("version 必须是正整数") from exc
    if version < 1:
        raise SemanticValidationError("version 必须是正整数")
    raw_rules = payload.get("rules")
    if not isinstance(raw_rules, list):
        raise SemanticValidationError("rules 必须是数组")
    rules = [_validate_rule(rule, index) for index, rule in enumerate(raw_rules)]
    seen_ids: set[str] = set()
    scopes: set[tuple[Any, ...]] = set()
    for rule in rules:
        if rule["rule_id"] in seen_ids:
            raise SemanticValidationError(f"rule_id 重复: {rule['rule_id']}")
        seen_ids.add(rule["rule_id"])
        scope = (
            rule["field"],
            rule["priority"],
            tuple(sorted(rule["source_types"])),
            tuple(sorted(rule["components"])),
        )
        if scope in scopes:
            raise SemanticValidationError(f"字段 {rule['field']} 存在同优先级冲突")
        scopes.add(scope)
    result = {
        "schema_version": "semantic_dictionary_v1",
        "dictionary_id": dictionary_id,
        "name": str(payload.get("name") or dictionary_id),
        "version": version,
        "rules": sorted(rules, key=lambda item: (-item["priority"], item["rule_id"])),
    }
    canonical = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    result["content_hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return result
