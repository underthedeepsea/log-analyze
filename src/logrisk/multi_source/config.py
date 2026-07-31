from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class MultiSourceConfigError(ValueError):
    """Raised when the deterministic correlation configuration is invalid."""


def load_multi_source_config(path: str | Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise MultiSourceConfigError("多来源关联配置文件无效") from exc
    unknown = set(payload) - {"schema_version", "enabled", "aliases", "rules"}
    if unknown:
        raise MultiSourceConfigError(f"多来源关联配置包含未知字段: {', '.join(sorted(unknown))}")
    if payload.get("schema_version") != 1:
        raise MultiSourceConfigError("多来源关联配置 schema_version 必须为 1")
    aliases = payload.get("aliases") or {}
    rules = payload.get("rules") or []
    if not isinstance(aliases, dict) or not isinstance(rules, list):
        raise MultiSourceConfigError("aliases 必须是对象且 rules 必须是数组")
    for rule in rules:
        if not isinstance(rule, dict) or not rule.get("rule_id"):
            raise MultiSourceConfigError("每条多来源规则必须包含 rule_id")
        if not 0 <= float(rule.get("confidence", 0)) <= 1:
            raise MultiSourceConfigError("规则 confidence 必须位于 0 到 1")
        if int(rule.get("max_gap_seconds", 0)) <= 0:
            raise MultiSourceConfigError("规则 max_gap_seconds 必须大于 0")
    return {
        "enabled": bool(payload.get("enabled", True)),
        "aliases": aliases,
        "rules": rules,
    }
