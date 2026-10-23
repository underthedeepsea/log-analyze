from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from logrisk.server_risk.contracts import ServerRiskError, digest


class RiskPolicies:
    def __init__(self, config_path: str | Path) -> None:
        self.path = Path(config_path)
        self.reload()

    def reload(self) -> None:
        config = yaml.safe_load(self.path.read_text()) or {} if self.path.is_file() else {"schema_version": "server_risk_config_v1", "sources": [], "policies": {}}
        if config.get("schema_version") != "server_risk_config_v1" or not isinstance(config.get("sources"), list) or not isinstance(config.get("policies"), dict):
            raise ServerRiskError("server_risk_config_invalid", "服务器风险配置合同无效")
        self.sources = {s["source_id"]: s for s in config["sources"]}
        if len(self.sources) != len(config["sources"]):
            raise ServerRiskError("server_risk_config_invalid", "来源ID重复")
        self.aliases = config.get("aliases", [])
        if not isinstance(self.aliases, list) or any(not isinstance(a, dict) or set(a) != {"alias", "cluster", "node_id"} or any(not isinstance(v, str) or not v for v in a.values()) for a in self.aliases):
            raise ServerRiskError("server_risk_config_invalid", "节点别名必须为明确alias/cluster/node映射")
        self.rules = config["policies"]
        self.version = digest(config)

    def source(self, source_id: str) -> dict[str, Any]:
        source = self.sources.get(source_id)
        if not source or not source.get("enabled", False):
            raise ServerRiskError("observation_source_unregistered", "观测来源未登记或未启用", 403)
        required = ("source_epoch", "config_revision", "environment", "cluster", "node_ids", "capability", "signals", "permitted_actors", "freshness_seconds", "max_gap_seconds", "allowed_lateness_seconds")
        if any(key not in source for key in required) or source["capability"] not in ("error_only", "complete_event_stream", "authoritative_state"):
            raise ServerRiskError("observation_source_config_invalid", "来源登记合同不完整")
        for key in ("freshness_seconds", "max_gap_seconds", "allowed_lateness_seconds"):
            if isinstance(source[key], bool) or not isinstance(source[key], int) or source[key] < (0 if key == "allowed_lateness_seconds" else 1):
                raise ServerRiskError("observation_source_config_invalid", "来源时序阈值无效")
        return source
