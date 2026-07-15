from __future__ import annotations

import re
from typing import Any


def rank_clusters(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for cluster in clusters:
        reasons: list[str] = []
        count = int(cluster.get("count") or 0)
        template = str(cluster.get("template") or "")
        tokens = template.split()
        wildcard_ratio = sum(bool(re.fullmatch(r"<[^>]+>", token)) for token in tokens) / len(tokens) if tokens else 0.0
        if count == 1:
            reasons.append("singleton")
        if wildcard_ratio >= 0.4:
            reasons.append("high_wildcard")
        if cluster.get("risk_level") in {"critical", "high"}:
            reasons.append("high_risk")
        if cluster.get("approved_rule_hit"):
            reasons.append("approved_rule_hit")
        item = dict(cluster, wildcard_ratio=round(wildcard_ratio, 6), sample_reasons=reasons)
        item["sample_priority"] = len(reasons) * 100 + min(count, 99)
        ranked.append(item)
    return sorted(ranked, key=lambda item: (-item["sample_priority"], str(item.get("cluster_id"))))
