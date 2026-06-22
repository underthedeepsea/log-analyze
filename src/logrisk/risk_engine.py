from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List

import yaml


def load_rules(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def match_template_rule(window: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any] | None:
    component = window.get("component") or "unknown"
    template = window.get("template") or ""
    for rule in rules.get("template_rules", []):
        match = rule.get("match", {})
        if match.get("component") and match["component"] != component:
            continue
        regex = match.get("template_regex")
        if regex and re.match(regex, template, re.IGNORECASE):
            return rule
    return None


def score_window(window: Dict[str, Any], rules: Dict[str, Any]) -> float:
    severity = str(window.get("severity") or "UNKNOWN").upper()
    component = str(window.get("component") or "unknown")
    severity_weight = float(rules.get("severity_weight", {}).get(severity, rules.get("severity_weight", {}).get("UNKNOWN", 0.6)))
    component_weight = float(rules.get("component_weight", {}).get(component, rules.get("component_weight", {}).get("unknown", 0.6)))

    matched = match_template_rule(window, rules)
    template_weight = float(matched.get("risk_weight", 40)) if matched else 40.0

    count = int(window.get("count") or 1)
    divisor = float(rules.get("scoring", {}).get("count_norm_divisor", 50))
    count_weight = min(2.0, 1.0 + count / divisor)

    max_score = float(rules.get("scoring", {}).get("max_score", 100))
    score = template_weight * severity_weight * component_weight * count_weight / 2.0
    return round(min(max_score, score), 2)


def level_of(score: float) -> str:
    if score >= 90:
        return "critical"
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def score_risk_entities(windows: list[Dict[str, Any]], rules: Dict[str, Any]) -> list[Dict[str, Any]]:
    # First score each window.
    scored = []
    for w in windows:
        sw = dict(w)
        sw["window_risk_score"] = score_window(sw, rules)
        rule = match_template_rule(sw, rules)
        if rule:
            sw["category"] = rule.get("category")
            sw["feature_hint"] = rule.get("feature_hint")
            sw["rule_name"] = rule.get("name")
        scored.append(sw)

    # Then group by entity and window.
    groups: Dict[tuple, list[Dict[str, Any]]] = defaultdict(list)
    for w in scored:
        key = (w.get("window_start"), w.get("window_end"), w.get("cluster"), w.get("entity_type"), w.get("entity_id"))
        groups[key].append(w)

    entities = []
    for (ws, we, cluster, etype, eid), items in groups.items():
        items_sorted = sorted(items, key=lambda x: x["window_risk_score"], reverse=True)
        # Aggregate score: max score + small bonus for multiple abnormal templates.
        base = items_sorted[0]["window_risk_score"] if items_sorted else 0
        bonus = min(10, max(0, len(items_sorted) - 1) * 3)
        total = min(100, round(base + bonus, 2))

        hints = [x.get("feature_hint") for x in items_sorted if x.get("feature_hint")]
        summary = hints[0] if hints else "发现异常模板，需要结合时间线、指标和原始日志进一步确认。"

        entities.append({
            "window_start": ws,
            "window_end": we,
            "cluster": cluster,
            "entity_type": etype,
            "entity_id": eid,
            "risk_score": total,
            "risk_level": level_of(total),
            "top_templates": items_sorted[:10],
            "affected_entities": sorted({p for x in items_sorted for p in x.get("affected_pods", [])}),
            "summary": summary,
        })

    return sorted(entities, key=lambda x: x["risk_score"], reverse=True)
