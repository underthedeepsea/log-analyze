from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Mapping


def _timestamp(value: Any) -> float:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def _pair_allowed(first: str, second: str, allowed: set[frozenset[str]]) -> bool:
    return first != second and frozenset((first, second)) in allowed


def correlate_observations(
    observations: list[Mapping[str, Any]],
    rule: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not rule.get("enabled", True):
        return []
    minimum_risk = float(rule.get("min_risk_score") or 0)
    minimum_count = int(rule.get("min_count") or 1)
    maximum_gap = float(rule.get("max_gap_seconds") or 0)
    allowed = {
        frozenset((str(pair[0]), str(pair[1])))
        for pair in rule.get("source_pairs") or []
        if isinstance(pair, (list, tuple)) and len(pair) == 2
    }
    valid = [
        dict(item)
        for item in observations
        if float(item.get("risk_score") or 0) >= minimum_risk
        and int(item.get("count") or 0) >= minimum_count
        and item.get("entity_keys")
    ]
    parent = list(range(len(valid)))

    def root(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    for left in range(len(valid)):
        for right in range(left + 1, len(valid)):
            first, second = valid[left], valid[right]
            if first.get("cluster") != second.get("cluster"):
                continue
            if not _pair_allowed(
                str(first.get("source_family")),
                str(second.get("source_family")),
                allowed,
            ):
                continue
            if not set(first["entity_keys"]) & set(second["entity_keys"]):
                continue
            gap = abs(_timestamp(first["window_start"]) - _timestamp(second["window_start"]))
            if gap > maximum_gap:
                continue
            left_root, right_root = root(left), root(right)
            parent[right_root] = left_root

    groups: dict[int, list[dict[str, Any]]] = {}
    for index, item in enumerate(valid):
        groups.setdefault(root(index), []).append(item)

    output = []
    for group in groups.values():
        source_families = sorted({str(item["source_family"]) for item in group})
        if len(group) < 2 or len(source_families) < 2:
            continue
        shared = set(group[0]["entity_keys"])
        for item in group[1:]:
            shared &= set(item["entity_keys"])
        if not shared:
            continue
        observation_ids = sorted(str(item["observation_id"]) for item in group)
        identity = "|".join((str(rule["rule_id"]), str(rule.get("version") or 1), *observation_ids))
        output.append({
            "correlation_id": "correlation-" + hashlib.sha256(identity.encode()).hexdigest()[:24],
            "rule_id": str(rule["rule_id"]),
            "rule_version": int(rule.get("version") or 1),
            "cluster": str(group[0]["cluster"]),
            "primary_entity_key": sorted(shared)[0],
            "window_start": min(str(item["window_start"]) for item in group),
            "window_end": max(str(item["window_end"]) for item in group),
            "confidence": float(rule.get("confidence") or 0),
            "risk_score": max(float(item.get("risk_score") or 0) for item in group),
            "source_families": source_families,
            "observation_ids": observation_ids,
        })
    return sorted(output, key=lambda item: item["correlation_id"])
