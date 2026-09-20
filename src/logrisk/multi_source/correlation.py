from __future__ import annotations

import hashlib
from bisect import bisect_left, bisect_right
from datetime import datetime
from typing import Any, Mapping


def _timestamp(value: Any) -> float:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def _has_timestamp(value: Any) -> bool:
    try:
        _timestamp(value)
    except (TypeError, ValueError):
        return False
    return True


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
        and _has_timestamp(item.get("window_start"))
    ]
    parent = list(range(len(valid)))
    edges: list[dict[str, Any]] = []
    pair_budget = max(1, int(rule.get("max_candidate_pairs") or 100000))
    comparisons = 0
    partial = False

    def root(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    buckets: dict[tuple[str, str, str], list[tuple[float, int]]] = {}
    for index, item in enumerate(valid):
        for entity in set(item["entity_keys"]):
            key = (str(item.get("cluster")), str(entity), str(item.get("source_family")))
            buckets.setdefault(key, []).append((_timestamp(item["window_start"]), index))
    for bucket in buckets.values():
        bucket.sort()
    for left, first in enumerate(valid):
        timestamp = _timestamp(first["window_start"])
        candidates: set[int] = set()
        remaining = pair_budget - comparisons
        family = str(first.get("source_family"))
        for pair in allowed:
            if family not in pair:
                continue
            for other_family in pair - {family}:
                for entity in set(first["entity_keys"]):
                    bucket = buckets.get((str(first.get("cluster")), str(entity), other_family), [])
                    start = bisect_left(bucket, (timestamp - maximum_gap, -1))
                    end = bisect_right(bucket, (timestamp + maximum_gap, len(valid)))
                    for position in range(start,end):
                        _, right = bucket[position]
                        if right > left:
                            candidates.add(right)
                        if len(candidates) > remaining:
                            partial = True
                            break
                    if partial:
                        break
                if partial:
                    break
            if partial:
                break
        for right in sorted(candidates):
            if comparisons >= pair_budget:
                partial = True
                break
            first, second = valid[left], valid[right]
            if first.get("cluster") != second.get("cluster"):
                continue
            if not _pair_allowed(
                str(first.get("source_family")),
                str(second.get("source_family")),
                allowed,
            ):
                continue
            comparisons += 1
            if not set(first["entity_keys"]) & set(second["entity_keys"]):
                continue
            gap = abs(_timestamp(first["window_start"]) - _timestamp(second["window_start"]))
            if gap > maximum_gap:
                continue
            left_root, right_root = root(left), root(right)
            parent[right_root] = left_root
            shared_entities = sorted(set(first["entity_keys"]) & set(second["entity_keys"]))
            left_id, right_id = sorted((str(first["observation_id"]), str(second["observation_id"])))
            edge_identity = "|".join((str(rule["rule_id"]), str(rule.get("version") or 1), left_id, right_id, *shared_entities))
            edges.append({
                "edge_id": "edge-" + hashlib.sha256(edge_identity.encode()).hexdigest()[:24],
                "left_observation_id": left_id,
                "right_observation_id": right_id,
                "entity_keys": shared_entities,
                "gap_seconds": gap,
            })
        if partial:
            break

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
        observation_ids = sorted(str(item["observation_id"]) for item in group)
        observation_set = set(observation_ids)
        group_edges = sorted(
            [edge for edge in edges if edge["left_observation_id"] in observation_set and edge["right_observation_id"] in observation_set],
            key=lambda edge: edge["edge_id"],
        )
        if not group_edges:
            continue
        identity = "|".join((str(rule["rule_id"]), str(rule.get("version") or 1), *observation_ids))
        output.append({
            "correlation_id": "correlation-" + hashlib.sha256(identity.encode()).hexdigest()[:24],
            "rule_id": str(rule["rule_id"]),
            "rule_version": int(rule.get("version") or 1),
            "cluster": str(group[0]["cluster"]),
            "primary_entity_key": sorted(shared)[0] if shared else group_edges[0]["entity_keys"][0],
            "group_scope": "shared_entity" if shared else "edge_connected",
            "edges": group_edges,
            "partial": partial,
            "window_start": min(str(item["window_start"]) for item in group),
            "window_end": max(str(item["window_end"]) for item in group),
            "confidence": float(rule.get("confidence") or 0),
            "risk_score": max(float(item.get("risk_score") or 0) for item in group),
            "source_families": source_families,
            "observation_ids": observation_ids,
        })
    return sorted(output, key=lambda item: item["correlation_id"])
