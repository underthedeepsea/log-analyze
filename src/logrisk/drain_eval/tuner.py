from __future__ import annotations

from itertools import product
from typing import Any


def grid_candidates() -> list[dict[str, Any]]:
    return [
        {
            "sim_th": sim_th,
            "depth": depth,
            "max_children": max_children,
            "parametrize_numeric_tokens": numeric,
            "extra_delimiters": delimiters,
        }
        for sim_th, depth, max_children, numeric, delimiters in product(
            (0.35, 0.40, 0.45, 0.50),
            (4, 5, 6),
            (100, 150, 250),
            (True, False),
            (("=",), ("=", ":")),
        )
    ]


def rank_candidates(candidates: list[dict[str, Any]], *, critical_recall_min: float = 1.0, over_merge_max: float = 0.02, normal_false_positive_max: float = 0.02) -> list[dict[str, Any]]:
    ranked = []
    for candidate in candidates:
        metrics = candidate.get("metrics") or {}
        gate_passed = (
            float(metrics.get("critical_risk_recall", 0)) >= critical_recall_min
            and float(metrics.get("over_merge_rate", 1)) <= over_merge_max
            and float(metrics.get("normal_log_false_positive_rate", 1)) <= normal_false_positive_max
        )
        quality = float(metrics.get("pairwise_grouping_f1", 0))
        speed = float(candidate.get("logs_per_second") or 0)
        ranked.append(dict(candidate, gate_passed=gate_passed, pareto_score=round(quality * 0.8 + min(speed / 10000, 1) * 0.2, 6)))
    return sorted(ranked, key=lambda item: (not item["gate_passed"], -item["pareto_score"], str(item.get("profile_id", ""))))
