from __future__ import annotations

from typing import Any


def evaluate_stability(runs: list[list[dict[str, Any]]]) -> dict[str, float]:
    if len(runs) < 2:
        return {"order_stability": 1.0, "fingerprint_stability": 1.0, "template_churn": 0.0}
    mappings = [{str(row.get("record_id")): str(row.get("predicted_group_id")) for row in run} for run in runs]
    common_ids = set.intersection(*(set(mapping) for mapping in mappings)) if mappings else set()
    stable = sum(len({mapping[record_id] for mapping in mappings}) == 1 for record_id in common_ids)
    score = stable / len(common_ids) if common_ids else 1.0
    return {"order_stability": round(score, 6), "fingerprint_stability": round(score, 6), "template_churn": round(1 - score, 6)}
