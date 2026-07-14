from __future__ import annotations

from logrisk.drain_eval.sampler import rank_clusters
from logrisk.drain_eval.tuner import grid_candidates, rank_candidates


def test_sampler_prioritizes_high_risk_wildcard_and_singleton_clusters():
    ranked = rank_clusters([
        {"cluster_id": "normal", "template": "ready", "count": 20, "risk_level": "low"},
        {"cluster_id": "risk", "template": "failed <*> code <NUM>", "count": 1, "risk_level": "critical"},
    ])

    assert ranked[0]["cluster_id"] == "risk"
    assert ranked[0]["sample_reasons"] == ["singleton", "high_wildcard", "high_risk"]


def test_grid_search_space_and_hard_gate_ranking():
    candidates = grid_candidates()
    ranked = rank_candidates([
        {"profile_id": "unsafe", "metrics": {"critical_risk_recall": 0.8, "over_merge_rate": 0.0, "normal_log_false_positive_rate": 0.0, "pairwise_grouping_f1": 0.99}, "logs_per_second": 1000},
        {"profile_id": "safe", "metrics": {"critical_risk_recall": 1.0, "over_merge_rate": 0.01, "normal_log_false_positive_rate": 0.01, "pairwise_grouping_f1": 0.95}, "logs_per_second": 500},
    ])

    assert len(candidates) == 144
    assert ranked[0]["profile_id"] == "safe"
    assert ranked[0]["gate_passed"] is True
    assert ranked[1]["gate_passed"] is False
