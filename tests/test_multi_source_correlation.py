from __future__ import annotations

from logrisk.multi_source.correlation import correlate_observations


RULE = {
    "rule_id": "cross-source-same-entity",
    "version": 1,
    "enabled": True,
    "source_pairs": [["kernel", "kubelet"]],
    "max_gap_seconds": 300,
    "min_risk_score": 40,
    "min_count": 1,
    "confidence": 0.92,
}


def observation(
    observation_id: str,
    source_family: str,
    *,
    cluster: str = "prod-a",
    node: str = "node-01",
    timestamp: str = "2026-07-30T10:00:00+00:00",
    risk_score: float = 60,
) -> dict:
    return {
        "observation_id": observation_id,
        "cluster": cluster,
        "source_family": source_family,
        "window_start": timestamp,
        "window_end": timestamp,
        "risk_score": risk_score,
        "count": 1,
        "entity_keys": [f"{cluster}/node/{node}"],
    }


def test_correlator_groups_allowed_sources_for_same_entity() -> None:
    result = correlate_observations([
        observation("obs-kernel", "kernel"),
        observation("obs-kubelet", "kubelet", timestamp="2026-07-30T10:03:00+00:00"),
    ], RULE)

    assert len(result) == 1
    assert result[0]["rule_id"] == "cross-source-same-entity"
    assert result[0]["primary_entity_key"] == "prod-a/node/node-01"
    assert result[0]["source_families"] == ["kernel", "kubelet"]
    assert result[0]["observation_ids"] == ["obs-kernel", "obs-kubelet"]
    assert result[0]["confidence"] == 0.92


def test_correlator_rejects_cross_cluster_single_source_and_low_risk_pairs() -> None:
    assert correlate_observations([
        observation("a", "kernel", cluster="prod-a"),
        observation("b", "kubelet", cluster="prod-b"),
    ], RULE) == []
    assert correlate_observations([
        observation("a", "kernel"),
        observation("b", "kernel"),
    ], RULE) == []
    assert correlate_observations([
        observation("a", "kernel", risk_score=20),
        observation("b", "kubelet"),
    ], RULE) == []
