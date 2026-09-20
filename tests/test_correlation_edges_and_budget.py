from __future__ import annotations

from logrisk.multi_source.correlation import correlate_observations


def test_bridge_group_keeps_valid_ab_and_bc_edges_without_global_shared_entity():
    rule = {
        "rule_id": "bridge", "version": 1, "enabled": True,
        "source_pairs": [["a", "b"], ["b", "c"]], "max_gap_seconds": 60,
        "min_risk_score": 1, "min_count": 1, "confidence": 1,
    }
    rows = [
        {"observation_id": "A", "cluster": "x", "source_family": "a", "window_start": "2026-01-01T00:00:00+00:00", "window_end": "2026-01-01T00:00:01+00:00", "risk_score": 5, "count": 1, "entity_keys": ["node/1"]},
        {"observation_id": "B", "cluster": "x", "source_family": "b", "window_start": "2026-01-01T00:00:10+00:00", "window_end": "2026-01-01T00:00:11+00:00", "risk_score": 5, "count": 1, "entity_keys": ["node/1", "pod/2"]},
        {"observation_id": "C", "cluster": "x", "source_family": "c", "window_start": "2026-01-01T00:00:20+00:00", "window_end": "2026-01-01T00:00:21+00:00", "risk_score": 5, "count": 1, "entity_keys": ["pod/2"]},
    ]

    result = correlate_observations(rows, rule)

    assert len(result) == 1
    assert {(edge["left_observation_id"], edge["right_observation_id"]) for edge in result[0]["edges"]} == {("A", "B"), ("B", "C")}
    assert result[0]["group_scope"] == "edge_connected"
    assert result[0]["partial"] is False


def test_unrelated_entities_do_not_consume_pair_budget(monkeypatch):
    import logrisk.multi_source.correlation as module
    calls = []
    original = module._pair_allowed
    monkeypatch.setattr(module,"_pair_allowed",lambda *args: calls.append(args) or original(*args))
    rows = [{"observation_id":str(index),"cluster":"x","source_family":"a" if index%2 else "b",
             "window_start":"2026-01-01T00:00:00+00:00","window_end":"2026-01-01T00:00:01+00:00",
             "risk_score":5,"count":1,"entity_keys":[f"node/{index}"]} for index in range(1000)]
    rows[-1]["entity_keys"] = rows[-2]["entity_keys"]
    result = correlate_observations(rows,{"rule_id":"r","source_pairs":[["a","b"]],"max_gap_seconds":60,"max_candidate_pairs":2})
    assert len(result) == 1
    assert len(calls) == 1
    assert not result[0]["partial"]
