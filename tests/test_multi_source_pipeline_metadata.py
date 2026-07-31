from __future__ import annotations

from logrisk.aggregator import aggregate_template_events
from logrisk.drain_miner import mine_template_events
from logrisk.normalizer import normalize_record


def test_drain3_and_aggregator_preserve_entity_route_without_mining_entity_values(tmp_path) -> None:
    normalized = normalize_record({
        "raw_log_id": "line-1",
        "timestamp": "2026-07-30T10:01:00+00:00",
        "cluster": "prod-a",
        "node": "node-01",
        "namespace": "payments",
        "pod": "api-7d8",
        "container": "api",
        "device": "0000:65:00.0",
        "source_type": "kubelet",
        "component": "kubelet",
        "message": "failed to start pod sandbox id=1234",
    })
    events = mine_template_events(
        [normalized],
        "configs/drain3_recommended.ini",
        tmp_path / "drain3",
    )
    windows = aggregate_template_events(events)

    assert normalized["device"] == "0000:65:00.0"
    assert events[0]["entity_route"]["status"] == "routed"
    assert windows[0]["entity_keys"] == [
        "prod-a/container/api",
        "prod-a/device/0000:65:00.0",
        "prod-a/namespace/payments",
        "prod-a/node/node-01",
        "prod-a/pod/payments/api-7d8",
    ]
    assert windows[0]["container"] == "api"
    assert windows[0]["device"] == "0000:65:00.0"
    assert "0000:65:00.0" not in windows[0]["template"]
