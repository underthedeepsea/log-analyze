from __future__ import annotations

from logrisk.multi_source.router import route_entities


def test_router_builds_explicit_entity_hierarchy_without_changing_cluster() -> None:
    route = route_entities({
        "cluster": "prod-a",
        "node": "Node-01",
        "namespace": "payments",
        "pod": "api-7d8",
        "container": "containerd://ABC123",
    })

    assert route["status"] == "routed"
    assert route["primary_entity"] == {
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-01",
        "entity_key": "prod-a/node/node-01",
    }
    assert {item["entity_key"] for item in route["entities"]} == {
        "prod-a/node/node-01",
        "prod-a/namespace/payments",
        "prod-a/pod/payments/api-7d8",
        "prod-a/container/containerd://abc123",
    }
    assert {
        (item["from_key"], item["relation"], item["to_key"])
        for item in route["relations"]
    } == {
        (
            "prod-a/container/containerd://abc123",
            "belongs_to",
            "prod-a/pod/payments/api-7d8",
        ),
        (
            "prod-a/pod/payments/api-7d8",
            "belongs_to",
            "prod-a/namespace/payments",
        ),
        (
            "prod-a/pod/payments/api-7d8",
            "runs_on",
            "prod-a/node/node-01",
        ),
    }


def test_router_applies_only_explicit_aliases_and_never_crosses_clusters() -> None:
    aliases = {"node": {"gpu01.internal": "gpu-node-01"}}

    first = route_entities(
        {"cluster": "prod-a", "node": "GPU01.INTERNAL"}, aliases=aliases
    )
    second = route_entities(
        {"cluster": "prod-b", "node": "GPU01.INTERNAL"}, aliases=aliases
    )

    assert first["primary_entity"]["entity_key"] == "prod-a/node/gpu-node-01"
    assert second["primary_entity"]["entity_key"] == "prod-b/node/gpu-node-01"


def test_router_marks_missing_identifiers_unroutable() -> None:
    route = route_entities({"cluster": "prod-a", "component": "kernel"})

    assert route == {
        "status": "unroutable",
        "primary_entity": None,
        "entities": [],
        "relations": [],
    }


def test_router_routes_explicit_device_to_node() -> None:
    route = route_entities({
        "cluster": "prod-a",
        "node": "gpu-node-01",
        "device": "0000:65:00.0",
    })

    assert route["relations"] == [{
        "from_key": "prod-a/device/0000:65:00.0",
        "relation": "attached_to",
        "to_key": "prod-a/node/gpu-node-01",
    }]
