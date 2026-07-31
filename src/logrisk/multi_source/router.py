from __future__ import annotations

from typing import Any, Mapping


ENTITY_ORDER = ("node", "namespace", "pod", "container", "device")


def _clean(value: Any) -> str | None:
    text = str(value or "").strip().casefold()
    return text or None


def route_entities(
    record: Mapping[str, Any],
    *,
    aliases: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    cluster = _clean(record.get("cluster")) or "default"
    alias_map = aliases or {}
    raw = {
        "node": _clean(record.get("node")),
        "namespace": _clean(record.get("namespace")),
        "pod": _clean(record.get("pod")),
        "container": _clean(record.get("container")),
        "device": _clean(record.get("device")),
    }
    for entity_type, value in tuple(raw.items()):
        if value:
            mapping = {
                _clean(source): _clean(target)
                for source, target in (alias_map.get(entity_type) or {}).items()
            }
            raw[entity_type] = mapping.get(value) or value

    identifiers: dict[str, str] = {}
    for entity_type in ENTITY_ORDER:
        value = raw[entity_type]
        if not value:
            continue
        if entity_type == "pod" and raw["namespace"]:
            value = f"{raw['namespace']}/{value}"
        identifiers[entity_type] = value

    if not identifiers:
        return {
            "status": "unroutable",
            "primary_entity": None,
            "entities": [],
            "relations": [],
        }

    entities = [
        {
            "cluster": cluster,
            "entity_type": entity_type,
            "entity_id": identifiers[entity_type],
            "entity_key": f"{cluster}/{entity_type}/{identifiers[entity_type]}",
        }
        for entity_type in ENTITY_ORDER
        if entity_type in identifiers
    ]
    by_type = {item["entity_type"]: item for item in entities}
    relations = []

    def relation(source: str, kind: str, target: str) -> None:
        if source in by_type and target in by_type:
            relations.append({
                "from_key": by_type[source]["entity_key"],
                "relation": kind,
                "to_key": by_type[target]["entity_key"],
            })

    relation("container", "belongs_to", "pod")
    relation("pod", "belongs_to", "namespace")
    relation("pod", "runs_on", "node")
    relation("device", "attached_to", "node")
    return {
        "status": "routed",
        "primary_entity": entities[0],
        "entities": entities,
        "relations": relations,
    }
