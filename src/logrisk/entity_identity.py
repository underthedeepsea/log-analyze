from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SCOPED_FEATURE_ENTITY_VERSION = 1


def entity_identity(record: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(record.get("cluster") or ""),
        str(record.get("entity_type") or ""),
        str(record.get("entity_id") or ""),
    )


def entity_key(record: Mapping[str, Any]) -> str:
    cluster, entity_type, entity_id = entity_identity(record)
    return "e." + ".".join(value.encode("utf-8").hex() for value in (entity_id, cluster, entity_type))


def legacy_entity_cursor(entity_id: str) -> str:
    return "e." + entity_id.encode("utf-8").hex() + "/" if entity_id else ""


def candidate_entity_identity(candidate: Mapping[str, Any]) -> tuple[str, str, str]:
    entity = candidate.get("entity")
    if not isinstance(entity, Mapping):
        entity = {}
    return entity_identity({
        "cluster": candidate.get("cluster"),
        "entity_type": entity.get("type") or candidate.get("entity_type"),
        "entity_id": entity.get("id") or candidate.get("entity_id"),
    })
