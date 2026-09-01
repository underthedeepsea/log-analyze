from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


def parse_ts(ts: str | None) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    value = str(ts).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def floor_window(dt: datetime, window_seconds: int) -> datetime:
    epoch = int(dt.timestamp())
    floored = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(floored, tz=dt.tzinfo or timezone.utc)


def aggregate_template_events(
    events: list[Dict[str, Any]],
    window_seconds: int = 300,
    max_samples_per_template: int = 3,
) -> list[Dict[str, Any]]:
    aggregator = TemplateEventAggregator(window_seconds, max_samples_per_template)
    for event in events:
        aggregator.add(event)
    return aggregator.finalize()


class TemplateEventAggregator:
    def __init__(self, window_seconds: int = 300, max_samples_per_template: int = 3) -> None:
        self.window_seconds = window_seconds
        self.max_samples_per_template = max_samples_per_template
        self.windows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    def add(self, event: Dict[str, Any]) -> None:
        dt = parse_ts(event.get("timestamp"))
        start = floor_window(dt, self.window_seconds)
        end = datetime.fromtimestamp(start.timestamp() + self.window_seconds, tz=start.tzinfo)

        entity_id = event.get("node") or event.get("pod") or "unknown"
        entity_type = "node" if event.get("node") else ("pod" if event.get("pod") else "unknown")
        key = (
            start.isoformat(),
            event.get("cluster"),
            entity_type,
            entity_id,
            event.get("component"),
            event.get("template_hash"),
            (event.get("risk_semantic") or {}).get("risk_type"),
        )

        if key not in self.windows:
            self.windows[key] = {
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "cluster": event.get("cluster"),
                "entity_type": entity_type,
                "entity_id": entity_id,
                "node": event.get("node"),
                "namespace": event.get("namespace"),
                "pod": event.get("pod"),
                "container": event.get("container"),
                "device": event.get("device"),
                "source_type": event.get("source_type"),
                "component": event.get("component"),
                "severity": event.get("severity"),
                "template_hash": event.get("template_hash"),
                "template_fingerprint": event.get("template_fingerprint"),
                "template_instance_hash": event.get("template_instance_hash"),
                "hash_version": event.get("hash_version"),
                "template": event.get("template"),
                "count": 0,
                "first_seen": event.get("timestamp"),
                "last_seen": event.get("timestamp"),
                "samples": [],
                "affected_namespaces": set(),
                "affected_pods": set(),
                "entity_keys": set(),
                "entity_relations": {},
                "semantic_field_counts": defaultdict(dict),
                "semantic_tags": set(),
                "typed_parameter_counts": {},
                "semantic_extractor_version": event.get("semantic_extractor_version"),
                "semantic_dictionary_versions": event.get("semantic_dictionary_versions") or {},
                "risk_semantic": event.get("risk_semantic"),
            }

        w = self.windows[key]
        w["count"] += 1
        ts = event.get("timestamp")
        if ts:
            w["last_seen"] = ts
        if event.get("raw_sample") and len(w["samples"]) < self.max_samples_per_template:
            w["samples"].append(event["raw_sample"])
        if event.get("namespace"):
            w["affected_namespaces"].add(event["namespace"])
        if event.get("pod"):
            w["affected_pods"].add(event["pod"])
        route = event.get("entity_route") or {}
        for entity in route.get("entities") or []:
            if entity.get("entity_key"):
                w["entity_keys"].add(str(entity["entity_key"]))
        for relation in route.get("relations") or []:
            relation_key = (
                str(relation.get("from_key") or ""),
                str(relation.get("relation") or ""),
                str(relation.get("to_key") or ""),
            )
            if all(relation_key):
                w["entity_relations"][relation_key] = dict(relation)
        for field, value in (event.get("semantic_fields") or {}).items():
            value_key = repr(value)
            entry = w["semantic_field_counts"][field].setdefault(value_key, {"value": value, "count": 0})
            entry["count"] += 1
        w["semantic_tags"].update(str(tag) for tag in (event.get("semantic_tags") or []) if str(tag))
        for parameter in event.get("typed_parameters") or []:
            if not isinstance(parameter, dict) or not parameter.get("field") or not parameter.get("typed_mask"):
                continue
            parameter_key = (str(parameter["field"]), str(parameter["typed_mask"]))
            entry = w["typed_parameter_counts"].setdefault(parameter_key, {
                "field": parameter_key[0],
                "typed_mask": parameter_key[1],
                "count": 0,
            })
            entry["count"] += 1

    def finalize(self) -> list[Dict[str, Any]]:
        out = []
        for w in self.windows.values():
            item = dict(w)
            item["affected_namespaces"] = sorted(w["affected_namespaces"])
            item["affected_pods"] = sorted(w["affected_pods"])
            item["entity_keys"] = sorted(w["entity_keys"])
            item["entity_relations"] = [
                w["entity_relations"][key] for key in sorted(w["entity_relations"])
            ]
            item["semantic_fields"] = {
                field: sorted(values.values(), key=lambda entry: (-entry["count"], str(entry["value"])))[:20]
                for field, values in sorted(w["semantic_field_counts"].items())
            }
            item["semantic_tags"] = sorted(w["semantic_tags"])
            item["typed_parameters"] = sorted(w["typed_parameter_counts"].values(), key=lambda entry: (entry["field"], entry["typed_mask"]))
            item.pop("semantic_field_counts", None)
            item.pop("typed_parameter_counts", None)
            out.append(item)

        return sorted(out, key=lambda x: (x["window_start"], x["entity_id"], -x["count"]))
