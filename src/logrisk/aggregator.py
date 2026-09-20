from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class EventTime:
    value: datetime | None
    quality: str
    received_at: datetime | None = None


def _parse_known_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def resolve_event_time(record: dict[str, Any], trusted_source_context: dict[str, Any]) -> EventTime:
    received_at = _parse_known_time(trusted_source_context.get("received_at"))
    direct = _parse_known_time(record.get("timestamp"))
    if direct is not None:
        return EventTime(direct, "event", received_at)
    boundary = _parse_known_time(
        trusted_source_context.get("window_start") or record.get("window_start") or record.get("first_seen")
    )
    if boundary is not None:
        return EventTime(boundary, "window", received_at)
    return EventTime(None, "unknown", received_at)


def parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return _parse_known_time(ts)


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
        event_time = resolve_event_time(event, {})
        dt = event_time.value
        start = floor_window(dt.astimezone(timezone.utc), self.window_seconds) if dt is not None else None
        end = datetime.fromtimestamp(start.timestamp() + self.window_seconds, tz=start.tzinfo) if start is not None else None

        entity_id = event.get("node") or event.get("pod") or "unknown"
        entity_type = "node" if event.get("node") else ("pod" if event.get("pod") else "unknown")
        key = (
            start.isoformat() if start is not None else "unknown",
            event.get("cluster"),
            entity_type,
            entity_id,
            event.get("component"),
            event.get("template_hash"),
            event.get("source_type"),
            event.get("semantic_extractor_version"),
            repr(sorted((event.get("semantic_dictionary_versions") or {}).items())),
            (event.get("risk_semantic") or {}).get("risk_type"),
        )

        if key not in self.windows:
            self.windows[key] = {
                "window_start": start.isoformat() if start is not None else None,
                "window_end": end.isoformat() if end is not None else None,
                "time_quality": event_time.quality,
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
                "severity_distribution": {},
                "first_seen": dt.astimezone(timezone.utc).isoformat() if dt is not None else None,
                "last_seen": dt.astimezone(timezone.utc).isoformat() if dt is not None else None,
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
        severity = str(event.get("severity") or "unknown").upper()
        w["severity_distribution"][severity] = w["severity_distribution"].get(severity,0) + 1
        ts = dt.astimezone(timezone.utc).isoformat() if dt is not None else None
        if _parse_known_time(ts) is not None:
            if not w.get("first_seen") or str(ts) < str(w["first_seen"]):
                w["first_seen"] = ts
            if not w.get("last_seen") or str(ts) > str(w["last_seen"]):
                w["last_seen"] = ts
        severity_order = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "NOTICE": 3, "WARNING": 4, "WARN": 4, "ERROR": 5, "CRITICAL": 6, "FATAL": 7}
        if severity_order.get(str(event.get("severity") or "").upper(), -1) > severity_order.get(str(w.get("severity") or "").upper(), -1):
            w["severity"] = event.get("severity")
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
                field: sorted(values.values(), key=lambda entry: (-entry["count"], str(entry["value"])))
                for field, values in sorted(w["semantic_field_counts"].items())
            }
            item["semantic_tags"] = sorted(w["semantic_tags"])
            item["typed_parameters"] = sorted(w["typed_parameter_counts"].values(), key=lambda entry: (entry["field"], entry["typed_mask"]))
            item.pop("semantic_field_counts", None)
            item.pop("typed_parameter_counts", None)
            out.append(item)

        return sorted(out, key=lambda x: (str(x.get("window_start") or ""), x["entity_id"], -x["count"]))
