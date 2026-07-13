from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


def parse_ts(ts: str | None) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    value = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.now(timezone.utc)


def floor_window(dt: datetime, window_seconds: int) -> datetime:
    epoch = int(dt.timestamp())
    floored = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(floored, tz=dt.tzinfo)


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
        )

        if key not in self.windows:
            self.windows[key] = {
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "cluster": event.get("cluster"),
                "entity_type": entity_type,
                "entity_id": entity_id,
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

    def finalize(self) -> list[Dict[str, Any]]:
        out = []
        for w in self.windows.values():
            item = dict(w)
            item["affected_namespaces"] = sorted(w["affected_namespaces"])
            item["affected_pods"] = sorted(w["affected_pods"])
            out.append(item)

        return sorted(out, key=lambda x: (x["window_start"], x["entity_id"], -x["count"]))
