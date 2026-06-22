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
    windows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for event in events:
        dt = parse_ts(event.get("timestamp"))
        start = floor_window(dt, window_seconds)
        end = datetime.fromtimestamp(start.timestamp() + window_seconds, tz=start.tzinfo)

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

        if key not in windows:
            windows[key] = {
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "cluster": event.get("cluster"),
                "entity_type": entity_type,
                "entity_id": entity_id,
                "source_type": event.get("source_type"),
                "component": event.get("component"),
                "severity": event.get("severity"),
                "template_hash": event.get("template_hash"),
                "template": event.get("template"),
                "count": 0,
                "first_seen": event.get("timestamp"),
                "last_seen": event.get("timestamp"),
                "samples": [],
                "affected_namespaces": set(),
                "affected_pods": set(),
            }

        w = windows[key]
        w["count"] += 1
        ts = event.get("timestamp")
        if ts:
            w["last_seen"] = ts
        if event.get("raw_sample") and len(w["samples"]) < max_samples_per_template:
            w["samples"].append(event["raw_sample"])
        if event.get("namespace"):
            w["affected_namespaces"].add(event["namespace"])
        if event.get("pod"):
            w["affected_pods"].add(event["pod"])

    out = []
    for w in windows.values():
        w["affected_namespaces"] = sorted(w["affected_namespaces"])
        w["affected_pods"] = sorted(w["affected_pods"])
        out.append(w)

    return sorted(out, key=lambda x: (x["window_start"], x["entity_id"], -x["count"]))
