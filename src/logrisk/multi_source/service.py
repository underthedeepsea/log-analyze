from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Any, Mapping

from .correlation import correlate_observations
from .repository import MultiSourceRepository
from .router import route_entities


KNOWN_SOURCE_FAMILIES = {"audit", "containerd", "journal", "kernel", "kubelet", "podlog"}


def _source_family(template: Mapping[str, Any]) -> str:
    component = str(template.get("component") or "unknown").casefold()
    source_type = str(template.get("source_type") or "unknown").casefold()
    return component if component in KNOWN_SOURCE_FAMILIES else source_type


def _observation_id(source_job_id: str | None, template: Mapping[str, Any], entity_keys: list[str]) -> str:
    identity = "|".join((
        str(source_job_id or ""),
        str(template.get("cluster") or "default"),
        str(template.get("window_start") or ""),
        str(template.get("window_end") or ""),
        str(template.get("template_hash") or ""),
        *sorted(entity_keys),
    ))
    return "observation-" + hashlib.sha256(identity.encode()).hexdigest()[:24]


class MultiSourceService:
    def __init__(
        self,
        repository: MultiSourceRepository,
        *,
        aliases: Mapping[str, Mapping[str, str]],
        rules: list[Mapping[str, Any]],
        enabled: bool = True,
    ) -> None:
        self.repository = repository
        self.aliases = aliases
        self.rules = [dict(rule) for rule in rules]
        self.enabled = bool(enabled)
        self.repository.seed_rules(self.rules)

    def _window_route(self, template: Mapping[str, Any]) -> dict[str, Any]:
        route = route_entities(template, aliases=self.aliases)
        existing = [str(key) for key in template.get("entity_keys") or [] if str(key)]
        if not existing:
            return route
        canonical_keys: dict[str, str] = {}
        for key in existing:
            parts = key.split("/", 2)
            if len(parts) != 3:
                continue
            cluster, entity_type, entity_id = parts
            alias_map = {
                str(source).strip().casefold(): str(target).strip().casefold()
                for source, target in (self.aliases.get(entity_type) or {}).items()
            }
            canonical_id = alias_map.get(entity_id.casefold(), entity_id.casefold())
            canonical_keys[key] = f"{cluster.casefold()}/{entity_type}/{canonical_id}"
        entity_keys = {
            str(item["entity_key"]) for item in route.get("entities") or []
        } | set(canonical_keys.values())
        relations = []
        seen_relations = set()
        for item in [*(route.get("relations") or []), *(template.get("entity_relations") or [])]:
            relation = {
                "from_key": canonical_keys.get(str(item.get("from_key")), str(item.get("from_key") or "")),
                "relation": str(item.get("relation") or ""),
                "to_key": canonical_keys.get(str(item.get("to_key")), str(item.get("to_key") or "")),
            }
            identity = (relation["from_key"], relation["relation"], relation["to_key"])
            if all(identity) and identity not in seen_relations:
                relations.append(relation)
                seen_relations.add(identity)
        return {
            **route,
            "entities": [{"entity_key": key} for key in sorted(entity_keys)],
            "relations": relations,
        }

    def ingest_risk_entities(
        self,
        risk_entities: list[Mapping[str, Any]],
        *,
        source_job_id: str | None,
    ) -> dict[str, int]:
        if not self.enabled:
            return {"observations": 0, "correlations": 0, "unroutable": 0}
        saved = 0
        unroutable = 0
        clusters: set[str] = set()
        earliest: datetime | None = None
        for entity in risk_entities:
            risk_score = float(entity.get("risk_score") or 0)
            risk_level = str(entity.get("risk_level") or "unknown")
            for source in entity.get("top_templates") or []:
                template = dict(source)
                template.setdefault("cluster", entity.get("cluster"))
                if entity.get("entity_type") and entity.get("entity_id"):
                    template.setdefault(str(entity["entity_type"]), entity["entity_id"])
                route = self._window_route(template)
                if route["status"] != "routed":
                    unroutable += 1
                    continue
                entity_keys = [str(item["entity_key"]) for item in route["entities"]]
                cluster = str(route["primary_entity"]["cluster"])
                window_start = str(template.get("window_start") or entity.get("window_start"))
                window_end = str(template.get("window_end") or entity.get("window_end"))
                if not window_start or not window_end or not template.get("template_hash"):
                    unroutable += 1
                    continue
                observation = {
                    "observation_id": _observation_id(source_job_id, template, entity_keys),
                    "source_job_id": source_job_id,
                    "cluster": cluster,
                    "source_type": str(template.get("source_type") or "unknown"),
                    "source_family": _source_family(template),
                    "component": str(template.get("component") or "unknown"),
                    "severity": template.get("severity"),
                    "template_hash": str(template["template_hash"]),
                    "template": str(template.get("template") or ""),
                    "count": max(1, int(template.get("count") or 1)),
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "window_start": window_start,
                    "window_end": window_end,
                    "entity_keys": entity_keys,
                    "relations": route["relations"],
                }
                self.repository.save_observation(observation)
                saved += 1
                clusters.add(cluster)
                value = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
                earliest = value if earliest is None else min(earliest, value)

        correlation_ids: set[str] = set()
        stored_rules = self.repository.list_rules()["items"]
        maximum_gap = max((int(rule.get("max_gap_seconds") or 0) for rule in stored_rules), default=0)
        since = (earliest - timedelta(seconds=maximum_gap)).isoformat() if earliest else ""
        for cluster in clusters:
            candidates = self.repository.recent_observations(cluster=cluster, since=since)
            for stored_rule in stored_rules:
                for correlation in correlate_observations(candidates, stored_rule):
                    self.repository.save_correlation(correlation)
                    correlation_ids.add(str(correlation["correlation_id"]))
        return {
            "observations": saved,
            "correlations": len(correlation_ids),
            "unroutable": unroutable,
        }

    def summary(self) -> dict[str, Any]:
        return self.repository.summary()

    def entities(self, *, limit: int = 200) -> dict[str, Any]:
        return self.repository.list_entities(limit=limit)

    def update_rule(
        self,
        rule_id: str,
        payload: Mapping[str, Any],
        *,
        actor: str | None,
        request_id: str,
    ) -> dict[str, Any]:
        return self.repository.update_rule(
            rule_id,
            payload,
            actor=actor,
            request_id=request_id,
        )

    def entity_timeline(
        self,
        entity_type: str,
        entity_id: str,
        *,
        cluster: str,
        limit: int = 200,
    ) -> dict[str, Any]:
        route = route_entities(
            {"cluster": cluster, entity_type: entity_id},
            aliases=self.aliases,
        )
        if route["primary_entity"] is None:
            raise ValueError("实体标识无效")
        entity_key = str(route["primary_entity"]["entity_key"])
        timeline = self.repository.entity_timeline(entity_key, limit=limit)
        timeline["correlations"] = self.repository.entity_correlations(entity_key)
        return timeline

    def correlation(self, correlation_id: str) -> dict[str, Any]:
        detail = self.repository.get_correlation(correlation_id)
        observations = [
            self.repository.get_observation(observation_id)
            for observation_id in detail["observation_ids"]
        ]
        return {**detail, "observations": observations}

    def entity_detail(
        self,
        entity_type: str,
        entity_id: str,
        *,
        cluster: str,
    ) -> dict[str, Any]:
        route = route_entities(
            {"cluster": cluster, entity_type: entity_id},
            aliases=self.aliases,
        )
        primary = route.get("primary_entity")
        if not primary:
            raise ValueError("实体标识无效")
        entity_key = str(primary["entity_key"])
        entity = next(
            (
                item
                for item in self.repository.list_entities(limit=1000)["items"]
                if item["entity_key"] == entity_key
            ),
            None,
        )
        if entity is None:
            raise KeyError("多来源实体不存在")
        timeline = self.repository.entity_timeline(entity_key)
        correlations = self.repository.entity_correlations(entity_key)
        return {
            "schema_version": "multi_source_entity_detail_v1",
            "entity": entity,
            "timeline_count": len(timeline["items"]),
            "correlation_count": len(correlations),
        }

    def rules_view(self) -> dict[str, Any]:
        return self.repository.list_rules()
