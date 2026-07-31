from __future__ import annotations

import json
import re
import uuid
from typing import Any, Mapping

from logrisk.database import Database, utc_now


class MultiSourceConflictError(RuntimeError):
    code = "multi_source_version_conflict"


_RULE_UPDATE_FIELDS = {
    "display_name",
    "enabled",
    "source_pairs",
    "max_gap_seconds",
    "min_risk_score",
    "min_count",
    "confidence",
    "expected_version",
}
_SOURCE_FAMILY_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if not value:
        return []
    parsed = json.loads(str(value))
    return parsed if isinstance(parsed, list) else []


class MultiSourceRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def seed_rules(self, rules: list[Mapping[str, Any]]) -> None:
        now = utc_now()
        with self.database.transaction() as connection:
            for source in rules:
                rule_id = str(source["rule_id"])
                current = connection.execute(
                    "SELECT rule_id FROM multi_source_rules WHERE rule_id=?", (rule_id,)
                ).fetchone()
                if current is not None:
                    continue
                definition = dict(source)
                definition.pop("display_name", None)
                definition.pop("enabled", None)
                connection.execute(
                    "INSERT INTO multi_source_rules(rule_id, display_name, enabled, definition_json, version, schema_version, created_at, updated_at) VALUES (?, ?, ?, ?, 1, 'multi_source_rule_v1', ?, ?)",
                    (
                        rule_id,
                        str(source.get("display_name") or rule_id),
                        bool(source.get("enabled", True)),
                        _json(definition),
                        now,
                        now,
                    ),
                )

    def list_rules(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM multi_source_rules ORDER BY rule_id"
            ).fetchall()
        return {"schema_version": "multi_source_rules_v1", "items": [self._rule(row) for row in rows]}

    def update_rule(
        self,
        rule_id: str,
        payload: Mapping[str, Any],
        *,
        actor: str | None,
        request_id: str,
    ) -> dict[str, Any]:
        expected = payload.get("expected_version")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
            raise ValueError("expected_version 必须是正整数")
        unexpected = sorted(set(payload) - _RULE_UPDATE_FIELDS)
        if unexpected:
            raise ValueError("不支持编辑字段: " + ", ".join(unexpected))
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM multi_source_rules WHERE rule_id=?", (rule_id,)
            ).fetchone()
            if row is None:
                raise KeyError("多来源关联规则不存在")
            if int(row["version"]) != expected:
                raise MultiSourceConflictError("多来源关联规则已被其他操作更新")
            current = self._rule(row)
            display_name = self._rule_display_name(payload.get("display_name", current["display_name"]))
            enabled = self._rule_enabled(payload.get("enabled", current["enabled"]))
            source_pairs = self._source_pairs(payload.get("source_pairs", current.get("source_pairs")))
            max_gap_seconds = self._integer_range(
                payload.get("max_gap_seconds", current.get("max_gap_seconds")),
                name="max_gap_seconds",
                minimum=1,
                maximum=86_400,
            )
            min_risk_score = self._number_range(
                payload.get("min_risk_score", current.get("min_risk_score")),
                name="min_risk_score",
                minimum=0,
                maximum=100,
            )
            min_count = self._integer_range(
                payload.get("min_count", current.get("min_count")),
                name="min_count",
                minimum=1,
                maximum=1_000_000,
            )
            confidence = self._number_range(
                payload.get("confidence", current.get("confidence")),
                name="confidence",
                minimum=0,
                maximum=1,
            )
            definition = {
                "rule_id": rule_id,
                "source_pairs": source_pairs,
                "max_gap_seconds": max_gap_seconds,
                "min_risk_score": min_risk_score,
                "min_count": min_count,
                "confidence": confidence,
            }
            version = expected + 1
            connection.execute(
                "UPDATE multi_source_rules SET display_name=?, enabled=?, definition_json=?, version=?, updated_at=? WHERE rule_id=?",
                (display_name, enabled, _json(definition), version, now, rule_id),
            )
            self._append_audit(
                connection,
                "rule.updated",
                "multi_source_rule",
                rule_id,
                actor,
                request_id,
                {
                    "display_name": display_name,
                    "enabled": enabled,
                    "source_pair_count": len(source_pairs),
                    "max_gap_seconds": max_gap_seconds,
                    "min_risk_score": min_risk_score,
                    "min_count": min_count,
                    "confidence": confidence,
                    "version": version,
                },
                now,
            )
            updated = connection.execute(
                "SELECT * FROM multi_source_rules WHERE rule_id=?", (rule_id,)
            ).fetchone()
        return self._rule(updated)

    def save_observation(self, observation: Mapping[str, Any]) -> dict[str, Any]:
        now = utc_now()
        values = (
            str(observation["observation_id"]),
            observation.get("source_job_id"),
            str(observation["cluster"]),
            str(observation["source_type"]),
            str(observation["source_family"]),
            str(observation["component"]),
            observation.get("severity"),
            str(observation["template_hash"]),
            str(observation["template"]),
            int(observation.get("count") or 1),
            float(observation.get("risk_score") or 0),
            str(observation.get("risk_level") or "unknown"),
            str(observation["window_start"]),
            str(observation["window_end"]),
            _json(list(observation.get("entity_keys") or [])),
            _json(list(observation.get("relations") or [])),
            now,
            now,
        )
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO multi_source_observations(observation_id, source_job_id, cluster, source_type, source_family, component, severity, template_hash, template, occurrence_count, risk_score, risk_level, window_start, window_end, entity_keys_json, relations_json, schema_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'multi_source_observation_v1', ?, ?) ON CONFLICT(observation_id) DO UPDATE SET occurrence_count=excluded.occurrence_count, risk_score=excluded.risk_score, risk_level=excluded.risk_level, entity_keys_json=excluded.entity_keys_json, relations_json=excluded.relations_json, updated_at=excluded.updated_at",
                values,
            )
        return dict(observation)

    def recent_observations(self, *, cluster: str, since: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM multi_source_observations WHERE cluster=? AND window_end>=? ORDER BY window_start",
                (cluster, since),
            ).fetchall()
        return [self._observation(row) for row in rows]

    def save_correlation(self, correlation: Mapping[str, Any]) -> dict[str, Any]:
        now = utc_now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO multi_source_correlations(correlation_id, rule_id, rule_version, cluster, primary_entity_key, window_start, window_end, confidence, risk_score, source_families_json, schema_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'multi_source_correlation_v1', ?, ?) ON CONFLICT(correlation_id) DO UPDATE SET window_start=excluded.window_start, window_end=excluded.window_end, confidence=excluded.confidence, risk_score=excluded.risk_score, source_families_json=excluded.source_families_json, updated_at=excluded.updated_at",
                (
                    str(correlation["correlation_id"]),
                    str(correlation["rule_id"]),
                    int(correlation["rule_version"]),
                    str(correlation["cluster"]),
                    str(correlation["primary_entity_key"]),
                    str(correlation["window_start"]),
                    str(correlation["window_end"]),
                    float(correlation["confidence"]),
                    float(correlation["risk_score"]),
                    _json(list(correlation.get("source_families") or [])),
                    now,
                    now,
                ),
            )
            connection.execute(
                "DELETE FROM multi_source_correlation_items WHERE correlation_id=?",
                (str(correlation["correlation_id"]),),
            )
            for position, observation_id in enumerate(correlation.get("observation_ids") or []):
                connection.execute(
                    "INSERT INTO multi_source_correlation_items(correlation_id, observation_id, position) VALUES (?, ?, ?)",
                    (str(correlation["correlation_id"]), str(observation_id), position),
                )
        return dict(correlation)

    def get_correlation(self, correlation_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM multi_source_correlations WHERE correlation_id=?",
                (correlation_id,),
            ).fetchone()
            items = connection.execute(
                "SELECT observation_id FROM multi_source_correlation_items WHERE correlation_id=? ORDER BY position",
                (correlation_id,),
            ).fetchall()
        if row is None:
            raise KeyError("多来源关联不存在")
        result = self._correlation(row)
        result["observation_ids"] = [str(item["observation_id"]) for item in items]
        return result

    def get_observation(self, observation_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM multi_source_observations WHERE observation_id=?",
                (observation_id,),
            ).fetchone()
        if row is None:
            raise KeyError("多来源观察不存在")
        return self._observation(row)

    def entity_timeline(self, entity_key: str, *, limit: int = 200) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM multi_source_observations ORDER BY window_start DESC LIMIT ?",
                (max(1, min(int(limit) * 5, 1000)),),
            ).fetchall()
        items = [
            self._observation(row)
            for row in rows
            if entity_key in _array(row["entity_keys_json"])
        ][:limit]
        return {"schema_version": "multi_source_timeline_v1", "items": items}

    def entity_correlations(self, entity_key: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM multi_source_correlations WHERE primary_entity_key=? ORDER BY window_start DESC LIMIT ?",
                (entity_key, max(1, min(int(limit), 500))),
            ).fetchall()
        return [self._correlation(row) for row in rows]

    def summary(self) -> dict[str, Any]:
        with self.database.connect() as connection:
            observation_count = int(connection.execute(
                "SELECT COUNT(*) FROM multi_source_observations"
            ).fetchone()[0])
            correlation_count = int(connection.execute(
                "SELECT COUNT(*) FROM multi_source_correlations"
            ).fetchone()[0])
            rows = connection.execute(
                "SELECT source_family, COUNT(*) AS count FROM multi_source_observations GROUP BY source_family ORDER BY source_family"
            ).fetchall()
        return {
            "schema_version": "multi_source_summary_v1",
            "observation_count": observation_count,
            "correlation_count": correlation_count,
            "source_coverage": [
                {"source_family": str(row["source_family"]), "count": int(row["count"])}
                for row in rows
            ],
        }

    def list_entities(self, *, limit: int = 200) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT entity_keys_json, source_family, risk_score, window_end FROM multi_source_observations ORDER BY window_end DESC"
            ).fetchall()
        entities: dict[str, dict[str, Any]] = {}
        for row in rows:
            for key in _array(row["entity_keys_json"]):
                item = entities.setdefault(str(key), {
                    "entity_key": str(key),
                    "source_families": set(),
                    "observation_count": 0,
                    "max_risk_score": 0.0,
                    "last_seen": str(row["window_end"]),
                })
                item["source_families"].add(str(row["source_family"]))
                item["observation_count"] += 1
                item["max_risk_score"] = max(item["max_risk_score"], float(row["risk_score"]))
        result = sorted(entities.values(), key=lambda item: (-item["max_risk_score"], item["entity_key"]))[:limit]
        for item in result:
            item["source_families"] = sorted(item["source_families"])
        return {"schema_version": "multi_source_entities_v1", "items": result}

    def list_audits(self, *, limit: int = 100) -> dict[str, Any]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM multi_source_audit_events ORDER BY created_at DESC, audit_id DESC LIMIT ?",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
        return {
            "items": [
                {
                    "audit_id": str(row["audit_id"]),
                    "action": str(row["action"]),
                    "resource_type": str(row["resource_type"]),
                    "resource_id": row["resource_id"],
                    "actor": row["actor"],
                    "request_id": str(row["request_id"]),
                    "attributes": json.loads(row["attributes_json"]) if isinstance(row["attributes_json"], str) else dict(row["attributes_json"]),
                    "created_at": str(row["created_at"]),
                }
                for row in rows
            ]
        }

    def _append_audit(
        self,
        connection: Any,
        action: str,
        resource_type: str,
        resource_id: str | None,
        actor: str | None,
        request_id: str,
        attributes: Mapping[str, Any],
        created_at: str,
    ) -> None:
        connection.execute(
            "INSERT INTO multi_source_audit_events(audit_id, action, resource_type, resource_id, actor, request_id, attributes_json, schema_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'multi_source_audit_v1', ?)",
            (
                "multi-source-audit-" + uuid.uuid4().hex,
                action,
                resource_type,
                resource_id,
                actor,
                request_id,
                _json(dict(attributes)),
                created_at,
            ),
        )

    @staticmethod
    def _rule(row: Any) -> dict[str, Any]:
        definition = json.loads(row["definition_json"]) if isinstance(row["definition_json"], str) else dict(row["definition_json"])
        return {
            "rule_id": str(row["rule_id"]),
            "display_name": str(row["display_name"]),
            "enabled": bool(row["enabled"]),
            "version": int(row["version"]),
            **definition,
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _rule_display_name(value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("display_name 不能为空")
        result = value.strip()
        if len(result) > 80:
            raise ValueError("display_name 不能超过 80 个字符")
        return result

    @staticmethod
    def _rule_enabled(value: Any) -> bool:
        if not isinstance(value, bool):
            raise ValueError("enabled 必须是布尔值")
        return value

    @staticmethod
    def _source_pairs(value: Any) -> list[list[str]]:
        if not isinstance(value, list) or not value:
            raise ValueError("source_pairs 至少需要一组来源")
        result: list[list[str]] = []
        seen: set[frozenset[str]] = set()
        for item in value:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise ValueError("source_pairs 每组必须包含两个来源")
            pair = []
            for source in item:
                if not isinstance(source, str):
                    raise ValueError("来源名称必须是字符串")
                normalized = source.strip().casefold()
                if not _SOURCE_FAMILY_PATTERN.fullmatch(normalized):
                    raise ValueError("来源名称只能使用小写字母、数字、下划线或短横线")
                pair.append(normalized)
            identity = frozenset(pair)
            if len(identity) != 2:
                raise ValueError("同一来源不能与自身关联")
            if identity in seen:
                raise ValueError("source_pairs 不能包含重复来源组合")
            seen.add(identity)
            result.append(pair)
        return result

    @staticmethod
    def _integer_range(value: Any, *, name: str, minimum: int, maximum: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
            raise ValueError(f"{name} 必须是 {minimum} 至 {maximum} 之间的整数")
        return value

    @staticmethod
    def _number_range(value: Any, *, name: str, minimum: float, maximum: float) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} 必须是 {minimum} 至 {maximum} 之间的数字")
        result = float(value)
        if not minimum <= result <= maximum:
            raise ValueError(f"{name} 必须是 {minimum} 至 {maximum} 之间的数字")
        return result

    @staticmethod
    def _observation(row: Any) -> dict[str, Any]:
        return {
            "observation_id": str(row["observation_id"]),
            "source_job_id": row["source_job_id"],
            "cluster": str(row["cluster"]),
            "source_type": str(row["source_type"]),
            "source_family": str(row["source_family"]),
            "component": str(row["component"]),
            "severity": row["severity"],
            "template_hash": str(row["template_hash"]),
            "template": str(row["template"]),
            "count": int(row["occurrence_count"]),
            "risk_score": float(row["risk_score"]),
            "risk_level": str(row["risk_level"]),
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"]),
            "entity_keys": _array(row["entity_keys_json"]),
            "relations": _array(row["relations_json"]),
        }

    @staticmethod
    def _correlation(row: Any) -> dict[str, Any]:
        return {
            "correlation_id": str(row["correlation_id"]),
            "rule_id": str(row["rule_id"]),
            "rule_version": int(row["rule_version"]),
            "cluster": str(row["cluster"]),
            "primary_entity_key": str(row["primary_entity_key"]),
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"]),
            "confidence": float(row["confidence"]),
            "risk_score": float(row["risk_score"]),
            "source_families": _array(row["source_families_json"]),
        }
