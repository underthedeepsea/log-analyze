from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from logrisk.database import SQLiteDatabase


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _parse(value: str | None, fallback: datetime | None = None) -> datetime:
    if not value:
        return fallback or datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _floor_window(timestamp: datetime, seconds: int) -> tuple[str, str]:
    start_epoch = int(timestamp.timestamp()) // seconds * seconds
    start = datetime.fromtimestamp(start_epoch, tz=timestamp.tzinfo or timezone.utc)
    return start.isoformat(), (start + timedelta(seconds=seconds)).isoformat()


class NodeRiskError(ValueError):
    def __init__(self, message: str, *, code: str = "invalid_node_risk_request", status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class NodeRiskService:
    def __init__(
        self,
        database: SQLiteDatabase,
        config_path: str | Path,
        *,
        clock: Callable[[], str] = _now,
    ) -> None:
        self.database = database
        self.config_path = Path(config_path)
        self.clock = clock
        self.config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        try:
            config = yaml.safe_load(self.config_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            raise NodeRiskError(f"节点风险配置不可读: {exc}") from exc
        if not isinstance(config, dict) or not isinstance(config.get("scoring"), dict):
            raise NodeRiskError("节点风险配置无效")
        weights = config["scoring"].get("weights") or {}
        if not math.isclose(sum(float(value) for value in weights.values()), 1.0, abs_tol=0.0001):
            raise NodeRiskError("节点风险评分权重之和必须为 1")
        return config

    @staticmethod
    def _fingerprint(event: dict[str, Any], source_record: dict[str, Any], source_job_id: str | None) -> str:
        source_id = (
            source_record.get("raw_log_id")
            or source_record.get("template_instance_hash")
            or source_record.get("template_hash")
            or hashlib.sha256(str(source_record.get("message_core") or "").encode("utf-8")).hexdigest()
        )
        source_window = source_record.get("window_start") or source_record.get("timestamp") or ""
        text = "|".join((str(source_job_id or ""), str(source_id), str(source_window), event["semantic_rule_id"], event["risk_type"]))
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _dedup_key(event: dict[str, Any], source_record: dict[str, Any]) -> str:
        fields = dict(event.get("semantic_fields") or {})
        context = {
            "cluster": str(source_record.get("cluster") or "default"),
            "node_id": str(source_record.get("node") or source_record.get("host") or source_record.get("hostname") or "unknown"),
            "risk_type": event["risk_type"],
            **fields,
        }
        parts = [str(context.get(field) or "") for field in event["dedup"]["key_fields"]]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    @staticmethod
    def _event(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["semantic_fields"] = json.loads(item.pop("semantic_fields_json"))
        item["evidence_refs"] = json.loads(item.pop("evidence_refs_json"))
        return item

    def ingest(
        self,
        semantic_event: dict[str, Any],
        *,
        source_record: dict[str, Any],
        source_job_id: str | None = None,
        source_trace_id: str | None = None,
        occurrence_count: int = 1,
    ) -> dict[str, Any]:
        if not semantic_event or semantic_event.get("risk_type") == "unknown.unclassified":
            raise NodeRiskError("只有已分类风险语义可以写入节点台账")
        cluster = str(source_record.get("cluster") or "default")
        node_id = str(source_record.get("node") or source_record.get("host") or source_record.get("hostname") or "")
        if not node_id:
            raise NodeRiskError("节点风险事件缺少 node_id")
        count = int(occurrence_count)
        if count <= 0:
            raise NodeRiskError("occurrence_count 必须大于 0")
        timestamp = _parse(source_record.get("timestamp"), _parse(self.clock()))
        window_seconds = int((semantic_event.get("dedup") or {}).get("window_seconds") or 300)
        window_start, window_end = _floor_window(timestamp, window_seconds)
        fingerprint = self._fingerprint(semantic_event, source_record, source_job_id)
        dedup_key = self._dedup_key(semantic_event, source_record)
        now = self.clock()
        evidence_refs = sorted({
            str(value)
            for value in (source_record.get("raw_log_id"), source_record.get("template_hash"))
            if value
        })
        with self.database.transaction() as connection:
            replay = connection.execute(
                "SELECT event_id FROM node_risk_ingestions WHERE source_event_fingerprint=?", (fingerprint,)
            ).fetchone()
            if replay:
                row = connection.execute("SELECT * FROM node_risk_events WHERE event_id=?", (replay["event_id"],)).fetchone()
                result = self._event(row)
            else:
                existing = connection.execute(
                    "SELECT * FROM node_risk_events WHERE dedup_key=? AND window_start=?", (dedup_key, window_start)
                ).fetchone()
                if existing:
                    merged_refs = sorted(set(json.loads(existing["evidence_refs_json"])) | set(evidence_refs))
                    connection.execute(
                        "UPDATE node_risk_events SET occurrence_count=occurrence_count+?, last_seen=?, evidence_refs_json=?, "
                        "updated_at=?, source_job_id=COALESCE(source_job_id, ?) WHERE event_id=?",
                        (count, timestamp.isoformat(), _json(merged_refs), now, source_job_id, existing["event_id"]),
                    )
                    event_id = existing["event_id"]
                else:
                    event_id = f"node-risk-{uuid.uuid4().hex}"
                    connection.execute(
                        "INSERT INTO node_risk_events(event_id, cluster, node_id, risk_domain, risk_category, risk_type, "
                        "risk_subtype, severity, base_score, confidence, semantic_rule_id, semantic_rule_version, dedup_key, "
                        "occurrence_count, first_seen, last_seen, window_start, window_end, status, semantic_fields_json, "
                        "evidence_refs_json, source_job_id, source_trace_id, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?)",
                        (
                            event_id, cluster, node_id, semantic_event["domain"], semantic_event["category"],
                            semantic_event["risk_type"], semantic_event.get("risk_subtype"), semantic_event["severity"],
                            float(semantic_event["base_score"]), float(semantic_event["confidence"]),
                            semantic_event["semantic_rule_id"], int(semantic_event["semantic_rule_version"]), dedup_key,
                            count, timestamp.isoformat(), timestamp.isoformat(), window_start, window_end,
                            _json(semantic_event.get("semantic_fields") or {}), _json(evidence_refs), source_job_id,
                            source_trace_id, now, now,
                        ),
                    )
                connection.execute(
                    "INSERT INTO node_risk_ingestions(source_event_fingerprint, event_id, source_job_id, occurrence_count, ingested_at) "
                    "VALUES (?, ?, ?, ?, ?)", (fingerprint, event_id, source_job_id, count, now),
                )
                row = connection.execute("SELECT * FROM node_risk_events WHERE event_id=?", (event_id,)).fetchone()
                result = self._event(row)
        self.recalculate(cluster, node_id)
        return result

    def _events(self, cluster: str, node_id: str, *, since: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM node_risk_events WHERE cluster=? AND node_id=?"
        params: list[Any] = [cluster, node_id]
        if since:
            query += " AND last_seen>=?"
            params.append(since)
        query += " ORDER BY last_seen DESC, event_id"
        with self.database.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._event(row) for row in rows]

    def _statistics(self, events: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
        def recent(days: int) -> list[dict[str, Any]]:
            threshold = now - timedelta(days=days)
            return [event for event in events if _parse(event["last_seen"]) >= threshold]
        day = recent(1)
        week = recent(7)
        month = recent(30)
        return {
            "event_count_24h": len(day),
            "event_count_7d": len(week),
            "event_count_30d": len(month),
            "occurrence_count_24h": sum(int(event["occurrence_count"]) for event in day),
            "occurrence_count_7d": sum(int(event["occurrence_count"]) for event in week),
            "occurrence_count_30d": sum(int(event["occurrence_count"]) for event in month),
            "distinct_risk_types_7d": len({event["risk_type"] for event in week}),
            "active_event_count": sum(event["status"] in {"active", "acknowledged"} for event in events),
            "recovered_event_count": sum(event["status"] == "recovered" for event in month),
            "severity_distribution": dict(Counter(event["severity"] for event in month)),
            "domain_distribution": dict(Counter(event["risk_domain"] for event in month)),
        }

    def _score(self, events: list[dict[str, Any]], statistics: dict[str, Any], now: datetime) -> dict[str, Any]:
        active = [event for event in events if event["status"] in {"active", "acknowledged"}]
        severity_points = {"critical": 100.0, "high": 75.0, "medium": 45.0, "low": 20.0}
        max_active = max((severity_points.get(event["severity"], 0) for event in active), default=0.0)
        recurrence = min(100.0, float(statistics["occurrence_count_24h"]))
        types = len({event["risk_type"] for event in active})
        domains = len({event["risk_domain"] for event in active})
        diversity_cfg = self.config["scoring"]["diversity"]
        diversity = min(100.0, (types / int(diversity_cfg["cap_types"]) + domains / int(diversity_cfg["cap_domains"])) * 50)
        duration_hours = max(((now - _parse(event["first_seen"])).total_seconds() / 3600 for event in active), default=0.0)
        duration = min(100.0, max(0.0, duration_hours / 168 * 100))
        freshest_hours = min(((now - _parse(event["last_seen"])).total_seconds() / 3600 for event in active), default=720.0)
        recency = max(0.0, 100 - freshest_hours / 24 * 100)
        impacted = {
            str(value)
            for event in active
            for key, value in event["semantic_fields"].items()
            if key in {"pci_bdf", "gpu_uuid", "pod", "container_id", "device"} and value
        }
        impact = min(100.0, max(1 if active else 0, len(impacted)) * 25.0)
        dimensions = {
            "max_active_severity": max_active,
            "recurrence": recurrence,
            "diversity": diversity,
            "duration": duration,
            "recency": recency,
            "impact": impact,
        }
        weights = self.config["scoring"]["weights"]
        contributions = {key: round(value * float(weights[key]), 2) for key, value in dimensions.items()}
        score = round(sum(contributions.values()), 2)
        reasons = []
        forced_level = None
        for override in self.config.get("hard_overrides") or []:
            if any(event["risk_type"] == override.get("risk_type") and event["status"] == override.get("status") for event in active):
                score = max(score, float(override["minimum_score"]))
                forced_level = override["force_level"]
                reasons.append(str(override["reason"]))
        if active and not reasons:
            primary = max(active, key=lambda event: (severity_points.get(event["severity"], 0), event["last_seen"]))
            reasons.append(f"当前主要风险为 {primary['risk_type']}，等级 {primary['severity']}")
        thresholds = self.config["scoring"]["thresholds"]
        level = forced_level or next(
            name for name in ("critical", "high", "medium", "low") if score >= float(thresholds[name])
        )
        primary = sorted(active, key=lambda event: (-severity_points.get(event["severity"], 0), event["risk_type"]))[:5]
        return {
            "overall_score": score,
            "overall_level": level,
            "confidence": round(sum(float(event["confidence"]) for event in active) / len(active), 4) if active else 1.0,
            "trend": "unknown",
            "primary_risks": [{"risk_type": event["risk_type"], "severity": event["severity"], "occurrence_count": event["occurrence_count"]} for event in primary],
            "assessment_reasons": reasons,
            "score_breakdown": {"dimensions": dimensions, "weights": weights, "contributions": contributions, "final_score": score},
        }

    def recalculate(self, cluster: str, node_id: str) -> dict[str, Any]:
        now = _parse(self.clock())
        events = self._events(cluster, node_id, since=(now - timedelta(days=30)).isoformat())
        if not events:
            raise NodeRiskError("节点风险不存在", code="node_risk_not_found", status_code=404)
        statistics = self._statistics(events, now)
        score = self._score(events, statistics, now)
        with self.database.connect() as connection:
            previous = connection.execute(
                "SELECT max_overall_score FROM node_risk_daily WHERE cluster=? AND node_id=? AND date<? "
                "ORDER BY date DESC LIMIT 1",
                (cluster, node_id, now.date().isoformat()),
            ).fetchone()
        if previous is not None:
            difference = score["overall_score"] - float(previous["max_overall_score"])
            score["trend"] = "rising" if difference >= 10 else ("falling" if difference <= -10 else "stable")
        latest = max(event["last_seen"] for event in events)
        snapshot = {
            "schema_version": "node_risk_snapshot_v1", "cluster": cluster, "node_id": node_id,
            **score, **{key: statistics[key] for key in (
                "active_event_count", "event_count_24h", "event_count_7d", "event_count_30d",
                "occurrence_count_24h", "distinct_risk_types_7d",
            )},
            "latest_risk_at": latest, "calculated_at": now.isoformat(),
        }
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO node_risk_snapshots(cluster, node_id, overall_score, overall_level, confidence, trend, "
                "active_event_count, event_count_24h, event_count_7d, event_count_30d, occurrence_count_24h, "
                "distinct_risk_types_7d, primary_risks_json, assessment_reasons_json, score_breakdown_json, latest_risk_at, calculated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(cluster, node_id) DO UPDATE SET overall_score=excluded.overall_score, overall_level=excluded.overall_level, "
                "confidence=excluded.confidence, trend=excluded.trend, active_event_count=excluded.active_event_count, "
                "event_count_24h=excluded.event_count_24h, event_count_7d=excluded.event_count_7d, event_count_30d=excluded.event_count_30d, "
                "occurrence_count_24h=excluded.occurrence_count_24h, distinct_risk_types_7d=excluded.distinct_risk_types_7d, "
                "primary_risks_json=excluded.primary_risks_json, assessment_reasons_json=excluded.assessment_reasons_json, "
                "score_breakdown_json=excluded.score_breakdown_json, latest_risk_at=excluded.latest_risk_at, calculated_at=excluded.calculated_at",
                (
                    cluster, node_id, score["overall_score"], score["overall_level"], score["confidence"], score["trend"],
                    statistics["active_event_count"], statistics["event_count_24h"], statistics["event_count_7d"],
                    statistics["event_count_30d"], statistics["occurrence_count_24h"], statistics["distinct_risk_types_7d"],
                    _json(score["primary_risks"]), _json(score["assessment_reasons"]), _json(score["score_breakdown"]),
                    latest, now.isoformat(),
                ),
            )
        self._refresh_daily(cluster, node_id, events, score["overall_score"], now)
        return snapshot

    def _refresh_daily(self, cluster: str, node_id: str, events: list[dict[str, Any]], overall_score: float, now: datetime) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            grouped.setdefault(_parse(event["last_seen"]).date().isoformat(), []).append(event)
        with self.database.transaction() as connection:
            for day, items in grouped.items():
                severity = Counter(item["severity"] for item in items)
                domains = Counter(item["risk_domain"] for item in items)
                types = Counter(item["risk_type"] for item in items)
                connection.execute(
                    "INSERT INTO node_risk_daily(cluster, node_id, date, event_count, occurrence_count, "
                    "distinct_risk_types, critical_count, high_count, medium_count, low_count, active_count, recovered_count, "
                    "max_event_score, max_overall_score, latest_risk_at, domain_distribution_json, type_distribution_json, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(cluster, node_id, date) DO UPDATE SET event_count=excluded.event_count, occurrence_count=excluded.occurrence_count, "
                    "distinct_risk_types=excluded.distinct_risk_types, critical_count=excluded.critical_count, high_count=excluded.high_count, "
                    "medium_count=excluded.medium_count, low_count=excluded.low_count, active_count=excluded.active_count, recovered_count=excluded.recovered_count, "
                    "max_event_score=excluded.max_event_score, max_overall_score=excluded.max_overall_score, latest_risk_at=excluded.latest_risk_at, "
                    "domain_distribution_json=excluded.domain_distribution_json, type_distribution_json=excluded.type_distribution_json, updated_at=excluded.updated_at",
                    (
                        cluster, node_id, day, len(items), sum(int(item["occurrence_count"]) for item in items),
                        len(types), severity["critical"], severity["high"], severity["medium"], severity["low"],
                        sum(item["status"] in {"active", "acknowledged"} for item in items),
                        sum(item["status"] == "recovered" for item in items), max(float(item["base_score"]) for item in items),
                        overall_score, max(item["last_seen"] for item in items), _json(domains), _json(types), now.isoformat(),
                    ),
                )

    @staticmethod
    def _snapshot(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["primary_risks"] = json.loads(item.pop("primary_risks_json"))
        item["assessment_reasons"] = json.loads(item.pop("assessment_reasons_json"))
        item["score_breakdown"] = json.loads(item.pop("score_breakdown_json"))
        return item

    def get_node(self, cluster: str, node_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM node_risk_snapshots WHERE cluster=? AND node_id=?", (cluster, node_id)
            ).fetchone()
        if row is None:
            raise NodeRiskError("节点风险不存在", code="node_risk_not_found", status_code=404)
        snapshot = self._snapshot(row)
        events = self._events(cluster, node_id)
        return {
            "schema_version": "node_risk_profile_v1", "cluster": cluster, "node_id": node_id,
            "snapshot": snapshot, "statistics": self._statistics(events, _parse(self.clock())),
            "events": events[:20],
        }

    def list_events(self, cluster: str, node_id: str, *, risk_type: str | None = None,
                    severity: str | None = None, status: str | None = None,
                    page: int = 1, page_size: int = 50) -> dict[str, Any]:
        if page < 1 or page_size < 1 or page_size > 100:
            raise NodeRiskError("分页参数无效")
        items = self._events(cluster, node_id)
        if risk_type:
            items = [item for item in items if item["risk_type"] == risk_type]
        if severity:
            items = [item for item in items if item["severity"] == severity]
        if status:
            items = [item for item in items if item["status"] == status]
        start = (page - 1) * page_size
        return {"schema_version": "node_risk_event_list_v1", "items": items[start:start + page_size], "total": len(items)}

    def list_nodes(self, *, cluster: str | None = None, level: str | None = None, domain: str | None = None, trend: str | None = None,
                   search: str | None = None, active_only: bool = False, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        if page < 1 or page_size < 1 or page_size > 100:
            raise NodeRiskError("分页参数无效")
        query = "SELECT * FROM node_risk_snapshots"
        params: list[Any] = []
        clauses = []
        if cluster:
            clauses.append("cluster=?")
            params.append(cluster)
        if level:
            clauses.append("overall_level=?")
            params.append(level)
        if trend:
            clauses.append("trend=?")
            params.append(trend)
        if domain:
            clauses.append("EXISTS (SELECT 1 FROM node_risk_events event WHERE event.cluster=node_risk_snapshots.cluster "
                           "AND event.node_id=node_risk_snapshots.node_id AND event.risk_domain=?)")
            params.append(domain)
        if search:
            clauses.append("node_id LIKE ?")
            params.append(f"%{search}%")
        if active_only:
            clauses.append("active_event_count>0")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY overall_score DESC, latest_risk_at DESC LIMIT ? OFFSET ?"
        count_params = list(params)
        params.extend([page_size, (page - 1) * page_size])
        with self.database.connect() as connection:
            rows = connection.execute(query, params).fetchall()
            total = connection.execute("SELECT COUNT(*) FROM node_risk_snapshots" + (" WHERE " + " AND ".join(clauses) if clauses else ""), count_params).fetchone()[0]
        return {"schema_version": "node_risk_list_v1", "items": [self._snapshot(row) for row in rows], "total": total}

    def daily(self, cluster: str, node_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM node_risk_daily WHERE cluster=? AND node_id=? ORDER BY date DESC LIMIT 90",
                (cluster, node_id),
            ).fetchall()
        return [dict(row) | {
            "domain_distribution": json.loads(row["domain_distribution_json"]),
            "type_distribution": json.loads(row["type_distribution_json"]),
        } for row in rows]

    def timeline(self, cluster: str, node_id: str) -> list[dict[str, Any]]:
        events = self._events(cluster, node_id)
        return [{"time": item["first_seen"], "event_type": "first_seen", "event_id": item["event_id"], "risk_type": item["risk_type"], "status": item["status"]} for item in events]

    def acknowledge_event(self, event_id: str, *, operator: str, reason: str) -> dict[str, Any]:
        return self._transition_event(event_id, "acknowledged", operator=operator, reason=reason)

    def recover_event(self, event_id: str, *, operator: str, reason: str) -> dict[str, Any]:
        return self._transition_event(event_id, "recovered", operator=operator, reason=reason)

    def _transition_event(self, event_id: str, status: str, *, operator: str, reason: str) -> dict[str, Any]:
        if not str(operator).strip() or not str(reason).strip():
            raise NodeRiskError("事件操作需要操作人和原因")
        now = self.clock()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM node_risk_events WHERE event_id=?", (event_id,)).fetchone()
            if row is None:
                raise NodeRiskError("风险事件不存在", code="node_risk_event_not_found", status_code=404)
            if status == "recovered":
                connection.execute("UPDATE node_risk_events SET status='recovered', recovered_at=?, updated_at=? WHERE event_id=?", (now, now, event_id))
            else:
                connection.execute("UPDATE node_risk_events SET status='acknowledged', acknowledged_at=?, acknowledged_by=?, updated_at=? WHERE event_id=?", (now, operator, now, event_id))
            connection.execute(
                "INSERT INTO node_risk_audit_events(audit_id, event_id, event_type, event_json, operator, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"node-audit-{uuid.uuid4().hex}", event_id, status, _json({"reason": reason}), operator, now),
            )
            cluster, node_id = row["cluster"], row["node_id"]
        self.recalculate(cluster, node_id)
        with self.database.connect() as connection:
            return self._event(connection.execute("SELECT * FROM node_risk_events WHERE event_id=?", (event_id,)).fetchone())
