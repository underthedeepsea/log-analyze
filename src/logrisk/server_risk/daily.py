from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Any

from logrisk.server_risk.replay import EvaluationContext
from logrisk.server_risk.contracts import ServerRiskError, cursor_encode, cursor_offset, day_bounds, digest, integer, utc


def covers_day(intervals: list[tuple[Any, Any]], start: Any, end: Any) -> bool:
    through = start
    for left, right in sorted(intervals):
        if right <= through:
            continue
        if left > through:
            return False
        through = right
        if through >= end:
            return True
    return False


def day_evidence(repository: Any, query: dict[str, str], *, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
    integer(limit, 1, 100)
    if hasattr(repository, "snapshot"):
        repository = repository.snapshot(query["cluster"], query["node_id"])
    start, end = day_bounds(query["date"], query["timezone"])
    facts = repository.facts(query["cluster"], query["node_id"])
    selected, exact, ambiguous = [], [], []
    for fact in facts:
        try:
            first, last = utc(fact["first_seen"]), utc(fact["last_seen"])
        except ServerRiskError:
            continue
        exclusive = fact["provenance"] == "registered_observation" and first != last
        if first < end and (last > start if exclusive else last >= start):
            item = dict(fact)
            item["count_quality"] = "known" if first >= start and (last <= end if exclusive else last < end) else "boundary_ambiguous"
            selected.append(item)
            (exact if item["count_quality"] == "known" else ambiguous).append(item)
    revision = digest({"facts": facts, "query": query})
    offset = 0
    scores = [s for s in repository.scores(query["cluster"], query["node_id"]) if start <= utc(s["assessed_at"]) < end]
    severity_counts: Counter = Counter()
    for fact in exact:
        severity_counts[fact["severity"]] += fact["count"]
    summary = {"occurrence_count": None, "known_occurrence_count": sum(f["count"] for f in exact), "count_quality": "partial", "event_count": len(selected), "risk_type_count": len({f["risk_type"] for f in selected}), "severity_counts": dict(severity_counts), "severity_record_counts": dict(Counter(f["severity"] for f in selected)), "coverage": "unknown", "missing_reasons": ["source_coverage_unregistered"] + (["boundary_ambiguous"] if ambiguous else []) + (["legacy_unverified"] if any(f["provenance"] == "legacy_unverified" for f in selected) else []), "projection_complete": False, "end_of_day_assessment": "unknown", "unfinished_episode_count": None, "episode_started_count": None, "episode_observed_count": None, "assessed_score_peak": max((s["overall_score"] for s in scores), default=None), "score_quality": "observed" if scores else "uncertain", "score_sample_refs": [s["sample_id"] for s in scores], "score_evidence_period": [{"assessed_at": s["assessed_at"], "score": s["overall_score"]} for s in scores]}
    decisions = []
    unfinished = []
    if repository.assessment is not None:
        assessment = repository.assessment.evaluate(query["cluster"], query["node_id"], exclusive_end=True, context=EvaluationContext.historical(end.isoformat()))
        rounds = assessment["episodes"]
        observed = [e for e in rounds if utc(e["first_adverse"]) < end and (not e["resolved_at"] or utc(e["resolved_at"]) >= start)]
        unfinished = [e for e in observed if not e["resolved_at"] or utc(e["resolved_at"]) >= end]
        for fact in selected:
            episode = next((e for e in reversed(rounds) if e["subject_key"] == fact.get("subject_key") and utc(e["first_adverse"]) <= utc(fact["first_seen"]) <= utc(e["last_adverse"])), None)
            if episode:
                fact["episode_ref"] = episode["episode_id"]
        summary["projection_complete"] = assessment["projection_complete"]
        if not assessment["projection_complete"]:
            summary["missing_reasons"].append("projection_pending")
        summary.update({"end_of_day_assessment": assessment["status"], "assessment_explanation": assessment, "evaluation_mode": "historical", "as_of": end.isoformat(), "policy_version": assessment["policy_version"], "unfinished_episode_count": len(unfinished), "episode_started_count": sum(start <= utc(e["first_adverse"]) < end for e in rounds), "episode_observed_count": len(observed)})
        summary.update({"known_episode_started_count": summary["episode_started_count"], "known_episode_observed_count": summary["episode_observed_count"], "known_unfinished_episode_count": summary["unfinished_episode_count"]})
        decisions = [d for d in repository.decisions(query["cluster"], query["node_id"]) if start <= utc(d["effective_at"]) < end]
        intervals: dict[tuple[Any, ...], list[Any]] = {}
        for row in repository.observations(query["cluster"], query["node_id"]):
            payload = row["payload"]
            if payload["kind"] != "coverage":
                continue
            c = payload["coverage"]
            try:
                source = repository.assessment.policies.source(row["source_id"])
            except ServerRiskError:
                continue
            rule = repository.assessment.policies.rules.get(payload["risk_type"], {})
            required = set(rule.get("required_signals", []))
            if source["capability"] == "complete_event_stream" and row["source_epoch"] == source["source_epoch"] and row["config_revision"] == source["config_revision"] and required and required.issubset(c["signal_codes"]) and not c["gaps"] and not c["dropped_count"] and not c["parse_failure_count"] and repository.assessment._frontier_complete(row):
                obj = payload["identity"]
                key = (obj["object_kind"], obj["object_id"], payload["risk_type"], row["source_id"], row["source_epoch"], row["config_revision"], obj.get("boot_id"))
                intervals.setdefault(key, []).append((utc(c["covered_start"]), utc(c["complete_through"])))
        covers = {key[:3] for key, parts in intervals.items() if covers_day(parts, start, end)}
        required_objects = []
        for source in repository.assessment.policies.sources.values():
            if source.get("enabled") and source.get("cluster") == query["cluster"] and query["node_id"] in source.get("node_ids", []):
                required_objects += [(obj["object_kind"], obj["object_id"], signal["risk_type"]) for obj in source.get("objects", []) for signal in source.get("signals", {}).values()]
        complete_coverage = bool(required_objects) and set(required_objects).issubset(covers) and assessment["projection_complete"] and not ambiguous and not any(f["provenance"] == "legacy_unverified" for f in selected)
        if complete_coverage:
            summary.update({"coverage": "complete", "occurrence_count": summary["known_occurrence_count"], "count_quality": "complete", "missing_reasons": [], "projection_complete": True})
        elif any(left < end and right > start for parts in intervals.values() for left, right in parts):
            summary["coverage"] = "partial"
        if not complete_coverage:
            summary.update({"episode_started_count": None, "episode_observed_count": None, "unfinished_episode_count": None})
        revision = digest({"facts": facts, "query": query, "summary": summary, "decisions": decisions, "unfinished": unfinished})
        offset = cursor_offset(cursor, query, revision)
    offset = cursor_offset(cursor, query, revision)
    return {"schema_version": "server_risk_day_evidence_v1", "query": query, "evidence_revision": revision, "summary": summary, "items": selected[offset:offset + limit], "total": len(selected), "quality": summary["count_quality"] if summary["coverage"] == "complete" else "partial" if selected else "unknown", "decisions": decisions, "unfinished_episodes": unfinished, "next_cursor": cursor_encode(query, offset + limit, revision) if offset + limit < len(selected) else None, "utc_start": start.isoformat(), "utc_end": end.isoformat(), "evaluation_mode": "historical", "as_of": end.isoformat()}


def daily_series(repository: Any, cluster: str, node: str, first: str, last: str, zone: str) -> dict[str, Any]:
    day_bounds(first, zone)
    day_bounds(last, zone)
    days = (date.fromisoformat(last) - date.fromisoformat(first)).days + 1
    if not 1 <= days <= 90:
        raise ServerRiskError("invalid_date_range", "日期范围必须为1至90日")
    if hasattr(repository, "snapshot"):
        repository = repository.snapshot(cluster, node)
    items = []
    for i in range(days):
        day = (date.fromisoformat(first) + timedelta(days=i)).isoformat()
        evidence = day_evidence(repository, {"cluster": cluster, "node_id": node, "date": day, "timezone": zone})
        items.append({"date": day, "timezone": zone, "utc_start": evidence["utc_start"], "utc_end": evidence["utc_end"], **evidence["summary"], "revision": evidence["evidence_revision"]})
    return {"schema_version": "server_risk_daily_series_v1", "query": {"cluster": cluster, "node_id": node, "from": first, "to": last, "timezone": zone}, "items": items, "revision": digest(items), "projection_complete": all(point["projection_complete"] for point in items)}
