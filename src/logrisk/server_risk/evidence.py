from __future__ import annotations

from typing import Any

from logrisk.server_risk.contracts import CLAIMS, ServerRiskError, digest, integer, json_text, strict_object, typed_fields
from logrisk.server_risk.daily import day_evidence

CHECK_KEYS = ("subject_key", "risk_type", "status", "evidence_at", "valid_until", "last_known_state", "reasons", "evidence_refs", "evaluation_mode", "as_of", "policy_version", "policy", "candidate_start", "hold_seconds", "observed_seconds", "remaining_seconds", "required_signals", "missing_conditions", "observed_interval", "source_boundary", "episode_refs", "decision_ref", "evidence_origin")
ASSESSMENT_KEYS = ("status", "reasons", "evidence_at", "valid_until", "last_known_state", "evaluation_mode", "as_of", "policy_version", "evidence_interval", "missing_conditions")


def safe_explanation(assessment: dict[str, Any]) -> dict[str, Any]:
    return {**{key: assessment.get(key) for key in ASSESSMENT_KEYS}, "checks": [{key: check.get(key) for key in CHECK_KEYS} for check in assessment.get("checks", [])]}


FACT_KEYS = ("fact_ref", "risk_ref", "subject_key", "episode_ref", "cluster", "node_id", "risk_type", "severity", "object_kind", "object_id", "first_seen", "last_seen", "time_quality", "provenance", "template_ref", "template_text", "processing_status", "manual_closed_at", "count_quality")


def snapshot(repository: Any, query: dict[str, str], now: str) -> dict[str, Any]:
    page = day_evidence(repository, query, limit=100)
    items = list(page["items"])
    while page["next_cursor"]:
        page = day_evidence(repository, query, cursor=page["next_cursor"], limit=100)
        items.extend(page["items"])
    safe_facts = []
    for fact in items:
        safe = {key: fact.get(key) for key in FACT_KEYS if fact.get(key) is None or isinstance(fact.get(key), str)}
        safe["count"] = integer(fact["count"], 1, 1000000000)
        safe["typed_fields"] = typed_fields(fact.get("typed_fields"))
        safe["fact_kind"] = "occurrence"
        safe_facts.append(safe)
    unfinished = []
    for episode in page.get("unfinished_episodes", []):
        identity = episode["identity"]
        unfinished.append({"fact_ref": "episode:" + episode["episode_id"], "risk_ref": episode["subject_key"], "subject_key": episode["subject_key"], "episode_ref": episode["episode_id"], "fact_kind": "unfinished_episode", "cluster": query["cluster"], "node_id": query["node_id"], "object_kind": identity["object_kind"], "object_id": identity["object_id"], "risk_type": episode["risk_type"], "severity": episode["severity"], "first_seen": episode["first_adverse"], "last_seen": episode["last_adverse"], "candidate_start": episode["candidate_start"], "resolved_at": episode["resolved_at"], "lifecycle": episode["lifecycle"], "count": None, "provenance": "registered_observation", "template_text": "", "typed_fields": {}})
    decisions = [{key: decision[key] for key in ("decision_ref", "episode_ref", "subject_key", "state", "effective_at", "valid_until", "superseded_by", "evidence_refs", "evaluation_mode", "as_of")} for decision in page.get("decisions", [])]
    data = {"schema_version": "node_risk_report_evidence_v1", "query": dict(query), "generated_as_of": now, "evidence_revision": page["evidence_revision"], "summary": page["summary"], "facts": safe_facts, "unfinished_episodes": unfinished, "decisions": decisions, "current_assessment": {"status": "unknown", "reasons": ["source_coverage_unregistered"], "checks": []}, "omitted_fact_count": 0, "historical_assessment": safe_explanation(page["summary"].get("assessment_explanation", {})), "evaluation_mode": "historical", "as_of": page["as_of"]}
    if repository.assessment is not None:
        current = repository.assessment.evaluate(query["cluster"], query["node_id"])
        data["current_assessment"] = safe_explanation(current)
        data["current_assessment"]["unresolved_episode_refs"] = [episode["episode_id"] for episode in current["episodes"] if episode["resolved_at"] is None]
        for fact in safe_facts + unfinished:
            fact["hardware"] = repository.assessment.policies.rules.get(fact["risk_type"], {}).get("mode") == "hardware" or "gpu" in fact["risk_type"].lower()
    if len(json_text(data).encode()) > 1048576:
        raise ServerRiskError("report_snapshot_limit", "报告事实快照超过资源上限")
    if "assessment_explanation" in data["summary"]:
        data["summary"]["assessment_explanation"] = data["historical_assessment"]
    data["evidence_digest"] = digest(data)
    return data


def validate_narrative(value: Any, evidence: dict[str, Any]) -> list[dict[str, Any]]:
    def invalid() -> None:
        raise ServerRiskError("report_evidence_validation_failed", "报告叙述无法与固定事实匹配")

    try:
        strict_object(value, {"evidence_digest", "blocks"}, {"evidence_digest", "blocks"})
        if value["evidence_digest"] != evidence["evidence_digest"] or not isinstance(value["blocks"], list) or not 1 <= len(value["blocks"]) <= 32:
            invalid()
        all_facts = evidence["facts"] + evidence.get("unfinished_episodes", [])
        refs = {f["fact_ref"]: f for f in all_facts}
        risks = {f["risk_ref"] for f in all_facts}
        unresolved = {f["episode_ref"] for f in evidence.get("unfinished_episodes", [])}.intersection(evidence["current_assessment"].get("unresolved_episode_refs", []))
        current_checks = {c["subject_key"]: c for c in evidence["current_assessment"].get("checks", [])}

        def recovered(fact: dict[str, Any]) -> bool:
            return bool(fact.get("subject_key")) and any(d["state"] == "clear" and not d["superseded_by"] and d["subject_key"] == fact["subject_key"] and d.get("episode_ref") == fact.get("episode_ref") for d in evidence["decisions"])

        for block in value["blocks"]:
            strict_object(block, {"claim_code", "fact_refs", "risk_refs"}, {"claim_code", "fact_refs", "risk_refs"})
            code = block["claim_code"]
            if not isinstance(code, str) or code not in CLAIMS or not isinstance(block["fact_refs"], list) or not isinstance(block["risk_refs"], list) or len(block["fact_refs"]) > 100 or len(block["risk_refs"]) > 100:
                invalid()
            if any(not isinstance(ref, str) or ref not in refs for ref in block["fact_refs"]) or any(not isinstance(ref, str) or ref not in risks for ref in block["risk_refs"]):
                invalid()
            facts = [refs[ref] for ref in block["fact_refs"]]
            if block["risk_refs"] and set(block["risk_refs"]) != {f["risk_ref"] for f in facts}:
                invalid()
            checks = {
                "risk_summary": bool(facts),
                "repeated_occurrence": bool(facts) and all(isinstance(f["count"], int) and f["count"] > 1 for f in facts),
                "multiple_objects": len({(f["object_kind"], f["object_id"]) for f in facts}) > 1,
                "historical_only": bool(facts) and all(current_checks.get(f.get("subject_key"), {"status": "unknown"})["status"] == "unknown" for f in facts),
                "coverage_gap": evidence["summary"]["coverage"] != "complete",
                "no_recovery_evidence": bool(facts) and all(not recovered(f) for f in facts),
                "observed_recovery": bool(facts) and all(recovered(f) for f in facts),
                "hardware_unresolved": bool(facts) and all(f.get("hardware") and f.get("episode_ref") in unresolved for f in facts),
            }
            if not checks[code]:
                invalid()
        unique = {json_text(block): block for block in value["blocks"]}
        return list(unique.values())
    except (ServerRiskError, TypeError, KeyError):
        invalid()
