from __future__ import annotations

from datetime import timedelta
from typing import Any

from logrisk.server_risk.replay import EvaluationContext
from logrisk.server_risk.contracts import ServerRiskError, decode, digest, json_text, utc

SEVERITY_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1, "unknown": 0}


class AssessmentService:
    def __init__(self, repository: Any, policies: Any, clock: Any) -> None:
        self.repository, self.policies, self.clock = repository, policies, clock

    def evaluate(self, cluster: str, node: str, *, at: str | None = None, rows: list[dict[str, Any]] | None = None, connection: Any = None, exclusive_end: bool = False, context: EvaluationContext | None = None) -> dict[str, Any]:
        context = context or EvaluationContext("current", at or self.clock())
        now = utc(context.as_of)
        rows = rows if rows is not None else self.repository.observations(cluster, node)
        groups: dict[str, list[Any]] = {}
        for row in rows:
            point_at_boundary = exclusive_end and row["interval_start"] == row["interval_end"] and utc(row["interval_end"]) == now
            if utc(row["interval_end"]) <= now and not point_at_boundary:
                groups.setdefault(row["subject_key"], []).append(row)
        subjects, episodes = [], []
        for subject, observations in sorted(groups.items()):
            observations.sort(key=lambda r: (utc(r["interval_start"]), r["payload"]["source_sequence"], r["observation_id"]))
            result, rounds = self._subject(subject, observations, now, connection, context.mode)
            subjects.append(result)
            episodes.extend(rounds)
        applicable = []
        for source_id in self.policies.sources:
            try:
                source = self.policies.source(source_id)
            except ServerRiskError:
                continue
            if source["cluster"] == cluster and node in source["node_ids"]:
                for obj in source.get("objects", []):
                    for risk_type in {s["risk_type"] for s in source["signals"].values()}:
                        applicable.append((obj["object_kind"], obj["object_id"], risk_type))
        observed = {(s["identity"]["object_kind"], s["identity"]["object_id"], s["risk_type"]) for s in subjects}
        reasons = []
        if not applicable:
            reasons.append("applicable_checks_unregistered")
        if set(applicable) - observed:
            reasons.append("required_observations_missing")
        legacy = [f for f in (self.repository.facts(cluster, node, connection=connection) if connection is not None else self.repository.facts(cluster, node)) if f["provenance"] != "registered_observation" and utc(f["first_seen"]) <= now]
        if legacy:
            reasons.append("legacy_unverified")
        historical = self.repository.historical_projection(cluster, node)
        if not historical["projection_complete"]:
            reasons.append("projection_pending")
        states = [s["status"] for s in subjects]
        state = "present" if "present" in states else "unknown" if reasons or "unknown" in states or not states else "recovering" if "recovering" in states else "clear"
        severity = max([s["severity"] for s in subjects if s["status"] != "clear"] + [f["severity"] for f in legacy] + ["unknown"], key=lambda s: SEVERITY_ORDER.get(s, 0))
        evidence_times = [s["evidence_at"] for s in subjects if s["evidence_at"]]
        valid = [s["valid_until"] for s in subjects if s["valid_until"]]
        revision = digest({"observations": [(r["observation_id"], r["payload_digest"]) for r in rows], "policy": self.policies.version, "legacy": legacy})
        return {"schema_version": "server_risk_node_assessment_v1", "subject": {"cluster": cluster, "node_id": node}, "status": state, "severity": severity, "assessed_at": now.isoformat(), "evidence_at": max(evidence_times) if evidence_times else None, "valid_until": min(valid) if valid and state != "unknown" else None, "last_known_state": next((s["last_known_state"] for s in reversed(subjects) if s["last_known_state"]), "present" if legacy else None), "reasons": sorted(set(reasons + [r for s in subjects for r in s["reasons"]])), "coverage": "complete" if state in ("clear", "recovering") and not reasons else "partial" if rows else "unknown", "revision": revision, "checks": subjects, "episodes": episodes, "projection_complete": historical["projection_complete"] and bool(rows or historical["frontiers"]) and all(s["projection_complete"] for s in subjects), "historical_frontiers": historical["frontiers"], "evaluation_mode": context.mode, "as_of": context.as_of, "policy_version": self.policies.version, "evidence_interval": {"start": min((r["interval_start"] for r in rows), default=None), "end": max((r["interval_end"] for r in rows), default=None)}, "missing_conditions": sorted(set(reasons + [r for s in subjects for r in s["reasons"]]))}

    def _frontier_complete(self, row: dict[str, Any], connection: Any = None) -> bool:
        key = "batch:" + digest([row["source_id"], row["idempotency_key"]])
        if connection is not None:
            saved = connection.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?", (key,)).fetchone()
        else:
            with self.repository.database.connect() as conn:
                saved = conn.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?", (key,)).fetchone()
        return bool(saved and saved["input_revision"] == row["batch_digest"] and saved["status"] == "complete")

    def _subject(self, subject: str, rows: list[Any], now: Any, connection: Any, evaluation_mode: str = "current") -> tuple[dict[str, Any], list[dict[str, Any]]]:
        last = rows[-1]
        payload = last["payload"]
        risk_type, identity = payload["risk_type"], payload["identity"]
        rule = self.policies.rules.get(risk_type)
        rounds = []
        current = None
        candidate = None
        last_positive = None
        positive_boundary = None
        rule = {key: value for key, value in (rule or {}).items() if key in ("mode", "hold_seconds", "recovery_signal", "positive_field", "positive_value", "requires_activity", "required_signals") and (isinstance(value, (str, int, float, bool)) or key == "required_signals" and isinstance(value, list) and all(isinstance(v, str) for v in value))}
        last_known = None
        state = "unknown"
        reasons = []
        validity = None
        evidence_at = None
        complete = True
        adverse_row = None
        evidence_mode = None
        for row in rows:
            p = row["payload"]
            when = utc(row["interval_end"])
            try:
                source = self.policies.source(row["source_id"])
            except ServerRiskError:
                complete = False
                reasons.append("source_unregistered")
                continue
            compatible = row["source_epoch"] == source["source_epoch"] and row["config_revision"] == source["config_revision"]
            processed = self._frontier_complete(row, connection)
            complete = complete and processed
            if p["kind"] == "adverse":
                if current is None or current["resolved_at"]:
                    current = {"episode_id": digest([subject, row["observation_id"]]), "subject_key": subject, "risk_type": risk_type, "identity": dict(p["identity"]), "severity": p["typed_fields"].get("severity", "high"), "first_adverse": row["interval_start"], "last_adverse": row["interval_end"], "candidate_start": None, "resolved_at": None, "previous_episode_id": rounds[-1]["episode_id"] if rounds else None, "lifecycle": "open"}
                    rounds.append(current)
                current["last_adverse"] = row["interval_end"]
                current["severity"] = max(current["severity"], p["typed_fields"].get("severity", "high"), key=lambda s: SEVERITY_ORDER.get(s, 0))
                adverse_row = row
                candidate = last_positive = None
                last_known = "present"
                state = "present" if compatible and (row["mode"] == "current" or evaluation_mode == "historical") and now - when <= timedelta(seconds=source["freshness_seconds"]) else "unknown"
                evidence_at = row["interval_end"]
                evidence_mode = row["mode"]
                validity = (when + timedelta(seconds=source["freshness_seconds"])).isoformat()
                if state == "unknown":
                    reasons.append("adverse_evidence_expired_or_historical")
                continue
            if p["kind"] == "recovery_candidate":
                state = "unknown"
                reasons.append("recovery_candidate_not_authoritative")
                continue
            if not rule or "hold_seconds" not in rule or rule.get("hold_seconds", -1) < 0:
                state = "unknown"
                reasons.append("recovery_policy_unregistered")
                continue
            if not compatible or not processed:
                state = "unknown"
                reasons.append("evidence_epoch_or_frontier_unverified")
                candidate = last_positive = None
                continue
            if rule.get("mode") != "hardware" and adverse_row and p["identity"].get("boot_id") != adverse_row["payload"]["identity"].get("boot_id"):
                state = "unknown"
                reasons.append("boot_incompatible")
                candidate = last_positive = None
                continue
            boundary = (row["source_id"], row["source_epoch"], row["config_revision"], p["identity"].get("boot_id"))
            if positive_boundary != boundary:
                candidate = last_positive = None
            positive_boundary = boundary
            positive = False
            if p["kind"] == "state_snapshot":
                positive = source["capability"] == "authoritative_state" and rule.get("mode") in ("state", "hardware") and p["signal_code"] == rule.get("recovery_signal") and p["typed_fields"].get(rule.get("positive_field")) == rule.get("positive_value")
                if rule.get("mode") == "hardware":
                    positive = positive and p["signal_code"] == "gpu_recovery_verified" and p["typed_fields"].get("verified") is True
                if not positive:
                    state = "unknown"
                    reasons.append("state_check_not_satisfied")
                    candidate = last_positive = None
                    continue
                if last_positive is None or when - last_positive > timedelta(seconds=source["max_gap_seconds"]):
                    candidate = when
                last_positive = when
            elif p["kind"] == "coverage":
                c = p["coverage"]
                needed = set(rule.get("required_signals", []))
                positive = source["capability"] == "complete_event_stream" and rule.get("mode") == "quiet" and bool(needed) and needed.issubset(c["signal_codes"]) and not c["gaps"] and c["dropped_count"] == 0 and c["parse_failure_count"] == 0 and utc(c["complete_through"]) == when
                if rule.get("requires_activity"):
                    positive = positive and p["typed_fields"].get("activity") is True and p["typed_fields"].get("success") is True
                if not positive:
                    state = "unknown"
                    reasons.append("coverage_incomplete_or_activity_missing")
                    candidate = last_positive = None
                    continue
                begin = max(utc(row["interval_start"]), utc(adverse_row["interval_end"])) if adverse_row else utc(row["interval_start"])
                if last_positive is None or begin > last_positive:
                    candidate = begin
                last_positive = when
            if positive:
                evidence_mode = row["mode"]
                evidence_at = row["interval_end"]
                evidence_mode = row["mode"]
                validity = (when + timedelta(seconds=source["freshness_seconds"])).isoformat()
                if current and not current["resolved_at"]:
                    current["candidate_start"] = candidate.isoformat()
                state = "clear" if (when - candidate).total_seconds() >= rule["hold_seconds"] else "recovering"
                last_known = state
                if state == "clear" and current and not current["resolved_at"]:
                    current["resolved_at"] = when.isoformat()
                    current["lifecycle"] = "resolved"
                reasons = []
        if evaluation_mode == "current" and evidence_mode == "historical":
            state = "unknown"
            reasons.append("historical_context_only")
        if validity and now >= utc(validity):
            state = "unknown"
            reasons.append("evidence_expired")
        if not complete:
            state = "unknown"
            reasons.append("projection_pending")
        if not evidence_at:
            state = "unknown"
            reasons.append("no_current_evidence")
        severity = max([r["payload"]["typed_fields"].get("severity", "high") for r in rows if r["payload"]["kind"] == "adverse"] + ["unknown"], key=lambda s: SEVERITY_ORDER.get(s, 0))
        return {"subject_key": subject, "identity": identity, "risk_type": risk_type, "status": state, "severity": severity, "evidence_at": evidence_at, "valid_until": validity if state != "unknown" else None, "last_known_state": last_known, "reasons": sorted(set(reasons)), "evidence_refs": [r["observation_id"] for r in rows], "projection_complete": complete, "evaluation_mode": evaluation_mode, "as_of": now.isoformat(), "policy_version": self.policies.version, "policy": rule, "candidate_start": candidate.isoformat() if candidate else None, "hold_seconds": rule.get("hold_seconds"), "observed_seconds": max(0, (last_positive - candidate).total_seconds()) if last_positive and candidate else 0, "remaining_seconds": max(0, rule.get("hold_seconds", 0) - (last_positive - candidate).total_seconds()) if last_positive and candidate else rule.get("hold_seconds"), "required_signals": rule.get("required_signals", [rule["recovery_signal"]] if rule.get("recovery_signal") else []), "missing_conditions": sorted(set(reasons)), "observed_interval": {"start": candidate.isoformat() if candidate else None, "end": last_positive.isoformat() if last_positive else None}, "evidence_origin": "synthetic" if self.policies.sources.get(last["source_id"], {}).get("environment") == "synthetic" else "registered", "source_boundary": {"source_id": last["source_id"], "source_epoch": last["source_epoch"], "config_revision": last["config_revision"], "boot_id": identity.get("boot_id")}, "episode_refs": [e["episode_id"] for e in rounds], "decision_ref": digest([subject, state, [r["observation_id"] for r in rows], self.policies.version, evaluation_mode, evidence_at])}, rounds

    def persist(self, cluster: str, node: str, conn: Any, now: str, *, context: EvaluationContext | None = None) -> None:
        rows = self.repository.observations(cluster, node, connection=conn)
        result = self.evaluate(cluster, node, at=now, rows=rows, connection=conn, context=context)
        for check in result["checks"]:
            active_ids = [e["episode_id"] for e in result["episodes"] if e["subject_key"] == check["subject_key"]]
            if active_ids:
                marks = ",".join("?" for _ in active_ids)
                conn.execute("UPDATE server_risk_episodes SET lifecycle='superseded' WHERE subject_key=? AND episode_id NOT IN (" + marks + ")", [check["subject_key"], *active_ids])
        for episode in result["episodes"]:
            conn.execute("INSERT INTO server_risk_episodes(episode_id,subject_key,risk_type,object_identity_json,first_adverse,last_adverse,candidate_start,resolved_at,previous_episode_id,lifecycle,latest_revision) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(episode_id) DO UPDATE SET last_adverse=excluded.last_adverse,candidate_start=excluded.candidate_start,resolved_at=excluded.resolved_at,lifecycle=excluded.lifecycle,latest_revision=excluded.latest_revision", (episode["episode_id"], episode["subject_key"], episode["risk_type"], json_text(episode["identity"]), episode["first_adverse"], episode["last_adverse"], episode["candidate_start"], episode["resolved_at"], episode["previous_episode_id"], episode["lifecycle"], result["revision"]))
        for check in result["checks"]:
            revision = digest([result["revision"], check])
            decision_id = check["decision_ref"]
            subject_rounds = [e for e in result["episodes"] if e["subject_key"] == check["subject_key"]]
            episode_id = subject_rounds[-1]["episode_id"] if subject_rounds else None
            conn.execute("INSERT INTO server_risk_decisions(decision_id,episode_id,subject_key,revision,effective_at,decided_at,state,valid_until,reason_codes_json,evidence_refs_json,policy_version) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", (decision_id, episode_id, check["subject_key"], revision, check["evidence_at"] or now, now, check["status"], check["valid_until"], json_text([*check["reasons"], "evaluation_mode:" + check["evaluation_mode"]]), json_text(check["evidence_refs"]), self.policies.version))
            previous = conn.execute("SELECT decision_id,effective_at,state,evidence_refs_json,reason_codes_json FROM server_risk_decisions WHERE subject_key=? AND decision_id<>? AND superseded_by IS NULL", (check["subject_key"], decision_id)).fetchall()
            subject_rows = [r for r in rows if r["subject_key"] == check["subject_key"]]
            current_refs = {r["observation_id"] for r in subject_rows}
            for saved in previous:
                historical_rows = [r for r in subject_rows if utc(r["interval_end"]) <= utc(saved["effective_at"])]
                saved_mode = "historical" if "evaluation_mode:historical" in decode(saved["reason_codes_json"]) else "current"
                historical_state = self._subject(check["subject_key"], historical_rows, utc(saved["effective_at"]), conn, saved_mode)[0]["status"] if historical_rows else "unknown"
                if historical_state != saved["state"] or not set(decode(saved["evidence_refs_json"])).issubset(current_refs):
                    conn.execute("UPDATE server_risk_decisions SET superseded_by=? WHERE decision_id=? AND superseded_by IS NULL", (decision_id, saved["decision_id"]))
