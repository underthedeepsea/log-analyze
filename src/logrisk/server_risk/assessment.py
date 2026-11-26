from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from contextlib import nullcontext
from datetime import timedelta
from itertools import chain
from typing import Any

from logrisk.server_risk.replay import EvaluationContext
from logrisk.server_risk.repository import MAX_SCOPE_ROWS, PAGE_ROWS
from logrisk.server_risk.contracts import ServerRiskError, decode, digest, json_text, utc

MAX_CURRENT_EPISODES = 200
SEVERITY_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1, "unknown": 0}


def _assessment_namespace(cluster: str, node: str, subject: str) -> str:
    return "assessment:" + digest([cluster, node, subject])


class AssessmentService:
    def __init__(self, repository: Any, policies: Any, clock: Any) -> None:
        self.repository, self.policies, self.clock = repository, policies, clock

    def evaluate(self, cluster: str, node: str, *, at: str | None = None, rows: list[dict[str, Any]] | None = None, connection: Any = None, exclusive_end: bool = False, context: EvaluationContext | None = None) -> dict[str, Any]:
        context = context or EvaluationContext("current", at or self.clock())
        now = utc(context.as_of)
        paged = False
        if rows is None:
            try:
                rows = self.repository.observations(cluster, node, as_of=context.as_of)
            except ServerRiskError as exc:
                if exc.code != "query_resource_limit" or context.mode != "current" or exclusive_end:
                    raise
                rows = []
                paged = True
        subjects, episodes = [], []
        if paged:
            with nullcontext(connection) if connection is not None else self.repository.current_snapshot() as conn:
                for subject in self.repository.current_subjects(cluster, node, context.as_of, conn):
                    result, rounds = self.current_subject(cluster, node, subject, now, conn, paged=True)
                    if not result:
                        continue
                    subjects.append(result)
                    episodes.extend(rounds)
                    if len(episodes) > MAX_SCOPE_ROWS:
                        raise ServerRiskError("query_resource_limit", "当前节点轮次数量超过资源上限")
        else:
            groups: dict[str, list[Any]] = {}
            for row in rows:
                point_at_boundary = exclusive_end and row["interval_start"] == row["interval_end"] and utc(row["interval_end"]) == now
                if utc(row["interval_end"]) <= now and not point_at_boundary:
                    groups.setdefault(row["subject_key"], []).append(row)
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
        try:
            if paged:
                facts = self.repository.facts(
                    cluster, node, connection=connection, include_registered=False, recorded_as_of=context.as_of,
                )
            elif connection is not None:
                facts = self.repository.facts(cluster, node, connection=connection)
            else:
                facts = self.repository.facts(cluster, node)
            legacy = [fact for fact in facts if fact["provenance"] != "registered_observation" and utc(fact["first_seen"]) <= now]
            historical = self.repository.historical_projection(cluster, node)
        except ServerRiskError as exc:
            if exc.code != "query_resource_limit" or not paged:
                raise
            legacy, historical = [], {"projection_complete": False, "frontiers": []}
        if legacy:
            reasons.append("legacy_unverified")
        if not historical["projection_complete"]:
            reasons.append("projection_pending")
        states = [s["status"] for s in subjects]
        state = "present" if "present" in states else "unknown" if reasons or "unknown" in states or not states else "recovering" if "recovering" in states else "clear"
        severity = max([s["severity"] for s in subjects if s["status"] != "clear"] + [f["severity"] for f in legacy] + ["unknown"], key=lambda s: SEVERITY_ORDER.get(s, 0))
        evidence_times = [s["evidence_at"] for s in subjects if s["evidence_at"]]
        valid = [s["valid_until"] for s in subjects if s["valid_until"]]
        if paged:
            observation_revision = [check["evidence_digest"] for check in subjects]
            evidence_start = min((check["evidence_interval"]["start"] for check in subjects), key=utc, default=None)
            evidence_end = max((check["evidence_interval"]["end"] for check in subjects), key=utc, default=None)
        else:
            observation_revision = [(r["observation_id"], r["payload_digest"]) for r in rows]
            evidence_start = min((r["interval_start"] for r in rows), default=None)
            evidence_end = max((r["interval_end"] for r in rows), default=None)
        revision = digest({"observations": observation_revision, "policy": self.policies.version, "legacy": legacy})
        return {"schema_version": "server_risk_node_assessment_v1", "subject": {"cluster": cluster, "node_id": node}, "status": state, "severity": severity, "assessed_at": now.isoformat(), "evidence_at": max(evidence_times) if evidence_times else None, "valid_until": min(valid) if valid and state != "unknown" else None, "last_known_state": next((s["last_known_state"] for s in reversed(subjects) if s["last_known_state"]), "present" if legacy else None), "reasons": sorted(set(reasons + [r for s in subjects for r in s["reasons"]])), "coverage": "complete" if state in ("clear", "recovering") and not reasons else "partial" if rows or subjects else "unknown", "revision": revision, "checks": subjects, "episodes": episodes, "projection_complete": historical["projection_complete"] and bool(rows or subjects or historical["frontiers"]) and all(s["projection_complete"] for s in subjects), "historical_frontiers": historical["frontiers"], "evaluation_mode": context.mode, "as_of": context.as_of, "policy_version": self.policies.version, "evidence_interval": {"start": evidence_start, "end": evidence_end}, "missing_conditions": sorted(set(reasons + [r for s in subjects for r in s["reasons"]]))}

    def current_subject(
        self, cluster: str, node: str, subject: str, now: Any, conn: Any, *, paged: bool = False,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        try:
            if not paged:
                rows = self.repository.observations(cluster, node, connection=conn, as_of=now.isoformat(), subject=subject)
                rows.sort(key=lambda row: (utc(row["interval_start"]), row["payload"]["source_sequence"], row["observation_id"]))
                return self._subject(subject, rows, now, conn) if rows else ({}, [])
        except ServerRiskError as exc:
            if exc.code != "query_resource_limit":
                raise
        stream = self.repository.iter_subject_observations(cluster, node, subject, now.isoformat(), conn)
        return self._subject(subject, stream, now, conn, paged=True)

    def _frontier_complete(self, row: dict[str, Any], connection: Any = None) -> bool:
        key = "batch:" + digest([row["source_id"], row["idempotency_key"]])
        if connection is not None:
            saved = connection.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?", (key,)).fetchone()
        else:
            with self.repository.database.connect() as conn:
                saved = conn.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?", (key,)).fetchone()
        return bool(saved and saved["input_revision"] == row["batch_digest"] and saved["status"] == "complete")

    def _subject(
        self, subject: str, rows: Iterable[dict[str, Any]], now: Any, connection: Any,
        evaluation_mode: str = "current", *, paged: bool = False,
        on_episode: Callable[[dict[str, Any]], None] | None = None,
        prior: dict[str, Any] | None = None, checkpoint: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        iterator = iter(rows)
        next_row = next(iterator, None)
        seed = prior or {}
        first = seed.get("first") or next_row
        if first is None:
            return {}, []
        last = seed.get("last") or first
        risk_type = first["payload"]["risk_type"]
        rule = self.policies.rules.get(risk_type)
        rounds = seed.get("rounds", []).copy()
        current = seed.get("current")
        candidate = utc(seed["candidate"]) if seed.get("candidate") else None
        last_positive = utc(seed["last_positive"]) if seed.get("last_positive") else None
        positive_boundary = tuple(seed["positive_boundary"]) if seed.get("positive_boundary") else None
        rule = {key: value for key, value in (rule or {}).items() if key in ("mode", "hold_seconds", "recovery_signal", "positive_field", "positive_value", "requires_activity", "required_signals") and (isinstance(value, (str, int, float, bool)) or key == "required_signals" and isinstance(value, list) and all(isinstance(v, str) for v in value))}
        last_known = seed.get("last_known")
        state = seed.get("state", "unknown")
        reasons = set(seed.get("reasons", ()))
        validity = seed.get("validity")
        evidence_at = seed.get("evidence_at")
        complete = seed.get("complete", True)
        adverse_row = seed.get("adverse_row")
        evidence_mode = seed.get("evidence_mode")
        adverse_expiry = utc(seed["adverse_expiry"]) if seed.get("adverse_expiry") else None
        proven_frontiers: dict[tuple[str, str, str], bool] = {}
        evidence_refs = seed.get("evidence_refs", []).copy()
        evidence_digest = bytes.fromhex(seed.get("evidence_digest", "0" * 64)) if paged else None
        evidence_count = seed.get("evidence_count", 0)
        episode_count = seed.get("episode_count", 0)
        evidence_ref = seed.get("evidence_ref")
        latest_end = seed.get("latest_end", first["interval_end"])
        earliest_change = None
        severity = seed.get("severity")

        def finish_episode(episode: dict[str, Any]) -> None:
            nonlocal episode_count
            episode_count += 1
            if on_episode is not None:
                on_episode(episode)
            rounds.append(episode)
            if paged and len(rounds) > MAX_CURRENT_EPISODES:
                del rounds[0]

        for row in chain((next_row,) if next_row is not None else (), iterator):
            last, p = row, row["payload"]
            when = utc(row["interval_end"])
            observation_ref = row["observation_id"]
            if evidence_digest is not None:
                evidence_digest = hashlib.sha256(
                    evidence_digest + json_text([observation_ref, row["payload_digest"]]).encode()
                ).digest()
                if earliest_change is None or when < earliest_change:
                    earliest_change = when
                evidence_count += 1
                if evidence_count <= MAX_SCOPE_ROWS:
                    evidence_refs.append(observation_ref)
                elif evidence_refs:
                    evidence_refs.clear()
                if when > utc(latest_end):
                    latest_end = row["interval_end"]
            else:
                evidence_refs.append(observation_ref)
            if p["kind"] == "adverse":
                candidate_severity = p["typed_fields"].get("severity", "high")
                if severity is None or SEVERITY_ORDER.get(candidate_severity, 0) > SEVERITY_ORDER.get(severity, 0):
                    severity = candidate_severity
            try:
                source = self.policies.source(row["source_id"])
            except ServerRiskError:
                complete = False
                reasons.add("source_unregistered")
                continue
            compatible = row["source_epoch"] == source["source_epoch"] and row["config_revision"] == source["config_revision"]
            frontier = (row["source_id"], row["idempotency_key"], row["batch_digest"])
            if frontier not in proven_frontiers:
                if len(proven_frontiers) >= PAGE_ROWS:
                    proven_frontiers.clear()
                proven_frontiers[frontier] = self._frontier_complete(row, connection)
            processed = proven_frontiers[frontier]
            complete = complete and processed
            if p["kind"] == "adverse":
                if current is None or current["resolved_at"]:
                    previous_episode = current["episode_id"] if current is not None else None
                    if current is not None:
                        finish_episode(current)
                    current = {"episode_id": digest([subject, observation_ref]), "subject_key": subject, "risk_type": risk_type, "identity": dict(p["identity"]), "severity": p["typed_fields"].get("severity", "high"), "first_adverse": row["interval_start"], "last_adverse": row["interval_end"], "candidate_start": None, "resolved_at": None, "previous_episode_id": previous_episode, "lifecycle": "open"}
                current["last_adverse"] = row["interval_end"]
                current["severity"] = max(current["severity"], p["typed_fields"].get("severity", "high"), key=lambda s: SEVERITY_ORDER.get(s, 0))
                adverse_row = row
                candidate = last_positive = None
                last_known = "present"
                state = "present" if compatible and (row["mode"] == "current" or evaluation_mode == "historical") else "unknown"
                evidence_at = row["interval_end"]
                evidence_ref = observation_ref
                evidence_mode = row["mode"]
                validity = (when + timedelta(seconds=source["freshness_seconds"])).isoformat()
                if state == "unknown":
                    reasons.add("adverse_evidence_expired_or_historical")
                else:
                    deadline = utc(validity)
                    adverse_expiry = min(deadline, adverse_expiry) if adverse_expiry else deadline
                continue
            if p["kind"] == "recovery_candidate":
                state = "unknown"
                reasons.add("recovery_candidate_not_authoritative")
                continue
            if not rule or "hold_seconds" not in rule or rule.get("hold_seconds", -1) < 0:
                state = "unknown"
                reasons.add("recovery_policy_unregistered")
                continue
            if not compatible or not processed:
                state = "unknown"
                reasons.add("evidence_epoch_or_frontier_unverified")
                candidate = last_positive = None
                continue
            if rule.get("mode") != "hardware" and adverse_row and p["identity"].get("boot_id") != adverse_row["payload"]["identity"].get("boot_id"):
                state = "unknown"
                reasons.add("boot_incompatible")
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
                    reasons.add("state_check_not_satisfied")
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
                    reasons.add("coverage_incomplete_or_activity_missing")
                    candidate = last_positive = None
                    continue
                begin = max(utc(row["interval_start"]), utc(adverse_row["interval_end"])) if adverse_row else utc(row["interval_start"])
                if last_positive is None or begin > last_positive:
                    candidate = begin
                last_positive = when
            if positive:
                evidence_at = row["interval_end"]
                evidence_ref = observation_ref
                evidence_mode = row["mode"]
                validity = (when + timedelta(seconds=source["freshness_seconds"])).isoformat()
                if current and not current["resolved_at"]:
                    current["candidate_start"] = candidate.isoformat()
                state = "clear" if (when - candidate).total_seconds() >= rule["hold_seconds"] else "recovering"
                last_known = state
                if state == "clear" and current and not current["resolved_at"]:
                    current["resolved_at"] = when.isoformat()
                    current["lifecycle"] = "resolved"
                reasons.clear()
                adverse_expiry = None
        if checkpoint is not None and evidence_digest is not None:
            checkpoint.update(
                version=1, first=first, last=last, rounds=rounds.copy(), current=current,
                candidate=candidate.isoformat() if candidate else None,
                last_positive=last_positive.isoformat() if last_positive else None,
                positive_boundary=positive_boundary, last_known=last_known, state=state, reasons=sorted(reasons),
                validity=validity, evidence_at=evidence_at, complete=complete, adverse_row=adverse_row,
                adverse_expiry=adverse_expiry.isoformat() if adverse_expiry else None,
                evidence_mode=evidence_mode, evidence_refs=evidence_refs, evidence_digest=evidence_digest.hex(),
                evidence_count=evidence_count, episode_count=episode_count, evidence_ref=evidence_ref,
                latest_end=latest_end, severity=severity,
                earliest_change=earliest_change.isoformat() if earliest_change else None,
            )
        if adverse_expiry and now > adverse_expiry:
            reasons.add("adverse_evidence_expired_or_historical")
        if evaluation_mode == "current" and evidence_mode == "historical":
            state = "unknown"
            reasons.add("historical_context_only")
        if validity and now >= utc(validity):
            state = "unknown"
            reasons.add("evidence_expired")
        if not complete:
            state = "unknown"
            reasons.add("projection_pending")
        if not evidence_at:
            state = "unknown"
            reasons.add("no_current_evidence")
        if current is not None:
            finish_episode(current)
        identity = last["payload"]["identity"]
        decision_evidence = evidence_refs if not paged or evidence_count <= MAX_SCOPE_ROWS else evidence_digest.hex()
        if evidence_digest is not None:
            evidence_refs = list(dict.fromkeys(
                ref for ref in (first["observation_id"], evidence_ref, last["observation_id"]) if ref is not None
            ))
        revision = evidence_digest.hex() if evidence_digest is not None else evidence_refs
        result = {"subject_key": subject, "identity": identity, "risk_type": risk_type, "status": state, "severity": severity if severity is not None else "unknown", "evidence_at": evidence_at, "valid_until": validity if state != "unknown" else None, "last_known_state": last_known, "reasons": sorted(set(reasons)), "evidence_refs": evidence_refs, "projection_complete": complete, "evaluation_mode": evaluation_mode, "as_of": now.isoformat(), "policy_version": self.policies.version, "policy": rule, "candidate_start": candidate.isoformat() if candidate else None, "hold_seconds": rule.get("hold_seconds"), "observed_seconds": max(0, (last_positive - candidate).total_seconds()) if last_positive and candidate else 0, "remaining_seconds": max(0, rule.get("hold_seconds", 0) - (last_positive - candidate).total_seconds()) if last_positive and candidate else rule.get("hold_seconds"), "required_signals": rule.get("required_signals", [rule["recovery_signal"]] if rule.get("recovery_signal") else []), "missing_conditions": sorted(set(reasons)), "observed_interval": {"start": candidate.isoformat() if candidate else None, "end": last_positive.isoformat() if last_positive else None}, "evidence_origin": "synthetic" if self.policies.sources.get(last["source_id"], {}).get("environment") == "synthetic" else "registered", "source_boundary": {"source_id": last["source_id"], "source_epoch": last["source_epoch"], "config_revision": last["config_revision"], "boot_id": identity.get("boot_id")}, "episode_refs": [e["episode_id"] for e in rounds], "decision_ref": digest([subject, state, decision_evidence, self.policies.version, evaluation_mode, evidence_at])}
        if evidence_digest is not None:
            result.update(
                evidence_count=evidence_count, evidence_digest=revision, evidence_refs_bounded=True,
                evidence_interval={"start": first["interval_start"], "end": latest_end},
            )
            if episode_count > len(rounds):
                result.update(episode_count=episode_count, episode_refs_bounded=True)
        return result, rounds

    def persist(
        self, cluster: str, node: str, conn: Any, now: str, *,
        context: EvaluationContext | None = None, subject: str | None = None,
        input_revision: str | None = None, affected_from: str | None = None,
    ) -> bool:
        context = context or EvaluationContext("current", now)
        if subject is None:
            # Explicit reconstruction retains its existing bounded node scope.
            rows = self.repository.observations(cluster, node, connection=conn, as_of=context.as_of)
            result = self.evaluate(cluster, node, at=now, rows=rows, connection=conn, context=context)
        else:
            self.repository.lock_current_scopes({(cluster, node, subject)}, conn)
            namespace = _assessment_namespace(cluster, node, subject)
            conn.execute(
                "INSERT INTO server_risk_projection_state(input_namespace,input_revision,status,updated_at) "
                "VALUES (?,?,'pending',?) ON CONFLICT(input_namespace) DO UPDATE SET input_revision=excluded.input_revision, "
                "status='pending',updated_at=excluded.updated_at",
                (namespace, input_revision, now),
            )
            policy_digest = digest([self.policies.version, self.policies.sources, self.policies.rules])
            checkpoint = self.repository.valid_current_checkpoint(
                cluster, node, subject, context.as_of, policy_digest, conn,
            ) if context.mode == "current" else None
            if checkpoint is not None:
                return self._persist_paged_subject(
                    cluster, node, subject, conn, now, context, input_revision, affected_from,
                    prior=checkpoint, policy_digest=policy_digest,
                )
            try:
                rows = self.repository.observations(cluster, node, connection=conn, as_of=context.as_of, subject=subject)
            except ServerRiskError as exc:
                if exc.code != "query_resource_limit":
                    raise
                return self._persist_paged_subject(
                    cluster, node, subject, conn, now, context, input_revision, affected_from,
                    policy_digest=policy_digest,
                )
            conn.execute(
                "DELETE FROM server_risk_current_checkpoints WHERE cluster=? AND node_id=? AND subject_key=?",
                (cluster, node, subject),
            )
            if not rows:
                return False
            rows.sort(key=lambda row: (utc(row["interval_start"]), row["payload"]["source_sequence"], row["observation_id"]))
            check, rounds = self._subject(subject, rows, utc(context.as_of), conn, context.mode)
            revision = digest({"observations": [(r["observation_id"], r["payload_digest"]) for r in rows], "policy": self.policies.version})
            result = {"checks": [check], "episodes": rounds, "revision": revision}
        affected = ""
        params: tuple[str, ...] = ()
        if affected_from is not None:
            value = utc(affected_from)
            if self.repository.database.provider == "postgres":
                affected = " AND effective_at>=? AND effective_at::timestamptz>=?::timestamptz"
            else:
                affected = (
                    " AND effective_at>=? AND (CASE WHEN length(effective_at)=25 THEN "
                    "substr(effective_at,1,19)||'.000000+00:00' ELSE effective_at END)>=?"
                )
            params = value.replace(microsecond=0).isoformat(), value.isoformat(timespec="microseconds")
        pending = []
        for check in result["checks"]:
            previous = conn.execute(
                "SELECT decision_id,effective_at,state,evidence_refs_json,reason_codes_json "
                "FROM server_risk_decisions WHERE cluster=? AND node_id=? AND subject_key=? "
                "AND decision_id<>? AND superseded_by IS NULL"
                + affected + " LIMIT ?",
                (cluster, node, check["subject_key"], check["decision_ref"], *params, MAX_SCOPE_ROWS + 1),
            ).fetchall()
            if len(previous) > MAX_SCOPE_ROWS:
                if subject is not None:
                    return self._persist_paged_subject(
                        cluster, node, subject, conn, now, context, input_revision, affected_from,
                    )
                raise ServerRiskError("query_resource_limit", "当前节点决定超过资源上限")
            pending.append(previous)
        for check in result["checks"]:
            active_ids = [e["episode_id"] for e in result["episodes"] if e["subject_key"] == check["subject_key"]]
            if active_ids:
                marks = ",".join("?" for _ in active_ids)
                conn.execute(
                    "UPDATE server_risk_episodes SET lifecycle='superseded' WHERE cluster=? AND node_id=? "
                    "AND subject_key=? AND episode_id NOT IN (" + marks + ")",
                    (cluster, node, check["subject_key"], *active_ids),
                )
        for episode in result["episodes"]:
            self._store_episode(conn, cluster, node, episode, result["revision"])
        for check, previous in zip(result["checks"], pending):
            revision = digest([result["revision"], check])
            decision_id = check["decision_ref"]
            subject_rounds = [e for e in result["episodes"] if e["subject_key"] == check["subject_key"]]
            episode_id = subject_rounds[-1]["episode_id"] if subject_rounds else None
            self._store_decision(conn, cluster, node, check, episode_id, revision, now)
            subject_rows = [r for r in rows if r["subject_key"] == check["subject_key"]]
            current_refs = {r["observation_id"] for r in subject_rows}
            for saved in previous:
                historical_rows = [r for r in subject_rows if utc(r["interval_end"]) <= utc(saved["effective_at"])]
                saved_mode = "historical" if "evaluation_mode:historical" in decode(saved["reason_codes_json"]) else "current"
                historical_state = self._subject(check["subject_key"], historical_rows, utc(saved["effective_at"]), conn, saved_mode)[0]["status"] if historical_rows else "unknown"
                if historical_state != saved["state"] or (not check.get("evidence_refs_bounded") and not set(decode(saved["evidence_refs_json"])).issubset(current_refs)):
                    conn.execute("UPDATE server_risk_decisions SET superseded_by=? WHERE decision_id=? AND superseded_by IS NULL", (decision_id, saved["decision_id"]))

        if subject is not None:
            conn.execute(
                "UPDATE server_risk_projection_state SET status='complete',last_fact_ref=?,evaluated_through=? "
                "WHERE input_namespace=? AND input_revision=?",
                (rows[-1]["observation_id"], context.as_of, namespace, input_revision),
            )
        return True


    @staticmethod
    def _store_episode(conn: Any, cluster: str, node: str, episode: dict[str, Any], revision: str | None) -> None:
        conn.execute(
            "INSERT INTO server_risk_episodes(episode_id,subject_key,cluster,node_id,risk_type,object_identity_json,"
            "first_adverse,last_adverse,candidate_start,resolved_at,previous_episode_id,lifecycle,latest_revision) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(episode_id) DO UPDATE SET "
            "last_adverse=excluded.last_adverse,candidate_start=excluded.candidate_start,"
            "resolved_at=excluded.resolved_at,previous_episode_id=excluded.previous_episode_id,"
            "lifecycle=excluded.lifecycle,latest_revision=excluded.latest_revision "
            "WHERE server_risk_episodes.cluster=excluded.cluster AND server_risk_episodes.node_id=excluded.node_id "
            "AND server_risk_episodes.subject_key=excluded.subject_key",
            (episode["episode_id"], episode["subject_key"], cluster, node, episode["risk_type"], json_text(episode["identity"]),
             episode["first_adverse"], episode["last_adverse"], episode["candidate_start"], episode["resolved_at"],
             episode["previous_episode_id"], episode["lifecycle"], revision),
        )

    def _store_decision(
        self, conn: Any, cluster: str, node: str, check: dict[str, Any], episode_id: str | None,
        revision: str, now: str,
    ) -> None:
        conn.execute(
            "INSERT INTO server_risk_decisions(decision_id,episode_id,subject_key,cluster,node_id,revision,effective_at,"
            "decided_at,state,valid_until,reason_codes_json,evidence_refs_json,policy_version) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
            (check["decision_ref"], episode_id, check["subject_key"], cluster, node, revision,
             check["evidence_at"] or now, now, check["status"], check["valid_until"],
             json_text([*check["reasons"], "evaluation_mode:" + check["evaluation_mode"]]),
             json_text(check["evidence_refs"]), self.policies.version),
        )

    def _persist_paged_subject(
        self, cluster: str, node: str, subject: str, conn: Any, now: str,
        context: EvaluationContext, input_revision: str | None, affected_from: str | None, *,
        prior: dict[str, Any] | None = None, policy_digest: str | None = None,
    ) -> bool:
        # The scoped assessment owner serializes this write. A late or revised
        # observation invalidates the prefix; a chronological append reads its suffix.
        if prior is None:
            conn.execute(
                "UPDATE server_risk_episodes SET lifecycle='superseded' WHERE cluster=? AND node_id=? AND subject_key=?",
                (cluster, node, subject),
            )

        def save_episode(episode: dict[str, Any]) -> None:
            self._store_episode(conn, cluster, node, episode, input_revision)

        after = ()
        if prior is not None:
            last = prior["last"]
            after = last["start_key"], last["sequence_key"], last["observation_id"]
        stream = self.repository.iter_subject_observations(cluster, node, subject, context.as_of, conn, after=after)
        checkpoint: dict[str, Any] = {}
        check, episodes = self._subject(
            subject, stream, utc(context.as_of), conn, context.mode, paged=True, on_episode=save_episode,
            prior=prior, checkpoint=checkpoint,
        )
        if not check:
            return False
        input_digest = check["evidence_digest"]
        if prior is None:
            conn.execute(
                "UPDATE server_risk_episodes SET latest_revision=? WHERE cluster=? AND node_id=? "
                "AND subject_key=? AND lifecycle<>'superseded'",
                (input_digest, cluster, node, subject),
            )
        decision_id = check["decision_ref"]
        self._store_decision(
            conn, cluster, node, check, episodes[-1]["episode_id"] if episodes else None,
            digest([input_digest, check]), now,
        )
        if prior is not None and affected_from is None:
            affected_from = checkpoint.get("earliest_change") or context.as_of
        if affected_from is not None and prior is not None and checkpoint.get("earliest_change"):
            affected_from = min(affected_from, checkpoint["earliest_change"], key=utc)
        earliest = utc(affected_from) if affected_from else None
        last_key: tuple[str, str] = ("", "")
        if earliest is not None:
            last_key = (earliest.replace(microsecond=0).isoformat(), "")
        prior_states: dict[tuple[str, str], str] = {}
        while True:
            previous = conn.execute(
                "SELECT decision_id,effective_at,state,evidence_refs_json,reason_codes_json "
                "FROM server_risk_decisions WHERE cluster=? AND node_id=? AND subject_key=? "
                "AND decision_id<>? AND superseded_by IS NULL "
                "AND (effective_at,decision_id)>(?,?) ORDER BY effective_at,decision_id LIMIT ?",
                (cluster, node, subject, decision_id, *last_key, PAGE_ROWS),
            ).fetchall()
            for saved in previous:
                last_key = saved["effective_at"], saved["decision_id"]
                when = utc(saved["effective_at"])
                if earliest is not None and when < earliest:
                    continue
                mode = "historical" if "evaluation_mode:historical" in decode(saved["reason_codes_json"]) else "current"
                history_key = saved["effective_at"], mode
                if history_key not in prior_states:
                    if len(prior_states) >= PAGE_ROWS:
                        prior_states.clear()
                    replay = self.repository.iter_subject_observations(cluster, node, subject, when.isoformat(), conn)
                    state, _ = self._subject(subject, replay, when, conn, mode, paged=True)
                    prior_states[history_key] = state.get("status", "unknown")
                if prior_states[history_key] != saved["state"] or not self.repository.active_observation_refs(
                    subject, decode(saved["evidence_refs_json"]), conn,
                ):
                    conn.execute(
                        "UPDATE server_risk_decisions SET superseded_by=? WHERE decision_id=? AND superseded_by IS NULL",
                        (decision_id, saved["decision_id"]),
                    )
            if len(previous) < PAGE_ROWS:
                break
        if check["projection_complete"] and context.mode == "current" and policy_digest:
            last = checkpoint["last"]
            conn.execute(
                "INSERT INTO server_risk_current_checkpoints(cluster,node_id,subject_key,policy_digest,as_of,start_key,"
                "sequence_key,observation_id,reducer_json,invalidated) VALUES (?,?,?,?,?,?,?,?,?,0) "
                "ON CONFLICT(cluster,node_id,subject_key) DO UPDATE SET policy_digest=excluded.policy_digest,"
                "as_of=excluded.as_of,start_key=excluded.start_key,sequence_key=excluded.sequence_key,"
                "observation_id=excluded.observation_id,reducer_json=excluded.reducer_json,invalidated=0",
                (cluster, node, subject, policy_digest, context.as_of, last["start_key"],
                 last["sequence_key"], last["observation_id"], json_text(checkpoint)),
            )
        else:
            conn.execute(
                "DELETE FROM server_risk_current_checkpoints WHERE cluster=? AND node_id=? AND subject_key=?",
                (cluster, node, subject),
            )
        conn.execute(
            "UPDATE server_risk_projection_state SET status=?,last_fact_ref=?,evaluated_through=? "
            "WHERE input_namespace=? AND input_revision=?",
            ("complete" if check["projection_complete"] else "pending", check["evidence_refs"][-1], context.as_of,
             _assessment_namespace(cluster, node, subject), input_revision),
        )
        return bool(check["projection_complete"])
