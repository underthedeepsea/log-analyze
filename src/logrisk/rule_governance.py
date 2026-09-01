from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from logrisk.database import SQLiteDatabase


RULE_STATUSES = {"active", "disabled", "under_review", "deprecated", "archived"}
FEEDBACK_OUTCOMES = {"confirmed", "false_positive"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RuleGovernanceError(RuntimeError):
    def __init__(self, message: str, *, code: str = "invalid_request", status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class RuleGovernanceRepository:
    def __init__(self, database: SQLiteDatabase) -> None:
        self.database = database

    @staticmethod
    def _rule(row: Any) -> dict[str, Any]:
        rule = json.loads(row["rule_json"])
        rule.update({
            "status": row["status"],
            "current_version": int(row["current_version"]),
            "next_review_at": row["next_review_at"],
            "schema_version": row["schema_version"],
            "approved_at": row["approved_at"],
            "created_at": rule.get("created_at") or row["approved_at"],
            "updated_at": row["updated_at"],
        })
        return rule

    def list_rules(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT rule_json, status, current_version, next_review_at, schema_version, approved_at, updated_at "
                "FROM approved_rules ORDER BY updated_at DESC, rule_id"
            ).fetchall()
        return [self._rule(row) for row in rows]

    def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT rule_json, status, current_version, next_review_at, schema_version, approved_at, updated_at "
                "FROM approved_rules WHERE rule_id=?",
                (rule_id,),
            ).fetchone()
        return self._rule(row) if row else None

    def commit_version(
        self,
        rule: dict[str, Any],
        *,
        expected_version: int,
        change_type: str,
        reason: str,
        operator: str,
        created_at: str,
        event: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT current_version FROM approved_rules WHERE rule_id=?",
                (rule["rule_id"],),
            ).fetchone()
            if row is None:
                raise RuleGovernanceError("规则不存在", code="rule_not_found", status_code=404)
            current_version = int(row["current_version"])
            if current_version != expected_version:
                raise RuleGovernanceError(
                    f"规则版本冲突，当前版本为 {current_version}",
                    code="version_conflict",
                    status_code=409,
                )
            version = current_version + 1
            snapshot = copy.deepcopy(rule)
            snapshot.update({
                "schema_version": "approved_rule_v2",
                "current_version": version,
                "updated_at": created_at,
            })
            connection.execute(
                "UPDATE approved_rules SET rule_json=?, status=?, current_version=?, next_review_at=?, "
                "problem_code=?, approval_key=?, schema_version='approved_rule_v2', updated_at=? WHERE rule_id=?",
                (
                    json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
                    snapshot["status"], version, snapshot.get("next_review_at"),
                    snapshot.get("problem_code"), snapshot.get("approval_key"), created_at,
                    snapshot["rule_id"],
                ),
            )
            connection.execute(
                "INSERT INTO rule_versions(rule_id, version, rule_json, change_type, change_reason, operator, "
                "created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, 'rule_version_v1')",
                (
                    snapshot["rule_id"], version,
                    json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
                    change_type, reason, operator, created_at,
                ),
            )
            connection.execute(
                "INSERT INTO rule_audit_events(event_id, rule_id, event_type, from_version, to_version, event_json, "
                "operator, created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'rule_audit_event_v1')",
                (
                    f"rule-event-{uuid.uuid4().hex}", snapshot["rule_id"], change_type,
                    current_version, version,
                    json.dumps(event or {"reason": reason}, ensure_ascii=False, separators=(",", ":")),
                    operator, created_at,
                ),
            )
        return snapshot

    def add_feedback(self, feedback: dict[str, Any]) -> None:
        with self.database.transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM approved_rules WHERE rule_id=?", (feedback["rule_id"],)
            ).fetchone() is None:
                raise RuleGovernanceError("规则不存在", code="rule_not_found", status_code=404)
            connection.execute(
                "INSERT INTO rule_feedback(feedback_id, rule_id, outcome, cluster, job_id, entity_id, note, operator, "
                "created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'rule_feedback_v1')",
                (
                    feedback["feedback_id"], feedback["rule_id"], feedback["outcome"],
                    feedback.get("cluster"), feedback.get("job_id"), feedback.get("entity_id"),
                    feedback.get("note"), feedback["operator"], feedback["created_at"],
                ),
            )
            connection.execute(
                "INSERT INTO rule_audit_events(event_id, rule_id, event_type, from_version, to_version, event_json, "
                "operator, created_at, schema_version) "
                "SELECT ?, rule_id, 'feedback_recorded', current_version, current_version, ?, ?, ?, 'rule_audit_event_v1' "
                "FROM approved_rules WHERE rule_id=?",
                (
                    f"rule-event-{uuid.uuid4().hex}",
                    json.dumps(feedback, ensure_ascii=False, separators=(",", ":")),
                    feedback["operator"], feedback["created_at"], feedback["rule_id"],
                ),
            )

    def versions(self, rule_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT version, rule_json, change_type, change_reason, operator, created_at, schema_version "
                "FROM rule_versions WHERE rule_id=? ORDER BY version DESC",
                (rule_id,),
            ).fetchall()
        return [
            {
                "version": int(row["version"]),
                "rule": json.loads(row["rule_json"]),
                "change_type": row["change_type"],
                "change_reason": row["change_reason"],
                "operator": row["operator"],
                "created_at": row["created_at"],
                "schema_version": row["schema_version"],
            }
            for row in rows
        ]

    def feedback(self, rule_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM rule_feedback WHERE rule_id=? ORDER BY created_at DESC, feedback_id DESC",
                (rule_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def audit_events(self, rule_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM rule_audit_events WHERE rule_id=? ORDER BY to_version DESC, created_at DESC, event_id DESC",
                (rule_id,),
            ).fetchall()
        return [dict(row) | {"event": json.loads(row["event_json"])} for row in rows]

    def activity(self, rule_id: str, *, since_7d: str, since_30d: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            reuse = connection.execute(
                "SELECT SUM(CASE WHEN reused_at>=? THEN 1 ELSE 0 END) AS hits_7d, COUNT(*) AS hits_30d, "
                "MAX(reused_at) AS last_hit_at, COUNT(DISTINCT NULLIF(cluster, '')) AS cluster_count "
                "FROM rule_reuse_events WHERE rule_id=? AND reused_at>=?",
                (since_7d, rule_id, since_30d),
            ).fetchone()
            feedback = connection.execute(
                "SELECT COUNT(*) AS total, SUM(CASE WHEN outcome='false_positive' THEN 1 ELSE 0 END) AS false_positives "
                "FROM rule_feedback WHERE rule_id=? AND created_at>=?",
                (rule_id, since_30d),
            ).fetchone()
        return {
            "hits_7d": int(reuse["hits_7d"] or 0),
            "hits_30d": int(reuse["hits_30d"] or 0),
            "last_hit_at": reuse["last_hit_at"],
            "cluster_count_30d": int(reuse["cluster_count"] or 0),
            "feedback_count_30d": int(feedback["total"] or 0),
            "false_positive_count_30d": int(feedback["false_positives"] or 0),
        }


class RuleGovernanceService:
    def __init__(self, repository: RuleGovernanceRepository, clock: Callable[[], str] = _now) -> None:
        self.repository = repository
        self.clock = clock

    @staticmethod
    def _required(value: Any, field: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise RuleGovernanceError(f"字段 {field} 不能为空")
        return text

    def _rule(self, rule_id: str) -> dict[str, Any]:
        rule = self.repository.get_rule(rule_id)
        if rule is None:
            raise RuleGovernanceError("规则不存在", code="rule_not_found", status_code=404)
        return rule

    def health(self, rule: dict[str, Any]) -> dict[str, Any]:
        now = datetime.fromisoformat(self.clock())
        activity = self.repository.activity(
            rule["rule_id"],
            since_7d=(now - timedelta(days=7)).isoformat(),
            since_30d=(now - timedelta(days=30)).isoformat(),
        )
        total = activity["feedback_count_30d"]
        false_positive_rate = activity["false_positive_count_30d"] / total if total else 0.0
        due = bool(rule.get("next_review_at") and str(rule["next_review_at"]) <= now.isoformat())
        score = 100 - round(false_positive_rate * 60)
        if activity["hits_30d"] == 0:
            score -= 20
        if due:
            score -= 20
        if rule.get("status") == "under_review":
            score -= 20
        score = max(0, min(100, score))
        reasons = []
        if activity["false_positive_count_30d"]:
            reasons.append("存在误报反馈")
        if activity["hits_30d"] == 0:
            reasons.append("30 天无命中")
        if due:
            reasons.append("已到复审时间")
        if rule.get("status") == "under_review":
            reasons.append("规则处于复审中")
        return {
            "schema_version": "rule_health_v1",
            **activity,
            "false_positive_rate_30d": round(false_positive_rate, 4),
            "next_review_at": rule.get("next_review_at"),
            "review_due": due,
            "score": score,
            "level": "healthy" if score >= 80 else ("attention" if score >= 60 else "risk"),
            "review_reasons": reasons,
        }

    def list_rules(self, *, status: str | None = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        if status and status not in RULE_STATUSES:
            raise RuleGovernanceError("规则状态无效")
        if page < 1 or page_size < 1 or page_size > 100:
            raise RuleGovernanceError("分页参数无效")
        items = [rule for rule in self.repository.list_rules() if not status or rule["status"] == status]
        enriched = [dict(rule, health=self.health(rule)) for rule in items]
        start = (page - 1) * page_size
        return {
            "schema_version": "rule_asset_list_v1",
            "items": enriched[start:start + page_size],
            "pagination": {"page": page, "page_size": page_size, "total": len(enriched)},
        }

    def get_rule(self, rule_id: str) -> dict[str, Any]:
        rule = self._rule(rule_id)
        return {
            "schema_version": "rule_asset_detail_v1",
            "rule": rule,
            "health": self.health(rule),
            "versions": self.repository.versions(rule_id),
            "feedback": self.repository.feedback(rule_id),
            "audit_events": self.repository.audit_events(rule_id),
        }

    def change_status(
        self,
        rule_id: str,
        status: str,
        expected_version: int,
        operator: str,
        reason: str,
    ) -> dict[str, Any]:
        if status not in RULE_STATUSES:
            raise RuleGovernanceError("规则状态无效")
        operator = self._required(operator, "operator")
        reason = self._required(reason, "reason")
        current = self._rule(rule_id)
        snapshot = dict(current, status=status)
        updated = self.repository.commit_version(
            snapshot,
            expected_version=int(expected_version),
            change_type="status_changed",
            reason=reason,
            operator=operator,
            created_at=self.clock(),
            event={"from_status": current["status"], "to_status": status, "reason": reason},
        )
        return self._write_result(updated)

    def record_feedback(
        self,
        rule_id: str,
        *,
        outcome: str,
        operator: str,
        note: str = "",
        cluster: str | None = None,
        job_id: str | None = None,
        entity_id: str | None = None,
    ) -> dict[str, Any]:
        if outcome not in FEEDBACK_OUTCOMES:
            raise RuleGovernanceError("反馈结论无效")
        operator = self._required(operator, "operator")
        self._rule(rule_id)
        feedback = {
            "schema_version": "rule_feedback_v1",
            "feedback_id": f"rule-feedback-{uuid.uuid4().hex}",
            "rule_id": rule_id,
            "outcome": outcome,
            "cluster": str(cluster).strip() if cluster else None,
            "job_id": str(job_id).strip() if job_id else None,
            "entity_id": str(entity_id).strip() if entity_id else None,
            "note": str(note or "").strip(),
            "operator": operator,
            "created_at": self.clock(),
        }
        self.repository.add_feedback(feedback)
        rule = self._rule(rule_id)
        return {
            "schema_version": "rule_write_result_v1",
            "request_id": f"request-{uuid.uuid4().hex}",
            "rule_id": rule_id,
            "version": rule["current_version"],
            "feedback": feedback,
            "health": self.health(rule),
        }

    def rollback(
        self,
        rule_id: str,
        *,
        target_version: int,
        expected_version: int,
        confirmed: bool,
        operator: str,
        reason: str,
    ) -> dict[str, Any]:
        if confirmed is not True:
            raise RuleGovernanceError("回滚必须显式确认", code="confirmation_required", status_code=400)
        operator = self._required(operator, "operator")
        reason = self._required(reason, "reason")
        current = self._rule(rule_id)
        target = next((item for item in self.repository.versions(rule_id) if item["version"] == int(target_version)), None)
        if target is None:
            raise RuleGovernanceError("目标规则版本不存在", code="version_not_found", status_code=404)
        snapshot = copy.deepcopy(target["rule"])
        snapshot.update({
            "rule_id": current["rule_id"],
            "signature": current["signature"],
            "approved_at": current["approved_at"],
            "created_at": current["created_at"],
            "reuse_count": current.get("reuse_count", 0),
            "last_reused_at": current.get("last_reused_at"),
            "schema_version": "approved_rule_v2",
        })
        updated = self.repository.commit_version(
            snapshot,
            expected_version=int(expected_version),
            change_type="rolled_back",
            reason=reason,
            operator=operator,
            created_at=self.clock(),
            event={"target_version": int(target_version), "reason": reason},
        )
        return self._write_result(updated)

    def review_queue(self) -> dict[str, Any]:
        items = []
        for rule in self.repository.list_rules():
            health = self.health(rule)
            if health["review_reasons"]:
                items.append(dict(rule, health=health, review_reasons=health["review_reasons"]))
        items.sort(key=lambda item: (item["health"]["score"], item["updated_at"]))
        return {"schema_version": "rule_review_queue_v1", "items": items, "total": len(items)}

    @staticmethod
    def _write_result(rule: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": "rule_write_result_v1",
            "request_id": f"request-{uuid.uuid4().hex}",
            "rule_id": rule["rule_id"],
            "version": rule["current_version"],
            "rule": rule,
        }
