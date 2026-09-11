from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from logrisk.ai_harness.evaluator import evaluate_feature_output
from logrisk.approval_dedup import group_id_for_key
from logrisk.database import Database, utc_now
from logrisk.feature_extractor_ollama import _attach_source_facts
from logrisk.feature_jobs import _sanitize_feature_payload
from logrisk.feature_semantic_partition import partition_feature_by_semantics
from logrisk.sqlite_stores import SQLiteFeatureJobStore


def reclassify_pending_candidate(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    """Preview a replacement for untouched, generated unresolved evidence only.

    This function neither writes nor approves anything. Callers must check the
    persisted status/version and replace the candidate and its group membership
    in one transaction. An empty result means to retain the original candidate.
    """
    original = _sanitize_feature_payload(candidate)
    templates = original.get("source_templates") or []
    components = "、".join(sorted(original.get("components") or [])) or "相关组件"
    expected_summary = (
        f"{components} 的 {len(original.get('template_hashes') or [])} 个所选模板"
        "尚不能确定单一异常类别，需人工复核。"
    )
    if (
        original.get("status") != "pending"
        or original.get("feature_type") != "unresolved_template_evidence"
        or original.get("title") != "未完全解析的日志证据（待复核）"
        or original.get("summary") != expected_summary
        or original.get("tags") != ["日志证据", "待复核"]
        or original.get("reviewer_note") or original.get("review_scope")
        or original.get("rule_id") or original.get("resolved_rule_id")
        or (original.get("evaluator_result") or {}).get("passed") is not True
    ):
        return []
    entity = {
        "entity_id": (original.get("entity") or {}).get("id") or original.get("entity_id"),
        "entity_type": (original.get("entity") or {}).get("type") or original.get("entity_type"),
        **{key: original.get(key) for key in (
            "cluster", "window_start", "window_end", "risk_score", "risk_level", "affected_entities",
        )},
        "top_templates": templates,
    }
    children = partition_feature_by_semantics(entity, original)
    if len(children) == 1 and children[0].get("feature_type") == original["feature_type"]:
        return []
    evidence = {"entity": original.get("entity") or {}, "templates": templates,
                "affected_entities": original.get("affected_entities") or []}
    result = []
    for index, child in enumerate(children):
        attached = _attach_source_facts(
            entity, child, str(original.get("model") or ""), str(original.get("provider") or ""),
        )
        checks = [evaluate_feature_output(
            feature=attached, entity=entity, evidence=evidence, final_candidate=final,
        ) for final in (False, True)]
        if not all(check["passed"] for check in checks):
            return []
        value = {**copy.deepcopy(original), **attached}
        value["candidate_id"] = original["candidate_id"] if index == 0 else hashlib.sha256(
            f"reclassification:{original['candidate_id']}:{attached['candidate_id']}".encode("utf-8")
        ).hexdigest()[:20]
        value.pop("approval_group_id", None)
        value.pop("duplicate_of", None)
        value["resolution_type"] = "manual"
        value["evaluator_result"] = {
            "passed": True, "errors": [], "score": min(check["score"] for check in checks),
            "warnings": [warning for check in checks for warning in check["warnings"]],
            "rule_results": [rule for check in checks for rule in check["rule_results"]],
        }
        value["reclassification"] = {
            "source_candidate_id": original["candidate_id"],
            "previous_approval_key": original.get("approval_key"),
            "previous_approval_group_id": original.get("approval_group_id"),
            "previous_template_hashes": original.get("template_hashes") or [],
            "revision": "1.37.3",
        }
        result.append(value)
    return result


def repair_pending_candidates(database: Database, *, apply: bool = False) -> dict[str, Any]:
    """Offline, explicit repair; candidates, memberships and job snapshots commit together."""
    with database.transaction() as connection:
        if connection.execute(
            "SELECT 1 FROM feature_jobs WHERE status IN ('queued', 'running') LIMIT 1"
        ).fetchone():
            raise ValueError("存在运行中的特征任务，不能执行离线审批修复")
        query = (
            "SELECT c.*, j.job_json FROM feature_candidates c JOIN feature_jobs j ON j.job_id=c.job_id "
            "WHERE c.status='pending' AND NOT EXISTS "
            "(SELECT 1 FROM feature_candidate_feedback f WHERE f.candidate_id=c.candidate_id)"
        )
        if database.provider == "postgres":
            query += " FOR UPDATE OF c"
        planned = []
        for row in connection.execute(query).fetchall():
            candidate = SQLiteFeatureJobStore._candidate_from_row(row)
            children = reclassify_pending_candidate(candidate) if candidate else []
            if children:
                planned.append((row, children))
        report = {
            "applied": apply,
            "candidates_reclassified": len(planned),
            "candidates_added": sum(len(children) - 1 for _, children in planned),
            "candidate_ids": [row["candidate_id"] for row, _ in planned],
        }
        if not apply or not planned:
            return report
        now = utc_now()
        affected_groups: set[str] = set()
        expansions = {}
        job_ids = set()
        for row, children in planned:
            candidate_id = row["candidate_id"]
            expansions[candidate_id] = [child["candidate_id"] for child in children]
            job_ids.add(row["job_id"])
            if row["approval_group_id"]:
                affected_groups.add(row["approval_group_id"])
            connection.execute("DELETE FROM approval_group_candidates WHERE candidate_id=?", (candidate_id,))
            for child in children:
                existing_group = connection.execute(
                    "SELECT approval_group_id FROM approval_groups WHERE approval_key=?", (child["approval_key"],),
                ).fetchone()
                group_id = existing_group[0] if existing_group else group_id_for_key(child["approval_key"])
                if not existing_group:
                    connection.execute(
                        "INSERT INTO approval_groups(approval_group_id, approval_key, problem_code, feature_type, "
                        "title, summary, importance, status, group_json, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', '{}', ?, ?)",
                        (group_id, child["approval_key"], child["problem_code"], child["feature_type"],
                         child["title"], child["summary"], child["importance"], now, now),
                    )
                affected_groups.add(group_id)
                child.update({"approval_group_id": group_id, "updated_at": now})
                values = (json.dumps(child, ensure_ascii=False), child["approval_key"], child["problem_code"], group_id)
                if child["candidate_id"] == candidate_id:
                    changed = connection.execute(
                        "UPDATE feature_candidates SET candidate_json=?, approval_key=?, problem_code=?, "
                        "approval_group_id=?, updated_at=? WHERE candidate_id=? AND status='pending' AND updated_at=?",
                        (*values, now, candidate_id, row["updated_at"]),
                    )
                    if changed.rowcount != 1:
                        raise ValueError("候选审批状态已变更，修复已回滚")
                else:
                    connection.execute(
                        "INSERT INTO feature_candidates(candidate_json, approval_key, problem_code, approval_group_id, "
                        "candidate_id, job_id, entity_id, status, resolution_type, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 'manual', ?, ?)",
                        (*values, child["candidate_id"], row["job_id"], row["entity_id"], row["created_at"], now),
                    )
                connection.execute(
                    "INSERT INTO approval_group_candidates(approval_group_id, candidate_id, job_id, entity_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?)", (group_id, child["candidate_id"], row["job_id"], row["entity_id"], now),
                )
        for group_id in affected_groups:
            group = dict(connection.execute("SELECT * FROM approval_groups WHERE approval_group_id=?", (group_id,)).fetchone())
            members = [json.loads(row[0]) for row in connection.execute(
                "SELECT c.candidate_json FROM feature_candidates c JOIN approval_group_candidates g "
                "ON g.candidate_id=c.candidate_id WHERE g.approval_group_id=? ORDER BY c.candidate_id", (group_id,),
            ).fetchall()]
            snapshot = json.loads(group.pop("group_json"))
            snapshot.update(group)
            ids = [member["candidate_id"] for member in members]
            entities = sorted({"|".join(str(value or "") for value in (
                member.get("cluster"), (member.get("entity") or {}).get("type"),
                (member.get("entity") or {}).get("id"),
            )) for member in members})
            first = [str((member.get("time_range") or {}).get("first_seen") or member.get("window_start"))
                     for member in members if (member.get("time_range") or {}).get("first_seen") or member.get("window_start")]
            last = [str((member.get("time_range") or {}).get("last_seen") or member.get("window_end"))
                    for member in members if (member.get("time_range") or {}).get("last_seen") or member.get("window_end")]
            snapshot.update({
                "candidate_ids": ids, "entity_keys": entities, "candidate_count": len(ids),
                "affected_entity_count": len(entities), "updated_at": now,
                "occurrence_count": sum(int(member.get("occurrence_count") or 0) for member in members),
                "first_seen": min(first) if first else None, "last_seen": max(last) if last else None,
                "primary_candidate_id": snapshot.get("primary_candidate_id") if snapshot.get("primary_candidate_id") in ids else next(iter(ids), None),
            })
            if not members and snapshot["status"] == "pending":
                snapshot["status"] = "superseded"
            connection.execute(
                "UPDATE approval_groups SET group_json=?, status=?, candidate_count=?, affected_entity_count=?, "
                "occurrence_count=?, first_seen=?, last_seen=?, updated_at=? WHERE approval_group_id=?",
                (json.dumps(snapshot, ensure_ascii=False), snapshot["status"], len(ids), len(entities),
                 snapshot["occurrence_count"], snapshot["first_seen"], snapshot["last_seen"], now, group_id),
            )
        for job_id in job_ids:
            for row in connection.execute("SELECT entity_id, entity_json FROM feature_job_entities WHERE job_id=?", (job_id,)).fetchall():
                entity = json.loads(row["entity_json"])
                entity["feature_ids"] = [child for cid in entity.get("feature_ids", []) for child in expansions.get(cid, [cid])]
                connection.execute("UPDATE feature_job_entities SET entity_json=?, updated_at=? WHERE job_id=? AND entity_id=?",
                                   (json.dumps(entity, ensure_ascii=False), now, job_id, row["entity_id"]))
            row = connection.execute("SELECT job_id, job_json FROM feature_jobs WHERE job_id=?", (job_id,)).fetchone()
            job = SQLiteFeatureJobStore._load_job_row(connection, row)
            job.pop("events", None)
            connection.execute("UPDATE feature_jobs SET job_json=?, updated_at=? WHERE job_id=?",
                               (json.dumps(job, ensure_ascii=False), now, job_id))
            sequence = connection.execute("SELECT COALESCE(MAX(sequence), 0)+1 FROM feature_job_events WHERE job_id=?", (job_id,)).fetchone()[0]
            event = {"type": "pending_candidates_reclassified", "sequence": sequence, "job_id": job_id,
                     "timestamp": now, "revision": "1.37.3", "replacements": {
                         row["candidate_id"]: expansions[row["candidate_id"]] for row, _ in planned if row["job_id"] == job_id
                     }}
            connection.execute("INSERT INTO feature_job_events(job_id, sequence, event_type, event_json, created_at) VALUES (?, ?, ?, ?, ?)",
                               (job_id, sequence, event["type"], json.dumps(event), now))
        return report
