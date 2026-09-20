from __future__ import annotations

import base64
import json
import uuid
from typing import Any

from logrisk.approval_queue import build_review_groups, _entity_key, _representative_key


def update_projection(connection: Any, candidate: dict[str, Any]) -> None:
    candidate_id = str(candidate["candidate_id"])
    if candidate.get("status") != "pending":
        connection.execute("DELETE FROM approval_candidate_projection WHERE candidate_id=?", (candidate_id,))
        return
    group = build_review_groups([candidate])[0]
    representative = _representative_key(candidate)
    connection.execute(
        "INSERT INTO approval_candidate_projection(candidate_id,review_key,status,semantic_safe,ambiguous,importance_rank,risk_score,created_at,entity_key,occurrence_count,first_seen,last_seen,group_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET review_key=excluded.review_key,status=excluded.status,semantic_safe=excluded.semantic_safe,ambiguous=excluded.ambiguous,"
        "importance_rank=excluded.importance_rank,risk_score=excluded.risk_score,created_at=excluded.created_at,entity_key=excluded.entity_key,occurrence_count=excluded.occurrence_count,first_seen=excluded.first_seen,last_seen=excluded.last_seen,group_json=excluded.group_json",
        (candidate_id,group["review_key"],"pending",int(group["semantic_safe"]),int(group["ambiguity"]),
         -representative[0],-representative[1],representative[2],_entity_key(candidate),group["occurrence_count"],
         group["first_seen"],group["last_seen"],json.dumps(group,ensure_ascii=False)),
    )


def rebuild_projection(connection: Any) -> None:
    """Explicit bounded backfill; caller owns one atomic publication transaction."""
    connection.execute("DELETE FROM approval_candidate_projection")
    after = ""
    while True:
        rows = connection.execute(
            "SELECT candidate_id,job_id,candidate_json,status,created_at,updated_at FROM feature_candidates WHERE candidate_id>? ORDER BY candidate_id LIMIT 250", (after,)
        ).fetchall()
        if not rows:
            break
        for row in rows:
            candidate = json.loads(row["candidate_json"])
            candidate.update({key: row[key] for key in ("candidate_id","job_id","status","created_at","updated_at")})
            update_projection(connection,candidate)
        after = rows[-1]["candidate_id"]
    connection.execute("UPDATE approval_projection_state SET generation=? WHERE singleton=1", (uuid.uuid4().hex,))


def page_projection(connection: Any, *, status: str, page_size: int, cursor: str | None = None,
                    after: str | None = None, selected_key: str | None = None) -> dict[str, Any]:
    generation = connection.execute("SELECT generation FROM approval_projection_state WHERE singleton=1").fetchone()[0]
    if cursor:
        try:
            decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
            if decoded["v"] != 1 or decoded["generation"] != generation or decoded["status"] != status:
                raise ValueError("stale or mismatched cursor")
            after = decoded["after"]
            if not isinstance(after, str):
                raise ValueError("cursor key type")
        except (ValueError,KeyError,TypeError,UnicodeError) as exc:
            raise ValueError("invalid_cursor: 审批游标无效或投影版本已改变") from exc
    keys = connection.execute(
        "SELECT review_key FROM approval_candidate_projection WHERE status=? AND review_key>? GROUP BY review_key ORDER BY review_key LIMIT ?",
        (status,after or "",page_size+1),
    ).fetchall()
    def group(key: str) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT group_json FROM approval_candidate_projection WHERE status=? AND review_key=? ORDER BY importance_rank DESC,risk_score DESC,created_at,candidate_id LIMIT 1", (status,key)
        ).fetchone()
        if row is None:
            return None
        value = json.loads(row[0])
        counts = connection.execute(
            "SELECT COUNT(*) AS candidates,SUM(occurrence_count) AS occurrences,COUNT(DISTINCT entity_key) AS entities,MIN(first_seen) AS first_seen,MAX(last_seen) AS last_seen FROM approval_candidate_projection WHERE status=? AND review_key=?", (status,key)
        ).fetchone()
        ids = connection.execute("SELECT candidate_id FROM approval_candidate_projection WHERE status=? AND review_key=? ORDER BY candidate_id LIMIT 501", (status,key)).fetchall()
        value.update(candidate_count=counts["candidates"],occurrence_count=counts["occurrences"],affected_entity_count=counts["entities"],first_seen=counts["first_seen"],last_seen=counts["last_seen"],candidate_ids=[row[0] for row in ids[:500]],candidate_ids_partial=len(ids)>500)
        return value
    totals = connection.execute(
        "SELECT COUNT(*) AS candidates,COUNT(DISTINCT review_key) AS groups,COALESCE(SUM(semantic_safe),0) AS canonical,COALESCE(SUM(ambiguous),0) AS ambiguous FROM approval_candidate_projection WHERE status=?", (status,)
    ).fetchone()
    n,g,s,a = (int(totals[key]) for key in ("candidates","groups","canonical","ambiguous"))
    metrics = {"logrisk_approval_candidates_total":n,"logrisk_approval_review_groups":g,"logrisk_approval_canonical_candidates":s,"logrisk_approval_fallback_candidates":n-s,"logrisk_approval_ambiguous_candidates":a,"logrisk_approval_semantic_safe_candidates":s,"canonical_problem_code_coverage":s/n if n else 0.0,"fallback_problem_code_ratio":(n-s)/n if n else 0.0,"approval_compression_ratio":1-g/n if n else 0.0,"semantic_ambiguity_ratio":a/n if n else 0.0}
    next_key = keys[page_size-1][0] if len(keys)>page_size else None
    token = base64.urlsafe_b64encode(json.dumps({"v":1,"generation":generation,"status":status,"after":next_key}).encode()).decode() if next_key else None
    return {"schema_version":"feature_approval_queue_v1","status":status,"total_groups":g,"total_candidates":n,"metrics":metrics,"next_cursor":token,"next_review_key":next_key,"selected_group":group(selected_key) if selected_key else None,"items":[group(row[0]) for row in keys[:page_size]]}
