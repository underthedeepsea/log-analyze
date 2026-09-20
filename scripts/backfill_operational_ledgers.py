#!/usr/bin/env python3
"""Conservative, resumable import of legacy SQLite metadata into operational ledgers.

The command is intentionally explicit: without ``--apply`` it only reads the
legacy tables and reports what is provable.  It never imports cache hits and
never treats an old trace id as proof of a physical attempt by itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from logrisk.database import Database, create_database
from logrisk.operational_ledgers import OperationalLedgerRepository


MAX_INT64 = 2**63 - 1
DEFAULT_PAGE_SIZE = 200
_COUNT_KEYS = (
    "input_count",
    "record_count",
    "total_records",
    "accepted_records",
    "processed_records",
)
_STATUS_MAP = {
    "queued": "pending",
    "pending": "pending",
    "running": "running",
    "partial": "partial",
    "completed": "completed",
    "complete": "completed",
    "success": "completed",
    "succeeded": "completed",
    "done": "completed",
    "failed": "failed",
    "error": "failed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
}


class BackfillError(RuntimeError):
    """A safe operator-facing backfill error."""


@dataclass(frozen=True)
class LegacyItem:
    kind: str
    legacy_id: str
    source_digest: str
    action: str
    reason_code: str | None
    row: dict[str, Any]
    payload: dict[str, Any]

    @property
    def item_key(self) -> str:
        return f"{self.kind}:{self.legacy_id}"


def _row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {str(key): row[key] for key in keys()}
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _canonical_digest(kind: str, legacy_id: str, row: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        {"kind": kind, "legacy_id": legacy_id, "row": dict(row)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_id(prefix: str, kind: str, legacy_id: str) -> str:
    digest = _canonical_digest(kind, legacy_id, {"id": legacy_id})
    return f"legacy-{prefix}-{digest[:40]}"


def _valid_count(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        number = int(value.strip())
    else:
        return None
    return number if 0 <= number <= MAX_INT64 else None


def _count_from_mapping(value: Mapping[str, Any]) -> int | None:
    for key in _COUNT_KEYS:
        if key in value:
            candidate = _valid_count(value.get(key))
            if candidate is not None:
                return candidate
    for key in ("summary", "metrics", "result", "progress", "stats"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            candidate = _count_from_mapping(nested)
            if candidate is not None:
                return candidate
    return None


def _reported_count(row: Mapping[str, Any], payload: Mapping[str, Any]) -> int | None:
    candidate = _count_from_mapping(payload)
    if candidate is not None:
        return candidate
    for key in ("result_json", "progress_json", "job_json"):
        candidate = _count_from_mapping(_json_object(row.get(key)))
        if candidate is not None:
            return candidate
    return _count_from_mapping(row)


def _timestamp(value: Any) -> str | None:
    """Canonicalize a proven legacy timestamp without inventing wall-clock time."""

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat()


def _first_timestamp(*values: Any) -> str | None:
    for value in values:
        timestamp = _timestamp(value)
        if timestamp is not None:
            return timestamp
    return None


def _timestamp_before(first: str, second: str) -> bool:
    return datetime.fromisoformat(first) < datetime.fromisoformat(second)


def _status(value: Any) -> str | None:
    return _STATUS_MAP.get(str(value or "").strip().lower())


def _proven_event_time(row: Mapping[str, Any], payload: Mapping[str, Any], *, completed: bool = False) -> str | None:
    if completed:
        return _first_timestamp(row.get("completed_at"), payload.get("completed_at"))
    return _first_timestamp(row.get("updated_at"), payload.get("updated_at"))


def _legacy_reason(status: str | None, *, completion_known: bool) -> str:
    if status == "completed":
        return "legacy_reported_input" if completion_known else "historical_completion_unknown"
    return {
        "failed": "legacy_failed_root_partial",
        "partial": "legacy_partial",
        "cancelled": "legacy_cancelled",
        "pending": "legacy_pending",
        "running": "legacy_running",
    }.get(status or "", "legacy_status_unknown")


def _table_rows(connection: Any, table: str, key: str, columns: str, *, page_size: int) -> Iterable[dict[str, Any]]:
    last_key = ""
    while True:
        try:
            rows = connection.execute(
                f"SELECT {columns} FROM {table} WHERE {key} > ? ORDER BY {key} LIMIT ?",
                (last_key, page_size),
            ).fetchall()
        except Exception as exc:
            message = str(exc).lower()
            if "no such table" in message or "does not exist" in message or "undefined table" in message:
                return
            raise
        if not rows:
            return
        for row in rows:
            value = _row_dict(row)
            yield value
            last_key = str(value.get(key) or last_key)
        if len(rows) < page_size:
            return


def _feature_item(row: dict[str, Any]) -> LegacyItem:
    legacy_id = str(row.get("job_id") or "")
    payload = _json_object(row.get("job_json"))
    status = _status(row.get("status") or payload.get("status"))
    completion = _proven_event_time(row, payload, completed=status == "completed")
    created = _first_timestamp(row.get("created_at"), payload.get("created_at")) or completion
    reason = _legacy_reason(status, completion_known=completion is not None)
    action = "apply" if legacy_id and created is not None else "skip"
    if not legacy_id:
        reason = "missing_legacy_id"
    elif created is None:
        reason = "legacy_event_time_unknown"
    elif completion is not None and _timestamp_before(completion, created):
        action, reason = "skip", "legacy_timestamp_order_invalid"
    elif status in {"failed", "partial", "cancelled", "running"} and completion is None:
        reason = "legacy_event_time_unknown"
    payload = {**payload, "_legacy_status": status, "_legacy_created_at": created, "_legacy_event_at": completion}
    return LegacyItem(
        "feature_job",
        legacy_id,
        _canonical_digest("feature_job", legacy_id, row),
        action,
        reason,
        row,
        {**payload, "_legacy_status": status},
    )


def _input_item(row: dict[str, Any]) -> LegacyItem:
    legacy_id = str(row.get("input_job_id") or "")
    payload = _json_object(row.get("job_json"))
    result = _json_object(row.get("result_json"))
    if result:
        payload = {**payload, "result": result}
    status = _status(row.get("status") or payload.get("status"))
    completion = _proven_event_time(row, payload, completed=status == "completed")
    created = _first_timestamp(row.get("created_at"), payload.get("created_at")) or completion
    action = "apply" if legacy_id and created is not None else "skip"
    reason = _legacy_reason(status, completion_known=completion is not None)
    if not legacy_id:
        reason = "missing_legacy_id"
    elif created is None:
        reason = "legacy_event_time_unknown"
    elif completion is not None and _timestamp_before(completion, created):
        action, reason = "skip", "legacy_timestamp_order_invalid"
    elif status in {"failed", "partial", "cancelled", "running"} and completion is None:
        reason = "legacy_event_time_unknown"
    payload = {**payload, "_legacy_status": status, "_legacy_created_at": created, "_legacy_event_at": completion}
    return LegacyItem(
        "input_job",
        legacy_id,
        _canonical_digest("input_job", legacy_id, row),
        action,
        reason,
        row,
        payload,
    )


def _trace_item(
    row: dict[str, Any],
    feature_job_ids: set[str],
    seen_physical_attempts: set[tuple[str, int]],
    seen_logical_attempts: set[tuple[str, int]],
) -> LegacyItem:
    legacy_id = str(row.get("trace_id") or "")
    payload = _json_object(row.get("trace_json"))
    status = str(row.get("status") or payload.get("status") or "").lower()
    reason: str | None = None
    action = "apply"
    if not legacy_id:
        action, reason = "skip", "missing_legacy_id"
    elif status == "cache_hit" or payload.get("cache_hit") is True:
        action, reason = "skip", "cache_excluded"
    else:
        physical = payload.get("physical_call_id") or payload.get("call_id")
        logical = payload.get("logical_call_id") or payload.get("logical_id")
        attempt = _valid_count(payload.get("attempt_index"))
        if not isinstance(physical, str) or not physical.strip() or not isinstance(logical, str) or not logical.strip() or attempt is None:
            action, reason = "skip", "ambiguous_trace_attempt"
        elif (physical, attempt) in seen_physical_attempts or (logical, attempt) in seen_logical_attempts:
            action, reason = "skip", "duplicate_trace_attempt"
        else:
            seen_physical_attempts.add((physical, attempt))
            seen_logical_attempts.add((logical, attempt))
        job_id = row.get("job_id") or payload.get("job_id")
        if action == "apply" and job_id and str(job_id) not in feature_job_ids:
            action, reason = "skip", "unlinked_trace_job"
        if action == "apply" and _first_timestamp(row.get("created_at"), payload.get("created_at")) is None:
            action, reason = "skip", "legacy_event_time_unknown"
    return LegacyItem(
        "trace",
        legacy_id,
        _canonical_digest("trace", legacy_id, row),
        action,
        reason,
        row,
        payload,
    )


def discover_items(database: Database, *, page_size: int = DEFAULT_PAGE_SIZE) -> list[LegacyItem]:
    """Read legacy metadata in bounded keyset pages and return import decisions."""

    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1 or page_size > 1000:
        raise ValueError("page_size 必须在 1 到 1000 之间")
    with database.connect() as connection:
        feature_rows = list(
            _table_rows(
                connection,
                "feature_jobs",
                "job_id",
                "job_id, status, job_json, created_at, completed_at, updated_at",
                page_size=page_size,
            )
        )
        feature_items = [_feature_item(row) for row in feature_rows]
        feature_job_ids = {item.legacy_id for item in feature_items if item.legacy_id and item.action == "apply"}
        linked_input_ids: set[str] = set()
        for item in feature_items:
            candidate = item.payload.get("input_job_id")
            if candidate:
                linked_input_ids.add(str(candidate))
        input_items: list[LegacyItem] = []
        for row in _table_rows(
            connection,
            "input_jobs",
            "input_job_id",
            "input_job_id, upload_id, status, stage, job_json, progress_json, result_json, created_at, updated_at",
            page_size=page_size,
        ):
            item = _input_item(row)
            if item.legacy_id in linked_input_ids:
                item = LegacyItem(item.kind, item.legacy_id, item.source_digest, "skip", "input_linked_to_feature_job", item.row, item.payload)
            input_items.append(item)
        seen_physical_attempts: set[tuple[str, int]] = set()
        seen_logical_attempts: set[tuple[str, int]] = set()
        trace_items = [
            _trace_item(row, feature_job_ids, seen_physical_attempts, seen_logical_attempts)
            for row in _table_rows(
                connection,
                "ai_traces",
                "trace_id",
                "trace_id, job_id, provider, model, status, trace_json, created_at",
                page_size=page_size,
            )
        ]
    return [*feature_items, *input_items, *trace_items]


def _existing_items(database: Database) -> dict[str, dict[str, Any]]:
    try:
        with database.connect() as connection:
            rows = connection.execute(
                "SELECT item_key, source_digest, status, reason_code FROM operational_backfill_items"
            ).fetchall()
    except Exception as exc:
        raise BackfillError("operational_backfill_items 不存在；请先执行 0024 迁移") from exc
    return {str(row["item_key"]): _row_dict(row) for row in rows}


def _new_root_values(item: LegacyItem) -> dict[str, Any]:
    row = item.row
    payload = item.payload
    if item.kind == "feature_job":
        root_id = _stable_id("analysis", item.kind, item.legacy_id)
        input_job_id = payload.get("input_job_id") or row.get("input_job_id")
    else:
        root_id = _stable_id("analysis", item.kind, item.legacy_id)
        input_job_id = item.legacy_id
    reported_count = _reported_count(row, payload)
    status = payload.get("_legacy_status")
    created_at = payload.get("_legacy_created_at")
    event_at = payload.get("_legacy_event_at")
    if created_at is None:
        created_at = event_at
    # A reported historical count is useful even when its old source is no
    # longer available.  It never becomes exact input_count or coverage.
    completed_at = event_at if status == "completed" and event_at is not None else None
    return {
        "analysis_run_id": root_id,
        "environment": "production",
        "scope_key": "default",
        "request_key": f"backfill:{item.item_key}",
        "input_job_id": str(input_job_id) if input_job_id else None,
        "provenance": "reported",
        "status": status,
        "expected_members": 1,
        "input_count": None,
        "reported_input_count": reported_count,
        "created_at": created_at,
        "completed_at": completed_at,
        "settled_at": completed_at,
        "legacy_status": status,
    }


def _insert_reported_root(ledger: OperationalLedgerRepository, connection: Any, item: LegacyItem) -> None:
    root = _new_root_values(item)
    if root["created_at"] is None:
        raise BackfillError("legacy root 缺少可信创建时间")
    ledger.create_analysis_run(
        analysis_run_id=root["analysis_run_id"],
        request_key=root["request_key"],
        environment=root["environment"],
        scope_key=root["scope_key"],
        input_job_id=root["input_job_id"],
        provenance="reported",
        expected_members=1,
        reported_input_count=root["reported_input_count"],
        created_at=root["created_at"],
        connection=connection,
    )
    member_id = f"{root['analysis_run_id']}:0"
    ledger.register_analysis_member(
        root["analysis_run_id"],
        member_id,
        created_at=root["created_at"],
        connection=connection,
    )
    if item.kind == "feature_job":
        # Binding is a compare-and-set operation.  A legacy binding is safe
        # because the synthetic member is permanently tied to this job id.
        ledger.bind_analysis_member(root["analysis_run_id"], member_id, item.legacy_id, connection=connection)
    status = root["legacy_status"]
    finished_at = root["completed_at"] or root["created_at"]
    if status == "completed" and root["completed_at"] is not None:
        ledger.finish_analysis_member(root["analysis_run_id"], member_id, "completed", finished_at=finished_at, connection=connection)
    elif status in {"failed", "partial", "cancelled", "running"}:
        ledger.finish_analysis_member(root["analysis_run_id"], member_id, status, finished_at=finished_at, connection=connection)


def _insert_confirmed_trace(
    ledger: OperationalLedgerRepository,
    connection: Any,
    item: LegacyItem,
    feature_root_ids: Mapping[str, str],
) -> None:
    row, payload = item.row, item.payload
    physical = str(payload.get("physical_call_id") or payload.get("call_id"))
    logical = str(payload.get("logical_call_id") or payload.get("logical_id"))
    attempt = _valid_count(payload.get("attempt_index"))
    if attempt is None:
        raise BackfillError("确认过的 Trace 缺少有效 attempt_index")
    job_id = str(row.get("job_id") or payload.get("job_id") or "")
    analysis_run_id = feature_root_ids.get(job_id)
    usage_value = payload.get("usage") if isinstance(payload.get("usage"), Mapping) else payload
    started_at = _first_timestamp(row.get("created_at"), payload.get("created_at"))
    if started_at is None:
        raise BackfillError("确认过的 Trace 缺少可信事件时间")
    status = str(row.get("status") or payload.get("status") or "unknown").lower()
    call_status = "succeeded" if status in {"success", "succeeded", "completed"} else "failed" if status in {"failed", "error", "model_failed", "evaluator_failed"} else "unknown"
    ledger.prepare_call(
        call_id=physical,
        logical_call_id=logical,
        attempt_index=attempt,
        analysis_run_id=analysis_run_id,
        environment="production",
        scope_key="default",
        call_kind="provider",
        provider=str(row.get("provider") or payload.get("provider") or "legacy"),
        model=str(row.get("model") or payload.get("model") or "legacy"),
        caller_kind="legacy_trace",
        caller_id=job_id or None,
        metadata_contract="legacy-trace-confirmed",
        connection=connection,
    )
    ledger.start_call(physical, started_at=started_at, connection=connection)
    ledger.finish_call(
        physical,
        status=call_status,
        usage=usage_value,
        error_code=payload.get("error_code"),
        finished_at=started_at,
        connection=connection,
    )


def _apply_item(
    database: Database,
    ledger: OperationalLedgerRepository,
    item: LegacyItem,
    feature_root_ids: Mapping[str, str],
) -> str:
    with database.transaction() as connection:
        if item.action == "skip":
            ledger.record_backfill_item(
                item_key=item.item_key,
                source_digest=item.source_digest,
                status="skipped",
                reason_code=item.reason_code,
                connection=connection,
            )
            return "skipped"
        if item.kind in {"feature_job", "input_job"}:
            _insert_reported_root(ledger, connection, item)
        elif item.kind == "trace":
            _insert_confirmed_trace(ledger, connection, item, feature_root_ids)
        else:
            raise BackfillError(f"未知 backfill item 类型: {item.kind}")
        ledger.record_backfill_item(
            item_key=item.item_key,
            source_digest=item.source_digest,
            status="applied",
            reason_code=item.reason_code,
            connection=connection,
        )
        return "applied"


def run_backfill(
    database: Database,
    *,
    apply: bool = False,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """Plan or apply a resumable backfill.  Each applied item is one transaction."""

    items = discover_items(database, page_size=page_size)
    existing = _existing_items(database)
    ledger = OperationalLedgerRepository(database)
    feature_root_ids = {
        item.legacy_id: _stable_id("analysis", item.kind, item.legacy_id)
        for item in items
        if item.kind == "feature_job" and item.action == "apply"
    }
    reasons: Counter[str] = Counter()
    by_kind: Counter[str] = Counter()
    result: dict[str, Any] = {
        "mode": "apply" if apply else "dry-run",
        "scanned": len(items),
        "planned": 0,
        "applied": 0,
        "skipped": 0,
        "already_done": 0,
        "conflicts": 0,
        "reasons": reasons,
        "by_kind": by_kind,
    }
    for item in items:
        by_kind[item.kind] += 1
        prior = existing.get(item.item_key)
        if prior is not None:
            if prior.get("source_digest") != item.source_digest:
                result["conflicts"] += 1
                reasons["source_changed"] += 1
                continue
            desired_status = "skipped" if item.action == "skip" else "applied"
            if str(prior.get("status")) != desired_status or prior.get("reason_code") != item.reason_code:
                result["conflicts"] += 1
                reasons["backfill_receipt_conflict"] += 1
                continue
            result["already_done"] += 1
            continue
        if item.reason_code:
            reasons[item.reason_code] += 1
        if item.action == "skip":
            result["skipped"] += 1
            if apply:
                _apply_item(database, ledger, item, feature_root_ids)
            continue
        result["planned"] += 1
        if apply:
            _apply_item(database, ledger, item, feature_root_ids)
            result["applied"] += 1
    result["reasons"] = dict(sorted(reasons.items()))
    result["by_kind"] = dict(sorted(by_kind.items()))
    return result


def _build_database(arguments: argparse.Namespace) -> Database:
    database_path = Path(arguments.database).expanduser()
    provider = arguments.provider
    if provider == "postgres" and not arguments.database_url:
        raise BackfillError("PostgreSQL backfill 需要 --database-url")
    return create_database(
        provider=provider,
        sqlite_path=database_path,
        state_root=database_path.parent,
        database_url=arguments.database_url,
        migrate=False,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill operational ledgers from legacy metadata")
    parser.add_argument("--database", required=True, help="SQLite database path (also used as state root for PostgreSQL)")
    parser.add_argument("--provider", choices=("sqlite", "postgres"), default="sqlite")
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--apply", action="store_true", help="Apply one resumable transaction per item")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = run_backfill(_build_database(arguments), apply=arguments.apply, page_size=arguments.page_size)
    except (BackfillError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
