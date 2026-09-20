from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from logrisk.database import Database, utc_now


METRIC_VERSION = "operational-ledgers-v1"
MAX_INT64 = (1 << 63) - 1

_PROVENANCES = {"verified", "reported", "unverified-input"}
_RECEIPT_PROVENANCES = {"verified", "reported"}
_ROOT_STATUSES = {"pending", "running", "partial", "completed", "failed", "cancelled"}
_MEMBER_STATUSES = {"pending", "running", "partial", "completed", "failed", "cancelled"}
_CALL_KINDS = {"provider", "agent_tool"}
_CALL_STATUSES = {"prepared", "started", "succeeded", "failed", "unknown"}
_TERMINAL_ROOT_STATUSES = {"completed", "failed", "cancelled"}
_TERMINAL_MEMBER_STATUSES = {"completed", "cancelled"}
_TERMINAL_CALL_STATUSES = {"succeeded", "failed", "unknown"}
_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "reasoning_tokens",
)


class OperationalLedgerError(ValueError):
    """Invalid or conflicting operational-ledger input."""


class OperationalLedgerConflict(OperationalLedgerError):
    """A replay or immutable ledger value disagrees with the stored fact."""


def _text(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise OperationalLedgerError(f"{field} 必须是字符串")
    result = value if allow_empty else value.strip()
    if not allow_empty and not result:
        raise OperationalLedgerError(f"{field} 不能为空")
    if "\x00" in result:
        raise OperationalLedgerError(f"{field} 含有非法字符")
    return result


def _nullable_int(value: Any, field: str, *, allow_none: bool = True) -> int | None:
    if value is None:
        if allow_none:
            return None
        raise OperationalLedgerError(f"{field} 不能为空")
    if isinstance(value, bool) or not isinstance(value, int):
        raise OperationalLedgerError(f"{field} 必须是非负整数")
    if value < 0 or value > MAX_INT64:
        raise OperationalLedgerError(f"{field} 超出 BIGINT 非负范围")
    return int(value)


def _sum_lengths(ranges: Sequence[Mapping[str, Any]]) -> int:
    total = 0
    for item in ranges:
        length = int(item["end"]) - int(item["start"])
        total += length
        if total > MAX_INT64:
            raise OperationalLedgerError("区间总长度超出 BIGINT 范围")
    return total


def _canonical_timestamp(value: Any, field: str = "timestamp") -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OperationalLedgerError(f"{field} 必须是有效 ISO 时间") from exc
    else:
        raise OperationalLedgerError(f"{field} 必须是带时区的 ISO 时间")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OperationalLedgerError(f"{field} 必须包含时区")
    return parsed.astimezone(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    return dict(row)


def source_identity(
    *,
    environment: str,
    scope_key: str,
    source_kind: str,
    identity_digest: str,
) -> str:
    """Return the canonical source identity digest.

    The digest intentionally includes only the four non-sensitive identity
    fields. Paths, credentials and presentation metadata never participate.
    """

    payload = {
        "environment": _text(environment, "environment"),
        "scope_key": _text(scope_key, "scope_key"),
        "source_kind": _text(source_kind, "source_kind"),
        "identity_digest": _text(identity_digest, "identity_digest"),
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def ingestion_batch_id(input_job_id: str, checkpoint_key: str) -> str:
    """Return the deterministic receipt identity for an input window."""

    payload = [_text(input_job_id, "input_job_id"), _text(checkpoint_key, "checkpoint_key")]
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _range_item(value: Any, index: int, *, require_source: bool = False) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise OperationalLedgerError(f"ranges[{index}] 必须是对象")
    allowed = {"partition_key", "start", "end", "source_id"}
    unknown = set(value) - allowed
    if unknown:
        raise OperationalLedgerError(f"ranges[{index}] 含有未支持字段")
    if "partition_key" not in value:
        raise OperationalLedgerError(f"ranges[{index}] 缺少 partition_key")
    partition_key = _text(value["partition_key"], f"ranges[{index}].partition_key", allow_empty=True)
    start = _nullable_int(value.get("start"), f"ranges[{index}].start", allow_none=False)
    end = _nullable_int(value.get("end"), f"ranges[{index}].end", allow_none=False)
    assert start is not None and end is not None
    if end <= start:
        raise OperationalLedgerError(f"ranges[{index}] 必须满足 end > start")
    source_value = value.get("source_id")
    if require_source and source_value is None:
        raise OperationalLedgerError(f"ranges[{index}] 缺少 source_id")
    result = {"partition_key": partition_key, "start": start, "end": end}
    if source_value is not None:
        result["source_id"] = _text(source_value, f"ranges[{index}].source_id")
    return result


def canonical_ranges(ranges: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Validate, sort and coalesce half-open intervals.

    Coalescing is limited to the same source (when present) and partition.
    Adjacent intervals are merged because they represent one contiguous set
    of proven positions; holes remain separate.
    """

    if isinstance(ranges, (str, bytes, bytearray, Mapping)):
        raise OperationalLedgerError("ranges 必须是区间对象列表")
    try:
        values = list(ranges)
    except TypeError as exc:
        raise OperationalLedgerError("ranges 必须是区间对象列表") from exc
    grouped: dict[tuple[str | None, str], list[dict[str, Any]]] = {}
    for index, value in enumerate(values):
        item = _range_item(value, index)
        key = (item.get("source_id"), item["partition_key"])
        grouped.setdefault(key, []).append(item)

    output: list[dict[str, Any]] = []
    for (source_id, partition_key), items in sorted(
        grouped.items(), key=lambda pair: ((pair[0][0] or ""), pair[0][1])
    ):
        items.sort(key=lambda item: (item["start"], item["end"]))
        current: dict[str, Any] | None = None
        for item in items:
            if current is None:
                current = dict(item)
                continue
            if item["start"] <= current["end"]:
                current["end"] = max(current["end"], item["end"])
                continue
            output.append(current)
            current = dict(item)
        if current is not None:
            output.append(current)
    if _sum_lengths(output) > MAX_INT64:
        raise OperationalLedgerError("区间总长度超出 BIGINT 范围")
    return output


def subtract_ranges(
    incoming: Sequence[Mapping[str, Any]],
    existing: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return incoming positions not already present in an interval set."""

    incoming_items = canonical_ranges(incoming)
    existing_items = canonical_ranges(existing)
    by_key: dict[tuple[str | None, str], list[dict[str, Any]]] = {}
    for item in existing_items:
        by_key.setdefault((item.get("source_id"), item["partition_key"]), []).append(item)

    output: list[dict[str, Any]] = []
    for item in incoming_items:
        key = (item.get("source_id"), item["partition_key"])
        cursor = item["start"]
        for occupied in by_key.get(key, ()):
            if occupied["end"] <= cursor:
                continue
            if occupied["start"] >= item["end"]:
                break
            if occupied["start"] > cursor:
                fragment = {
                    "partition_key": item["partition_key"],
                    "start": cursor,
                    "end": min(occupied["start"], item["end"]),
                }
                if item.get("source_id") is not None:
                    fragment["source_id"] = item["source_id"]
                if fragment["start"] < fragment["end"]:
                    output.append(fragment)
            cursor = max(cursor, occupied["end"])
            if cursor >= item["end"]:
                break
        if cursor < item["end"]:
            fragment = {
                "partition_key": item["partition_key"],
                "start": cursor,
                "end": item["end"],
            }
            if item.get("source_id") is not None:
                fragment["source_id"] = item["source_id"]
            output.append(fragment)
    return canonical_ranges(output)


def _usage_alias(value: Mapping[str, Any], aliases: Sequence[str], nested: Sequence[tuple[str, str]] = ()) -> tuple[Any, bool]:
    for alias in aliases:
        if alias in value:
            return value[alias], True
    for parent, child in nested:
        candidate = value.get(parent)
        if isinstance(candidate, Mapping) and child in candidate:
            return candidate[child], True
    return None, False


def normalize_usage(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normalize whitelisted Provider usage fields without coercion.

    Missing fields stay ``None``.  Integer-looking floats, booleans, negative
    values and non-finite numbers are invalid rather than silently rounded.
    """

    empty = {field: None for field in _TOKEN_FIELDS}
    empty.update({"invalid_usage": False, "usage_quality": "unknown"})
    if value is None:
        return empty
    if not isinstance(value, Mapping):
        empty["invalid_usage"] = True
        empty["usage_quality"] = "invalid"
        return empty

    aliases: dict[str, tuple[Sequence[str], Sequence[tuple[str, str]]]] = {
        "input_tokens": (("input_tokens", "prompt_tokens", "prompt_eval_count"), ()),
        "output_tokens": (("output_tokens", "completion_tokens", "eval_count"), ()),
        "total_tokens": (("total_tokens",), ()),
        "cached_input_tokens": (("cached_input_tokens", "cached_tokens"), (("prompt_tokens_details", "cached_tokens"),)),
        "reasoning_tokens": (("reasoning_tokens",), (("completion_tokens_details", "reasoning_tokens"),)),
    }
    invalid = False
    present: dict[str, bool] = {}
    for field in _TOKEN_FIELDS:
        raw, was_present = _usage_alias(value, *aliases[field])
        present[field] = was_present
        if not was_present or raw is None:
            continue
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0 or raw > MAX_INT64:
            invalid = True
            continue
        empty[field] = int(raw)

    if not present["total_tokens"] and empty["input_tokens"] is not None and empty["output_tokens"] is not None:
        derived = empty["input_tokens"] + empty["output_tokens"]
        if derived > MAX_INT64:
            invalid = True
        else:
            empty["total_tokens"] = derived
    empty["invalid_usage"] = invalid
    if invalid:
        empty["usage_quality"] = "invalid"
    elif empty["total_tokens"] is not None:
        empty["usage_quality"] = "known"
    elif any(empty[field] is not None for field in _TOKEN_FIELDS):
        empty["usage_quality"] = "partial"
    else:
        empty["usage_quality"] = "unknown"
    return empty


def tokens_per_1000(tokens: int | None, records: int | None) -> Decimal | None:
    """Return exact token density rounded half-up to two decimal places."""

    normalized_tokens = _nullable_int(tokens, "tokens")
    normalized_records = _nullable_int(records, "records")
    if normalized_tokens is None or normalized_records is None or normalized_records == 0:
        return None
    return (Decimal(normalized_tokens) * Decimal(1000) / Decimal(normalized_records)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


class OperationalLedgerRepository:
    """Durable SQLite/PostgreSQL writer for the operational ledgers."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def _write_context(self, connection: Any = None) -> Any:
        return nullcontext(connection) if connection is not None else self.database.transaction()

    def _for_update(self, sql: str) -> str:
        return sql + (" FOR UPDATE" if getattr(self.database, "provider", "sqlite") == "postgres" else "")

    def _source(self, source: Mapping[str, Any]) -> dict[str, str]:
        if not isinstance(source, Mapping):
            raise OperationalLedgerError("source 必须是对象")
        required = ("source_id", "environment", "scope_key", "source_kind", "identity_digest")
        missing = [field for field in required if field not in source]
        if missing:
            raise OperationalLedgerError(f"source 缺少字段: {', '.join(missing)}")
        normalized = {field: _text(source[field], field) for field in required}
        expected = source_identity(
            environment=normalized["environment"],
            scope_key=normalized["scope_key"],
            source_kind=normalized["source_kind"],
            identity_digest=normalized["identity_digest"],
        )
        if normalized["source_id"] != expected:
            raise OperationalLedgerError("source_id 与规范来源身份不一致")
        return normalized

    def _lock_source(self, connection: Any, source: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._source(source)
        connection.execute(
            "INSERT INTO operational_sources(source_id, environment, scope_key, source_kind, identity_digest, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(source_id) DO NOTHING",
            (
                normalized["source_id"],
                normalized["environment"],
                normalized["scope_key"],
                normalized["source_kind"],
                normalized["identity_digest"],
                utc_now(),
            ),
        )
        row = connection.execute(
            self._for_update("SELECT * FROM operational_sources WHERE source_id=?"),
            (normalized["source_id"],),
        ).fetchone()
        if row is None:
            raise OperationalLedgerError("来源登记失败")
        persisted = _row_dict(row)
        for field in ("environment", "scope_key", "source_kind", "identity_digest"):
            if str(persisted[field]) != normalized[field]:
                raise OperationalLedgerConflict("source_id 已绑定到不同来源身份")
        return persisted

    @staticmethod
    def _range_rows(rows: Sequence[Any]) -> list[dict[str, Any]]:
        return [
            {
                "partition_key": str(row["partition_key"]),
                "start": int(row["start_position"]),
                "end": int(row["end_position"]),
            }
            for row in rows
        ]

    @staticmethod
    def _receipt_result(row: Any) -> dict[str, Any]:
        result = _row_dict(row)
        try:
            result["ranges"] = json.loads(str(result.get("ranges_json") or "[]"))
        except (TypeError, json.JSONDecodeError):
            result["ranges"] = []
        return result

    def record_ingestion_batch(
        self,
        *,
        batch_id: str,
        source: Mapping[str, Any],
        input_job_id: str,
        checkpoint_key: str,
        parser_version: str,
        ranges: Sequence[Mapping[str, Any]],
        actual_count: int | None,
        provenance: str = "verified",
        committed_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        batch_key = _text(batch_id, "batch_id")
        input_key = _text(input_job_id, "input_job_id")
        checkpoint = _text(checkpoint_key, "checkpoint_key")
        parser = _text(parser_version, "parser_version")
        if provenance not in _RECEIPT_PROVENANCES:
            raise OperationalLedgerError("ingestion provenance 必须是 verified 或 reported")
        normalized_source = self._source(source)
        normalized_ranges = canonical_ranges(ranges)
        if any(item.get("source_id") is not None for item in normalized_ranges):
            raise OperationalLedgerError("ingestion ranges 不得携带 source_id")
        range_payload = [
            {"partition_key": item["partition_key"], "start": item["start"], "end": item["end"]}
            for item in normalized_ranges
        ]
        range_count = _sum_lengths(range_payload)
        normalized_count = _nullable_int(actual_count, "actual_count")
        if provenance == "verified" and normalized_count != range_count:
            raise OperationalLedgerError("verified actual_count 必须等于区间并集长度")
        committed = _canonical_timestamp(committed_at, "committed_at") if committed_at is not None else utc_now()

        with self._write_context(connection) as current:
            existing = current.execute(
                "SELECT * FROM operational_ingestion_batches WHERE input_job_id=? AND checkpoint_key=?",
                (input_key, checkpoint),
            ).fetchone()
            if existing is not None:
                persisted = self._receipt_result(existing)
                comparable = (
                    str(persisted["batch_id"]) == batch_key
                    and str(persisted["source_id"]) == normalized_source["source_id"]
                    and str(persisted["parser_version"]) == parser
                    and str(persisted["ranges_json"]) == _json(range_payload)
                    and persisted["actual_count"] == normalized_count
                    and str(persisted["provenance"]) == provenance
                )
                if not comparable:
                    raise OperationalLedgerConflict("相同 input_job_id/checkpoint_key 的收据内容冲突")
                return persisted
            by_id = current.execute(
                "SELECT * FROM operational_ingestion_batches WHERE batch_id=?", (batch_key,)
            ).fetchone()
            if by_id is not None:
                raise OperationalLedgerConflict("batch_id 已绑定到不同窗口")

            self._lock_source(current, normalized_source)
            existing_rows = current.execute(
                "SELECT partition_key, start_position, end_position FROM operational_source_ranges "
                "WHERE source_id=? AND kind='ingested' ORDER BY partition_key, start_position, end_position",
                (normalized_source["source_id"],),
            ).fetchall()
            existing_ranges = self._range_rows(existing_rows)
            uncovered = subtract_ranges(range_payload, existing_ranges)
            newly_count = _sum_lengths(uncovered)
            stored_newly_count: int | None = newly_count if provenance == "verified" else None
            current.execute(
                "INSERT INTO operational_ingestion_batches(batch_id, source_id, input_job_id, checkpoint_key, parser_version, "
                "ranges_json, actual_count, newly_ingested_count, provenance, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_key,
                    normalized_source["source_id"],
                    input_key,
                    checkpoint,
                    parser,
                    _json(range_payload),
                    normalized_count,
                    stored_newly_count,
                    provenance,
                    committed,
                ),
            )
            if provenance == "verified":
                for item in uncovered:
                    range_id = hashlib.sha256(
                        _json(
                            {
                                "source_id": normalized_source["source_id"],
                                "kind": "ingested",
                                "partition_key": item["partition_key"],
                                "start": item["start"],
                                "end": item["end"],
                                "origin_id": batch_key,
                            }
                        ).encode("utf-8")
                    ).hexdigest()
                    current.execute(
                        "INSERT INTO operational_source_ranges(range_id, source_id, kind, partition_key, start_position, end_position, first_event_at, origin_id) "
                        "VALUES (?, ?, 'ingested', ?, ?, ?, ?, ?)",
                        (
                            range_id,
                            normalized_source["source_id"],
                            item["partition_key"],
                            item["start"],
                            item["end"],
                            committed,
                            batch_key,
                        ),
                    )
            row = current.execute(
                "SELECT * FROM operational_ingestion_batches WHERE batch_id=?", (batch_key,)
            ).fetchone()
            assert row is not None
            return self._receipt_result(row)

    def list_ingestion_batches(
        self,
        input_job_id: str,
        *,
        after_batch_id: str | None = None,
        limit: int = 500,
        connection: Any = None,
    ) -> dict[str, Any]:
        """Read immutable input receipts with a bounded batch-id keyset page."""

        input_key = _text(input_job_id, "input_job_id")
        page_size = _nullable_int(limit, "limit", allow_none=False)
        assert page_size is not None
        if page_size < 1 or page_size > 1000:
            raise OperationalLedgerError("limit 必须在 1..1000 范围内")
        after = _text(after_batch_id, "after_batch_id") if after_batch_id is not None else None
        where = "WHERE b.input_job_id=?"
        params: list[Any] = [input_key]
        if after is not None:
            where += " AND b.batch_id>?"
            params.append(after)
        params.append(page_size + 1)
        sql = (
            "SELECT b.batch_id, b.source_id, b.input_job_id, b.checkpoint_key, b.parser_version, "
            "b.actual_count, b.newly_ingested_count, b.provenance, b.committed_at, b.ranges_json, "
            "s.environment AS source_environment, s.scope_key AS source_scope_key, "
            "s.source_kind AS source_kind, s.identity_digest AS source_identity_digest "
            "FROM operational_ingestion_batches b JOIN operational_sources s ON s.source_id=b.source_id "
            f"{where} ORDER BY b.batch_id ASC LIMIT ?"
        )
        with (nullcontext(connection) if connection is not None else self.database.connect()) as current:
            rows = current.execute(sql, params).fetchall()
        has_more = len(rows) > page_size
        rows = rows[:page_size]
        items: list[dict[str, Any]] = []
        for row in rows:
            item = _row_dict(row)
            raw_ranges = item.pop("ranges_json", "[]")
            try:
                item["ranges"] = json.loads(str(raw_ranges or "[]"))
            except (TypeError, json.JSONDecodeError) as exc:
                raise OperationalLedgerError("数据库中的 ingestion ranges 无效") from exc
            item["source"] = {
                "source_id": item["source_id"],
                "environment": item.pop("source_environment"),
                "scope_key": item.pop("source_scope_key"),
                "source_kind": item.pop("source_kind"),
                "identity_digest": item.pop("source_identity_digest"),
            }
            items.append(item)
        return {
            "items": items,
            "next_after_batch_id": items[-1]["batch_id"] if has_more and items else None,
            "has_more": has_more,
        }

    @staticmethod
    def _analysis_range_payload(rows: Sequence[Any]) -> list[dict[str, Any]]:
        return canonical_ranges(
            [
                {
                    "source_id": str(row["source_id"]),
                    "partition_key": str(row["partition_key"]),
                    "start": int(row["start_position"]),
                    "end": int(row["end_position"]),
                }
                for row in rows
            ]
        )

    def _analysis_detail(self, connection: Any, analysis_run_id: str) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM operational_analysis_runs WHERE analysis_run_id=?", (analysis_run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"分析根不存在: {analysis_run_id}")
        result = _row_dict(row)
        range_rows = connection.execute(
            "SELECT source_id, partition_key, start_position, end_position FROM operational_analysis_ranges "
            "WHERE analysis_run_id=? ORDER BY source_id, partition_key, start_position, end_position",
            (analysis_run_id,),
        ).fetchall()
        result["ranges"] = self._analysis_range_payload(range_rows)
        member_rows = connection.execute(
            "SELECT * FROM operational_analysis_members WHERE analysis_run_id=? ORDER BY member_id",
            (analysis_run_id,),
        ).fetchall()
        result["members"] = [_row_dict(member) for member in member_rows]
        return result

    def _normalized_analysis_ranges(
        self,
        ranges: Sequence[Mapping[str, Any]],
        *,
        source_id: str | None,
    ) -> list[dict[str, Any]]:
        normalized = canonical_ranges(ranges)
        result: list[dict[str, Any]] = []
        for item in normalized:
            item_source = item.get("source_id")
            if item_source is None:
                if source_id is None:
                    raise OperationalLedgerError("analysis ranges 必须携带 source_id")
                item_source = source_id
            elif source_id is not None and item_source != source_id:
                raise OperationalLedgerError("analysis source_id 与区间来源不一致")
            result.append({**item, "source_id": _text(item_source, "source_id")})
        return canonical_ranges(result)

    def _validate_root_sources(
        self,
        connection: Any,
        ranges: Sequence[Mapping[str, Any]],
        *,
        environment: str,
        scope_key: str,
        source_id: str | None,
    ) -> None:
        source_ids = sorted({str(item["source_id"]) for item in ranges})
        if source_id is not None:
            source_ids = sorted(set(source_ids) | {source_id})
        for current_source_id in source_ids:
            row = connection.execute(
                "SELECT environment, scope_key FROM operational_sources WHERE source_id=?",
                (current_source_id,),
            ).fetchone()
            if row is None:
                raise OperationalLedgerError("analysis range 引用了未登记来源")
            if str(row["environment"]) != environment or str(row["scope_key"]) != scope_key:
                raise OperationalLedgerError("analysis root 与来源环境或作用域不一致")

    def create_analysis_run(
        self,
        *,
        analysis_run_id: str | None = None,
        request_key: str,
        environment: str,
        scope_key: str,
        input_job_id: str | None = None,
        source_id: str | None = None,
        ranges: Sequence[Mapping[str, Any]] = (),
        input_count: int | None = None,
        reported_input_count: int | None = None,
        provenance: str = "verified",
        expected_members: int = 1,
        parent_run_id: str | None = None,
        created_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id") if analysis_run_id is not None else str(uuid.uuid4())
        request = _text(request_key, "request_key")
        env = _text(environment, "environment")
        scope = _text(scope_key, "scope_key")
        input_job = _text(input_job_id, "input_job_id") if input_job_id is not None else None
        scalar_source = _text(source_id, "source_id") if source_id is not None else None
        if provenance not in _PROVENANCES:
            raise OperationalLedgerError("analysis provenance 无效")
        members = _nullable_int(expected_members, "expected_members", allow_none=False)
        assert members is not None
        if members < 1:
            raise OperationalLedgerError("expected_members 必须大于零")
        verified_count = _nullable_int(input_count, "input_count")
        reported_count = _nullable_int(reported_input_count, "reported_input_count")
        if provenance == "verified":
            if reported_count is not None:
                raise OperationalLedgerError("verified root 不得填写 reported_input_count")
        elif verified_count is not None:
            raise OperationalLedgerError("reported root 不得填写 input_count")
        normalized_ranges = self._normalized_analysis_ranges(ranges, source_id=scalar_source)
        range_count = _sum_lengths(normalized_ranges)
        if provenance == "verified":
            if verified_count is None:
                verified_count = range_count
            elif verified_count != range_count:
                raise OperationalLedgerError("verified input_count 必须等于冻结区间并集长度")
        if parent_run_id is not None:
            parent = _text(parent_run_id, "parent_run_id")
        else:
            parent = None
        created = _canonical_timestamp(created_at, "created_at") if created_at is not None else utc_now()

        with self._write_context(connection) as current:
            existing = current.execute(
                "SELECT * FROM operational_analysis_runs WHERE environment=? AND request_key=?",
                (env, request),
            ).fetchone()
            if existing is not None:
                persisted = _row_dict(existing)
                if str(persisted["analysis_run_id"]) != root_id and analysis_run_id is not None:
                    raise OperationalLedgerConflict("request_key 已绑定到其他分析根")
                existing_ranges = current.execute(
                    "SELECT source_id, partition_key, start_position, end_position FROM operational_analysis_ranges "
                    "WHERE analysis_run_id=? ORDER BY source_id, partition_key, start_position, end_position",
                    (persisted["analysis_run_id"],),
                ).fetchall()
                same = (
                    str(persisted["scope_key"]) == scope
                    and (persisted["input_job_id"] == input_job)
                    and (persisted["source_id"] == scalar_source)
                    and str(persisted["provenance"]) == provenance
                    and int(persisted["expected_members"]) == members
                    and persisted["input_count"] == verified_count
                    and persisted["reported_input_count"] == reported_count
                    and (persisted["parent_run_id"] == parent)
                    and self._analysis_range_payload(existing_ranges) == normalized_ranges
                )
                if not same:
                    raise OperationalLedgerConflict("相同环境和 request_key 的分析根内容冲突")
                return self._analysis_detail(current, str(persisted["analysis_run_id"]))

            by_id = current.execute(
                "SELECT * FROM operational_analysis_runs WHERE analysis_run_id=?", (root_id,)
            ).fetchone()
            if by_id is not None:
                raise OperationalLedgerConflict("analysis_run_id 已存在")
            if scalar_source is not None:
                self._validate_root_sources(current, normalized_ranges, environment=env, scope_key=scope, source_id=scalar_source)
            else:
                self._validate_root_sources(current, normalized_ranges, environment=env, scope_key=scope, source_id=None)
            if parent is not None:
                parent_row = current.execute(
                    "SELECT environment, scope_key, created_at FROM operational_analysis_runs WHERE analysis_run_id=?",
                    (parent,),
                ).fetchone()
                if parent_row is None:
                    raise OperationalLedgerError("parent_run_id 不存在")
                if str(parent_row["environment"]) != env or str(parent_row["scope_key"]) != scope:
                    raise OperationalLedgerError("parent root 与新 root 环境或作用域不一致")
                if created < str(parent_row["created_at"]):
                    raise OperationalLedgerError("analysis root created_at 不得早于 parent root")
            current.execute(
                "INSERT INTO operational_analysis_runs(analysis_run_id, environment, scope_key, request_key, input_job_id, source_id, parent_run_id, provenance, status, expected_members, input_count, reported_input_count, created_at, updated_at, completed_at, settled_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, NULL, NULL)",
                (
                    root_id,
                    env,
                    scope,
                    request,
                    input_job,
                    scalar_source,
                    parent,
                    provenance,
                    members,
                    verified_count,
                    reported_count,
                    created,
                    created,
                ),
            )
            for item in normalized_ranges:
                current.execute(
                    "INSERT INTO operational_analysis_ranges(analysis_run_id, source_id, partition_key, start_position, end_position) VALUES (?, ?, ?, ?, ?)",
                    (root_id, item["source_id"], item["partition_key"], item["start"], item["end"]),
                )
            return self._analysis_detail(current, root_id)

    def register_analysis_member(
        self,
        analysis_run_id: str,
        member_id: str,
        *,
        created_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id")
        member_key = _text(member_id, "member_id")
        created = _canonical_timestamp(created_at, "created_at") if created_at is not None else utc_now()
        with self._write_context(connection) as current:
            root = current.execute(
                self._for_update("SELECT * FROM operational_analysis_runs WHERE analysis_run_id=?"), (root_id,)
            ).fetchone()
            if root is None:
                raise KeyError(f"分析根不存在: {root_id}")
            existing = current.execute(
                "SELECT * FROM operational_analysis_members WHERE member_id=?", (member_key,)
            ).fetchone()
            if existing is not None:
                if str(existing["analysis_run_id"]) != root_id:
                    raise OperationalLedgerConflict("member_id 已绑定到其他分析根")
                return _row_dict(existing)
            if str(root["status"]) in _TERMINAL_ROOT_STATUSES:
                raise OperationalLedgerConflict("终态分析根不可新增成员")
            if created < str(root["created_at"]):
                raise OperationalLedgerError("analysis member created_at 不得早于 root")
            count = current.execute(
                "SELECT COUNT(*) AS count FROM operational_analysis_members WHERE analysis_run_id=?", (root_id,)
            ).fetchone()
            if int(count["count"]) >= int(root["expected_members"]):
                raise OperationalLedgerConflict("分析根成员数已达到 expected_members")
            current.execute(
                "INSERT INTO operational_analysis_members(member_id, analysis_run_id, feature_job_id, status, created_at, updated_at) VALUES (?, ?, NULL, 'pending', ?, ?)",
                (member_key, root_id, created, created),
            )
            row = current.execute(
                "SELECT * FROM operational_analysis_members WHERE member_id=?", (member_key,)
            ).fetchone()
            assert row is not None
            return _row_dict(row)

    def bind_analysis_member(
        self,
        analysis_run_id: str,
        member_id: str,
        feature_job_id: str,
        *,
        connection: Any = None,
    ) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id")
        member_key = _text(member_id, "member_id")
        job_key = _text(feature_job_id, "feature_job_id")
        with self._write_context(connection) as current:
            root = current.execute(
                self._for_update("SELECT status FROM operational_analysis_runs WHERE analysis_run_id=?"), (root_id,)
            ).fetchone()
            if root is None:
                raise KeyError(f"分析根不存在: {root_id}")
            row = current.execute(
                self._for_update("SELECT * FROM operational_analysis_members WHERE member_id=?"), (member_key,)
            ).fetchone()
            if row is None or str(row["analysis_run_id"]) != root_id:
                raise KeyError("分析成员不存在")
            existing_job = row["feature_job_id"]
            if existing_job is not None and str(existing_job) != job_key:
                raise OperationalLedgerConflict("分析成员已绑定其他 feature_job_id")
            if existing_job is not None:
                return _row_dict(row)
            if str(root["status"]) in _TERMINAL_ROOT_STATUSES:
                raise OperationalLedgerConflict("终态分析根不可绑定成员")
            current.execute(
                "UPDATE operational_analysis_members SET feature_job_id=?, updated_at=? WHERE member_id=?",
                (job_key, utc_now(), member_key),
            )
            updated = current.execute(
                "SELECT * FROM operational_analysis_members WHERE member_id=?", (member_key,)
            ).fetchone()
            assert updated is not None
            return _row_dict(updated)

    def _settle_analysis_run(self, connection: Any, root_id: str, root: Any) -> None:
        if str(root["status"]) == "completed":
            return
        if str(root["status"]) in {"failed", "cancelled"}:
            return
        member_counts = connection.execute(
            "SELECT COUNT(*) AS registered, SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed "
            "FROM operational_analysis_members WHERE analysis_run_id=?",
            (root_id,),
        ).fetchone()
        registered = int(member_counts["registered"] or 0)
        completed = int(member_counts["completed"] or 0)
        expected = int(root["expected_members"])
        if registered != expected or completed != expected:
            return

        completed_member_time = connection.execute(
            "SELECT MAX(updated_at) AS completed_at FROM operational_analysis_members "
            "WHERE analysis_run_id=? AND status='completed'",
            (root_id,),
        ).fetchone()
        completed_at = str((completed_member_time["completed_at"] if completed_member_time else None) or utc_now())
        if str(root["provenance"]) == "verified":
            source_rows = connection.execute(
                "SELECT DISTINCT source_id FROM operational_analysis_ranges WHERE analysis_run_id=? ORDER BY source_id",
                (root_id,),
            ).fetchall()
            for source_row in source_rows:
                source_id = str(source_row["source_id"])
                source = connection.execute(
                    self._for_update("SELECT source_id FROM operational_sources WHERE source_id=?"), (source_id,)
                ).fetchone()
                if source is None:
                    raise OperationalLedgerError("分析结算引用了不存在来源")
                root_rows = connection.execute(
                    "SELECT partition_key, start_position, end_position FROM operational_analysis_ranges "
                    "WHERE analysis_run_id=? AND source_id=? ORDER BY partition_key, start_position, end_position",
                    (root_id, source_id),
                ).fetchall()
                existing_rows = connection.execute(
                    "SELECT partition_key, start_position, end_position FROM operational_source_ranges "
                    "WHERE source_id=? AND kind='covered' ORDER BY partition_key, start_position, end_position",
                    (source_id,),
                ).fetchall()
                root_ranges = self._range_rows(root_rows)
                existing_ranges = self._range_rows(existing_rows)
                uncovered = subtract_ranges(root_ranges, existing_ranges)
                for item in uncovered:
                    range_id = hashlib.sha256(
                        _json(
                            {
                                "source_id": source_id,
                                "kind": "covered",
                                "partition_key": item["partition_key"],
                                "start": item["start"],
                                "end": item["end"],
                                "origin_id": root_id,
                            }
                        ).encode("utf-8")
                    ).hexdigest()
                    connection.execute(
                        "INSERT INTO operational_source_ranges(range_id, source_id, kind, partition_key, start_position, end_position, first_event_at, origin_id) "
                        "VALUES (?, ?, 'covered', ?, ?, ?, ?, ?)",
                        (range_id, source_id, item["partition_key"], item["start"], item["end"], completed_at, root_id),
                    )
        connection.execute(
            "UPDATE operational_analysis_runs SET status='completed', completed_at=?, settled_at=?, updated_at=? WHERE analysis_run_id=?",
            (completed_at, completed_at, completed_at, root_id),
        )

    def finish_analysis_member(
        self,
        analysis_run_id: str,
        member_id: str,
        status: str,
        *,
        finished_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id")
        member_key = _text(member_id, "member_id")
        target = _text(status, "status")
        if target not in _MEMBER_STATUSES:
            raise OperationalLedgerError("analysis member status 无效")
        requested_finished = _canonical_timestamp(finished_at, "finished_at") if finished_at is not None else None
        with self._write_context(connection) as current:
            root = current.execute(
                self._for_update("SELECT * FROM operational_analysis_runs WHERE analysis_run_id=?"), (root_id,)
            ).fetchone()
            if root is None:
                raise KeyError(f"分析根不存在: {root_id}")
            member = current.execute(
                self._for_update("SELECT * FROM operational_analysis_members WHERE member_id=?"), (member_key,)
            ).fetchone()
            if member is None or str(member["analysis_run_id"]) != root_id:
                raise KeyError("分析成员不存在")
            current_status = str(member["status"])
            root_status = str(root["status"])
            if root_status == "completed":
                if current_status == target:
                    return _row_dict(member)
                raise OperationalLedgerConflict("已结算分析根不可修改")
            if root_status == "cancelled" or current_status == "cancelled":
                if current_status == target:
                    return _row_dict(member)
                raise OperationalLedgerConflict("取消后的分析成员不可修改")
            if current_status == "completed":
                if target == "completed":
                    return _row_dict(member)
                raise OperationalLedgerConflict("已完成成员不可回退")
            if current_status == "failed" and target == "failed":
                return _row_dict(member)
            now = requested_finished or utc_now()
            if now < str(member["created_at"]) or now < str(root["created_at"]):
                raise OperationalLedgerError("finished_at 不得早于 root/member 创建时间")
            current.execute(
                "UPDATE operational_analysis_members SET status=?, updated_at=? WHERE member_id=?",
                (target, now, member_key),
            )
            if target == "cancelled":
                current.execute(
                    "UPDATE operational_analysis_runs SET status='cancelled', updated_at=? WHERE analysis_run_id=?",
                    (now, root_id),
                )
            elif target in {"failed", "partial"}:
                current.execute(
                    "UPDATE operational_analysis_runs SET status='partial', updated_at=? WHERE analysis_run_id=? AND status NOT IN ('completed', 'cancelled', 'failed')",
                    (now, root_id),
                )
            elif target == "running":
                current.execute(
                    "UPDATE operational_analysis_runs SET status='running', updated_at=? WHERE analysis_run_id=? AND status IN ('pending', 'partial')",
                    (now, root_id),
                )
            elif target == "completed":
                current.execute(
                    "UPDATE operational_analysis_runs SET status='running', updated_at=? WHERE analysis_run_id=? AND status IN ('pending', 'partial')",
                    (now, root_id),
                )
            refreshed_root = current.execute(
                self._for_update("SELECT * FROM operational_analysis_runs WHERE analysis_run_id=?"), (root_id,)
            ).fetchone()
            assert refreshed_root is not None
            if target == "completed":
                self._settle_analysis_run(current, root_id, refreshed_root)
            updated = current.execute(
                "SELECT * FROM operational_analysis_members WHERE member_id=?", (member_key,)
            ).fetchone()
            assert updated is not None
            return _row_dict(updated)

    def complete_analysis_run(
        self,
        analysis_run_id: str,
        *,
        completed_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id")
        if completed_at is not None:
            _canonical_timestamp(completed_at, "completed_at")
        with self._write_context(connection) as current:
            root = current.execute(
                self._for_update("SELECT * FROM operational_analysis_runs WHERE analysis_run_id=?"), (root_id,)
            ).fetchone()
            if root is None:
                raise KeyError(f"分析根不存在: {root_id}")
            if str(root["status"]) == "completed":
                return self._analysis_detail(current, root_id)
            # Historical settlement times enter through finish_analysis_member.
            # Keep this helper idempotent and never date an incomplete root.
            self._settle_analysis_run(current, root_id, root)
            return self._analysis_detail(current, root_id)

    def get_analysis_run(self, analysis_run_id: str, *, connection: Any = None) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id")
        with (nullcontext(connection) if connection is not None else self.database.connect()) as current:
            return self._analysis_detail(current, root_id)

    @staticmethod
    def _call_result(row: Any) -> dict[str, Any]:
        result = _row_dict(row)
        result["invalid_usage"] = bool(result.get("invalid_usage"))
        result["usage"] = {field: result.get(field) for field in _TOKEN_FIELDS}
        result["usage"].update(
            {
                "invalid_usage": result["invalid_usage"],
                "usage_quality": result.get("usage_quality"),
            }
        )
        return result

    def prepare_call(
        self,
        *,
        call_id: str,
        logical_call_id: str,
        attempt_index: int,
        analysis_run_id: str | None = None,
        environment: str,
        scope_key: str,
        call_kind: str,
        provider: str | None = None,
        model: str | None = None,
        tool_name: str | None = None,
        caller_kind: str,
        caller_id: str | None = None,
        metadata_contract: str = "v1",
        connection: Any = None,
    ) -> dict[str, Any]:
        call_key = _text(call_id, "call_id")
        logical_key = _text(logical_call_id, "logical_call_id")
        index = _nullable_int(attempt_index, "attempt_index", allow_none=False)
        assert index is not None
        env = _text(environment, "environment")
        scope = _text(scope_key, "scope_key")
        kind = _text(call_kind, "call_kind")
        if kind not in _CALL_KINDS:
            raise OperationalLedgerError("call_kind 无效")
        caller = _text(caller_kind, "caller_kind")
        contract = _text(metadata_contract, "metadata_contract")
        optional = {
            field: (_text(value, field) if value is not None else None)
            for field, value in (("provider", provider), ("model", model), ("tool_name", tool_name), ("caller_id", caller_id))
        }
        root_id = _text(analysis_run_id, "analysis_run_id") if analysis_run_id is not None else None
        with self._write_context(connection) as current:
            if root_id is not None:
                root = current.execute(
                    "SELECT environment, scope_key FROM operational_analysis_runs WHERE analysis_run_id=?", (root_id,)
                ).fetchone()
                if root is None:
                    raise KeyError(f"分析根不存在: {root_id}")
                if str(root["environment"]) != env or str(root["scope_key"]) != scope:
                    raise OperationalLedgerError("physical call 与分析根环境或作用域不一致")
            existing = current.execute(
                "SELECT * FROM operational_physical_calls WHERE call_id=?", (call_key,)
            ).fetchone()
            immutable = {
                "analysis_run_id": root_id,
                "logical_call_id": logical_key,
                "attempt_index": index,
                "environment": env,
                "scope_key": scope,
                "call_kind": kind,
                "provider": optional["provider"],
                "model": optional["model"],
                "tool_name": optional["tool_name"],
                "caller_kind": caller,
                "caller_id": optional["caller_id"],
                "metadata_contract": contract,
            }
            if existing is not None:
                for field, expected in immutable.items():
                    if existing[field] != expected:
                        raise OperationalLedgerConflict("相同 call_id 的物理调用字段冲突")
                return self._call_result(existing)
            same_logical = current.execute(
                "SELECT call_id FROM operational_physical_calls WHERE environment=? AND logical_call_id=? AND attempt_index=?",
                (env, logical_key, index),
            ).fetchone()
            if same_logical is not None:
                raise OperationalLedgerConflict("相同 logical_call_id/attempt_index 已有其他 call_id")
            now = utc_now()
            current.execute(
                "INSERT INTO operational_physical_calls(call_id, analysis_run_id, logical_call_id, attempt_index, environment, scope_key, call_kind, provider, model, tool_name, caller_kind, caller_id, status, prepared_at, started_at, finished_at, input_tokens, output_tokens, total_tokens, cached_input_tokens, reasoning_tokens, usage_quality, invalid_usage, error_code, provenance, metadata_contract) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'unknown', ?, NULL, ?, ?)",
                (
                    call_key,
                    root_id,
                    logical_key,
                    index,
                    env,
                    scope,
                    kind,
                    optional["provider"],
                    optional["model"],
                    optional["tool_name"],
                    caller,
                    optional["caller_id"],
                    now,
                    0,
                    "verified" if root_id is not None else "unknown",
                    contract,
                ),
            )
            row = current.execute(
                "SELECT * FROM operational_physical_calls WHERE call_id=?", (call_key,)
            ).fetchone()
            assert row is not None
            return self._call_result(row)

    def start_call(
        self,
        call_id: str,
        *,
        started_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        call_key = _text(call_id, "call_id")
        started = _canonical_timestamp(started_at, "started_at") if started_at is not None else utc_now()
        with self._write_context(connection) as current:
            row = current.execute(
                self._for_update("SELECT * FROM operational_physical_calls WHERE call_id=?"), (call_key,)
            ).fetchone()
            if row is None:
                raise KeyError(f"物理调用不存在: {call_key}")
            status = str(row["status"])
            if status == "prepared":
                current.execute(
                    "UPDATE operational_physical_calls SET status='started', started_at=? WHERE call_id=?",
                    (started, call_key),
                )
            elif status == "started":
                return self._call_result(row)
            elif status in _TERMINAL_CALL_STATUSES:
                return self._call_result(row)
            else:
                raise OperationalLedgerConflict("物理调用状态不可开始")
            updated = current.execute(
                "SELECT * FROM operational_physical_calls WHERE call_id=?", (call_key,)
            ).fetchone()
            assert updated is not None
            return self._call_result(updated)

    def finish_call(
        self,
        call_id: str,
        *,
        status: str,
        usage: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        finished_at: str | datetime | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        call_key = _text(call_id, "call_id")
        target = _text(status, "status")
        if target not in _TERMINAL_CALL_STATUSES:
            raise OperationalLedgerError("finish_call status 必须是 succeeded、failed 或 unknown")
        normalized_usage = normalize_usage(usage)
        error = _text(error_code, "error_code") if error_code is not None else None
        with self._write_context(connection) as current:
            row = current.execute(
                self._for_update("SELECT * FROM operational_physical_calls WHERE call_id=?"), (call_key,)
            ).fetchone()
            if row is None:
                raise KeyError(f"物理调用不存在: {call_key}")
            existing_status = str(row["status"])
            if existing_status in _TERMINAL_CALL_STATUSES and existing_status != target:
                raise OperationalLedgerConflict("物理调用终态冲突")
            merged: dict[str, Any] = {}
            for field in _TOKEN_FIELDS:
                old = row[field]
                new = normalized_usage[field]
                if old is not None and new is not None and int(old) != int(new):
                    raise OperationalLedgerConflict(f"物理调用 {field} 已有冲突值")
                merged[field] = int(old) if old is not None else new
            old_invalid = bool(row["invalid_usage"])
            merged_invalid = old_invalid or bool(normalized_usage["invalid_usage"])
            if (
                merged["total_tokens"] is None
                and merged["input_tokens"] is not None
                and merged["output_tokens"] is not None
            ):
                derived_total = merged["input_tokens"] + merged["output_tokens"]
                if derived_total > MAX_INT64:
                    merged_invalid = True
                else:
                    merged["total_tokens"] = derived_total
            if merged_invalid:
                quality = "invalid"
            elif merged["total_tokens"] is not None:
                quality = "known"
            elif any(merged[field] is not None for field in _TOKEN_FIELDS):
                quality = "partial"
            else:
                quality = "unknown"
            old_error = row["error_code"]
            if old_error is not None and error is not None and str(old_error) != error:
                raise OperationalLedgerConflict("物理调用 error_code 已有冲突值")
            final_error = str(old_error) if old_error is not None else error
            final_finished = row["finished_at"]
            if final_finished is not None and finished_at is not None:
                supplied_finished = _canonical_timestamp(finished_at, "finished_at")
                if str(final_finished) != supplied_finished:
                    raise OperationalLedgerConflict("物理调用 finished_at 已有冲突值")
            elif final_finished is None:
                final_finished = _canonical_timestamp(finished_at, "finished_at") if finished_at is not None else utc_now()
            current.execute(
                "UPDATE operational_physical_calls SET status=?, finished_at=?, input_tokens=?, output_tokens=?, total_tokens=?, cached_input_tokens=?, reasoning_tokens=?, usage_quality=?, invalid_usage=?, error_code=? WHERE call_id=?",
                (
                    target,
                    final_finished,
                    merged["input_tokens"],
                    merged["output_tokens"],
                    merged["total_tokens"],
                    merged["cached_input_tokens"],
                    merged["reasoning_tokens"],
                    quality,
                    1 if merged_invalid else 0,
                    final_error,
                    call_key,
                ),
            )
            updated = current.execute(
                "SELECT * FROM operational_physical_calls WHERE call_id=?", (call_key,)
            ).fetchone()
            assert updated is not None
            return self._call_result(updated)

    def record_backfill_item(
        self,
        *,
        item_key: str,
        source_digest: str,
        status: str,
        reason_code: str | None = None,
        connection: Any = None,
    ) -> dict[str, Any]:
        """Persist one idempotent historical-import decision."""

        key = _text(item_key, "item_key")
        digest = _text(source_digest, "source_digest")
        disposition = _text(status, "status")
        if disposition not in {"applied", "skipped"}:
            raise OperationalLedgerError("backfill status 必须是 applied 或 skipped")
        reason = _text(reason_code, "reason_code") if reason_code is not None else None
        with self._write_context(connection) as current:
            existing = current.execute(
                self._for_update("SELECT * FROM operational_backfill_items WHERE item_key=?"), (key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["source_digest"] != digest
                    or str(existing["status"]) != disposition
                    or existing["reason_code"] != reason
                ):
                    raise OperationalLedgerConflict("backfill item 已有冲突内容")
                return _row_dict(existing)
            now = utc_now()
            current.execute(
                "INSERT INTO operational_backfill_items(item_key, source_digest, status, reason_code, updated_at) VALUES (?, ?, ?, ?, ?)",
                (key, digest, disposition, reason, now),
            )
            row = current.execute(
                "SELECT * FROM operational_backfill_items WHERE item_key=?", (key,)
            ).fetchone()
            assert row is not None
            return _row_dict(row)


__all__ = [
    "MAX_INT64",
    "METRIC_VERSION",
    "OperationalLedgerConflict",
    "OperationalLedgerError",
    "OperationalLedgerRepository",
    "canonical_ranges",
    "ingestion_batch_id",
    "normalize_usage",
    "source_identity",
    "subtract_ranges",
    "tokens_per_1000",
]
