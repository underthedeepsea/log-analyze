from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping

from logrisk.operational_ledgers import tokens_per_1000


METRIC_VERSION = "operational-ledgers-v1"
DISPLAY_TIMEZONE = "Asia/Shanghai"
DEFAULT_ENVIRONMENT = "production"
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
_CURSOR_VERSION = 1
_ENVIRONMENTS = {"production", "local-test", "all"}
_TIME_BASES = {"activity", "completed_run_cohort"}
_FILTER_KEYS = {
    "environment",
    "scope_key",
    "from",
    "to",
    "time_basis",
    "provider",
    "model",
    "page_size",
    "cursor",
}
_PUBLIC_FILTER_KEYS = (
    "environment",
    "scope_key",
    "from",
    "to",
    "time_basis",
    "provider",
    "model",
    "page_size",
)
_RUN_COLUMNS = (
    "analysis_run_id",
    "environment",
    "scope_key",
    "request_key",
    "input_job_id",
    "source_id",
    "parent_run_id",
    "provenance",
    "status",
    "expected_members",
    "input_count",
    "reported_input_count",
    "created_at",
    "updated_at",
    "completed_at",
    "settled_at",
)
_CALL_COLUMNS = (
    "call_id",
    "analysis_run_id",
    "logical_call_id",
    "attempt_index",
    "environment",
    "scope_key",
    "call_kind",
    "provider",
    "model",
    "tool_name",
    "caller_kind",
    "caller_id",
    "status",
    "prepared_at",
    "started_at",
    "finished_at",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "reasoning_tokens",
    "usage_quality",
    "invalid_usage",
    "error_code",
    "provenance",
    "metadata_contract",
)


class OperationalAnalyticsError(ValueError):
    """A validation or cursor error safe for API adapters to expose."""

    def __init__(self, message: str, *, code: str = "invalid_filters") -> None:
        super().__init__(message)
        self.code = code
        self.status_code = 422


def _row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {str(key): row[key] for key in keys()}
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _parse_timestamp(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise OperationalAnalyticsError(f"{field} 不能为空", code="invalid_timestamp")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OperationalAnalyticsError(f"{field} 时间格式无效", code="invalid_timestamp") from exc
    else:
        raise OperationalAnalyticsError(f"{field} 时间格式无效", code="invalid_timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OperationalAnalyticsError(f"{field} 必须包含时区", code="invalid_timestamp")
    return parsed.astimezone(timezone.utc).isoformat()


def _filter_text(value: Any, field: str, *, maximum: int = 256) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise OperationalAnalyticsError(f"{field} 必须是字符串")
    text = value.strip()
    if not text:
        raise OperationalAnalyticsError(f"{field} 不能为空")
    if len(text) > maximum:
        raise OperationalAnalyticsError(f"{field} 过长")
    return text


def _page_size(value: Any) -> int:
    if value is None:
        return DEFAULT_PAGE_SIZE
    if isinstance(value, bool):
        raise OperationalAnalyticsError("page_size 必须是正整数")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.isdigit():
        number = int(value)
    else:
        raise OperationalAnalyticsError("page_size 必须是正整数")
    if number < 1 or number > MAX_PAGE_SIZE:
        raise OperationalAnalyticsError(f"page_size 必须在 1 到 {MAX_PAGE_SIZE} 之间")
    return number


def _filter_digest(filters: Mapping[str, Any]) -> str:
    payload = {key: filters.get(key) for key in _PUBLIC_FILTER_KEYS}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_filters(filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Validate and canonicalize filters shared by every analytics entrypoint."""

    if filters is None:
        source: Mapping[str, Any] = {}
    elif isinstance(filters, Mapping):
        source = filters
    else:
        raise OperationalAnalyticsError("analytics filters 必须是 JSON object")
    unknown = sorted(str(key) for key in source if key not in _FILTER_KEYS)
    if unknown:
        raise OperationalAnalyticsError(f"analytics filters 包含未知字段: {', '.join(unknown)}")

    environment = source.get("environment", DEFAULT_ENVIRONMENT)
    if environment is None:
        environment = DEFAULT_ENVIRONMENT
    if not isinstance(environment, str) or environment not in _ENVIRONMENTS:
        raise OperationalAnalyticsError("environment 无效")
    scope_key = _filter_text(source.get("scope_key"), "scope_key")
    time_basis = source.get("time_basis", "activity")
    if time_basis is None:
        time_basis = "activity"
    if not isinstance(time_basis, str) or time_basis not in _TIME_BASES:
        raise OperationalAnalyticsError("time_basis 无效")
    provider = _filter_text(source.get("provider"), "provider")
    model = _filter_text(source.get("model"), "model")
    from_value = _parse_timestamp(source.get("from"), "from")
    to_value = _parse_timestamp(source.get("to"), "to")
    if from_value is not None and to_value is not None and from_value >= to_value:
        raise OperationalAnalyticsError("from 必须早于 to", code="invalid_time_range")
    cursor = source.get("cursor")
    if cursor is not None:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 4096:
            raise OperationalAnalyticsError("cursor 无效", code="invalid_cursor")
    result = {
        "environment": environment,
        "scope_key": scope_key,
        "from": from_value,
        "to": to_value,
        "time_basis": time_basis,
        "provider": provider,
        "model": model,
        "page_size": _page_size(source.get("page_size")),
        "cursor": cursor,
    }
    result["filter_digest"] = _filter_digest(result)
    return result


normalize_analytics_filters = normalize_filters


def _public_filters(filters: Mapping[str, Any]) -> dict[str, Any]:
    return {key: filters.get(key) for key in _PUBLIC_FILTER_KEYS}


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _metric_count(value: Any) -> int:
    return int(value or 0)


def _environment_clause(alias: str, filters: Mapping[str, Any]) -> tuple[list[str], list[Any]]:
    environment = filters["environment"]
    if environment == "all":
        return [], []
    return [f"{alias}.environment = ?"], [environment]


def _scope_clause(alias: str, filters: Mapping[str, Any]) -> tuple[list[str], list[Any]]:
    scope_key = filters.get("scope_key")
    if scope_key is None:
        return [], []
    return [f"{alias}.scope_key = ?"], [scope_key]


def _window_clause(alias: str, column: str, filters: Mapping[str, Any], *, lifetime: bool = False) -> tuple[list[str], list[Any]]:
    if lifetime:
        return [], []
    clauses: list[str] = []
    params: list[Any] = []
    if filters.get("from") is not None:
        clauses.append(f"{alias}.{column} >= ?")
        params.append(filters["from"])
    if filters.get("to") is not None:
        clauses.append(f"{alias}.{column} < ?")
        params.append(filters["to"])
    return clauses, params


def _activity_root_clause(alias: str, filters: Mapping[str, Any], *, lifetime: bool = False) -> tuple[list[str], list[Any]]:
    if lifetime or (filters.get("from") is None and filters.get("to") is None):
        return [], []
    terms: list[str] = []
    params: list[Any] = []
    for column in ("updated_at", "created_at"):
        clauses, values = _window_clause(alias, column, filters)
        if not clauses:
            terms.append(f"{alias}.{column} IS NOT NULL")
        else:
            terms.append(" AND ".join(clauses))
            params.extend(values)
    return ["(" + " OR ".join(terms) + ")"], params


def _cohort_root_clause(alias: str, filters: Mapping[str, Any], *, lifetime: bool = False) -> tuple[list[str], list[Any]]:
    clauses = [f"{alias}.status = 'completed'", f"{alias}.completed_at IS NOT NULL"]
    params: list[Any] = []
    date_clauses, date_params = _window_clause(alias, "completed_at", filters, lifetime=lifetime)
    clauses.extend(date_clauses)
    params.extend(date_params)
    return clauses, params


def _cursor_payload(cursor: str, filters: Mapping[str, Any]) -> dict[str, str]:
    try:
        raw = base64.b64decode(
            cursor.encode("ascii") + b"=" * (-len(cursor) % 4),
            altchars=b"-_",
            validate=True,
        )
        payload = json.loads(raw.decode("utf-8"))
    except (binascii.Error, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise OperationalAnalyticsError("cursor 无效", code="invalid_cursor") from exc
    if not isinstance(payload, Mapping) or payload.get("version") != _CURSOR_VERSION:
        raise OperationalAnalyticsError("cursor 版本无效", code="invalid_cursor")
    if payload.get("filter_digest") != filters["filter_digest"]:
        raise OperationalAnalyticsError("cursor 与当前筛选条件不匹配", code="invalid_cursor")
    timestamp = payload.get("timestamp")
    item_id = payload.get("id")
    if not isinstance(timestamp, str) or not isinstance(item_id, str) or not timestamp or not item_id:
        raise OperationalAnalyticsError("cursor 排序位置无效", code="invalid_cursor")
    return {"timestamp": timestamp, "id": item_id}


def _make_cursor(timestamp: str, item_id: str, filters: Mapping[str, Any]) -> str:
    payload = {
        "version": _CURSOR_VERSION,
        "timestamp": timestamp,
        "id": item_id,
        "filter_digest": filters["filter_digest"],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def _safe_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    text = str(value)
    stripped = text.lstrip(" \t\r\n")
    leading = text[: len(text) - len(stripped)]
    if any(character in leading for character in "\t\r") or (stripped and stripped[0] in "=+-@"):
        return "'" + text
    return text


class OperationalAnalytics:
    """Read-only, bounded SQL views over the operational ledger tables."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository
        self.database = getattr(repository, "database", repository)
        if not callable(getattr(self.database, "connect", None)):
            raise TypeError("OperationalAnalytics 需要带 connect() 的 ledger repository")

    def _range_union(self, connection: Any, filters: Mapping[str, Any], *, kind: str, lifetime: bool) -> int:
        source_clauses, source_params = _environment_clause("s", filters)
        scope_clauses, scope_params = _scope_clause("s", filters)
        event_clauses, event_params = _window_clause("sr", "first_event_at", filters, lifetime=lifetime)
        where = ["sr.kind = ?", *source_clauses, *scope_clauses, *event_clauses]
        params: list[Any] = [kind, *source_params, *scope_params, *event_params]
        query = (
            "SELECT sr.source_id, sr.partition_key, sr.start_position, sr.end_position "
            "FROM operational_source_ranges sr JOIN operational_sources s ON s.source_id=sr.source_id "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY sr.source_id, sr.partition_key, sr.start_position, sr.end_position"
        )
        return self._union_rows(connection.execute(query, params))

    @staticmethod
    def _union_rows(rows: Any) -> int:
        total = 0
        active_source: str | None = None
        active_partition: str | None = None
        active_start: int | None = None
        active_end: int | None = None
        for row in rows:
            source = str(row["source_id"])
            partition = str(row["partition_key"])
            start = int(row["start_position"])
            end = int(row["end_position"])
            if end <= start:
                continue
            if (
                active_source != source
                or active_partition != partition
                or active_start is None
                or start > active_end
            ):
                if active_start is not None and active_end is not None:
                    total += active_end - active_start
                active_source, active_partition = source, partition
                active_start, active_end = start, end
            elif end > active_end:
                active_end = end
        if active_start is not None and active_end is not None:
            total += active_end - active_start
        return total

    def _cohort_union(self, connection: Any, filters: Mapping[str, Any], *, lifetime: bool) -> int:
        clauses, params = _environment_clause("r", filters)
        scope_clauses, scope_params = _scope_clause("r", filters)
        root_clauses, root_params = _cohort_root_clause("r", filters, lifetime=lifetime)
        clauses = [*clauses, *scope_clauses, *root_clauses, "r.provenance='verified'"]
        params = [*params, *scope_params, *root_params]
        query = (
            "SELECT ar.source_id, ar.partition_key, ar.start_position, ar.end_position "
            "FROM operational_analysis_ranges ar "
            "JOIN operational_analysis_runs r ON r.analysis_run_id=ar.analysis_run_id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY ar.source_id, ar.partition_key, ar.start_position, ar.end_position"
        )
        return self._union_rows(connection.execute(query, params))

    def _run_metrics(self, connection: Any, filters: Mapping[str, Any], *, lifetime: bool) -> dict[str, int]:
        environment_clauses, environment_params = _environment_clause("r", filters)
        scope_clauses, scope_params = _scope_clause("r", filters)
        base = [*environment_clauses, *scope_clauses]
        base_params = [*environment_params, *scope_params]
        if filters["time_basis"] == "completed_run_cohort":
            completed_time_clauses, completed_time_params = _window_clause("r", "completed_at", filters, lifetime=lifetime)
            workload_time_clauses, workload_time_params = [], []
        else:
            completed_time_clauses, completed_time_params = _window_clause("r", "completed_at", filters, lifetime=lifetime)
            workload_time_clauses, workload_time_params = _window_clause("r", "settled_at", filters, lifetime=lifetime)
        completed_where = [
            *base,
            "r.status = 'completed'",
            "r.completed_at IS NOT NULL",
            *completed_time_clauses,
        ]
        completed_params = [*base_params, *completed_time_params]
        query = (
            "SELECT COUNT(*) AS completed_runs, "
            "COALESCE(SUM(CASE WHEN r.provenance='verified' AND r.input_count IS NOT NULL "
            "AND r.settled_at IS NOT NULL THEN r.input_count ELSE 0 END), 0) AS analysis_workload, "
            "COALESCE(SUM(CASE WHEN r.provenance<>'verified' AND r.reported_input_count IS NOT NULL "
            "AND r.settled_at IS NOT NULL THEN r.reported_input_count ELSE 0 END), 0) AS reported_workload, "
            "SUM(CASE WHEN r.provenance<>'verified' AND r.reported_input_count IS NOT NULL THEN 1 ELSE 0 END) AS reported_completed_runs, "
            "SUM(CASE WHEN r.provenance<>'verified' AND r.reported_input_count IS NULL THEN 1 ELSE 0 END) AS unknown_reported_runs, "
            "SUM(CASE WHEN r.provenance='verified' AND r.input_count IS NULL THEN 1 ELSE 0 END) AS unknown_exact_runs "
            "FROM operational_analysis_runs r "
            f"WHERE {' AND '.join(completed_where)}"
        )
        row = _row_dict(connection.execute(query, completed_params).fetchone())

        workload_where = [
            *base,
            "r.status = 'completed'",
            "r.completed_at IS NOT NULL",
            "r.settled_at IS NOT NULL",
            *workload_time_clauses,
        ]
        # Cohort selection is by completed_at; the workload date is deliberately
        # not reapplied so a settled root remains in its selected cohort.
        if filters["time_basis"] == "completed_run_cohort" and not lifetime:
            workload_where = completed_where
            workload_params = completed_params
        else:
            workload_params = [*base_params, *workload_time_params]
        if workload_where != completed_where:
            workload_query = (
                "SELECT COALESCE(SUM(CASE WHEN r.provenance='verified' AND r.input_count IS NOT NULL THEN r.input_count ELSE 0 END), 0) AS analysis_workload, "
                "COALESCE(SUM(CASE WHEN r.provenance<>'verified' AND r.reported_input_count IS NOT NULL THEN r.reported_input_count ELSE 0 END), 0) AS reported_workload "
                "FROM operational_analysis_runs r "
                f"WHERE {' AND '.join(workload_where)}"
            )
            workload_row = _row_dict(connection.execute(workload_query, workload_params).fetchone())
            row["analysis_workload"] = workload_row.get("analysis_workload")
            row["reported_workload"] = workload_row.get("reported_workload")

        partial_where = [*base, "r.status = 'partial'"]
        partial_time, partial_params = _window_clause("r", "updated_at", filters, lifetime=lifetime)
        partial_where.extend(partial_time)
        partial_query = (
            "SELECT COUNT(*) AS partial_runs FROM operational_analysis_runs r "
            f"WHERE {' AND '.join(partial_where)}"
        )
        partial_row = _row_dict(connection.execute(partial_query, [*base_params, *partial_params]).fetchone())
        return {
            "analysis_workload": _metric_count(row.get("analysis_workload")),
            "reported_workload": _metric_count(row.get("reported_workload")),
            "completed_runs": _metric_count(row.get("completed_runs")),
            "partial_runs": _metric_count(partial_row.get("partial_runs")),
            "reported_completed_runs": _metric_count(row.get("reported_completed_runs")),
            "unknown_reported_runs": _metric_count(row.get("unknown_reported_runs")),
            "unknown_exact_runs": _metric_count(row.get("unknown_exact_runs")),
        }

    def _call_metrics(self, connection: Any, filters: Mapping[str, Any], *, lifetime: bool) -> dict[str, Any]:
        params: list[Any] = []
        joins: list[str] = []
        clauses: list[str] = ["pc.started_at IS NOT NULL"]
        if filters["time_basis"] == "completed_run_cohort" and not lifetime:
            joins.append("JOIN operational_analysis_runs r ON r.analysis_run_id=pc.analysis_run_id")
            root_env, root_env_params = _environment_clause("r", filters)
            root_scope, root_scope_params = _scope_clause("r", filters)
            root_time, root_time_params = _cohort_root_clause("r", filters)
            clauses.extend([*root_env, *root_scope, *root_time])
            params.extend([*root_env_params, *root_scope_params, *root_time_params])
        else:
            call_env, call_env_params = _environment_clause("pc", filters)
            call_scope, call_scope_params = _scope_clause("pc", filters)
            clauses.extend([*call_env, *call_scope])
            params.extend([*call_env_params, *call_scope_params])
            if filters["time_basis"] == "activity":
                call_time, call_time_params = _window_clause("pc", "started_at", filters, lifetime=lifetime)
                clauses.extend(call_time)
                params.extend(call_time_params)
        # Provider/model filters select call rows.  They intentionally never
        # change the source or root workload queries.
        if filters.get("provider") is not None:
            clauses.append("pc.provider = ?")
            params.append(filters["provider"])
        if filters.get("model") is not None:
            clauses.append("pc.model = ?")
            params.append(filters["model"])
        query = (
            "SELECT "
            "SUM(CASE WHEN pc.call_kind='provider' THEN 1 ELSE 0 END) AS provider_calls, "
            "SUM(CASE WHEN pc.call_kind='agent_tool' THEN 1 ELSE 0 END) AS agent_tool_calls, "
            "SUM(CASE WHEN pc.call_kind='provider' AND pc.input_tokens IS NOT NULL THEN pc.input_tokens ELSE 0 END) AS input_tokens_sum, "
            "SUM(CASE WHEN pc.call_kind='provider' AND pc.input_tokens IS NOT NULL THEN 1 ELSE 0 END) AS input_known_count, "
            "SUM(CASE WHEN pc.call_kind='provider' AND pc.output_tokens IS NOT NULL THEN pc.output_tokens ELSE 0 END) AS output_tokens_sum, "
            "SUM(CASE WHEN pc.call_kind='provider' AND pc.output_tokens IS NOT NULL THEN 1 ELSE 0 END) AS output_known_count, "
            "SUM(CASE WHEN pc.call_kind='provider' AND pc.total_tokens IS NOT NULL THEN pc.total_tokens ELSE 0 END) AS known_total, "
            "SUM(CASE WHEN pc.call_kind='provider' AND pc.total_tokens IS NULL THEN 1 ELSE 0 END) AS unknown_call_count, "
            "SUM(CASE WHEN pc.call_kind='provider' AND COALESCE(pc.invalid_usage, 0) <> 0 THEN 1 ELSE 0 END) AS invalid_call_count "
            "FROM operational_physical_calls pc "
            + " ".join(joins)
            + f" WHERE {' AND '.join(clauses)}"
        )
        row = _row_dict(connection.execute(query, params).fetchone())
        provider_calls = _metric_count(row.get("provider_calls"))
        unknown_count = _metric_count(row.get("unknown_call_count"))
        input_known_count = _metric_count(row.get("input_known_count"))
        output_known_count = _metric_count(row.get("output_known_count"))
        return {
            "provider_calls": provider_calls,
            "agent_tool_calls": _metric_count(row.get("agent_tool_calls")),
            "tokens": {
                "input_tokens": 0 if provider_calls == 0 else (_metric_count(row.get("input_tokens_sum")) if input_known_count else None),
                "output_tokens": 0 if provider_calls == 0 else (_metric_count(row.get("output_tokens_sum")) if output_known_count else None),
                "total_tokens": 0 if provider_calls == 0 else (_metric_count(row.get("known_total")) if unknown_count == 0 else None),
                "known_total": _metric_count(row.get("known_total")),
                "unknown_call_count": unknown_count,
                "invalid_call_count": _metric_count(row.get("invalid_call_count")),
            },
        }

    def _efficiency(self, connection: Any, filters: Mapping[str, Any], *, lifetime: bool) -> dict[str, Any]:
        if filters["time_basis"] == "activity" and not lifetime:
            return {
                "tokens_per_1000_records": None,
                "known_tokens": None,
                "corresponding_workload": None,
                "partial": False,
                "reason": "activity_not_completed_cohort",
            }
        env_clauses, env_params = _environment_clause("r", filters)
        scope_clauses, scope_params = _scope_clause("r", filters)
        root_clauses, root_params = _cohort_root_clause("r", filters, lifetime=lifetime)
        clauses = [
            *env_clauses,
            *scope_clauses,
            *root_clauses,
            "r.provenance='verified'",
            "r.input_count IS NOT NULL",
            "pc.started_at IS NOT NULL",
            "pc.call_kind='provider'",
        ]
        params = [*env_params, *scope_params, *root_params]
        if filters.get("provider") is not None:
            clauses.append("pc.provider = ?")
            params.append(filters["provider"])
        if filters.get("model") is not None:
            clauses.append("pc.model = ?")
            params.append(filters["model"])
        query = (
            "SELECT pc.analysis_run_id, r.input_count, COUNT(*) AS call_count, "
            "SUM(CASE WHEN pc.total_tokens IS NOT NULL THEN 1 ELSE 0 END) AS known_count, "
            "COALESCE(SUM(CASE WHEN pc.total_tokens IS NOT NULL THEN pc.total_tokens ELSE 0 END), 0) AS known_tokens "
            "FROM operational_physical_calls pc "
            "JOIN operational_analysis_runs r ON r.analysis_run_id=pc.analysis_run_id "
            f"WHERE {' AND '.join(clauses)} "
            "GROUP BY pc.analysis_run_id, r.input_count"
        )
        rows = connection.execute(query, params)
        known_tokens = 0
        corresponding_workload = 0
        partial = False
        any_calls = False
        for row in rows:
            any_calls = True
            call_count = int(row["call_count"] or 0)
            known_count = int(row["known_count"] or 0)
            partial = partial or known_count < call_count
            if known_count:
                known_tokens += int(row["known_tokens"] or 0)
                corresponding_workload += int(row["input_count"] or 0)
        ratio = tokens_per_1000(known_tokens, corresponding_workload) if any_calls else None
        return {
            "tokens_per_1000_records": str(ratio) if ratio is not None else None,
            "known_tokens": known_tokens,
            "corresponding_workload": corresponding_workload,
            "partial": partial,
        }

    def _period_metrics(
        self, connection: Any, filters: Mapping[str, Any], *, lifetime: bool
    ) -> tuple[dict[str, Any], dict[str, int], dict[str, Any]]:
        cohort = filters["time_basis"] == "completed_run_cohort" and not lifetime
        if cohort:
            cohort_input = self._cohort_union(connection, filters, lifetime=False)
            ingested_records = cohort_input
            covered_records = cohort_input
            input_volume_basis = "completed_run_cohort"
        else:
            ingested_records = self._range_union(connection, filters, kind="ingested", lifetime=lifetime)
            covered_records = self._range_union(connection, filters, kind="covered", lifetime=lifetime)
            input_volume_basis = "ingestion_activity"
        runs = self._run_metrics(connection, filters, lifetime=lifetime)
        calls = self._call_metrics(connection, filters, lifetime=lifetime)
        efficiency = self._efficiency(connection, filters, lifetime=lifetime)
        return {
            "ingested_records": ingested_records,
            "covered_records": covered_records,
            "analysis_workload": runs["analysis_workload"],
            "reported_workload": runs["reported_workload"],
            "completed_runs": runs["completed_runs"],
            "partial_runs": runs["partial_runs"],
            "provider_calls": calls["provider_calls"],
            "agent_tool_calls": calls["agent_tool_calls"],
            "tokens": calls["tokens"],
            "efficiency": efficiency,
            "input_volume_basis": input_volume_basis,
        }, runs, calls

    def summary(self, filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
        normalized = normalize_filters(filters)
        with self.database.connect() as connection:
            lifetime, lifetime_runs, lifetime_calls = self._period_metrics(connection, normalized, lifetime=True)
            window, window_runs, window_calls = self._period_metrics(connection, normalized, lifetime=False)
        quality = {
            "reported_completed_runs": window_runs["reported_completed_runs"],
            "unverified_input_runs": window_runs["reported_completed_runs"],
            "unknown_reported_runs": window_runs["unknown_reported_runs"],
            "unknown_exact_runs": window_runs["unknown_exact_runs"],
            "unknown_usage_calls": window_calls["tokens"]["unknown_call_count"],
            "invalid_usage_calls": window_calls["tokens"]["invalid_call_count"],
            "partial": bool(window["partial_runs"] or window["efficiency"].get("partial")),
            "lifetime": {
                "reported_completed_runs": lifetime_runs["reported_completed_runs"],
                "unverified_input_runs": lifetime_runs["reported_completed_runs"],
                "unknown_reported_runs": lifetime_runs["unknown_reported_runs"],
                "unknown_exact_runs": lifetime_runs["unknown_exact_runs"],
                "unknown_usage_calls": lifetime_calls["tokens"]["unknown_call_count"],
                "invalid_usage_calls": lifetime_calls["tokens"]["invalid_call_count"],
                "partial": bool(lifetime["partial_runs"] or lifetime["efficiency"].get("partial")),
            },
        }
        return {
            "metric_version": METRIC_VERSION,
            "time_basis": normalized["time_basis"],
            "display_timezone": DISPLAY_TIMEZONE,
            "filters": _public_filters(normalized),
            "lifetime": lifetime,
            "window": window,
            "quality": quality,
        }

    @staticmethod
    def _run_order_expression() -> str:
        return "COALESCE(r.updated_at, r.created_at, r.analysis_run_id)"

    def _page(self, connection: Any, filters: Mapping[str, Any], *, kind: str) -> tuple[list[dict[str, Any]], str | None, bool]:
        cursor_position = _cursor_payload(filters["cursor"], filters) if filters.get("cursor") else None
        if kind == "runs":
            alias = "r"
            columns = ", ".join(f"r.{column}" for column in _RUN_COLUMNS)
            clauses, params = _environment_clause(alias, filters)
            scope_clauses, scope_params = _scope_clause(alias, filters)
            clauses.extend(scope_clauses)
            params.extend(scope_params)
            if filters["time_basis"] == "completed_run_cohort":
                root_clauses, root_params = _cohort_root_clause(alias, filters)
                clauses.extend(root_clauses)
                params.extend(root_params)
                order_expression = "r.completed_at"
            else:
                activity_clauses, activity_params = _activity_root_clause(alias, filters)
                clauses.extend(activity_clauses)
                params.extend(activity_params)
                order_expression = self._run_order_expression()
            item_id = "r.analysis_run_id"
            table = "operational_analysis_runs r"
        else:
            alias = "pc"
            columns = ", ".join(f"pc.{column}" for column in _CALL_COLUMNS)
            clauses = ["pc.started_at IS NOT NULL"]
            params = []
            joins = ""
            if filters["time_basis"] == "completed_run_cohort":
                joins = " JOIN operational_analysis_runs r ON r.analysis_run_id=pc.analysis_run_id"
                root_env, root_env_params = _environment_clause("r", filters)
                root_scope, root_scope_params = _scope_clause("r", filters)
                root_time, root_time_params = _cohort_root_clause("r", filters)
                clauses.extend([*root_env, *root_scope, *root_time])
                params.extend([*root_env_params, *root_scope_params, *root_time_params])
            else:
                call_env, call_env_params = _environment_clause(alias, filters)
                call_scope, call_scope_params = _scope_clause(alias, filters)
                call_time, call_time_params = _window_clause(alias, "started_at", filters)
                clauses.extend([*call_env, *call_scope, *call_time])
                params.extend([*call_env_params, *call_scope_params, *call_time_params])
            if filters.get("provider") is not None:
                clauses.append("pc.provider = ?")
                params.append(filters["provider"])
            if filters.get("model") is not None:
                clauses.append("pc.model = ?")
                params.append(filters["model"])
            item_id = "pc.call_id"
            order_expression = "pc.started_at"
            table = "operational_physical_calls pc" + joins
        if cursor_position:
            clauses.append(f"({order_expression} < ? OR ({order_expression} = ? AND {item_id} < ?))")
            params.extend([cursor_position["timestamp"], cursor_position["timestamp"], cursor_position["id"]])
        query = (
            f"SELECT {columns}, {order_expression} AS _analytics_order_at "
            f"FROM {table} WHERE {' AND '.join(clauses)} "
            f"ORDER BY {order_expression} DESC, {item_id} DESC LIMIT ?"
        )
        rows = connection.execute(query, [*params, filters["page_size"] + 1]).fetchall()
        has_more = len(rows) > filters["page_size"]
        rows = rows[: filters["page_size"]]
        items: list[dict[str, Any]] = []
        for row in rows:
            item = _row_dict(row)
            item.pop("_analytics_order_at", None)
            for key in ("expected_members", "input_count", "reported_input_count", "attempt_index", "input_tokens", "output_tokens", "total_tokens", "cached_input_tokens", "reasoning_tokens"):
                if key in item:
                    item[key] = _int_or_none(item[key])
            items.append(item)
        next_cursor = None
        if has_more and items:
            last = rows[-1]
            last_item = _row_dict(last)
            next_cursor = _make_cursor(str(last_item["_analytics_order_at"]), str(last_item[item_id.split(".")[-1]]), filters)
        return items, next_cursor, has_more

    def runs(self, filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
        normalized = normalize_filters(filters)
        with self.database.connect() as connection:
            items, next_cursor, has_more = self._page(connection, normalized, kind="runs")
        return {
            "items": items,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "metric_version": METRIC_VERSION,
            "time_basis": normalized["time_basis"],
            "filters": _public_filters(normalized),
        }

    def calls(self, filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
        normalized = normalize_filters(filters)
        with self.database.connect() as connection:
            items, next_cursor, has_more = self._page(connection, normalized, kind="calls")
        return {
            "items": items,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "metric_version": METRIC_VERSION,
            "time_basis": normalized["time_basis"],
            "filters": _public_filters(normalized),
        }

    def export_csv(self, filters: Mapping[str, Any] | None = None, kind: str = "runs") -> Iterator[bytes]:
        normalized = normalize_filters(filters)
        if kind not in {"runs", "calls"}:
            raise OperationalAnalyticsError("export kind 无效")
        return self._export_csv(normalized, kind)

    def _export_csv(self, filters: Mapping[str, Any], kind: str) -> Iterator[bytes]:
        columns = _RUN_COLUMNS if kind == "runs" else _CALL_COLUMNS
        metadata_columns = ("metric_version", "time_basis", "display_timezone", "environment", "scope_key", "provenance")
        header = [*metadata_columns, *columns]
        metadata = {
            "metric_version": METRIC_VERSION,
            "time_basis": filters["time_basis"],
            "display_timezone": DISPLAY_TIMEZONE,
            "environment": filters["environment"],
            "scope_key": filters.get("scope_key"),
            "provenance": "ledger",
        }
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\r\n")
        writer.writerow(header)
        yield b"\xef\xbb\xbf" + buffer.getvalue().encode("utf-8")
        cursor: str | None = None
        while True:
            page_filters = dict(filters)
            page_filters["cursor"] = cursor
            with self.database.connect() as connection:
                items, cursor, has_more = self._page(connection, page_filters, kind=kind)
            if not items:
                break
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer, lineterminator="\r\n")
            for item in items:
                values = [metadata[key] for key in metadata_columns]
                values.extend(item.get(column) for column in columns)
                writer.writerow([_safe_csv_value(value) for value in values])
            yield buffer.getvalue().encode("utf-8")
            if not has_more:
                break


__all__ = [
    "DEFAULT_ENVIRONMENT",
    "DEFAULT_PAGE_SIZE",
    "DISPLAY_TIMEZONE",
    "MAX_PAGE_SIZE",
    "METRIC_VERSION",
    "OperationalAnalytics",
    "OperationalAnalyticsError",
    "normalize_analytics_filters",
    "normalize_filters",
]
