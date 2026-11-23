from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from logrisk.agentic.workflow_repository import WorkflowRepository
from logrisk.database import Database, utc_now
from logrisk.server_risk.contracts import ServerRiskError, utc

DISPLAY_ZONE = ZoneInfo("Asia/Shanghai")
ENVIRONMENT = "production"
PUBLIC_IDENTIFIER = re.compile(r"[\w%][\w .:/%\-]{0,127}")
PRIVATE_IDENTIFIER = re.compile(
    r"(?:[A-Za-z0-9]{32,}|(?:AKIA|ASIA)[A-Za-z0-9]{16}|(?:^|[ /.:_%-])(?:sk[-_]|pk[-_]|gh[pousr]_|github_pat_|xox[baprs]-|AIza|eyJ)"
    r"|api[_-]?key|password|authorization|access[_-]?token|client[_-]?secret)",
    flags=re.IGNORECASE,
)
SOURCE_EXCLUSIONS = {"synthetic", "demo", "demonstration", "simulation", "local-test"}

USAGE_SUMS = """
    COUNT(*) AS started_calls,
    COUNT(CASE WHEN invalid_usage=0 AND
        (input_tokens IS NOT NULL OR output_tokens IS NOT NULL OR total_tokens IS NOT NULL)
        THEN 1 END) AS receipted_calls,
    COUNT(CASE WHEN invalid_usage=0 AND total_tokens IS NOT NULL
        THEN 1 END) AS total_receipts,
    COUNT(CASE WHEN invalid_usage=0 AND total_tokens IS NOT NULL AND
        input_tokens IS NOT NULL AND output_tokens IS NOT NULL
        THEN 1 END) AS complete_calls,
    SUM(CASE WHEN invalid_usage=0 THEN input_tokens END) AS input_tokens,
    SUM(CASE WHEN invalid_usage=0 THEN output_tokens END) AS output_tokens,
    SUM(CASE WHEN invalid_usage=0 THEN
        COALESCE(total_tokens, input_tokens, output_tokens)
        END) AS known_tokens
"""
USAGE_WHERE = "environment=? AND call_kind='provider' AND started_at>=? AND started_at<=?"


def _data(value: Any) -> dict[str, Any]:
    try:
        result = json.loads(value) if isinstance(value, str) else value
    except ValueError:
        return {}
    return result if isinstance(result, dict) else {}


def _safe_identifier(value: Any) -> str | None:
    if not isinstance(value, str) or not PUBLIC_IDENTIFIER.fullmatch(value):
        return None
    return value if "://" not in value and not PRIVATE_IDENTIFIER.search(value) else None


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _block(
    data: dict[str, Any] | None, *, reason: str | None = None, unavailable: bool = False,
) -> dict[str, Any]:
    return {
        "status": "unavailable" if unavailable else "partial" if reason else "available",
        "data": data,
        **({"reason": reason} if reason else {}),
    }


def _usage_totals(row: Any) -> dict[str, int | float | None]:
    started, receipted, complete, total_receipts = (
        int(row[field]) for field in ("started_calls", "receipted_calls", "complete_calls", "total_receipts")
    )
    counts = {key: int(row[key]) if row[key] is not None else None for key in (
        "input_tokens", "output_tokens", "known_tokens",
    )}
    return {
        "started_calls": started,
        "receipted_calls": receipted,
        "complete_calls": complete,
        "receipt_rate": receipted / started if started else None,
        "total_tokens": counts["known_tokens"] if started and total_receipts == started else None,
        **counts,
    }


class StreamOverview:
    """Read-only workbench facts. Every block retains its own evidence and coverage."""

    def __init__(
        self, database: Database, risk_service: Any, *, workflows_enabled: bool,
        clock: Callable[[], str] = utc_now,
    ) -> None:
        self.database = database
        self.risk_service = risk_service
        self.workflows = WorkflowRepository(database)
        self.workflows_enabled = workflows_enabled
        self.clock = clock

    def read(self) -> dict[str, Any]:
        now = utc(self.clock())
        midnight = now.astimezone(DISPLAY_ZONE).replace(hour=0, minute=0, second=0, microsecond=0)
        dates = [midnight - timedelta(days=offset) for offset in range(6, -1, -1)]
        return {
            "as_of": now.isoformat(),
            "timezone": DISPLAY_ZONE.key,
            "risk": self._section(lambda: self._risk(dates), "服务器风险观测暂时不可读取。"),
            "usage": self._section(lambda: self._usage(midnight, now), "物理调用台账暂时不可读取。"),
            "workflows": self._section(lambda: self._workflows(now), "多专家运行记录暂时不可读取。"),
            "activity": self._section(lambda: self._activity(dates, now), "识别工作量暂时不可读取。"),
            "approvals": self._section(lambda: self._approvals(midnight, now), "审批队列暂时不可读取。"),
        }

    @staticmethod
    def _section(load: Callable[[], dict[str, Any]], reason: str) -> dict[str, Any]:
        try:
            return load()
        except Exception:
            # Do not disclose database diagnostics or identifiers from unreadable rows.
            return _block(None, reason=reason, unavailable=True)

    def _risk(self, dates: list[datetime]) -> dict[str, Any]:
        if self.risk_service is None:
            return _block(None, reason="服务器风险服务暂时不可用。", unavailable=True)
        trend, hosts = [], []
        today: dict[str, Any] = {}
        complete = True
        day_names = [day.date().isoformat() for day in dates]
        try:
            summaries = self.risk_service.overview_daily_summaries(day_names, today_limit=8)
        except ServerRiskError as exc:
            if exc.code != "query_resource_limit":
                raise
            return _block(None, reason="服务器风险总览达到窗口或事实读取上限。单台服务器风险可独立查看。", unavailable=True)
        for value, result in zip(day_names, summaries):
            summary = result["summary"]
            observed = summary["host_count"] > 0
            complete &= result["quality"]["complete"]
            today = {
                "hosts": summary["host_count"] if observed else None,
                "high_hosts": summary["high_host_count"] if observed else None,
                "groups": summary["risk_count"] if observed else None,
                "known_occurrences": summary["known_occurrence_count"] if observed else None,
                "unknown_groups": summary["unknown_occurrence_groups"] if observed else None,
                "recovery_groups": summary["recovery_counts"] if observed else None,
            }
            trend.append({"date": value, "hosts": today["hosts"], "high_hosts": today["high_hosts"]})
        for row in result["items"]:
            cluster, node = _safe_identifier(row["cluster"]), _safe_identifier(row["node_id"])
            if cluster and node:
                hosts.append({
                    "cluster": cluster, "node": node, "level": row["highest_level"],
                    "groups": row["risk_count"], "known_occurrences": row["known_occurrence_count"],
                    "last_seen": utc(row["last_seen"]).isoformat(),
                })
        any_observed = any(point["hosts"] is not None for point in trend)
        return _block({"today": today, "trend": trend, "hosts": hosts},
                      reason=None if complete else "主机按有风险观测的北京自然日去重。来源覆盖不足或缺少观测的日期可能存在缺口。",
                      unavailable=not any_observed)

    def _usage(self, midnight: datetime, now: datetime) -> dict[str, Any]:
        today = (_stamp(midnight), _stamp(now))
        with self.database.connect() as conn:
            total = _usage_totals(conn.execute(
                f"SELECT {USAGE_SUMS} FROM operational_physical_calls WHERE {USAGE_WHERE}",
                (ENVIRONMENT, *today),
            ).fetchone())
            rows = conn.execute(
                f"SELECT provider, model, {USAGE_SUMS} FROM operational_physical_calls WHERE {USAGE_WHERE} "
                "GROUP BY provider, model ORDER BY known_tokens DESC NULLS LAST, started_calls DESC, provider, model LIMIT 12",
                (ENVIRONMENT, *today),
            )
            models = [{
                "provider": _safe_identifier(row["provider"]), "model": _safe_identifier(row["model"]),
                **_usage_totals(row),
            } for row in rows]
            buckets = []
            for hour in range(0, now.astimezone(DISPLAY_ZONE).hour + 1, 3):
                start = midnight + timedelta(hours=hour)
                end = start + timedelta(hours=3)
                counts = _usage_totals(conn.execute(
                    f"SELECT {USAGE_SUMS} FROM operational_physical_calls WHERE {USAGE_WHERE} AND started_at<?",
                    (ENVIRONMENT, _stamp(start), today[1], _stamp(end)),
                ).fetchone())
                buckets.append({"start_at": start.isoformat(), "end_at": min(end, now).isoformat(), **counts})
        data = {
            **total, "models": models, "buckets": buckets, "estimated_cost_usd": None,
            "coverage": "physical_ledger_only",
        }
        coverage_note = "自动记账覆盖特征提取（含真实模型评测）与 Agent 规划。报告、回放和旧 Trace 缺少完整覆盖。"
        if not total["started_calls"]:
            return _block(data, reason=f"今日无已登记的物理模型调用。{coverage_note}", unavailable=True)
        if total["complete_calls"] != total["started_calls"]:
            return _block(data, reason=f"部分已登记调用缺少有效或完整的用量回执。{coverage_note} 历史价格未锁定。")
        return _block(data, reason=f"已登记调用的用量回执完整。{coverage_note} 历史价格未锁定。")

    def _workflows(self, now: datetime) -> dict[str, Any]:
        data = self.workflows.overview(
            now.isoformat(), (now - timedelta(hours=1)).isoformat(), enabled=self.workflows_enabled,
        )
        if not self.workflows_enabled:
            return _block(data, reason="多专家工作流未启用。已登记的运行仍可查看。", unavailable=not data["runs"])
        return _block(data)

    def _activity(self, dates: list[datetime], now: datetime) -> dict[str, Any]:
        day_starts = [_stamp(day) for day in dates]
        end = _stamp(now)
        with self.database.connect() as conn:
            # newly_ingested_count is the verified range difference at commit, so each source
            # position contributes at most once even across retried or overlapping batches.
            source = (
                "FROM operational_ingestion_batches b JOIN operational_sources s ON s.source_id=b.source_id "
                "WHERE s.environment=? AND b.provenance='verified' AND b.committed_at<=?"
            )
            total = conn.execute("SELECT SUM(b.newly_ingested_count) " + source, (ENVIRONMENT, end)).fetchone()[0]
            daily = []
            for day, start, next_start in zip(dates, day_starts, [*day_starts[1:], _stamp(dates[-1] + timedelta(days=1))]):
                count = conn.execute(
                    "SELECT SUM(b.newly_ingested_count) " + source + " AND b.committed_at>=? AND b.committed_at<?",
                    (ENVIRONMENT, end, start, next_start),
                ).fetchone()[0]
                daily.append({"date": day.date().isoformat(), "records": int(count) if count is not None else None})
            committed, windows, records = self._committed_activity(conn, dates, now)
            rules = int(conn.execute("SELECT COUNT(*) FROM approved_rules").fetchone()[0])
            unknown = sum(
                int(row["open_windows"])
                for row in conn.execute(
                    "SELECT t.source_identity_json, COUNT(*) AS open_windows FROM unknown_template_queue q "
                    "JOIN streaming_tasks t ON t.task_id=q.task_id WHERE q.status=? GROUP BY t.task_id,t.source_identity_json",
                    ("open",),
                )
                if self._real_source(row["source_identity_json"])
            )
        drain = {
            "compression_rate": (records - windows) / records if records and windows is not None and windows <= records else None,
            "template_windows": windows,
            "unknown_templates": unknown,
        }
        return _block({
            "today_records": daily[-1]["records"], "seven_day_records": daily,
            "total_records": int(total) if total is not None else None, "approved_rules": rules, "drain": drain,
            "committed_batch_records": committed,
        }, reason="已消费日志按持久提交的文件和 Kafka 流批次统计，缺少批次计量的日期留空。去重账本单列已验证收据。"
           if any(point["records"] is None for point in committed["seven_day_records"])
           or committed["total_records"] is None or drain["compression_rate"] is None else None)

    @staticmethod
    def _real_source(descriptor: Any) -> bool:
        identity = _data(_data(descriptor).get("identity"))
        environment = identity.get("environment")
        return not isinstance(environment, str) or environment.casefold() not in SOURCE_EXCLUSIONS

    def _committed_activity(
        self, conn: Any, dates: list[datetime], now: datetime,
    ) -> tuple[dict[str, Any], int | None, int | None]:
        # One row per time bucket, including an earlier-history bucket for the cumulative value.
        # Each commit contributes once per task. Cross-task replay is deliberately kept separate
        # from the canonical range ledger used by the verified record metrics.
        bucket = "CASE " + " ".join(
            f"WHEN c.committed_at>=? THEN {index}" for index in range(len(dates) - 1, -1, -1)
        ) + " ELSE -1 END"
        if self.database.provider == "postgres":
            environment = "t.source_identity_json->'identity'->>'environment'"
            records = "c.summary_json->>'record_count'"
            windows = "c.summary_json->>'template_count'"
            records = f"CASE WHEN jsonb_typeof(c.summary_json->'record_count')='number' AND ({records}) ~ '^[0-9]+$' THEN CAST({records} AS NUMERIC) END"
            windows = f"CASE WHEN jsonb_typeof(c.summary_json->'template_count')='number' AND ({windows}) ~ '^[0-9]+$' THEN CAST({windows} AS NUMERIC) END"
        else:
            environment = "json_extract(t.source_identity_json,'$.identity.environment')"
            records = "CASE WHEN json_type(c.summary_json,'$.record_count')='integer' THEN json_extract(c.summary_json,'$.record_count') END"
            windows = "CASE WHEN json_type(c.summary_json,'$.template_count')='integer' THEN json_extract(c.summary_json,'$.template_count') END"
        placeholders = ",".join("?" for _ in SOURCE_EXCLUSIONS)
        rows = {
            int(row["bucket"]): row for row in conn.execute(
                f"SELECT bucket, COUNT(*) AS commits, "
                f"COUNT(CASE WHEN records>=0 THEN 1 END) AS recorded_batches, "
                f"SUM(CASE WHEN records>=0 THEN records END) AS records, "
                f"COUNT(CASE WHEN windows>=0 THEN 1 END) AS window_batches, "
                f"SUM(CASE WHEN windows>=0 THEN windows END) AS windows, "
                f"COUNT(CASE WHEN windows>records THEN 1 END) AS inconsistent_batches "
                f"FROM (SELECT {bucket} AS bucket, {records} AS records, {windows} AS windows "
                f"FROM streaming_window_commits c JOIN streaming_tasks t ON t.task_id=c.task_id "
                f"WHERE c.committed_at<=? AND LOWER(COALESCE({environment},'')) NOT IN ({placeholders})) batch_facts "
                f"GROUP BY bucket",
                (*(_stamp(day) for day in reversed(dates)), _stamp(now), *sorted(SOURCE_EXCLUSIONS)),
            )
        }

        def known(row: Any) -> int | None:
            return int(row["records"]) if row and row["recorded_batches"] == row["commits"] else None

        today = rows.get(len(dates) - 1)
        today_records = known(today)
        today_windows = int(today["windows"]) if today and today["window_batches"] == today["commits"] else None
        total = sum(int(row["records"]) for row in rows.values()) if rows and all(
            known(row) is not None for row in rows.values()
        ) else None
        summary = {
            "today_records": today_records, "total_records": total,
            "seven_day_records": [
                {"date": day.date().isoformat(), "records": known(rows.get(index))}
                for index, day in enumerate(dates)
            ],
        }
        return summary, today_windows, today_records if today and today["inconsistent_batches"] == 0 else None

    def _approvals(self, midnight: datetime, now: datetime) -> dict[str, Any]:
        with self.database.connect() as conn:
            pending = int(conn.execute(
                "SELECT COUNT(DISTINCT review_key) FROM approval_candidate_projection WHERE status='pending'",
            ).fetchone()[0])
            since = now - timedelta(hours=1)
            approved = set()
            for row in conn.execute(
                "SELECT candidate_id,result_json FROM approval_decisions WHERE created_at>=? AND created_at<=?",
                (_stamp(since), _stamp(now)),
            ):
                result = _data(row["result_json"])
                if result.get("status") == "approved" and result.get("resolution_type") == "manual":
                    try:
                        decision_time = utc(result.get("approved_at"))
                    except ValueError:
                        continue
                    if since <= decision_time <= now:
                        approved.add(str(result.get("approval_group_id") or row["candidate_id"]))
        history = [
            {"at": (midnight + timedelta(hours=hour)).isoformat(), "pending_groups": None}
            for hour in range(0, now.astimezone(DISPLAY_ZONE).hour + 1, 2)
            if midnight + timedelta(hours=hour) < now
        ]
        history.append({"at": now.astimezone(DISPLAY_ZONE).isoformat(), "pending_groups": pending})
        return _block({
            "pending_groups": pending, "approved_last_hour": len(approved), "history": history,
        }, reason="当前按审批组去重。历史缺少可重建的队列快照，保留空值。" if len(history) > 1 else None)
