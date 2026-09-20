from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.operational_analytics import (
    OperationalAnalytics,
    OperationalAnalyticsError,
    normalize_filters,
)
from logrisk.operational_ledgers import OperationalLedgerRepository, source_identity


def _source(environment: str = "production", scope_key: str = "default") -> dict[str, str]:
    source = {
        "environment": environment,
        "scope_key": scope_key,
        "source_kind": "file",
        "identity_digest": f"bytes-{environment}-{scope_key}",
    }
    source["source_id"] = source_identity(**source)
    return source


def _seed_root(
    ledger: OperationalLedgerRepository,
    source: dict[str, str],
    *,
    request_key: str,
    created_at: str,
    completed_at: str,
    input_count: int = 10,
    member_id: str | None = None,
) -> str:
    root = ledger.create_analysis_run(
        request_key=request_key,
        environment=source["environment"],
        scope_key=source["scope_key"],
        source_id=source["source_id"],
        ranges=[{"source_id": source["source_id"], "partition_key": "", "start": 0, "end": input_count}],
        input_count=input_count,
        created_at=created_at,
    )
    member = member_id or f"{root['analysis_run_id']}:0"
    ledger.register_analysis_member(root["analysis_run_id"], member, created_at=created_at)
    ledger.finish_analysis_member(root["analysis_run_id"], member, "completed", finished_at=completed_at)
    return root["analysis_run_id"]


def _seed_call(
    ledger: OperationalLedgerRepository,
    root_id: str | None,
    *,
    call_id: str,
    started_at: str,
    provider: str = "fake",
    model: str = "model",
    usage: dict | None = None,
    call_kind: str = "provider",
) -> None:
    ledger.prepare_call(
        call_id=call_id,
        logical_call_id=f"logical-{call_id}",
        attempt_index=0,
        analysis_run_id=root_id,
        environment="production",
        scope_key="default",
        call_kind=call_kind,
        provider=provider if call_kind == "provider" else None,
        model=model if call_kind == "provider" else None,
        tool_name="tool" if call_kind == "agent_tool" else None,
        caller_kind="test",
    )
    ledger.start_call(call_id, started_at=started_at)
    ledger.finish_call(call_id, status="succeeded", usage=usage, finished_at=started_at)


def test_summary_uses_activity_clocks_and_cohort_union(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "analytics.sqlite3")
    ledger = OperationalLedgerRepository(database)
    source = _source()
    ledger.record_ingestion_batch(
        batch_id="batch-1",
        source=source,
        input_job_id="input-1",
        checkpoint_key="window-1",
        parser_version="parser-v1",
        ranges=[{"partition_key": "", "start": 0, "end": 10}],
        actual_count=10,
        committed_at="2026-01-01T01:00:00+00:00",
    )
    root = _seed_root(
        ledger,
        source,
        request_key="root-1",
        created_at="2026-01-01T02:00:00+00:00",
        completed_at="2026-01-02T02:00:00+00:00",
    )
    _seed_call(
        ledger,
        root,
        call_id="call-1",
        started_at="2026-01-01T03:00:00+00:00",
        usage={"input_tokens": 17_000, "output_tokens": 0},
    )
    analytics = OperationalAnalytics(ledger)

    activity = analytics.summary(
        {
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-02T00:00:00Z",
        }
    )
    assert activity["window"]["ingested_records"] == 10
    assert activity["window"]["covered_records"] == 0
    assert activity["window"]["analysis_workload"] == 0
    assert activity["window"]["provider_calls"] == 1
    assert activity["window"]["efficiency"]["reason"] == "activity_not_completed_cohort"
    assert activity["lifetime"]["analysis_workload"] == 10
    assert activity["lifetime"]["efficiency"]["tokens_per_1000_records"] == "1700000.00"

    cohort = analytics.summary(
        {
            "time_basis": "completed_run_cohort",
            "from": "2026-01-02T00:00:00Z",
            "to": "2026-01-03T00:00:00Z",
        }
    )
    assert cohort["window"]["ingested_records"] == 10
    assert cohort["window"]["covered_records"] == 10
    assert cohort["window"]["analysis_workload"] == 10
    assert cohort["window"]["provider_calls"] == 1
    assert cohort["window"]["efficiency"]["tokens_per_1000_records"] == "1700000.00"
    assert cohort["window"]["input_volume_basis"] == "completed_run_cohort"


def test_summary_respects_environment_and_keeps_zero_unknown_partial(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "analytics.sqlite3")
    ledger = OperationalLedgerRepository(database)
    production = _source()
    local = _source("local-test")
    for source, prefix in ((production, "p"), (local, "l")):
        ledger.record_ingestion_batch(
            batch_id=f"{prefix}-batch",
            source=source,
            input_job_id=f"{prefix}-input",
            checkpoint_key="window",
            parser_version="parser",
            ranges=[{"partition_key": "", "start": 0, "end": 2}],
            actual_count=2,
            committed_at="2026-01-01T00:00:00+00:00",
        )
    root = _seed_root(
        ledger,
        production,
        request_key="root-usage",
        created_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:01:00+00:00",
        input_count=2,
    )
    _seed_call(
        ledger,
        root,
        call_id="known-zero",
        started_at="2026-01-01T00:02:00+00:00",
        usage={"input_tokens": 0, "output_tokens": 0},
    )
    _seed_call(
        ledger,
        root,
        call_id="unknown",
        started_at="2026-01-01T00:03:00+00:00",
        usage=None,
    )
    _seed_call(
        ledger,
        None,
        call_id="agent",
        started_at="2026-01-01T00:04:00+00:00",
        call_kind="agent_tool",
        usage=None,
    )
    summary = OperationalAnalytics(ledger).summary()
    assert summary["window"]["ingested_records"] == 2
    assert summary["window"]["tokens"]["input_tokens"] == 0
    assert summary["window"]["tokens"]["output_tokens"] == 0
    assert summary["window"]["tokens"]["total_tokens"] is None
    assert summary["window"]["tokens"]["known_total"] == 0
    assert summary["window"]["tokens"]["unknown_call_count"] == 1
    assert summary["window"]["agent_tool_calls"] == 1
    assert summary["quality"]["unknown_usage_calls"] == 1
    assert OperationalAnalytics(ledger).summary({"environment": "local-test"})["window"]["ingested_records"] == 2
    assert OperationalAnalytics(ledger).summary({"environment": "all"})["window"]["ingested_records"] == 4


def test_keyset_cursor_binds_filters_and_excludes_prepared_calls(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "analytics.sqlite3")
    ledger = OperationalLedgerRepository(database)
    for index in range(3):
        ledger.prepare_call(
            call_id=f"prepared-{index}",
            logical_call_id=f"prepared-logical-{index}",
            attempt_index=0,
            environment="production",
            scope_key="default",
            call_kind="provider",
            provider="fake",
            model="model",
            caller_kind="test",
        )
    for index in range(3):
        _seed_call(
            ledger,
            None,
            call_id=f"started-{index}",
            started_at=f"2026-01-01T00:0{index}:00+00:00",
        )
    analytics = OperationalAnalytics(ledger)
    first = analytics.calls({"page_size": 2})
    assert len(first["items"]) == 2
    assert first["has_more"] is True
    second = analytics.calls({"page_size": 2, "cursor": first["next_cursor"]})
    assert {item["call_id"] for item in first["items"]}.isdisjoint(item["call_id"] for item in second["items"])
    with pytest.raises(OperationalAnalyticsError, match="匹配"):
        analytics.calls({"page_size": 2, "provider": "other", "cursor": first["next_cursor"]})


def test_filters_and_csv_are_strict_and_formula_safe(tmp_path: Path) -> None:
    with pytest.raises(OperationalAnalyticsError):
        normalize_filters({"from": "2026-01-01T00:00:00"})
    with pytest.raises(OperationalAnalyticsError):
        normalize_filters({"page_size": True})
    with pytest.raises(OperationalAnalyticsError):
        normalize_filters({"from": "2026-01-02T00:00:00Z", "to": "2026-01-01T00:00:00Z"})

    database = SQLiteDatabase(tmp_path / "analytics.sqlite3")
    ledger = OperationalLedgerRepository(database)
    for index in range(501):
        _seed_call(
            ledger,
            None,
            call_id=f"call-{index:04d}",
            started_at="2026-01-01T00:00:00+00:00",
            provider="=unsafe" if index == 0 else "fake",
        )
    chunks = list(OperationalAnalytics(ledger).export_csv({"page_size": 200}, "calls"))
    payload = b"".join(chunks)
    assert payload.startswith(b"\xef\xbb\xbf")
    rows = list(csv.reader(io.StringIO(payload[3:].decode("utf-8"))))
    assert len(rows) == 502
    assert rows[0][0:6] == ["metric_version", "time_basis", "display_timezone", "environment", "scope_key", "provenance"]
    assert any("'=unsafe" in row for row in rows[1:])
