from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.operational_ledgers import (
    OperationalLedgerConflict,
    OperationalLedgerError,
    OperationalLedgerRepository,
    canonical_ranges,
    ingestion_batch_id,
    normalize_usage,
    source_identity,
    subtract_ranges,
    tokens_per_1000,
)


def _source(*, scope_key: str = "default", identity_digest: str = "bytes-v1") -> dict[str, str]:
    source = {
        "environment": "local-test",
        "scope_key": scope_key,
        "source_kind": "file",
        "identity_digest": identity_digest,
    }
    source["source_id"] = source_identity(**source)
    return source


def _batch(
    repository: OperationalLedgerRepository,
    source: dict[str, str],
    checkpoint: str,
    start: int,
    end: int,
    *,
    input_job_id: str = "input-1",
    connection=None,
) -> dict:
    return repository.record_ingestion_batch(
        batch_id=ingestion_batch_id(input_job_id, checkpoint),
        source=source,
        input_job_id=input_job_id,
        checkpoint_key=checkpoint,
        parser_version="parser-v1",
        ranges=[{"partition_key": "", "start": start, "end": end}],
        actual_count=end - start,
        connection=connection,
    )


def test_migration_0024_is_paired_and_idempotent(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    SQLiteDatabase(tmp_path / "logrisk.sqlite3")

    with database.connect() as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'operational_%'"
            )
        }
        assert tables == {
            "operational_sources",
            "operational_ingestion_batches",
            "operational_source_ranges",
            "operational_analysis_runs",
            "operational_analysis_ranges",
            "operational_analysis_members",
            "operational_physical_calls",
            "operational_backfill_items",
        }
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == len(list(Path("database/migrations").glob("*.sql")))

    for path in (
        Path("database/migrations/0024_operational_ledgers.sql"),
        Path("database/postgres/migrations/0024_operational_ledgers.sql"),
    ):
        text = path.read_text(encoding="utf-8")
        assert text.count("CREATE TABLE operational_") == 8
        assert "operational_analysis_ranges" in text


def test_range_helpers_preserve_holes_and_reject_bad_endpoints() -> None:
    incoming = canonical_ranges(
        [
            {"partition_key": "0", "start": 9, "end": 10},
            {"partition_key": "0", "start": 1, "end": 3},
            {"partition_key": "0", "start": 3, "end": 4},
        ]
    )
    assert incoming == [
        {"partition_key": "0", "start": 1, "end": 4},
        {"partition_key": "0", "start": 9, "end": 10},
    ]
    assert subtract_ranges(
        [{"partition_key": "0", "start": 1, "end": 10}],
        [{"partition_key": "0", "start": 3, "end": 4}, {"partition_key": "0", "start": 7, "end": 8}],
    ) == [
        {"partition_key": "0", "start": 1, "end": 3},
        {"partition_key": "0", "start": 4, "end": 7},
        {"partition_key": "0", "start": 8, "end": 10},
    ]
    for bad in (
        [{"partition_key": "0", "start": -1, "end": 2}],
        [{"partition_key": "0", "start": 2, "end": 2}],
        [{"partition_key": "0", "start": 1.0, "end": 2}],
        [{"partition_key": "0", "start": True, "end": 2}],
    ):
        with pytest.raises(OperationalLedgerError):
            canonical_ranges(bad)


def test_usage_normalization_keeps_zero_and_unknown_distinct() -> None:
    assert normalize_usage(None)["usage_quality"] == "unknown"
    assert normalize_usage({"input_tokens": 0, "output_tokens": 0}) == {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
        "invalid_usage": False,
        "usage_quality": "known",
    }
    assert normalize_usage(
        {
            "prompt_tokens": 120,
            "completion_tokens": 30,
            "total_tokens": 130,
            "prompt_tokens_details": {"cached_tokens": 40},
            "completion_tokens_details": {"reasoning_tokens": 20},
        }
    ) == {
        "input_tokens": 120,
        "output_tokens": 30,
        "total_tokens": 130,
        "cached_input_tokens": 40,
        "reasoning_tokens": 20,
        "invalid_usage": False,
        "usage_quality": "known",
    }
    assert normalize_usage({"prompt_eval_count": 120, "eval_count": 30})["total_tokens"] == 150
    for bad in ({"input_tokens": True}, {"input_tokens": 1.0}, {"input_tokens": -1}, {"input_tokens": float("nan")}):
        result = normalize_usage(bad)
        assert result["invalid_usage"] is True
        assert result["input_tokens"] is None
        assert result["usage_quality"] == "invalid"
    assert tokens_per_1000(17000, 101000) == Decimal("168.32")
    assert tokens_per_1000(0, 0) is None
    with pytest.raises(OperationalLedgerError):
        tokens_per_1000(True, 10)  # type: ignore[arg-type]


def test_ingestion_union_receipts_are_idempotent_and_scope_safe(tmp_path: Path) -> None:
    repository = OperationalLedgerRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    source = _source()
    first = _batch(repository, source, "window-1", 1, 1001)
    assert first["actual_count"] == 1000
    assert first["newly_ingested_count"] == 1000
    second = repository.record_ingestion_batch(
        batch_id=ingestion_batch_id("input-1", "window-2"),
        source=source,
        input_job_id="input-1",
        checkpoint_key="window-2",
        parser_version="parser-v2",
        ranges=[{"partition_key": "", "start": 500, "end": 1201}],
        actual_count=701,
    )
    assert second["newly_ingested_count"] == 200
    replay = repository.record_ingestion_batch(
        batch_id=first["batch_id"],
        source=source,
        input_job_id="input-1",
        checkpoint_key="window-1",
        parser_version="parser-v1",
        ranges=[{"partition_key": "", "start": 1, "end": 1001}],
        actual_count=1000,
    )
    assert replay["newly_ingested_count"] == 1000
    with pytest.raises(OperationalLedgerConflict):
        repository.record_ingestion_batch(
            batch_id=first["batch_id"],
            source=source,
            input_job_id="input-1",
            checkpoint_key="window-1",
            parser_version="different-parser",
            ranges=[{"partition_key": "", "start": 1, "end": 1001}],
            actual_count=1000,
        )
    other_scope = _source(scope_key="other")
    isolated = _batch(repository, other_scope, "window-1", 1, 1001, input_job_id="input-2")
    assert isolated["newly_ingested_count"] == 1000
    with repository.database.connect() as connection:
        rows = connection.execute(
            "SELECT source_id, kind, start_position, end_position FROM operational_source_ranges ORDER BY source_id, kind"
        ).fetchall()
    assert len(rows) == 3


def test_ingestion_transaction_rolls_back_with_external_connection(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    with pytest.raises(RuntimeError):
        with database.transaction() as connection:
            _batch(repository, source, "window-1", 1, 11, connection=connection)
            raise RuntimeError("rollback")
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM operational_sources").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM operational_ingestion_batches").fetchone()[0] == 0


def test_receipt_page_decodes_ranges_and_uses_one_connection_shape(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    _batch(repository, source, "window-1", 1, 3)
    _batch(repository, source, "window-2", 3, 5)
    page = repository.list_ingestion_batches("input-1", limit=1)
    assert page["has_more"] is True
    assert page["items"][0]["ranges"] == [{"end": 3, "partition_key": "", "start": 1}]
    assert "ranges_json" not in page["items"][0]
    assert page["items"][0]["source"] == source
    second = repository.list_ingestion_batches("input-1", after_batch_id=page["next_after_batch_id"], limit=1)
    assert len(second["items"]) == 1
    assert second["has_more"] is False


def test_historical_timestamps_are_aware_and_settlement_uses_last_member(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    _batch(repository, source, "window-1", 1, 11)
    root = repository.create_analysis_run(
        analysis_run_id="run-history",
        request_key="history",
        environment="local-test",
        scope_key="default",
        source_id=source["source_id"],
        ranges=[{"source_id": source["source_id"], "partition_key": "", "start": 1, "end": 11}],
        input_count=10,
        expected_members=2,
        created_at="2026-01-01T00:00:00+00:00",
    )
    repository.register_analysis_member("run-history", "member-a", created_at="2026-01-01T00:00:01+00:00")
    repository.register_analysis_member("run-history", "member-b", created_at="2026-01-01T00:00:02+00:00")
    repository.finish_analysis_member("run-history", "member-a", "completed", finished_at="2026-01-01T00:00:03+00:00")
    member_replay = repository.finish_analysis_member(
        "run-history", "member-a", "completed", finished_at="2026-01-01T00:00:04+00:00"
    )
    assert member_replay["updated_at"] == "2026-01-01T00:00:03+00:00"
    incomplete = repository.complete_analysis_run("run-history", completed_at="2026-01-01T00:00:04+00:00")
    assert incomplete["completed_at"] is None
    repository.finish_analysis_member("run-history", "member-b", "completed", finished_at="2026-01-01T00:00:05+00:00")
    completed = repository.get_analysis_run("run-history")
    assert completed["completed_at"] == "2026-01-01T00:00:05+00:00"
    assert completed["settled_at"] == "2026-01-01T00:00:05+00:00"
    with pytest.raises(OperationalLedgerError):
        repository.finish_analysis_member("run-history", "member-b", "completed", finished_at="2026-01-01T00:00:00")


def test_analysis_settles_once_after_member_retry_and_reanalysis(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    _batch(repository, source, "window-1", 1, 1001)
    root = repository.create_analysis_run(
        request_key="analysis-1",
        environment=source["environment"],
        scope_key=source["scope_key"],
        input_job_id="input-1",
        source_id=source["source_id"],
        ranges=[{"source_id": source["source_id"], "partition_key": "", "start": 1, "end": 1001}],
        input_count=1000,
        expected_members=2,
    )
    assert [repository.register_analysis_member(root["analysis_run_id"], member)["member_id"] for member in ("m-1", "m-2")] == ["m-1", "m-2"]
    bound = repository.bind_analysis_member(root["analysis_run_id"], "m-1", "feature-job-1")
    assert repository.bind_analysis_member(root["analysis_run_id"], "m-1", "feature-job-1") == bound
    repository.finish_analysis_member(root["analysis_run_id"], "m-1", "failed")
    assert repository.get_analysis_run(root["analysis_run_id"])["status"] == "partial"
    repository.finish_analysis_member(root["analysis_run_id"], "m-1", "running")
    repository.finish_analysis_member(root["analysis_run_id"], "m-1", "completed")
    repository.finish_analysis_member(root["analysis_run_id"], "m-2", "completed")
    settled = repository.get_analysis_run(root["analysis_run_id"])
    assert settled["status"] == "completed"
    assert settled["settled_at"] is not None

    repository.complete_analysis_run(root["analysis_run_id"])
    rerun = repository.create_analysis_run(
        request_key="analysis-2",
        environment=source["environment"],
        scope_key=source["scope_key"],
        input_job_id="input-1",
        source_id=source["source_id"],
        ranges=settled["ranges"],
        input_count=1000,
        parent_run_id=root["analysis_run_id"],
    )
    repository.register_analysis_member(rerun["analysis_run_id"], "m-rerun")
    repository.finish_analysis_member(rerun["analysis_run_id"], "m-rerun", "completed")
    assert repository.get_analysis_run(rerun["analysis_run_id"])["status"] == "completed"
    with database.connect() as connection:
        counts = {
            row["kind"]: row["count"]
            for row in connection.execute(
                "SELECT kind, COUNT(*) AS count FROM operational_source_ranges GROUP BY kind"
            ).fetchall()
        }
    assert counts == {"covered": 1, "ingested": 1}


def test_physical_call_replay_fills_missing_usage_without_inflating_or_conflicting(tmp_path: Path) -> None:
    repository = OperationalLedgerRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    prepared = repository.prepare_call(
        call_id="call-1",
        logical_call_id="feature/entity-1",
        attempt_index=0,
        environment="local-test",
        scope_key="default",
        call_kind="provider",
        provider="fake",
        model="model",
        caller_kind="feature_extractor",
    )
    assert prepared["status"] == "prepared"
    repository.start_call("call-1")
    first = repository.finish_call("call-1", status="succeeded", usage={"input_tokens": 120})
    assert first["usage_quality"] == "partial"
    merged = repository.finish_call("call-1", status="succeeded", usage={"output_tokens": 30})
    assert merged["input_tokens"] == 120
    assert merged["output_tokens"] == 30
    assert merged["total_tokens"] == 150
    assert merged["usage_quality"] == "known"
    replay = repository.finish_call("call-1", status="succeeded", usage={"total_tokens": 150})
    assert replay["total_tokens"] == 150
    with pytest.raises(OperationalLedgerConflict):
        repository.finish_call("call-1", status="succeeded", usage={"total_tokens": 151})


def test_backfill_item_is_atomic_and_conflicts_on_digest_or_disposition(tmp_path: Path) -> None:
    repository = OperationalLedgerRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    first = repository.record_backfill_item(item_key="feature_job:job-1", source_digest="digest-1", status="applied")
    replay = repository.record_backfill_item(item_key="feature_job:job-1", source_digest="digest-1", status="applied")
    assert replay == first
    with pytest.raises(OperationalLedgerConflict):
        repository.record_backfill_item(item_key="feature_job:job-1", source_digest="digest-2", status="applied")
    with pytest.raises(OperationalLedgerConflict):
        repository.record_backfill_item(item_key="feature_job:job-1", source_digest="digest-1", status="skipped")
