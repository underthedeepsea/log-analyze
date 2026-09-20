from __future__ import annotations

import json

from logrisk.database import SQLiteDatabase
from logrisk.sqlite_stores import SQLiteAITraceLogger


def test_sqlite_trace_detail_list_and_summary_do_not_use_full_read(tmp_path, monkeypatch):
    logger = SQLiteAITraceLogger(SQLiteDatabase(tmp_path / "db.sqlite3"))
    for index in range(5):
        logger.append({
            "trace_id": f"trace-{index}", "job_id": "job", "provider": "fake", "model": "m",
            "status": "success", "prompt_id": "p", "prompt_hash": "h", "latency_ms": index,
            "created_at": f"2026-09-17T00:00:0{index}+00:00", "large": "x" * 1000,
        })
    monkeypatch.setattr(logger, "_read", lambda: (_ for _ in ()).throw(AssertionError("full read")))

    assert logger.get_trace("trace-3")["trace_id"] == "trace-3"
    assert len(logger.list_traces(job_id="job", limit=2)) == 2
    assert logger.summary_today("2026-09-17T12:00:00+00:00")["today_calls"] == 5


def test_sqlite_trace_detail_deserializes_only_target_row(tmp_path, monkeypatch):
    logger = SQLiteAITraceLogger(SQLiteDatabase(tmp_path / "db.sqlite3"))
    for index in range(20):
        logger.append({"trace_id": f"t-{index}", "status": "success", "created_at": "2026-09-17T00:00:00+00:00"})
    calls = 0
    original = json.loads

    def counted(value, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(value, *args, **kwargs)

    monkeypatch.setattr("logrisk.sqlite_stores.json.loads", counted)
    assert logger.get_trace("t-10")["trace_id"] == "t-10"
    assert calls == 1
