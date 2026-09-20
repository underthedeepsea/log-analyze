from __future__ import annotations

import json

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.streaming_state import (
    StreamingIncompleteError,
    StreamingStateError,
    StreamingStateRepository,
)


def repository(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "source.log"
    source.write_text("safe\n", encoding="utf-8")
    subject = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = subject.create_or_load(
        descriptor=FileIncrementalSource(source).descriptor(), config_hash="c" * 64,
    )
    return subject, task


def complete_summary(**overrides):
    value = {
        "summary_schema_version": 2,
        "record_count": 1,
        "template_count": 1,
        "unknown_template_count": 0,
        "risk_semantic_matches": 0,
        "template_event_count": 1,
        "partition_count": 1,
        "worker_count": 0,
        "parallel": False,
        "node_risk_enabled": False,
        "process_start_method": "not_applicable",
    }
    value.update(overrides)
    return value


def test_cursor_only_and_missing_window_never_publish_complete_prefix(tmp_path):
    subject, task = repository(tmp_path)
    with subject.database.transaction() as connection:
        payload = subject.get_task(task["task_id"])
        payload["cursor"] = SourceCursor("file", {"offset": 10, "line": 2}).to_dict()
        connection.execute(
            "UPDATE streaming_tasks SET cursor_json=?,task_json=? WHERE task_id=?",
            (json.dumps(payload["cursor"]), json.dumps(payload), task["task_id"]),
        )
    with pytest.raises(StreamingIncompleteError) as caught:
        subject.require_complete_prefix(task["task_id"])
    assert caught.value.code == "STREAMING_PREFIX_INCOMPLETE"
    assert caught.value.details["result_authoritative"] is False

    second, task = repository(tmp_path / "second")
    window = {"template_hash": "h", "window_start": "2026-01-01T00:00:00+00:00", "count": 1}
    second.commit_window(
        task["task_id"], window_id="one", cursor=SourceCursor("file", {"offset": 10}),
        templates=[], windows=[window], summary=complete_summary(),
    )
    with second.database.transaction() as connection:
        connection.execute("DELETE FROM streaming_batch_windows WHERE task_id=?", (task["task_id"],))
    with pytest.raises(StreamingIncompleteError, match="payload hash"):
        second.require_complete_prefix(task["task_id"])


def test_empty_task_is_complete_and_new_summary_types_are_strict(tmp_path):
    subject, task = repository(tmp_path)
    evidence = subject.require_complete_prefix(task["task_id"])
    assert evidence.committed_batches == evidence.record_count == 0
    assert evidence.frontier == SourceCursor.empty().to_dict()

    with pytest.raises(StreamingStateError, match="parallel"):
        subject.commit_window(
            task["task_id"], window_id="bad", cursor=SourceCursor("file", {"offset": 1}),
            templates=[], windows=[], summary=complete_summary(parallel="false"),
        )


def test_summary_preserves_unknown_and_reduces_start_methods(tmp_path):
    subject, task = repository(tmp_path)
    for index, method in enumerate(("spawn", "fork", "not_applicable")):
        subject.commit_window(
            task["task_id"], window_id=str(index), cursor=SourceCursor("file", {"offset": index + 1}),
            templates=[], windows=[], summary=complete_summary(
                record_count=0, template_count=0, template_event_count=0,
                partition_count=0, worker_count=index, parallel=index > 0,
                process_start_method=method,
            ),
        )
    summary = subject.committed_summary(task["task_id"])
    assert summary["process_start_method"] == "mixed"
    assert summary["worker_count"] == 2
    assert summary["parallel"] is True

    subject.commit_window(
        task["task_id"], window_id="legacy", cursor=SourceCursor("file", {"offset": 4}),
        templates=[], windows=[], summary={"record_count": 0, "parallel": None},
    )
    summary = subject.committed_summary(task["task_id"])
    assert "parallel" in summary["unknown_fields"]
    # A known True proves any=True; missing batch evidence remains visible.
    assert summary["parallel"] is True
    assert summary["completeness"] == "legacy_partial"
    assert "parallel" not in summary["invalid_fields"]
