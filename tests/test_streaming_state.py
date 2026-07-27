from __future__ import annotations

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.streaming_state import StreamingStateError, StreamingStateRepository


def test_commit_window_is_idempotent_and_advances_checkpoint(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    path = tmp_path / "messages"
    path.write_text("error one\n", encoding="utf-8")
    repository = StreamingStateRepository(database)
    task = repository.create_or_load(
        descriptor=FileIncrementalSource(path, filename="messages").descriptor(),
        config_hash="a" * 64,
    )
    cursor = SourceCursor("file", {"offset": 10, "line": 2})
    template = {
        "template_hash": "abc",
        "component": "kernel",
        "template": "error <*>",
        "count": 1,
        "window_start": "2026-07-27T00:00:00+00:00",
    }

    first = repository.commit_window(
        task["task_id"],
        window_id="node-a:1",
        cursor=cursor,
        templates=[template],
    )
    second = repository.commit_window(
        task["task_id"],
        window_id="node-a:1",
        cursor=cursor,
        templates=[template],
    )

    assert first is True
    assert second is False
    assert repository.get_task(task["task_id"])["cursor"] == cursor.to_dict()
    assert repository.list_unknown_templates(task_id=task["task_id"])[0]["occurrence_count"] == 1


def test_commit_window_rejects_raw_log_fields(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    path = tmp_path / "messages"
    path.write_text("error one\n", encoding="utf-8")
    repository = StreamingStateRepository(database)
    task = repository.create_or_load(
        descriptor=FileIncrementalSource(path, filename="messages").descriptor(),
        config_hash="b" * 64,
    )

    with pytest.raises(StreamingStateError, match="原始日志字段"):
        repository.commit_window(
            task["task_id"],
            window_id="node-a:1",
            cursor=SourceCursor("file", {"offset": 10, "line": 2}),
            templates=[{"template_hash": "abc", "message": "do not persist this"}],
        )


def test_commit_window_requires_a_valid_window_timestamp(tmp_path):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    path = tmp_path / "messages"
    path.write_text("error one\n", encoding="utf-8")
    repository = StreamingStateRepository(database)
    task = repository.create_or_load(
        descriptor=FileIncrementalSource(path, filename="messages").descriptor(),
        config_hash="c" * 64,
    )

    with pytest.raises(StreamingStateError, match="window_start"):
        repository.commit_window(
            task["task_id"],
            window_id="node-a:1",
            cursor=SourceCursor("file", {"offset": 10, "line": 2}),
            templates=[{"template_hash": "abc", "window_start": "not-a-time"}],
        )
