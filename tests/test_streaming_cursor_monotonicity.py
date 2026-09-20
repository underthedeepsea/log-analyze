from __future__ import annotations

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.streaming_state import StreamingConflictError, StreamingStateRepository


def test_replayed_old_commit_does_not_move_task_cursor_back(tmp_path):
    path = tmp_path / "source.log"
    path.write_text("x\n", encoding="utf-8")
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = repository.create_or_load(descriptor=FileIncrementalSource(path, filename="source.log").descriptor(), config_hash="a" * 64)
    one = {"template_hash": "h", "component": "c", "window_start": "2026-01-01T00:00:00+00:00", "count": 1}
    repository.commit_window(task["task_id"], window_id="batch-1", cursor=SourceCursor("file", {"offset": 100}), templates=[], windows=[one])
    repository.commit_window(task["task_id"], window_id="batch-2", cursor=SourceCursor("file", {"offset": 200}), templates=[], windows=[one])
    repository.commit_window(task["task_id"], window_id="batch-1", cursor=SourceCursor("file", {"offset": 100}), templates=[], windows=[one])

    assert repository.get_task(task["task_id"])["cursor"]["value"]["offset"] == 200
    assert len(repository.iter_committed_windows(task["task_id"])) == 2


def test_same_batch_id_with_different_payload_is_conflict(tmp_path):
    path = tmp_path / "source.log"
    path.write_text("x\n", encoding="utf-8")
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = repository.create_or_load(descriptor=FileIncrementalSource(path, filename="source.log").descriptor(), config_hash="b" * 64)
    cursor = SourceCursor("file", {"offset": 10})
    repository.commit_window(task["task_id"], window_id="batch", cursor=cursor, templates=[], windows=[])
    with pytest.raises(StreamingConflictError):
        repository.commit_window(task["task_id"], window_id="batch", cursor=cursor, templates=[], windows=[{
            "template_hash": "different", "component": "c", "window_start": "2026-01-01T00:00:00+00:00", "count": 1,
        }])


def test_new_batch_rejects_stale_frontier_and_regression(tmp_path):
    path = tmp_path / "source.log"
    path.write_text("x\n",encoding="utf-8")
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = repository.create_or_load(descriptor=FileIncrementalSource(path,filename="source.log").descriptor(),config_hash="c"*64)
    repository.commit_window(task["task_id"],window_id="first",cursor=SourceCursor("file",{"offset":100}),templates=[])
    with pytest.raises(StreamingConflictError,match="前沿"):
        repository.commit_window(task["task_id"],window_id="second",cursor=SourceCursor("file",{"offset":200}),expected_cursor=SourceCursor.empty(),templates=[])
    with pytest.raises(StreamingConflictError,match="回退"):
        repository.commit_window(task["task_id"],window_id="third",cursor=SourceCursor("file",{"offset":50}),templates=[])
