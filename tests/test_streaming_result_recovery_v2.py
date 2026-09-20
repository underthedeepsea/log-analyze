from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.streaming_state import StreamingStateRepository


def test_committed_windows_include_classified_prefix_for_resume(tmp_path):
    path = tmp_path / "source.log"
    path.write_text("x\n", encoding="utf-8")
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = repository.create_or_load(descriptor=FileIncrementalSource(path, filename="source.log").descriptor(), config_hash="c" * 64)
    for batch, count in ((1, 4), (2, 4), (3, 4)):
        repository.commit_window(
            task["task_id"], window_id=f"batch-{batch}", cursor=SourceCursor("file", {"offset": batch * 10}),
            templates=[], windows=[{"template_hash": f"classified-{batch}", "component": "c", "window_start": "2026-01-01T00:00:00+00:00", "count": count, "risk_semantic": {"risk_type": "x"}}],
            summary={"record_count": count},
        )

    windows = repository.iter_committed_windows(task["task_id"])
    assert sum(item["count"] for item in windows) == 12
    assert len(windows) == 3
