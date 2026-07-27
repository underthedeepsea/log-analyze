from __future__ import annotations

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.streaming_state import StreamingConflictError, StreamingStateRepository


class FailAfterFirstCommitRepository(StreamingStateRepository):
    def __init__(self, database):
        super().__init__(database)
        self.commit_calls = 0

    def commit_window(self, *args, **kwargs):
        self.commit_calls += 1
        if self.commit_calls > 1:
            raise RuntimeError("injected commit failure")
        return super().commit_window(*args, **kwargs)


def test_failed_pipeline_resumes_only_uncommitted_windows(tmp_path):
    source = tmp_path / "events.jsonl"
    source.write_text(
        '{"timestamp":"2026-07-27T00:00:00+00:00","node":"node-a","component":"kernel","severity":"ERROR","message":"error one"}\n'
        '{"timestamp":"2026-07-27T00:10:00+00:00","node":"node-a","component":"kernel","severity":"ERROR","message":"error two"}\n',
        encoding="utf-8",
    )
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    failing = FailAfterFirstCommitRepository(database)

    with pytest.raises(RuntimeError, match="injected commit failure"):
        run_large_file_pipeline(
            input_job_id="input_job_resume",
            input_path=source,
            filename="events.jsonl",
            config_path="configs/drain3_recommended.ini",
            rules_path="configs/risk_rules.yaml",
            state_dir=tmp_path / "state",
            worker_count=1,
            streaming_repository=failing,
            stream_batch_records=1,
        )

    task_id = failing.list_tasks()[0]["task_id"]
    assert failing.get_task(task_id)["status"] == "failed"
    resumed = run_large_file_pipeline(
        input_job_id="input_job_resume",
        input_path=source,
        filename="events.jsonl",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        streaming_repository=StreamingStateRepository(database),
        resume_task_id=task_id,
        stream_batch_records=1,
    )

    assert resumed["summary"]["streaming_resumed"] is True
    assert resumed["summary"]["total_raw_logs"] == 1
    assert resumed["summary"]["streaming_windows_newly_committed"] == 1
    assert resumed["summary"]["streaming_windows_committed"] == 2
    assert failing.get_task(task_id)["status"] == "completed"
    assert len(failing.list_commits(task_id)) == 2


def test_resume_rejects_a_rewritten_source_and_marks_conflict(tmp_path):
    source = tmp_path / "messages"
    source.write_text("error one\n", encoding="utf-8")
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = StreamingStateRepository(database)
    first = run_large_file_pipeline(
        input_job_id="input_job_conflict",
        input_path=source,
        filename="messages",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        streaming_repository=repository,
    )
    task_id = first["summary"]["streaming_task_id"]
    source.write_text("error two\n", encoding="utf-8")

    with pytest.raises(StreamingConflictError, match="输入文件"):
        run_large_file_pipeline(
            input_job_id="input_job_conflict",
            input_path=source,
            filename="messages",
            config_path="configs/drain3_recommended.ini",
            rules_path="configs/risk_rules.yaml",
            state_dir=tmp_path / "state",
            worker_count=1,
            streaming_repository=repository,
            resume_task_id=task_id,
        )

    assert repository.get_task(task_id)["status"] == "conflict"
