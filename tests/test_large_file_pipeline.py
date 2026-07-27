from __future__ import annotations

from pathlib import Path

from logrisk.database import SQLiteDatabase
from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.semantic.store import SemanticDictionaryStore
from logrisk.streaming_state import StreamingStateRepository


def test_large_file_pipeline_reports_parallel_drain3_metadata_and_progress(tmp_path):
    source = tmp_path / "messages"
    source.write_text(
        "Jun 10 10:00:00 node-a kernel: kernel error alpha\n"
        "Jun 10 10:00:01 node-b kernel: kernel error beta\n",
        encoding="utf-8",
    )
    updates: list[dict[str, object]] = []

    result = run_large_file_pipeline(
        input_job_id="input_job_test",
        input_path=source,
        filename="messages",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=4,
        progress_callback=updates.append,
    )

    summary = result["summary"]
    assert summary["drain3_parallel"] is True
    assert summary["drain3_worker_count"] == 2
    assert summary["drain3_partition_count"] == 2
    assert any(update.get("drain3_partitions_total") == 2 for update in updates)
    assert any(update.get("drain3_partitions_completed") == 2 for update in updates)
    assert summary["streaming_spool"] is True
    assert summary["drain3_process_start_method"] == "spawn"
    assert (tmp_path / "state" / "input_jobs" / "input_job_test" / "spool" / "manifest.json").exists()


def test_large_file_pipeline_uses_pinned_semantic_snapshot(tmp_path):
    source = tmp_path / "messages"
    source.write_text(
        "Jun 10 10:00:00 node-a kernel: NVRM: Xid 79, GPU has fallen off the bus\n",
        encoding="utf-8",
    )
    snapshot = SemanticDictionaryStore(
        tmp_path / "semantic",
        Path("configs/semantic_dictionary").resolve(),
    ).active_snapshot()

    result = run_large_file_pipeline(
        input_job_id="input_job_semantic",
        input_path=source,
        filename="messages",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        semantic_snapshot=snapshot,
    )

    assert result["top_templates"][0]["semantic_fields"]["xid_code"] == [{"value": 79, "count": 1}]
    assert result["summary"]["semantic_dictionary_versions"]["nvidia"]["version"] == 1


def test_large_file_pipeline_persists_streaming_checkpoint_and_unknown_templates(tmp_path):
    source = tmp_path / "messages"
    source.write_text("Jun 10 10:00:00 node-a app: unusual failure one\n", encoding="utf-8")
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))

    result = run_large_file_pipeline(
        input_job_id="input_job_streaming",
        input_path=source,
        filename="messages",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        streaming_repository=repository,
    )

    task_id = result["summary"]["streaming_task_id"]
    task = repository.get_task(task_id)

    assert task["status"] == "completed"
    assert task["cursor"]["kind"] == "file"
    assert task["cursor"]["value"]["offset"] == source.stat().st_size
    assert repository.list_commits(task_id)
    assert repository.list_unknown_templates(task_id=task_id)


def test_large_file_pipeline_resumes_from_checkpoint_for_appended_file_data(tmp_path):
    source = tmp_path / "messages"
    source.write_text("Jun 10 10:00:00 node-a app: failure one\n", encoding="utf-8")
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    first = run_large_file_pipeline(
        input_job_id="input_job_resume",
        input_path=source,
        filename="messages",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        streaming_repository=repository,
    )
    task_id = first["summary"]["streaming_task_id"]
    source.write_text(
        "Jun 10 10:00:00 node-a app: failure one\n"
        "Jun 10 10:01:00 node-a app: failure two\n",
        encoding="utf-8",
    )

    resumed = run_large_file_pipeline(
        input_job_id="input_job_resume",
        input_path=source,
        filename="messages",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        streaming_repository=repository,
        resume_task_id=task_id,
    )

    assert resumed["summary"]["total_raw_logs"] == 1
    assert repository.get_task(task_id)["cursor"]["value"]["offset"] == source.stat().st_size
    assert len(repository.list_commits(task_id)) == 2
