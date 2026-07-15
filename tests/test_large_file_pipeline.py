from __future__ import annotations

from pathlib import Path

from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.semantic.store import SemanticDictionaryStore


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
