from __future__ import annotations

from logrisk.large_file_pipeline import run_large_file_pipeline


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
