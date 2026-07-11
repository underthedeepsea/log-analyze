from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable

from logrisk.stream_input_parser import iter_log_records_from_file
from pipeline.manual_import_pipeline import analyze_records


ProgressCallback = Callable[[dict[str, Any]], None]


def run_large_file_pipeline(
    *,
    input_job_id: str,
    input_path: str | Path,
    filename: str,
    config_path: str | Path,
    rules_path: str | Path,
    state_dir: str | Path,
    window_seconds: int = 300,
    worker_count: int | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    started = time.monotonic()
    records: list[dict[str, Any]] = []

    def emit(stage: str, progress: float, **extra: Any) -> None:
        if progress_callback:
            payload = {
                "input_job_id": input_job_id,
                "status": "running",
                "stage": stage,
                "size_bytes": input_path.stat().st_size,
                "records_parsed": len(records),
                "lines_read": len(records),
                "progress": progress,
                "elapsed_seconds": round(time.monotonic() - started, 2),
            }
            payload.update(extra)
            progress_callback(payload)

    emit("reading", 0.05)
    for record in iter_log_records_from_file(input_path, filename=filename):
        records.append(record)
        if len(records) % 5000 == 0:
            emit("reading", 0.25)
    requested_workers = worker_count or (os.cpu_count() or 1)

    def report_mining(progress: dict[str, int]) -> None:
        total = progress["records_total"]
        completed = progress["records_processed"]
        emit(
            "drain3_mining",
            0.45 + (0.45 * completed / total if total else 0.45),
            drain3_partitions_total=progress["partition_count"],
            drain3_partitions_completed=progress["partitions_completed"],
            drain3_records_processed=completed,
        )

    emit("drain3_mining", 0.45)
    result = analyze_records(
        records,
        config_path=str(config_path),
        rules_path=str(rules_path),
        state_dir=str(state_dir),
        window_seconds=window_seconds,
        drain_worker_count=requested_workers,
        drain_partition_by_node=True,
        drain_progress_callback=report_mining,
    )
    result.setdefault("summary", {})
    result["summary"].update({
        "input_job_id": input_job_id,
        "filename": filename,
        "large_file": True,
        "lines_read": len(records),
        "records_parsed": len(records),
    })
    emit("completed", 1.0)
    return result
