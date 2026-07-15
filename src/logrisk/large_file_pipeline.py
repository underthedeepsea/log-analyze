from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable

from logrisk.partition_spool import spool_normalized_records, update_manifest_status
from logrisk.risk_engine import load_rules, score_risk_entities
from logrisk.stream_input_parser import iter_log_records_from_file
from logrisk.streaming_drain_pipeline import mine_spooled_partitions
from logrisk.semantic.extractor import SemanticExtractor


ProgressCallback = Callable[[dict[str, Any]], None]
DEFAULT_MAX_DECOMPRESSED_BYTES = 1024 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = 100.0
DEFAULT_MAX_LINE_BYTES = 1024 * 1024


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
    max_decompressed_bytes: int = DEFAULT_MAX_DECOMPRESSED_BYTES,
    max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
    max_drain_workers: int = 4,
    reserve_cpu_cores: int = 1,
    process_start_method: str = "spawn",
    semantic_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    started = time.monotonic()
    parsed = 0
    job_root = Path(state_dir) / "input_jobs" / input_job_id
    spool_dir = job_root / "spool"

    def emit(stage: str, progress: float, **extra: Any) -> None:
        if progress_callback:
            payload = {
                "input_job_id": input_job_id,
                "status": "running",
                "stage": stage,
                "size_bytes": input_path.stat().st_size,
                "records_parsed": parsed,
                "lines_read": parsed,
                "progress": progress,
                "elapsed_seconds": round(time.monotonic() - started, 2),
            }
            payload.update(extra)
            progress_callback(payload)

    def source_records():
        nonlocal parsed
        for record in iter_log_records_from_file(
            input_path,
            filename=filename,
            max_decompressed_bytes=max_decompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_line_bytes=max_line_bytes,
        ):
            parsed += 1
            yield record

    emit("spooling", 0.05)
    semantic_extractor = SemanticExtractor.from_snapshot(semantic_snapshot) if semantic_snapshot else None
    manifest = spool_normalized_records(
        source_records(),
        spool_dir=spool_dir,
        partition_by_node=True,
        progress_callback=lambda count: emit("spooling", 0.35),
        semantic_extractor=semantic_extractor,
    )
    update_manifest_status(spool_dir, manifest, "MINING")
    requested_workers = worker_count or (os.cpu_count() or 1)

    def report_mining(completed: int, total: int) -> None:
        emit(
            "drain3_mining",
            0.4 + (0.45 * completed / total if total else 0.45),
            drain3_partitions_total=total,
            drain3_partitions_completed=completed,
            drain3_records_processed=sum(
                int(item["record_count"]) for item in manifest["partitions"][:completed]
            ),
        )

    template_windows, mining = mine_spooled_partitions(
        spool_dir=spool_dir,
        manifest=manifest,
        config_path=config_path,
        state_dir=Path(state_dir) / "drain3",
        window_seconds=window_seconds,
        requested_workers=requested_workers,
        max_workers=max_drain_workers,
        reserve_cpu_cores=reserve_cpu_cores,
        process_start_method=process_start_method,
        progress_callback=report_mining,
    )
    update_manifest_status(spool_dir, manifest, "AGGREGATING")
    risk_entities = score_risk_entities(template_windows, load_rules(rules_path))
    reduced = max(0, parsed - len(template_windows))
    result = {
        "summary": {
            "total_raw_logs": parsed,
            "total_normalized_logs": parsed,
            "total_template_events": mining["template_event_count"],
            "total_template_windows": len(template_windows),
            "drain3_reduced_logs": reduced,
            "drain3_compression_ratio_percent": round(reduced / parsed * 100, 2) if parsed else 0.0,
            "drain3_parallel": mining["parallel"],
            "drain3_worker_count": mining["worker_count"],
            "drain3_partition_count": mining["partition_count"],
            "drain3_process_start_method": mining["process_start_method"],
            "total_risk_entities": len(risk_entities),
            "critical_entities": sum(item.get("risk_level") == "critical" for item in risk_entities),
            "high_entities": sum(item.get("risk_level") == "high" for item in risk_entities),
            "input_job_id": input_job_id,
            "filename": filename,
            "large_file": True,
            "lines_read": parsed,
            "records_parsed": parsed,
            "streaming_spool": True,
            "semantic_enrichment": semantic_snapshot is not None,
            "semantic_dictionary_versions": (semantic_snapshot or {}).get("versions", {}),
        },
        "risk_entities": risk_entities,
        "top_templates": sorted(template_windows, key=lambda item: item.get("count", 0), reverse=True)[:20],
    }
    update_manifest_status(spool_dir, manifest, "COMPLETED")
    emit("completed", 1.0)
    return result
