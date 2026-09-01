from __future__ import annotations

import os
import time
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from logrisk.incremental_sources import FileIncrementalSource, IncrementalSource, IncrementalSourceError, SourceCursor
from logrisk.partition_spool import spool_normalized_records, update_manifest_status
from logrisk.risk_engine import load_rules, match_template_rule, score_risk_entities
from logrisk.stream_input_parser import iter_log_records_from_file
from logrisk.streaming_drain_pipeline import mine_spooled_partitions
from logrisk.streaming_state import StreamingConflictError, StreamingStateRepository, StreamingTaskBusyError
from logrisk.semantic.extractor import SemanticExtractor
from logrisk.node_risk import NodeRiskError, NodeRiskService
from logrisk.risk_semantics import RiskSemanticError, RiskSemanticService
from logrisk.multi_source.service import MultiSourceService


ProgressCallback = Callable[[dict[str, Any]], None]
DEFAULT_MAX_DECOMPRESSED_BYTES = 1024 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = 100.0
DEFAULT_MAX_LINE_BYTES = 1024 * 1024
MAX_STREAM_BATCH_RECORDS = 10000


def _validate_batch_records(value: int) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"stream_batch_records 必须在 1 到 {MAX_STREAM_BATCH_RECORDS} 之间") from exc
    if not 1 <= normalized <= MAX_STREAM_BATCH_RECORDS:
        raise ValueError(f"stream_batch_records 必须在 1 到 {MAX_STREAM_BATCH_RECORDS} 之间")
    return normalized


def run_incremental_pipeline(
    *,
    input_job_id: str,
    source: IncrementalSource,
    source_name: str,
    config_path: str | Path,
    rules_path: str | Path,
    state_dir: str | Path,
    streaming_repository: StreamingStateRepository,
    window_seconds: int = 300,
    worker_count: int | None = None,
    progress_callback: ProgressCallback | None = None,
    max_drain_workers: int = 4,
    reserve_cpu_cores: int = 1,
    process_start_method: str = "spawn",
    semantic_snapshot: dict[str, Any] | None = None,
    risk_semantics: RiskSemanticService | None = None,
    node_risks: NodeRiskService | None = None,
    multi_source: MultiSourceService | None = None,
    resume_task_id: str | None = None,
    stream_batch_records: int = MAX_STREAM_BATCH_RECORDS,
    source_size_bytes: int = 0,
) -> dict[str, Any]:
    stream_batch_records = _validate_batch_records(stream_batch_records)
    config_hash = hashlib.sha256(Path(config_path).read_bytes()).hexdigest()
    source_cursor = SourceCursor.empty()
    streaming_resumed = False
    if resume_task_id:
        streaming_task = streaming_repository.get_task(resume_task_id)
        previous_status = str(streaming_task.get("status") or "")
        if streaming_task.get("config_hash") != config_hash:
            streaming_repository.mark_failed(resume_task_id, "Drain3 配置已变化，不能继续恢复", conflict=True)
            raise StreamingConflictError("Drain3 配置已变化，不能继续恢复")
        try:
            source.validate_descriptor(streaming_task.get("source") or {})
        except IncrementalSourceError as exc:
            streaming_repository.mark_failed(resume_task_id, str(exc), conflict=True)
            raise StreamingConflictError(str(exc)) from exc
        source_cursor = SourceCursor.from_dict(streaming_task.get("cursor"))
        streaming_resumed = bool(
            source_cursor.value
            or previous_status in {"failed", "interrupted", "conflict"}
        )
    else:
        streaming_task = streaming_repository.create_or_load(
            descriptor=source.descriptor(),
            config_hash=config_hash,
        )
        source_cursor = SourceCursor.from_dict(streaming_task.get("cursor"))
    task_id = str(streaming_task["task_id"])
    try:
        streaming_task = streaming_repository.claim_task(task_id)
    except StreamingTaskBusyError:
        raise
    except Exception as exc:
        streaming_repository.mark_failed(task_id, str(exc))
        raise
    pending_external_commit = SourceCursor.from_dict(streaming_task.get("pending_external_commit"))
    if pending_external_commit.value:
        try:
            source.commit(pending_external_commit)
            streaming_task = streaming_repository.clear_pending_external_commit(task_id, pending_external_commit)
            source_cursor = SourceCursor.from_dict(streaming_task.get("cursor"))
        except Exception as exc:
            streaming_repository.mark_failed(task_id, str(exc))
            raise
    return _run_checkpointed_source_batches(
        input_job_id=input_job_id,
        source=source,
        source_name=source_name,
        source_size_bytes=source_size_bytes,
        source_cursor=source_cursor,
        streaming_task=streaming_task,
        streaming_resumed=streaming_resumed,
        streaming_repository=streaming_repository,
        config_path=config_path,
        rules_path=rules_path,
        state_dir=state_dir,
        window_seconds=window_seconds,
        requested_workers=worker_count or (os.cpu_count() or 1),
        max_drain_workers=max_drain_workers,
        reserve_cpu_cores=reserve_cpu_cores,
        process_start_method=process_start_method,
        semantic_snapshot=semantic_snapshot,
        risk_semantics=risk_semantics,
        node_risks=node_risks,
        multi_source=multi_source,
        progress_callback=progress_callback,
        started=time.monotonic(),
        batch_records=stream_batch_records,
    )


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
    risk_semantics: RiskSemanticService | None = None,
    node_risks: NodeRiskService | None = None,
    multi_source: MultiSourceService | None = None,
    streaming_repository: StreamingStateRepository | None = None,
    resume_task_id: str | None = None,
    stream_batch_records: int = MAX_STREAM_BATCH_RECORDS,
) -> dict[str, Any]:
    input_path = Path(input_path)
    started = time.monotonic()
    parsed = 0
    job_root = Path(state_dir) / "input_jobs" / input_job_id
    spool_dir = job_root / "spool"
    streaming_task: dict[str, Any] | None = None
    source_cursor = SourceCursor.empty()
    current_cursor = source_cursor
    source = FileIncrementalSource(
        input_path,
        filename=filename,
        max_decompressed_bytes=max_decompressed_bytes,
        max_compression_ratio=max_compression_ratio,
        max_line_bytes=max_line_bytes,
    )
    if streaming_repository is not None:
        return run_incremental_pipeline(
            input_job_id=input_job_id,
            source=source,
            source_name=filename,
            source_size_bytes=input_path.stat().st_size,
            config_path=config_path,
            rules_path=rules_path,
            state_dir=state_dir,
            streaming_repository=streaming_repository,
            window_seconds=window_seconds,
            worker_count=worker_count,
            max_drain_workers=max_drain_workers,
            reserve_cpu_cores=reserve_cpu_cores,
            process_start_method=process_start_method,
            semantic_snapshot=semantic_snapshot,
            risk_semantics=risk_semantics,
            node_risks=node_risks,
            multi_source=multi_source,
            progress_callback=progress_callback,
            resume_task_id=resume_task_id,
            stream_batch_records=stream_batch_records,
        )

    def emit(stage: str, progress: float, **extra: Any) -> None:
        if progress_callback:
            elapsed = max(time.monotonic() - started, 0.001)
            payload = {
                "input_job_id": input_job_id,
                "status": "running",
                "stage": stage,
                "size_bytes": input_path.stat().st_size,
                "records_parsed": parsed,
                "lines_read": parsed,
                "progress": progress,
                "elapsed_seconds": round(elapsed, 2),
                "throughput_records_per_second": round(parsed / elapsed, 2),
            }
            if streaming_task is not None:
                payload.update({
                    "streaming_task_id": streaming_task["task_id"],
                    "checkpoint_cursor": current_cursor.to_dict(),
                    "windows_committed": int(streaming_task.get("windows_committed") or 0),
                })
            payload.update(extra)
            progress_callback(payload)

    def source_records():
        nonlocal parsed
        records = iter_log_records_from_file(
            input_path,
            filename=filename,
            max_decompressed_bytes=max_decompressed_bytes,
            max_compression_ratio=max_compression_ratio,
            max_line_bytes=max_line_bytes,
        )
        for record in records:
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
    semantic_matches = 0
    node_risk_ingestions = 0
    if risk_semantics:
        for window in template_windows:
            try:
                semantic_event = risk_semantics.match(window)
            except RiskSemanticError as exc:
                if exc.code != "semantic_unclassified":
                    raise
                risk_semantics.record_unclassified(window)
                continue
            window["risk_semantic"] = semantic_event
            semantic_matches += int(window.get("count") or 1)
            if node_risks and window.get("entity_type") == "node":
                source_record = dict(window, node=window.get("entity_id"))
                try:
                    node_risks.ingest(
                        semantic_event,
                        source_record=source_record,
                        source_job_id=input_job_id,
                        occurrence_count=int(window.get("count") or 1),
                    )
                    node_risk_ingestions += int(window.get("count") or 1)
                except NodeRiskError:
                    continue
    update_manifest_status(spool_dir, manifest, "AGGREGATING")
    risk_entities = score_risk_entities(template_windows, load_rules(rules_path))
    multi_source_result = (
        multi_source.ingest_risk_entities(risk_entities, source_job_id=input_job_id)
        if multi_source else {"observations": 0, "correlations": 0, "unroutable": 0}
    )
    streaming_window_count = 0
    unknown_template_count = 0
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
            "risk_semantic_matches": semantic_matches,
            "node_risk_ingestions": node_risk_ingestions,
            "multi_source": multi_source_result,
            "streaming_task_id": streaming_task.get("task_id") if streaming_task else None,
            "streaming_resumed": False,
            "checkpoint_cursor": current_cursor.to_dict() if streaming_task else None,
            "streaming_windows_committed": int(streaming_task.get("windows_committed") or 0) if streaming_task else 0,
            "streaming_windows_newly_committed": streaming_window_count,
            "unknown_template_count": unknown_template_count,
        },
        "risk_entities": risk_entities,
        "top_templates": sorted(template_windows, key=lambda item: item.get("count", 0), reverse=True)[:20],
    }
    update_manifest_status(spool_dir, manifest, "COMPLETED")
    emit("completed", 1.0)
    return result


def _streaming_template_snapshot(window: dict[str, Any]) -> dict[str, Any]:
    """Select only aggregate, sanitized fields for the persistent unknown-template queue."""

    return {
        "template_hash": window.get("template_hash"),
        "component": window.get("component"),
        "template": window.get("template"),
        "count": window.get("count") or 0,
        "window_start": window.get("window_start"),
        "window_end": window.get("window_end"),
        "severity": window.get("severity"),
        "category": window.get("category"),
        "semantic_fields": window.get("semantic_fields") or {},
    }


def _safe_streaming_result(result: dict[str, Any]) -> dict[str, Any]:
    """Keep persisted streaming results aggregate-only and free of raw log fields."""

    safe_entities = []
    for entity in result.get("risk_entities") or []:
        safe_entity = {
            key: entity.get(key)
            for key in (
                "window_start", "window_end", "cluster", "entity_type", "entity_id",
                "risk_score", "risk_level", "affected_entities", "summary",
            )
        }
        safe_entity["top_templates"] = [
            _streaming_template_snapshot(template)
            for template in entity.get("top_templates") or []
        ]
        safe_entities.append(safe_entity)
    return {
        "summary": dict(result.get("summary") or {}),
        "risk_entities": safe_entities,
        "top_templates": [
            _streaming_template_snapshot(template)
            for template in result.get("top_templates") or []
        ],
    }


def _merge_template_windows(windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge identical aggregate keys that were split across bounded batches."""

    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for window in windows:
        key = (
            window.get("window_start"), window.get("window_end"), window.get("cluster"),
            window.get("entity_type"), window.get("entity_id"), window.get("component"),
            window.get("template_hash"), (window.get("risk_semantic") or {}).get("risk_type"),
        )
        current = merged.get(key)
        if current is None:
            current = dict(window)
            for field in ("affected_namespaces", "affected_pods", "entity_keys", "entity_relations"):
                if isinstance(current.get(field), list):
                    current[field] = list(current[field])
            merged[key] = current
            continue
        current["count"] = int(current.get("count") or 0) + int(window.get("count") or 0)
        for field in ("affected_namespaces", "affected_pods", "entity_keys"):
            current[field] = sorted(set(current.get(field) or []) | set(window.get(field) or []))
        relations = {
            json.dumps(item, ensure_ascii=False, sort_keys=True): item
            for item in (current.get("entity_relations") or [])
        }
        relations.update({
            json.dumps(item, ensure_ascii=False, sort_keys=True): item
            for item in (window.get("entity_relations") or [])
        })
        current["entity_relations"] = [relations[item] for item in sorted(relations)]
        if str(window.get("last_seen") or "") > str(current.get("last_seen") or ""):
            current["last_seen"] = window.get("last_seen")
    return list(merged.values())


def _run_checkpointed_source_batches(
    *,
    input_job_id: str,
    source: IncrementalSource,
    source_name: str,
    source_size_bytes: int,
    source_cursor: SourceCursor,
    streaming_task: dict[str, Any],
    streaming_resumed: bool,
    streaming_repository: StreamingStateRepository,
    config_path: str | Path,
    rules_path: str | Path,
    state_dir: str | Path,
    window_seconds: int,
    requested_workers: int,
    max_drain_workers: int,
    reserve_cpu_cores: int,
    process_start_method: str,
    semantic_snapshot: dict[str, Any] | None,
    risk_semantics: RiskSemanticService | None,
    node_risks: NodeRiskService | None,
    multi_source: MultiSourceService | None,
    progress_callback: ProgressCallback | None,
    started: float,
    batch_records: int,
) -> dict[str, Any]:
    """Process bounded source batches and advance the checkpoint only after commit.

    The batch id is derived from the next committed source cursor.  This makes a
    retry idempotent: an interrupted batch is read again, while already
    committed input is skipped by the file cursor.  The spool directory is
    intentionally reused because Drain3 state is stored separately and remains
    the authority for the next batch.
    """

    batch_records = _validate_batch_records(batch_records)
    parsed = 0
    all_windows: list[dict[str, Any]] = []
    current_cursor = source_cursor
    semantic_matches = 0
    node_risk_ingestions = 0
    newly_committed = 0
    unknown_template_count = 0
    mining_totals = {
        "template_event_count": 0,
        "partition_count": 0,
        "worker_count": 0,
        "parallel": False,
        "process_start_method": process_start_method,
    }
    rules = load_rules(rules_path)
    semantic_extractor = SemanticExtractor.from_snapshot(semantic_snapshot) if semantic_snapshot else None
    source_kind = source.descriptor().kind
    persistent_spool_dir = Path(state_dir) / "input_jobs" / input_job_id / "spool" if source_kind != "kafka" else None

    def emit(stage: str, progress: float, **extra: Any) -> None:
        if progress_callback is None:
            return
        elapsed = max(time.monotonic() - started, 0.001)
        payload = {
            "input_job_id": input_job_id,
            "streaming_task_id": streaming_task["task_id"],
            "status": "running",
            "stage": stage,
            "size_bytes": source_size_bytes,
            "records_parsed": parsed,
            "lines_read": parsed,
            "progress": progress,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_records_per_second": round(parsed / elapsed, 2),
            "checkpoint_cursor": current_cursor.to_dict(),
            "windows_committed": int(streaming_task.get("windows_committed") or 0),
        }
        payload.update(extra)
        progress_callback(payload)

    def enrich_windows(windows: list[dict[str, Any]]) -> tuple[int, int]:
        matched = 0
        ingested = 0
        if risk_semantics is None:
            return matched, ingested
        for window in windows:
            try:
                semantic_event = risk_semantics.match(window)
            except RiskSemanticError as exc:
                if exc.code != "semantic_unclassified":
                    raise
                risk_semantics.record_unclassified(window)
                continue
            window["risk_semantic"] = semantic_event
            matched += int(window.get("count") or 1)
            if node_risks is None or window.get("entity_type") != "node":
                continue
            source_record = dict(window, node=window.get("entity_id"))
            try:
                node_risks.ingest(
                    semantic_event,
                    source_record=source_record,
                    source_job_id=input_job_id,
                    occurrence_count=int(window.get("count") or 1),
                )
                ingested += int(window.get("count") or 1)
            except NodeRiskError:
                continue
        return matched, ingested

    def process_batch(batch: list[dict[str, Any]], checkpoint: SourceCursor, batch_spool_dir: Path) -> None:
        nonlocal semantic_matches, node_risk_ingestions, newly_committed, unknown_template_count, streaming_task
        if not batch:
            return
        streaming_task = streaming_repository.mark_stage(str(streaming_task["task_id"]), "SPOOLING")
        emit("spooling", min(0.35, 0.05 + parsed / max(1, parsed + batch_records)))
        manifest = spool_normalized_records(
            batch,
            spool_dir=batch_spool_dir,
            partition_by_node=True,
            semantic_extractor=semantic_extractor,
        )
        update_manifest_status(batch_spool_dir, manifest, "MINING")
        streaming_task = streaming_repository.mark_stage(str(streaming_task["task_id"]), "MINING")
        template_windows, mining = mine_spooled_partitions(
            spool_dir=batch_spool_dir,
            manifest=manifest,
            config_path=config_path,
            state_dir=Path(state_dir) / "drain3",
            window_seconds=window_seconds,
            requested_workers=requested_workers,
            max_workers=max_drain_workers,
            reserve_cpu_cores=reserve_cpu_cores,
            process_start_method=process_start_method,
        )
        mining_totals["template_event_count"] += int(mining["template_event_count"])
        mining_totals["partition_count"] += int(mining["partition_count"])
        mining_totals["worker_count"] = max(int(mining_totals["worker_count"]), int(mining["worker_count"]))
        mining_totals["parallel"] = bool(mining_totals["parallel"] or mining["parallel"])
        matched, ingested = enrich_windows(template_windows)
        semantic_matches += matched
        node_risk_ingestions += ingested
        unknown = [
            _streaming_template_snapshot(window)
            for window in template_windows
            if not window.get("risk_semantic") and match_template_rule(window, rules) is None
        ]
        streaming_task = streaming_repository.mark_stage(str(streaming_task["task_id"]), "AGGREGATING")
        cursor_hash = hashlib.sha256(
            json.dumps(checkpoint.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:32]
        window_id = f"{source.descriptor().kind}-cursor:{cursor_hash}"
        committed = streaming_repository.commit_window(
            str(streaming_task["task_id"]),
            window_id=window_id,
            cursor=checkpoint,
            templates=unknown,
            summary={
                "record_count": len(batch),
                "template_count": len(template_windows),
                "unknown_template_count": len(unknown),
            },
        )
        source.commit(checkpoint)
        streaming_repository.clear_pending_external_commit(str(streaming_task["task_id"]), checkpoint)
        if committed:
            newly_committed += 1
            unknown_template_count += len(unknown)
            streaming_task["windows_committed"] = int(streaming_task.get("windows_committed") or 0) + 1
        all_windows.extend(template_windows)
        update_manifest_status(batch_spool_dir, manifest, "COMPLETED")
        emit("aggregating", 0.9, windows_pending=0)

    def flush(batch: list[dict[str, Any]], checkpoint: SourceCursor) -> None:
        if not batch:
            return
        if source_kind == "kafka":
            with tempfile.TemporaryDirectory(prefix="logrisk-kafka-") as temporary_root:
                process_batch(batch, checkpoint, Path(temporary_root) / "spool")
            return
        process_batch(batch, checkpoint, persistent_spool_dir or Path(state_dir) / "input_jobs" / input_job_id / "spool")

    batch: list[dict[str, Any]] = []
    try:
        for item in source.read(source_cursor):
            batch.append(item.record)
            current_cursor = item.next_cursor
            parsed += 1
            if len(batch) >= batch_records:
                flush(batch, current_cursor)
                batch = []
        flush(batch, current_cursor)
    except Exception as exc:
        streaming_repository.mark_failed(str(streaming_task["task_id"]), str(exc))
        raise

    all_windows = _merge_template_windows(all_windows)
    risk_entities = score_risk_entities(all_windows, rules)
    multi_source_result = (
        multi_source.ingest_risk_entities(risk_entities, source_job_id=input_job_id)
        if multi_source else {"observations": 0, "correlations": 0, "unroutable": 0}
    )
    reduced = max(0, parsed - len(all_windows))
    result = {
        "summary": {
            "total_raw_logs": parsed,
            "total_normalized_logs": parsed,
            "total_template_events": mining_totals["template_event_count"],
            "total_template_windows": len(all_windows),
            "drain3_reduced_logs": reduced,
            "drain3_compression_ratio_percent": round(reduced / parsed * 100, 2) if parsed else 0.0,
            "drain3_parallel": mining_totals["parallel"],
            "drain3_worker_count": mining_totals["worker_count"],
            "drain3_partition_count": mining_totals["partition_count"],
            "drain3_process_start_method": mining_totals["process_start_method"],
            "total_risk_entities": len(risk_entities),
            "critical_entities": sum(item.get("risk_level") == "critical" for item in risk_entities),
            "high_entities": sum(item.get("risk_level") == "high" for item in risk_entities),
            "input_job_id": input_job_id,
            "filename": source_name,
            "large_file": True,
            "lines_read": parsed,
            "records_parsed": parsed,
            "streaming_spool": True,
            "semantic_enrichment": semantic_snapshot is not None,
            "semantic_dictionary_versions": (semantic_snapshot or {}).get("versions", {}),
            "risk_semantic_matches": semantic_matches,
            "node_risk_ingestions": node_risk_ingestions,
            "multi_source": multi_source_result,
            "streaming_task_id": streaming_task["task_id"],
            "streaming_resumed": streaming_resumed,
            "checkpoint_cursor": current_cursor.to_dict(),
            "streaming_windows_committed": int(streaming_task.get("windows_committed") or 0),
            "streaming_windows_newly_committed": newly_committed,
            "unknown_template_count": unknown_template_count,
        },
        "risk_entities": risk_entities,
        "top_templates": sorted(all_windows, key=lambda item: item.get("count", 0), reverse=True)[:20],
    }
    streaming_repository.save_result(str(streaming_task["task_id"]), _safe_streaming_result(result))
    streaming_task = streaming_repository.mark_completed(str(streaming_task["task_id"]))
    emit("completed", 1.0)
    return result
