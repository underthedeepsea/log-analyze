from __future__ import annotations

import os
import time
import hashlib
import json
import tempfile
import multiprocessing
from concurrent.futures import CancelledError as FutureCancelledError, ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Mapping

from logrisk.incremental_sources import (
    FileIncrementalSource,
    IncrementalSource,
    IncrementalSourceError,
    SourceCursor,
    SourceDescriptor,
    SourceRecord,
)
from logrisk.operational_ledgers import canonical_ranges, ingestion_batch_id, source_identity
from logrisk.partition_spool import spool_normalized_records, update_manifest_status
from logrisk.miner_generations import prepare_generation, seal_generation
from logrisk.risk_engine import load_rules, match_template_rule, score_risk_entities
from logrisk.stream_input_parser import iter_log_records_from_file
from logrisk.streaming_drain_pipeline import mine_spooled_partitions
from logrisk.streaming_state import (
    StreamingConflictError,
    StreamingStateError,
    StreamingStateRepository,
    StreamingTaskBusyError,
)
from logrisk.semantic.extractor import SemanticExtractor
from logrisk.node_risk import NodeRiskError, NodeRiskService
from logrisk.risk_semantics import RiskSemanticError, RiskSemanticService
from logrisk.multi_source.service import MultiSourceService


ProgressCallback = Callable[[dict[str, Any]], None]
DEFAULT_MAX_DECOMPRESSED_BYTES = 1024 * 1024 * 1024
DEFAULT_MAX_COMPRESSION_RATIO = 100.0
DEFAULT_MAX_LINE_BYTES = 1024 * 1024
MAX_STREAM_BATCH_RECORDS = 10000
MAX_STREAM_BATCH_BYTES = 16 * 1024 * 1024


class _LazyProcessPoolExecutor:
    """Create the task-owned process pool only when parallel mining starts."""

    def __init__(
        self,
        *,
        max_workers: int,
        process_start_method: str,
        executor_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.max_workers = max_workers
        self.process_start_method = process_start_method
        self._executor_factory = executor_factory
        self._executor: Any | None = None

    def submit(self, function: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        if self._executor is None:
            factory = self._executor_factory or ProcessPoolExecutor
            self._executor = factory(
                max_workers=self.max_workers,
                mp_context=multiprocessing.get_context(self.process_start_method),
            )
        return self._executor.submit(function, *args, **kwargs)

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = True) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)


def _shutdown_executor(executor: Any, active_error: BaseException | None) -> None:
    """Preserve the active business/cancellation error if cleanup also fails."""
    try:
        executor.shutdown(wait=True, cancel_futures=True)
    except BaseException as cleanup_error:
        if active_error is None:
            raise
        setattr(active_error, "_logrisk_cleanup_error", type(cleanup_error).__name__)


def _exception_text(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    cleanup = getattr(exc, "_logrisk_cleanup_error", None)
    return f"{text}（执行器清理失败：{cleanup}）" if cleanup else text


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
    ledger_repository: Any | None = None,
    environment: str = "production",
    scope_key: str = "default",
    parser_version: str = "stream_input_parser_v1",
) -> dict[str, Any]:
    stream_batch_records = _validate_batch_records(stream_batch_records)
    config_hash = hashlib.sha256(Path(config_path).read_bytes()).hexdigest()
    if ledger_repository is not None and isinstance(source, FileIncrementalSource):
        # The persisted descriptor must carry the same immutable identity that
        # the receipt uses, including the explicit source scope.
        source.environment = str(environment or "production")
        source.scope_key = str(scope_key or "default")
        source.immutable_identity = True
    if ledger_repository is not None:
        if getattr(streaming_repository, "ledger_repository", None) is None:
            streaming_repository.ledger_repository = ledger_repository
        elif streaming_repository.ledger_repository is not ledger_repository:
            raise StreamingConflictError("流式任务与 operational ledger Provider 不一致")
    source_descriptor = source.descriptor()
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
            descriptor=source_descriptor,
            config_hash=config_hash,
        )
        source_cursor = SourceCursor.from_dict(streaming_task.get("cursor"))
    task_id = str(streaming_task["task_id"])
    # A failed claim does not establish ownership, so it must never publish a
    # terminal state for a task that may belong to another Worker.
    streaming_task = streaming_repository.claim_task(task_id)
    lease_token = str(streaming_task.get("lease_token") or "")
    try:
        streaming_repository.require_complete_prefix(task_id)
        pending_external_commit = SourceCursor.from_dict(streaming_task.get("pending_external_commit"))
        if pending_external_commit.value:
            source.commit(pending_external_commit)
            streaming_task = streaming_repository.clear_pending_external_commit(
                task_id, pending_external_commit, expected_lease_token=lease_token,
            )
            source_cursor = SourceCursor.from_dict(streaming_task.get("cursor"))
        return _run_checkpointed_source_batches(
            input_job_id=input_job_id,
            source=source,
            source_descriptor=source_descriptor,
            source_name=source_name,
            source_size_bytes=source_size_bytes,
            source_cursor=source_cursor,
            streaming_task=streaming_task,
            lease_token=lease_token,
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
            ledger_repository=ledger_repository,
            environment=environment,
            scope_key=scope_key,
            parser_version=parser_version,
        )
    except BaseException as exc:
        interrupted = not isinstance(exc, Exception) or isinstance(exc, FutureCancelledError)
        try:
            streaming_repository.finish_claim(
                task_id,
                lease_token,
                error=_exception_text(exc),
                interrupted=interrupted,
                conflict=isinstance(exc, StreamingConflictError),
            )
        except BaseException:
            # The original business/cancellation exception is the caller's
            # actionable failure. A database outage can make terminal-state
            # persistence impossible and must not replace that exception.
            pass
        raise


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
    ledger_repository: Any | None = None,
    environment: str = "production",
    scope_key: str = "default",
    parser_version: str = "stream_input_parser_v1",
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
        environment=environment,
        scope_key=scope_key,
        immutable_identity=ledger_repository is not None,
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
            ledger_repository=ledger_repository,
            environment=environment,
            scope_key=scope_key,
            parser_version=parser_version,
        )
    if ledger_repository is not None:
        raise StreamingStateError(
            "operational ledger 需要 checkpointed streaming_repository；非流式 helper 不提供可验证收据"
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
        "window_start": window.get("window_start") or "unknown",
        "window_end": window.get("window_end"),
        "time_quality": window.get("time_quality") or ("unknown" if not window.get("window_start") else "event"),
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
    safe_result = {
        "summary": dict(result.get("summary") or {}),
        "risk_entities": safe_entities,
        "top_templates": [
            _streaming_template_snapshot(template)
            for template in result.get("top_templates") or []
        ],
    }
    for field in ("schema_version", "result_ref", "complete", "next_cursor"):
        if field in result:
            safe_result[field] = result[field]
    return safe_result


def _merge_template_windows(windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge identical aggregate keys that were split across bounded batches."""

    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for window in windows:
        key = (
            window.get("window_start"), window.get("window_end"), window.get("cluster"),
            window.get("entity_type"), window.get("entity_id"), window.get("component"),
            window.get("template_hash"), (window.get("risk_semantic") or {}).get("risk_type"),
            window.get("source_type"), window.get("semantic_extractor_version"),
            json.dumps(window.get("semantic_dictionary_versions") or {}, sort_keys=True),
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
        current["semantic_tags"] = sorted(set(current.get("semantic_tags") or []) | set(window.get("semantic_tags") or []))
        distributions = {}
        for source in (current.get("severity_distribution") or {},window.get("severity_distribution") or {}):
            for severity,count in source.items():
                distributions[severity] = distributions.get(severity,0) + int(count)
        if distributions:
            current["severity_distribution"] = distributions
        parameters = {}
        for entry in (current.get("typed_parameters") or []) + (window.get("typed_parameters") or []):
            key = (str(entry.get("field") or ""),str(entry.get("typed_mask") or ""))
            value = parameters.setdefault(key,dict(entry,count=0))
            value["count"] += int(entry.get("count") or 0)
        if parameters:
            current["typed_parameters"] = [parameters[key] for key in sorted(parameters)]
        severity_order = {"trace": 0, "debug": 1, "info": 2, "notice": 3, "warning": 4, "warn": 4, "error": 5, "critical": 6, "fatal": 7}
        if severity_order.get(str(window.get("severity") or "").lower(), -1) > severity_order.get(str(current.get("severity") or "").lower(), -1):
            current["severity"] = window.get("severity")
        if window.get("first_seen") and (not current.get("first_seen") or str(window["first_seen"]) < str(current["first_seen"])):
            current["first_seen"] = window["first_seen"]
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
        semantic_counts: dict[str, dict[str, dict[str, Any]]] = {}
        for source in (current.get("semantic_fields") or {}, window.get("semantic_fields") or {}):
            for field, values in source.items():
                target = semantic_counts.setdefault(str(field), {})
                for entry in values if isinstance(values, list) else []:
                    value_key = json.dumps(entry.get("value"), ensure_ascii=False, sort_keys=True)
                    item = target.setdefault(value_key, {"value": entry.get("value"), "count": 0})
                    item["count"] += int(entry.get("count") or 0)
        if semantic_counts:
            current["semantic_fields"] = {
                field: sorted(values.values(), key=lambda item: (-item["count"], str(item["value"])))
                for field, values in sorted(semantic_counts.items())
            }
    return [merged[key] for key in sorted(merged, key=lambda item: tuple(str(value or "") for value in item))]


def _run_checkpointed_source_batches(
    *,
    input_job_id: str,
    source: IncrementalSource,
    source_name: str,
    source_size_bytes: int,
    source_cursor: SourceCursor,
    source_descriptor: SourceDescriptor,
    streaming_task: dict[str, Any],
    lease_token: str,
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
    ledger_repository: Any | None,
    environment: str,
    scope_key: str,
    parser_version: str,
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
    source_kind = source_descriptor.kind
    persistent_spool_dir = Path(state_dir) / "input_jobs" / input_job_id / "spool" if source_kind != "kafka" else None
    ledger_source = (
        _ledger_source(
            source,
            descriptor=source_descriptor,
            environment=environment,
            scope_key=scope_key,
        )
        if ledger_repository is not None
        else None
    )
    if ledger_repository is not None and ledger_source is None:
        raise IncrementalSourceError("来源缺少可验证的 immutable identity，不能记录 operational receipt")
    receipt_ids: list[str] = []
    seen_positions: set[tuple[str, int, int]] = set()
    overall_provenance = "verified"
    shared_executor: _LazyProcessPoolExecutor | None = None
    if requested_workers > 1:
        available = max(1, (os.cpu_count() or 1) - max(0, reserve_cpu_cores))
        executor_workers = min(max(1, requested_workers), max(1, max_drain_workers), available)
        if executor_workers > 1:
            shared_executor = _LazyProcessPoolExecutor(
                max_workers=executor_workers,
                process_start_method=process_start_method,
            )

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

    def enrich_windows(windows: list[dict[str, Any]]) -> int:
        matched = 0
        if risk_semantics is None:
            return matched
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
        return matched

    def process_batch(
        batch: list[dict[str, Any]],
        checkpoint: SourceCursor,
        batch_ranges: list[dict[str, Any]],
        receipt_ranges: list[dict[str, Any]],
        batch_provenance: str,
        batch_spool_dir: Path,
    ) -> None:
        nonlocal semantic_matches, node_risk_ingestions, newly_committed, unknown_template_count, streaming_task
        if not batch:
            commit_receipt_only(checkpoint, receipt_ranges, batch_provenance)
            return
        streaming_task = streaming_repository.mark_claim_stage(
            str(streaming_task["task_id"]), lease_token, "SPOOLING"
        )
        emit("spooling", min(0.35, 0.05 + parsed / max(1, parsed + batch_records)))
        manifest = spool_normalized_records(
            batch,
            spool_dir=batch_spool_dir,
            partition_by_node=True,
            semantic_extractor=semantic_extractor,
        )
        update_manifest_status(batch_spool_dir, manifest, "MINING")
        streaming_task = streaming_repository.mark_claim_stage(
            str(streaming_task["task_id"]), lease_token, "MINING"
        )
        generation_dir = prepare_generation(
            Path(state_dir) / "miner_generations" / str(streaming_task["task_id"]),
            streaming_task.get("miner_generation"),
        )
        template_windows, mining = mine_spooled_partitions(
            spool_dir=batch_spool_dir,
            manifest=manifest,
            config_path=config_path,
            state_dir=generation_dir,
            window_seconds=window_seconds,
            requested_workers=requested_workers,
            max_workers=max_drain_workers,
            reserve_cpu_cores=reserve_cpu_cores,
            process_start_method=process_start_method,
            executor=shared_executor,
        )
        mining_totals["template_event_count"] += int(mining["template_event_count"])
        mining_totals["partition_count"] += int(mining["partition_count"])
        mining_totals["worker_count"] = max(int(mining_totals["worker_count"]), int(mining["worker_count"]))
        mining_totals["parallel"] = bool(mining_totals["parallel"] or mining["parallel"])
        matched = enrich_windows(template_windows)
        semantic_matches += matched
        unknown = [
            _streaming_template_snapshot(window)
            for window in template_windows
            if not window.get("risk_semantic") and match_template_rule(window, rules) is None
        ]
        streaming_task = streaming_repository.mark_claim_stage(
            str(streaming_task["task_id"]), lease_token, "AGGREGATING"
        )
        cursor_hash = hashlib.sha256(
            json.dumps(checkpoint.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:32]
        window_id = f"{source_kind}-cursor:{cursor_hash}"
        namespace = hashlib.sha256(json.dumps(source_descriptor.to_dict(),sort_keys=True).encode()).hexdigest()
        for index,window in enumerate(template_windows):
            window.update(source_namespace=namespace,source_item_id=str(index))
        generation = seal_generation(generation_dir)
        committed = streaming_repository.commit_window(
            str(streaming_task["task_id"]),
            window_id=window_id,
            cursor=checkpoint,
            templates=unknown,
            windows=template_windows,
            summary={
                "record_count": len(batch),
                "template_count": len(template_windows),
                "unknown_template_count": len(unknown),
                "risk_semantic_matches": matched,
                "template_event_count": int(mining["template_event_count"]),
                "partition_count": int(mining["partition_count"]),
                "worker_count": int(mining["worker_count"]),
                "parallel": bool(mining["parallel"]),
                "node_risk_enabled": node_risks is not None,
            },
            ledger_batch=(
                {
                    "batch_id": ingestion_batch_id(input_job_id, window_id),
                    "source": ledger_source,
                    "input_job_id": input_job_id,
                    "checkpoint_key": window_id,
                    "parser_version": parser_version,
                    "ranges": receipt_ranges,
                    "actual_count": _range_count(receipt_ranges) if batch_provenance == "verified" else None,
                    "provenance": batch_provenance,
                }
                if ledger_repository is not None
                else None
            ),
            fencing_token=lease_token,
            miner_generation=generation,
            expected_cursor=streaming_task.get("cursor"),
        )
        source.commit(checkpoint)
        streaming_repository.clear_pending_external_commit(
            str(streaming_task["task_id"]), checkpoint, expected_lease_token=lease_token,
        )
        if committed:
            newly_committed += 1
            unknown_template_count += len(unknown)
            streaming_task["windows_committed"] = int(streaming_task.get("windows_committed") or 0) + 1
            if node_risks is not None:
                contributions = []
                for window in template_windows:
                    semantic_event = window.get("risk_semantic")
                    if not semantic_event or window.get("entity_type") != "node":
                        continue
                    source_record = dict(
                        window, node=window.get("entity_id"), source_batch_id=window_id,
                        semantic_revision=(semantic_snapshot or {}).get("versions") or {},
                    )
                    contributions.append({
                        "semantic_event": semantic_event, "source_record": source_record,
                        "source_job_id": input_job_id,
                        "occurrence_count": int(window.get("count") or 1),
                    })
                if contributions:
                    try:
                        node_risks.ingest_batch(contributions)
                        node_risk_ingestions += sum(int(item["occurrence_count"]) for item in contributions)
                    except NodeRiskError:
                        pass
        if ledger_repository is not None:
            receipt_ids.append(ingestion_batch_id(input_job_id, window_id))
        update_manifest_status(batch_spool_dir, manifest, "COMPLETED")
        emit("aggregating", 0.9, windows_pending=0)

    def commit_receipt_only(
        checkpoint: SourceCursor,
        ranges: list[dict[str, Any]],
        provenance: str,
    ) -> None:
        """Commit an empty/duplicate source window without mining it."""

        nonlocal newly_committed, streaming_task
        if ledger_repository is None:
            return
        cursor_hash = hashlib.sha256(
            json.dumps(checkpoint.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:32]
        window_id = f"{source_kind}-cursor:{cursor_hash}"
        committed = streaming_repository.commit_window(
            str(streaming_task["task_id"]),
            window_id=window_id,
            cursor=checkpoint,
            templates=[],
            windows=[],
            summary={"record_count": 0, "template_count": 0, "unknown_template_count": 0,
                     "risk_semantic_matches": 0, "template_event_count": 0, "partition_count": 0,
                     "worker_count": 0, "parallel": False, "node_risk_enabled": node_risks is not None},
            ledger_batch={
                "batch_id": ingestion_batch_id(input_job_id, window_id),
                "source": ledger_source,
                "input_job_id": input_job_id,
                "checkpoint_key": window_id,
                "parser_version": parser_version,
                "ranges": ranges,
                "actual_count": _range_count(ranges) if provenance == "verified" else None,
                "provenance": provenance,
            },
            fencing_token=lease_token,
            expected_cursor=streaming_task.get("cursor"),
        )
        source.commit(checkpoint)
        streaming_repository.clear_pending_external_commit(
            str(streaming_task["task_id"]), checkpoint, expected_lease_token=lease_token,
        )
        if committed:
            newly_committed += 1
            streaming_task["windows_committed"] = int(streaming_task.get("windows_committed") or 0) + 1
        receipt_ids.append(ingestion_batch_id(input_job_id, window_id))

    def flush(
        batch: list[dict[str, Any]],
        checkpoint: SourceCursor,
        batch_ranges: list[dict[str, Any]],
        receipt_ranges: list[dict[str, Any]],
        batch_provenance: str,
    ) -> None:
        if not batch and not receipt_ranges:
            return
        if source_kind == "kafka":
            with tempfile.TemporaryDirectory(prefix="logrisk-kafka-") as temporary_root:
                process_batch(
                    batch,
                    checkpoint,
                    batch_ranges,
                    receipt_ranges,
                    batch_provenance,
                    Path(temporary_root) / "spool",
                )
            return
        process_batch(
            batch,
            checkpoint,
            batch_ranges,
            receipt_ranges,
            batch_provenance,
            persistent_spool_dir or Path(state_dir) / "input_jobs" / input_job_id / "spool",
        )

    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    batch_ranges: list[dict[str, Any]] = []
    receipt_ranges: list[dict[str, Any]] = []
    batch_provenance = "verified"
    batch_error: BaseException | None = None
    try:
        # Replay committed facts before new input; contribution fingerprints make
        # recovery after DB commit / broker failure idempotent for node effects.
        if node_risks is not None:
            after_key = None
            while True:
                restored = streaming_repository.iter_committed_windows(str(streaming_task["task_id"]), after_key=after_key, limit=250)
                if not restored:
                    break
                contributions = [{
                    "semantic_event": window["risk_semantic"],
                    "source_record": dict(window, node=window.get("entity_id"), semantic_revision=(semantic_snapshot or {}).get("versions") or {}),
                    "source_job_id": input_job_id, "occurrence_count": int(window.get("count") or 1),
                } for window in restored if window.get("risk_semantic") and window.get("entity_type") == "node"]
                if contributions:
                    node_risks.ingest_batch(contributions)
                last = restored[-1]
                after_key = (str(last["source_batch_id"]), int(last["_commit_item_index"]))
        for item in source.read(source_cursor):
            current_cursor = _cursor_at_least(item.next_cursor, current_cursor)
            position = _source_record_position(item, source_kind)
            if ledger_repository is not None and position is None:
                batch_provenance = "reported"
                overall_provenance = "reported"
            if position is not None:
                position_key = (
                    str(position["partition_key"]),
                    int(position["start"]),
                    int(position["end"]),
                )
                receipt_ranges.append(position)
                if position_key in seen_positions:
                    continue
                seen_positions.add(position_key)
                batch_ranges.append(position)
            batch.append(item.record)
            batch_bytes += len(json.dumps(item.record, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            parsed += 1
            if len(batch) >= batch_records or batch_bytes >= MAX_STREAM_BATCH_BYTES:
                flush(
                    batch,
                    current_cursor,
                    canonical_ranges(batch_ranges),
                    canonical_ranges(receipt_ranges),
                    batch_provenance,
                )
                batch = []
                batch_bytes = 0
                batch_ranges = []
                receipt_ranges = []
                batch_provenance = "verified"
        if batch or receipt_ranges:
            flush(
                batch,
                current_cursor,
                canonical_ranges(batch_ranges),
                canonical_ranges(receipt_ranges),
                batch_provenance,
            )
        elif ledger_repository is not None and not receipt_ids and not int(
            streaming_task.get("windows_committed") or 0
        ):
            commit_receipt_only(current_cursor, [], "verified")
    except BaseException as exc:
        batch_error = exc
        raise
    finally:
        if shared_executor is not None:
            _shutdown_executor(shared_executor, batch_error)

    from logrisk.streaming_results import StreamingResultRepository
    result_repository = StreamingResultRepository(streaming_repository.database)
    result_ref = result_repository.build(str(streaming_task["task_id"]),rules)
    result_totals = result_repository.summary(result_ref)
    first_page = result_repository.entities(result_ref)
    top_templates = result_repository.top_windows(result_ref)
    risk_entities = first_page["items"]
    result_is_paged = first_page["next_key"] is not None
    committed_totals = streaming_repository.committed_summary(str(streaming_task["task_id"]))
    invocation_records = parsed
    parsed = int(committed_totals.get("record_count") or 0)
    unknown_template_count = committed_totals["unknown_template_count"]
    semantic_matches = committed_totals["risk_semantic_matches"]
    for field in ("template_event_count", "partition_count", "worker_count", "parallel"):
        mining_totals[field] = committed_totals[field]
    node_risk_ingestions = 0
    if node_risks is not None:
        after_key = None
        while True:
            restored = streaming_repository.iter_committed_windows(str(streaming_task["task_id"]), after_key=after_key, limit=250)
            if not restored:
                break
            count = node_risks.committed_ingestion_count(restored, semantic_revision=(semantic_snapshot or {}).get("versions") or {})
            if count is None:
                node_risk_ingestions = None
                break
            node_risk_ingestions += count
            last = restored[-1]
            after_key = (str(last["source_batch_id"]), int(last["_commit_item_index"]))
    elif committed_totals["node_risk_enabled"] is not False:
        node_risk_ingestions = None
    multi_source_result = {"observations":0,"correlations":0,"unroutable":0}
    if multi_source:
        page = first_page
        clusters: set[str] = set()
        earliest = None
        while True:
            part=multi_source.ingest_risk_entities(page["items"],source_job_id=input_job_id,correlate=False)
            for field in ("observations","unroutable"):
                multi_source_result[field]+=part[field]
            clusters.update(part.get("clusters") or [])
            if part.get("earliest"):
                earliest=min(earliest,part["earliest"]) if earliest else part["earliest"]
            if not page["next_key"]:
                break
            page=result_repository.entities(result_ref,after=page["next_key"])
        multi_source_result["correlations"]=multi_source.correlate_clusters(clusters,earliest)
    reduced = max(0, parsed - int(result_totals["windows"]))
    result = {
        "summary": {
            "total_raw_logs": parsed,
            "total_normalized_logs": parsed,
            "total_template_events": mining_totals["template_event_count"],
            "total_template_windows": result_totals["windows"],
            "drain3_reduced_logs": reduced,
            "drain3_compression_ratio_percent": round(reduced / parsed * 100, 2) if parsed else 0.0,
            "drain3_parallel": mining_totals["parallel"],
            "drain3_worker_count": mining_totals["worker_count"],
            "drain3_partition_count": mining_totals["partition_count"],
            "drain3_process_start_method": mining_totals["process_start_method"],
            "total_risk_entities": result_totals["entities"],
            "critical_entities": result_totals["critical"],
            "high_entities": result_totals["high"],
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
            "streaming_result_completeness": "complete",
            "streaming_summary_completeness": committed_totals["completeness"],
            "node_risk_ingestions_completeness": "unknown" if node_risk_ingestions is None else "verified",
            "invocation": {"records_parsed": invocation_records, "newly_committed_batches": newly_committed,
                           "process_start_method": process_start_method},
            "unknown_template_count": unknown_template_count,
            "provenance": overall_provenance if ledger_repository is not None and ledger_source is not None else "unverified-input",
            "source": ledger_source,
            "ingestion_receipt_ids": _receipt_ids_for_input_job(ledger_repository, input_job_id, receipt_ids),
        },
        "risk_entities": risk_entities,
        "top_templates": top_templates,
    }
    result["risk_entities"].sort(key=lambda item: -float(item["risk_score"]))
    if result_is_paged:
        result.update(schema_version="streaming_result_ref_v1",result_ref=result_ref,complete=False,next_cursor=first_page["next_key"])
    streaming_repository.save_result(
        str(streaming_task["task_id"]),
        _safe_streaming_result(result),
        expected_lease_token=lease_token,
    )
    streaming_task = streaming_repository.complete_claim(
        str(streaming_task["task_id"]), lease_token
    )
    emit("completed", 1.0)
    return result


def _range_count(ranges: list[Mapping[str, Any]]) -> int:
    return sum(int(item["end"]) - int(item["start"]) for item in canonical_ranges(ranges))


def _source_record_position(item: SourceRecord, source_kind: str) -> dict[str, Any] | None:
    metadata_value = getattr(item, "metadata", None)
    if metadata_value is None:
        metadata_value = getattr(item, "position", None)
    metadata = metadata_value if isinstance(metadata_value, Mapping) else None
    if metadata:
        if {"partition_key", "start", "end"}.issubset(metadata):
            partition_value = metadata["partition_key"]
            if isinstance(partition_value, bool) or not isinstance(partition_value, str):
                raise IncrementalSourceError("来源记录 partition_key 无效")
            partition_key = partition_value
            start = _position_integer(metadata["start"], "start")
            end = _position_integer(metadata["end"], "end")
            if start < 0 or end <= start:
                raise IncrementalSourceError("来源记录 position 必须是非负半开区间")
            return {"partition_key": partition_key, "start": start, "end": end}
        if {"partition", "offset"}.issubset(metadata):
            partition_value = metadata["partition"]
            if isinstance(partition_value, bool) or not isinstance(partition_value, (str, int)):
                raise IncrementalSourceError("Kafka 来源 partition 无效")
            partition = str(partition_value)
            offset = _position_integer(metadata["offset"], "offset")
            if offset < 0:
                raise IncrementalSourceError("Kafka 来源记录 offset 不能为负数")
            return {"partition_key": partition, "start": offset, "end": offset + 1}
        raise IncrementalSourceError("来源记录 position 缺少 partition/start/end")
    if source_kind == "file":
        line_no = item.record.get("_line_no")
        if isinstance(line_no, int) and not isinstance(line_no, bool) and line_no >= 1:
            return {"partition_key": "", "start": line_no, "end": line_no + 1}
    return None


def _position_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise IncrementalSourceError(f"来源记录 {field} 必须是整数")
    return int(value)


def _cursor_at_least(candidate: SourceCursor, current: SourceCursor) -> SourceCursor:
    """Keep a replayed adapter from moving the durable cursor backwards."""

    if not current.kind:
        return candidate
    if candidate.kind != current.kind:
        return current
    if candidate.kind == "file":
        candidate_key = (
            int(candidate.value.get("offset") or 0),
            int(candidate.value.get("line") or 0),
        )
        current_key = (
            int(current.value.get("offset") or 0),
            int(current.value.get("line") or 0),
        )
        return candidate if candidate_key >= current_key else current
    if candidate.kind == "kafka":
        for field in ("partitions", "high_water"):
            candidate_map = candidate.value.get(field)
            current_map = current.value.get(field)
            if not isinstance(current_map, Mapping):
                continue
            if not isinstance(candidate_map, Mapping):
                return current
            candidate_values = {str(key): _cursor_integer(value, field) for key, value in candidate_map.items()}
            current_values = {str(key): _cursor_integer(value, field) for key, value in current_map.items()}
            if set(candidate_values) < set(current_values) or any(
                candidate_values[key] < current_values[key] for key in current_values
            ):
                return current
        if "offset" in current.value and "offset" in candidate.value:
            if str(candidate.value.get("partition")) != str(current.value.get("partition")):
                return current
            if _cursor_integer(candidate.value.get("offset"), "offset") < _cursor_integer(
                current.value.get("offset"), "offset"
            ):
                return current
    return candidate


def _cursor_integer(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise IncrementalSourceError(f"Checkpoint {field} 必须是整数")
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise IncrementalSourceError(f"Checkpoint {field} 必须是整数")


def _receipt_ids_for_input_job(
    ledger_repository: Any | None,
    input_job_id: str,
    current: list[str],
) -> list[str]:
    """Return durable receipt IDs so a resumed result cannot lose linkage."""

    if ledger_repository is None:
        return current
    list_batches = getattr(ledger_repository, "list_ingestion_batches", None)
    if not callable(list_batches):
        return current
    result: list[str] = []
    after: str | None = None
    try:
        while True:
            page = list_batches(input_job_id, after_batch_id=after, limit=500)
            if not isinstance(page, Mapping) or not isinstance(page.get("items"), list):
                return current
            result.extend(
                str(item.get("batch_id"))
                for item in page["items"]
                if isinstance(item, Mapping) and item.get("batch_id")
            )
            if not page.get("has_more"):
                break
            next_after = page.get("next_after_batch_id")
            if not isinstance(next_after, str) or not next_after or next_after == after:
                return current
            after = next_after
    except Exception:
        return current
    return result or current


def _ledger_source(
    source: IncrementalSource,
    *,
    descriptor: SourceDescriptor | None = None,
    environment: str,
    scope_key: str,
) -> dict[str, str] | None:
    descriptor = descriptor or source.descriptor()
    identity = dict(descriptor.identity or {})
    identity_digest = str(identity.get("identity_digest") or "").strip()
    if descriptor.kind == "file":
        digest = getattr(source, "content_digest", None)
        if callable(digest):
            identity_digest = str(digest()).strip()
    if not identity_digest:
        return None
    source_id = source_identity(
        environment=environment,
        scope_key=scope_key,
        source_kind=descriptor.kind,
        identity_digest=identity_digest,
    )
    return {
        "source_id": source_id,
        "environment": environment,
        "scope_key": scope_key,
        "source_kind": descriptor.kind,
        "identity_digest": identity_digest,
    }
