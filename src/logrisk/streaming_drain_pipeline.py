from __future__ import annotations

import json
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Any

from logrisk.aggregator import TemplateEventAggregator
from logrisk.drain_miner import Drain3ShardManager, mine_template_event


def mine_partition_file(
    partition_path: str,
    output_path: str,
    config_path: str,
    state_dir: str,
    state_scope: str | None,
    parameter_extraction_mode: str,
) -> dict[str, Any]:
    manager = Drain3ShardManager(config_path, state_dir)
    count = 0
    with Path(partition_path).open("r", encoding="utf-8") as source, Path(output_path).open("w", encoding="utf-8") as target:
        for line in source:
            record = json.loads(line)
            event = mine_template_event(
                record,
                manager,
                state_scope=state_scope,
                parameter_extraction_mode=parameter_extraction_mode,
            )
            target.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    for miner in manager._miners.values():
        miner.save_state("batch committed snapshot")
    return {"output_path": output_path, "record_count": count}


def mine_spooled_partitions(
    *,
    spool_dir: str | Path,
    manifest: dict[str, Any],
    config_path: str | Path,
    state_dir: str | Path,
    window_seconds: int,
    requested_workers: int,
    max_workers: int = 4,
    reserve_cpu_cores: int = 1,
    process_start_method: str = "spawn",
    parameter_extraction_mode: str = "off",
    progress_callback=None,
    executor: Any | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(spool_dir)
    event_dir = root.parent / "template_events"
    event_dir.mkdir(parents=True, exist_ok=True)
    partitions = manifest["partitions"]
    available = max(1, (os.cpu_count() or 1) - max(0, reserve_cpu_cores))
    worker_count = min(max(1, requested_workers), max(1, max_workers), available, len(partitions)) if partitions else 0
    ordered_results: list[dict[str, Any] | None] = [None] * len(partitions)
    tasks = []
    for partition in partitions:
        key = partition["partition_key"]
        tasks.append((
            str(root / partition["path"]),
            str(event_dir / (partition["partition_id"] + ".jsonl")),
            str(config_path),
            str(state_dir),
            key[1] if len(key) == 4 else None,
            parameter_extraction_mode,
        ))
    if worker_count > 1:
        owned_executor = executor is None
        active_executor = executor or ProcessPoolExecutor(
            max_workers=worker_count, mp_context=multiprocessing.get_context(process_start_method)
        )
        mining_error: BaseException | None = None
        try:
            pending_tasks = iter(enumerate(tasks))
            futures: dict[Any, int] = {}
            completed = 0
            while True:
                while len(futures) < worker_count * 2:
                    indexed_task = next(pending_tasks, None)
                    if indexed_task is None:
                        break
                    index, task = indexed_task
                    futures[active_executor.submit(mine_partition_file, *task)] = index
                if not futures:
                    break
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    index = futures.pop(future)
                    ordered_results[index] = future.result()
                    completed += 1
                    if progress_callback:
                        progress_callback(completed, len(tasks))
        except BaseException as exc:
            mining_error = exc
            raise
        finally:
            if owned_executor:
                try:
                    active_executor.shutdown(wait=True, cancel_futures=True)
                except BaseException as cleanup_error:
                    if mining_error is None:
                        raise
                    setattr(mining_error, "_logrisk_cleanup_error", type(cleanup_error).__name__)
    else:
        for index, task in enumerate(tasks, start=1):
            ordered_results[index - 1] = mine_partition_file(*task)
            if progress_callback:
                progress_callback(index, len(tasks))

    results = [result for result in ordered_results if result is not None]
    aggregator = TemplateEventAggregator(window_seconds=window_seconds)
    for result in results:
        with Path(result["output_path"]).open("r", encoding="utf-8") as handle:
            for line in handle:
                aggregator.add(json.loads(line))
    return aggregator.finalize(), {
        "partition_count": len(partitions),
        "worker_count": worker_count,
        "parallel": worker_count > 1,
        "process_start_method": process_start_method,
        "template_event_count": sum(item["record_count"] for item in results),
    }
