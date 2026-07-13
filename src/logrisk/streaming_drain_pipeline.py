from __future__ import annotations

import json
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
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
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(spool_dir)
    event_dir = root.parent / "template_events"
    event_dir.mkdir(parents=True, exist_ok=True)
    partitions = manifest["partitions"]
    available = max(1, (os.cpu_count() or 1) - max(0, reserve_cpu_cores))
    worker_count = min(max(1, requested_workers), max(1, max_workers), available, len(partitions)) if partitions else 0
    results = []
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
        with ProcessPoolExecutor(max_workers=worker_count, mp_context=multiprocessing.get_context(process_start_method)) as executor:
            futures = [executor.submit(mine_partition_file, *task) for task in tasks]
            for index, future in enumerate(as_completed(futures), start=1):
                results.append(future.result())
                if progress_callback:
                    progress_callback(index, len(tasks))
    else:
        for index, task in enumerate(tasks, start=1):
            results.append(mine_partition_file(*task))
            if progress_callback:
                progress_callback(index, len(tasks))

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
