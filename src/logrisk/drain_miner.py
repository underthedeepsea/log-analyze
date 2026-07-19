from __future__ import annotations

import hashlib
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

from .template_identity import HASH_VERSION, template_fingerprint, template_instance_hash


class Drain3ShardManager:
    def __init__(self, config_path: str | Path, state_dir: str | Path):
        self.config_path = Path(config_path)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._miners: Dict[Tuple[str, ...], TemplateMiner] = {}

    def _load_config(self) -> TemplateMinerConfig:
        config = TemplateMinerConfig()
        config.load(str(self.config_path))
        # Some drain3 versions may load this value as string through ConfigParser.
        try:
            config.parameter_extraction_cache_capacity = int(config.parameter_extraction_cache_capacity)
        except Exception:
            pass
        return config

    def get_miner(
        self,
        cluster: str,
        source_type: str,
        component: str,
        state_scope: str | None = None,
    ) -> TemplateMiner:
        key = (cluster, source_type, component) if state_scope is None else (
            cluster,
            state_scope,
            source_type,
            component,
        )
        if key in self._miners:
            return self._miners[key]
        safe = "__".join(x.replace("/", "_").replace(" ", "_") for x in key)
        state_file = self.state_dir / f"{safe}.bin"
        miner = TemplateMiner(
            persistence_handler=FilePersistence(str(state_file)),
            config=self._load_config(),
        )
        self._miners[key] = miner
        return miner


def stable_template_hash(cluster: str, source_type: str, component: str, template: str) -> str:
    raw = f"{cluster}|{source_type}|{component}|{template}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def mine_template_event(
    record: Dict[str, Any],
    shard_manager: Drain3ShardManager,
    state_scope: str | None = None,
    *,
    parameter_extraction_mode: str = "off",
) -> Dict[str, Any]:
    cluster = str(record.get("cluster") or "default")
    source_type = str(record.get("source_type") or "unknown")
    component = str(record.get("component") or "unknown")
    message_core = str(record["message_core"])

    miner = shard_manager.get_miner(cluster, source_type, component, state_scope=state_scope)
    result = miner.add_log_message(message_core)
    template = result["template_mined"]
    template_hash = stable_template_hash(cluster, source_type, component, template)
    fingerprint = template_fingerprint(source_type, component, template)
    instance_hash = template_instance_hash(
        cluster,
        str(record.get("node") or state_scope or "unknown"),
        source_type,
        component,
        template,
    )

    if parameter_extraction_mode not in {"off", "all"}:
        raise ValueError("parameter_extraction_mode 必须是 off 或 all")
    extracted = None
    if parameter_extraction_mode == "all":
        extracted = miner.extract_parameters(template, message_core, exact_matching=True)
        if extracted is None:
            extracted = miner.extract_parameters(template, message_core, exact_matching=False)

    parameters = [
        {"type": p.mask_name, "value": p.value}
        for p in (extracted or [])
    ]

    return {
        "schema_version": "template_event_v2" if "semantic_fields" in record else "template_event_v1",
        "event_id": record.get("raw_log_id"),
        "timestamp": record.get("timestamp"),
        "cluster": cluster,
        "node": record.get("node"),
        "namespace": record.get("namespace"),
        "pod": record.get("pod"),
        "container": record.get("container"),
        "source_type": source_type,
        "component": component,
        "severity": record.get("severity"),
        "template_hash": template_hash,
        "template_fingerprint": fingerprint,
        "template_instance_hash": instance_hash,
        "hash_version": HASH_VERSION,
        "template": template,
        "parameters": parameters,
        "semantic_fields": record.get("semantic_fields") or {},
        "semantic_tags": record.get("semantic_tags") or [],
        "typed_parameters": record.get("typed_parameters") or [],
        "semantic_extractor_version": record.get("semantic_extractor_version"),
        "semantic_dictionary_versions": record.get("semantic_dictionary_versions") or {},
        "risk_semantic": record.get("risk_semantic"),
        "message_core": message_core,
        "raw_sample": record.get("raw_log"),
        "change_type": result.get("change_type"),
    }


def mining_partition_key(record: Dict[str, Any], *, partition_by_node: bool) -> Tuple[str, ...]:
    key = (str(record.get("cluster") or "default"),)
    if partition_by_node:
        key += (str(record.get("node") or "unknown"),)
    return key + (
        str(record.get("source_type") or "unknown"),
        str(record.get("component") or "unknown"),
    )


def _mine_indexed_partition(
    indexed_records: list[tuple[int, Dict[str, Any]]],
    config_path: str,
    state_dir: str,
    state_scope: str | None,
    parameter_extraction_mode: str,
) -> list[tuple[int, Dict[str, Any]]]:
    manager = Drain3ShardManager(config_path=config_path, state_dir=state_dir)
    return [
        (index, mine_template_event(
            record,
            manager,
            state_scope=state_scope,
            parameter_extraction_mode=parameter_extraction_mode,
        ))
        for index, record in indexed_records
    ]


def mine_template_events(
    records: list[Dict[str, Any]],
    config_path: str | Path,
    state_dir: str | Path,
    *,
    worker_count: int = 1,
    partition_by_node: bool = False,
    progress_callback: Callable[[Dict[str, int]], None] | None = None,
    return_metadata: bool = False,
    parameter_extraction_mode: str = "off",
    process_start_method: str = "spawn",
    max_workers: int = 4,
    reserve_cpu_cores: int = 1,
) -> list[Dict[str, Any]] | tuple[list[Dict[str, Any]], Dict[str, int | bool]]:
    partitions: Dict[Tuple[str, ...], list[tuple[int, Dict[str, Any]]]] = {}
    for index, record in enumerate(records):
        key = mining_partition_key(record, partition_by_node=partition_by_node)
        partitions.setdefault(key, []).append((index, record))

    partition_items = list(partitions.items())
    partition_count = len(partition_items)
    available_cpus = max(1, (os.cpu_count() or 1) - max(0, int(reserve_cpu_cores)))
    effective_workers = min(
        max(1, int(worker_count)),
        max(1, int(max_workers)),
        available_cpus,
        partition_count,
    ) if partition_count else 0
    metadata: Dict[str, int | bool] = {
        "partition_count": partition_count,
        "worker_count": effective_workers,
        "parallel": effective_workers > 1,
        "process_start_method": process_start_method,
    }
    if progress_callback:
        progress_callback({
            "partition_count": partition_count,
            "partitions_completed": 0,
            "records_processed": 0,
            "records_total": len(records),
        })

    ordered_events: list[Dict[str, Any] | None] = [None] * len(records)
    completed_partitions = 0
    processed_records = 0

    def collect(indexed_events: list[tuple[int, Dict[str, Any]]]) -> None:
        nonlocal completed_partitions, processed_records
        for index, event in indexed_events:
            ordered_events[index] = event
        completed_partitions += 1
        processed_records += len(indexed_events)
        if progress_callback:
            progress_callback({
                "partition_count": partition_count,
                "partitions_completed": completed_partitions,
                "records_processed": processed_records,
                "records_total": len(records),
            })

    config = str(config_path)
    state = str(state_dir)
    if effective_workers > 1:
        context = multiprocessing.get_context(process_start_method)
        with ProcessPoolExecutor(max_workers=effective_workers, mp_context=context) as executor:
            futures = {
                executor.submit(
                    _mine_indexed_partition,
                    indexed_records,
                    config,
                    state,
                    key[1] if partition_by_node else None,
                    parameter_extraction_mode,
                ): key
                for key, indexed_records in partition_items
            }
            for future in as_completed(futures):
                collect(future.result())
    else:
        for key, indexed_records in partition_items:
            collect(_mine_indexed_partition(
                indexed_records,
                config,
                state,
                key[1] if partition_by_node else None,
                parameter_extraction_mode,
            ))

    events = [event for event in ordered_events if event is not None]
    if return_metadata:
        return events, metadata
    return events
