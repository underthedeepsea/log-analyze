from __future__ import annotations

import hashlib
import json
import os
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable

from logrisk.drain_miner import mining_partition_key
from logrisk.normalizer import normalize_record


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def spool_normalized_records(
    records: Iterable[dict[str, Any]],
    *,
    spool_dir: str | Path,
    partition_by_node: bool = True,
    progress_callback=None,
    semantic_extractor=None,
    max_open_files: int = 64,
) -> dict[str, Any]:
    root = Path(spool_dir)
    root.mkdir(parents=True, exist_ok=True)
    try:
        import resource
        soft_limit = int(resource.getrlimit(resource.RLIMIT_NOFILE)[0])
    except (ImportError, AttributeError, OSError, ValueError):
        soft_limit = 256
    writer_budget = max(1, min(int(max_open_files), max(1, soft_limit - 32)))
    handles: OrderedDict[tuple[str, ...], Any] = OrderedDict()
    ever_opened: set[tuple[str, ...]] = set()
    entries: dict[tuple[str, ...], dict[str, Any]] = {}
    total = 0
    try:
        for record in records:
            normalized = normalize_record(record)
            if semantic_extractor is not None:
                normalized = semantic_extractor.enrich(normalized)
            key = mining_partition_key(normalized, partition_by_node=partition_by_node)
            if key not in handles:
                digest = hashlib.sha256("\x1f".join(key).encode("utf-8")).hexdigest()[:16]
                partition_id = f"partition-{digest}"
                filename = partition_id + ".jsonl"
                if len(handles) >= writer_budget:
                    _, evicted = handles.popitem(last=False)
                    evicted.close()
                handles[key] = (root / filename).open("a" if key in ever_opened else "w", encoding="utf-8")
                ever_opened.add(key)
                entries.setdefault(key, {
                    "partition_id": partition_id, "partition_key": list(key),
                    "path": filename, "record_count": 0,
                })
            else:
                handles.move_to_end(key)
            handles[key].write(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n")
            entries[key]["record_count"] += 1
            total += 1
            if progress_callback and total % 5000 == 0:
                progress_callback(total)
    finally:
        for handle in handles.values():
            handle.close()
    manifest = {
        "schema_version": "1.0",
        "status": "SPOOLING",
        "total_records": total,
        "max_open_files": writer_budget,
        "partitions": [entries[key] for key in sorted(entries)],
    }
    _atomic_json(root / "manifest.json", manifest)
    return manifest


def update_manifest_status(spool_dir: str | Path, manifest: dict[str, Any], status: str) -> None:
    manifest["status"] = status
    _atomic_json(Path(spool_dir) / "manifest.json", manifest)
