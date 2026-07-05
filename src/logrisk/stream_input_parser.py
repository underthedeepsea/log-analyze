from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterator


def iter_log_records_from_file(
    path: str | Path,
    *,
    filename: str | None = None,
    encoding: str = "utf-8",
    jsonl_bad_line_policy: str = "as_plain_text",
    max_decompressed_bytes: int | None = None,
) -> Iterator[dict[str, Any]]:
    path = Path(path)
    name = (filename or path.name).lower()
    is_gz = name.endswith(".gz")
    logical_name = name[:-3] if is_gz else name
    suffix = Path(logical_name).suffix.lower()
    if suffix == ".json":
        raise ValueError("超过 10MB 的 JSON 数组请转换为 JSONL、LOG、TXT 或 GZ")
    opener = gzip.open if is_gz else open
    bytes_read = 0
    with opener(path, "rt", encoding=encoding, errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            bytes_read += len(line.encode(encoding, errors="ignore"))
            if max_decompressed_bytes is not None and bytes_read > max_decompressed_bytes:
                raise ValueError("Decompressed content exceeds configured limit")
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if suffix in {".jsonl", ".ndjson"}:
                try:
                    yield _coerce_record(json.loads(line), line_no)
                except json.JSONDecodeError:
                    if jsonl_bad_line_policy == "strict":
                        raise
                    if jsonl_bad_line_policy == "skip":
                        continue
                    yield {"message": line, "_line_no": line_no, "_parse_error": "jsonl_decode_failed"}
            else:
                yield {"message": line, "_line_no": line_no}


def _coerce_record(item: Any, line_no: int) -> dict[str, Any]:
    if isinstance(item, dict):
        item = dict(item)
        item.setdefault("_line_no", line_no)
        return item
    if isinstance(item, str):
        return {"message": item, "_line_no": line_no}
    return {"message": json.dumps(item, ensure_ascii=False), "_line_no": line_no}
