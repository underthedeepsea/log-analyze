from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterator


class InputLimitError(ValueError):
    def __init__(self, details: dict[str, Any]) -> None:
        self.details = details
        super().__init__(json.dumps(details, ensure_ascii=False, separators=(",", ":")))


def iter_log_records_from_file(
    path: str | Path,
    *,
    filename: str | None = None,
    encoding: str = "utf-8",
    jsonl_bad_line_policy: str = "as_plain_text",
    max_decompressed_bytes: int | None = None,
    max_line_bytes: int | None = None,
    compressed_size_bytes: int | None = None,
    max_compression_ratio: float | None = None,
    start_offset: int = 0,
    start_line: int = 1,
) -> Iterator[dict[str, Any]]:
    path = Path(path)
    if start_offset < 0:
        raise ValueError("起始字节 Offset 不能小于 0")
    if start_line < 1:
        raise ValueError("起始行号必须大于 0")
    name = (filename or path.name).lower()
    is_gz = name.endswith(".gz")
    if is_gz and start_offset:
        raise ValueError("压缩日志暂不支持按字节 Offset 恢复")
    logical_name = name[:-3] if is_gz else name
    suffix = Path(logical_name).suffix.lower()
    if suffix == ".json":
        raise ValueError("超过 10MB 的 JSON 数组请转换为 JSONL、LOG、TXT 或 GZ")
    opener = gzip.open if is_gz else open
    compressed_size = compressed_size_bytes if compressed_size_bytes is not None else (path.stat().st_size if is_gz else None)
    bytes_read = 0
    with opener(path, "rb") as handle:
        if not is_gz:
            handle.seek(start_offset)
        for line_no, raw_line in enumerate(handle, start=start_line):
            line_bytes = len(raw_line)
            if max_line_bytes is not None and line_bytes > max_line_bytes:
                raise InputLimitError({
                    "error_code": "line_too_large",
                    "limit": max_line_bytes,
                    "actual": line_bytes,
                    "line_no": line_no,
                })
            bytes_read += line_bytes
            if max_decompressed_bytes is not None and bytes_read > max_decompressed_bytes:
                raise InputLimitError({
                    "error_code": "decompressed_size_exceeded",
                    "limit": max_decompressed_bytes,
                    "actual": bytes_read,
                    "line_no": line_no,
                })
            if is_gz and max_compression_ratio is not None and compressed_size and bytes_read / compressed_size > max_compression_ratio:
                raise InputLimitError({
                    "error_code": "compression_ratio_exceeded",
                    "limit": max_compression_ratio,
                    "actual": round(bytes_read / compressed_size, 2),
                    "line_no": line_no,
                })
            line = raw_line.decode(encoding, errors="replace").rstrip("\n")
            if not line.strip():
                continue
            if suffix in {".jsonl", ".ndjson"}:
                try:
                    record = _coerce_record(json.loads(line), line_no)
                except json.JSONDecodeError:
                    if jsonl_bad_line_policy == "strict":
                        raise
                    if jsonl_bad_line_policy == "skip":
                        continue
                    record = {"message": line, "_line_no": line_no, "_parse_error": "jsonl_decode_failed"}
            else:
                record = {"message": line, "_line_no": line_no}
            record["_byte_offset_end"] = handle.tell()
            yield record


def _coerce_record(item: Any, line_no: int) -> dict[str, Any]:
    if isinstance(item, dict):
        item = dict(item)
        item.setdefault("_line_no", line_no)
        return item
    if isinstance(item, str):
        return {"message": item, "_line_no": line_no}
    return {"message": json.dumps(item, ensure_ascii=False), "_line_no": line_no}
