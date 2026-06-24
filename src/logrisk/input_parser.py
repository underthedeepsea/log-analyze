from __future__ import annotations

import json
from typing import Any, Dict, List


STRUCTURED_SUFFIXES = {".json", ".jsonl", ".ndjson"}


def _normalize_item(item: Any, location: str) -> Dict[str, Any]:
    if isinstance(item, str):
        return {"message": item}
    if isinstance(item, dict):
        return item
    raise ValueError(f"{location} 不是日志字符串或 JSON object")


def normalize_json_container(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, dict):
        for key in ("logs", "data", "records", "entries"):
            if isinstance(value.get(key), list):
                value = value[key]
                break
    if not isinstance(value, list):
        raise ValueError("输入 JSON 必须是数组或包含 logs/data/records/entries 数组的 object")
    return [_normalize_item(item, f"记录 {index}") for index, item in enumerate(value)]


def parse_log_content(filename: str, content: str) -> List[Dict[str, Any]]:
    if not isinstance(content, str) or not content.strip():
        raise ValueError("输入日志不能为空")
    stripped = content.strip()
    suffix = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError as document_error:
        decoded = None
    else:
        return normalize_json_container(decoded)

    rows: List[Dict[str, Any]] = []
    jsonl_error: json.JSONDecodeError | None = None
    for line_no, line in enumerate(stripped.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(_normalize_item(json.loads(line), f"第 {line_no} 行"))
        except json.JSONDecodeError as exc:
            jsonl_error = exc
            rows = []
            break
    if rows:
        return rows

    if suffix in STRUCTURED_SUFFIXES:
        detail = jsonl_error or document_error
        raise ValueError(f"输入内容不是有效 JSON/JSONL: {detail}")
    return [{"message": line.strip()} for line in stripped.splitlines() if line.strip()]
