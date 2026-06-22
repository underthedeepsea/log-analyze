from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


def read_json_or_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() == ".jsonl":
        rows: List[Dict[str, Any]] = []
        for line_no, line in enumerate(text.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if isinstance(item, str):
                item = {"message": item}
            if not isinstance(item, dict):
                raise ValueError(f"Line {line_no} is not a JSON object: {item!r}")
            rows.append(item)
        return rows

    data = json.loads(text)
    if isinstance(data, dict):
        for key in ("logs", "data", "records", "entries"):
            if isinstance(data.get(key), list):
                data = data[key]
                break

    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list or an object containing logs/data/records/entries.")

    out: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, str):
            out.append({"message": item})
        elif isinstance(item, dict):
            out.append(item)
        else:
            raise ValueError(f"Unsupported log item: {item!r}")
    return out


def write_json(path: str | Path, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: str | Path, rows: Iterable[Dict[str, Any]]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
