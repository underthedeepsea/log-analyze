from __future__ import annotations

import json
import os
import threading
import uuid
from functools import wraps
from pathlib import Path
from typing import Any

from logrisk.drain_eval.schema import DrainQualityError, now_iso, require_object, validate_gold_record


_STORE_LOCK = threading.RLock()


def synchronized(method):
    @wraps(method)
    def wrapped(*args, **kwargs):
        with _STORE_LOCK:
            return method(*args, **kwargs)
    return wrapped


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


class DatasetStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.path = self.root / "datasets.json"

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": "drain_dataset_index_v1", "items": []}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DrainQualityError(f"Dataset 索引不可读: {exc}") from exc
        if isinstance(payload, list):
            payload = {"schema_version": "drain_dataset_index_v1", "items": payload}
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise DrainQualityError("Dataset 索引格式无效")
        payload.setdefault("schema_version", "drain_dataset_index_v1")
        return payload

    @synchronized
    def create(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        name = source.get("name")
        records = source.get("records")
        if not isinstance(name, str) or not name.strip():
            raise DrainQualityError("Dataset name 不能为空")
        if not isinstance(records, list) or not records:
            raise DrainQualityError("Dataset records 不能为空")
        validated = [validate_gold_record(record) for record in records]
        record_ids = [record["record_id"] for record in validated]
        if len(record_ids) != len(set(record_ids)):
            raise DrainQualityError("Dataset record_id 不可重复")
        now = now_iso()
        item = {
            "schema_version": "drain_dataset_v1",
            "dataset_id": str(source.get("dataset_id") or f"dataset_{uuid.uuid4().hex[:12]}"),
            "name": name.strip(),
            "description": str(source.get("description") or ""),
            "version": str(source.get("version") or "1.0.0"),
            "split": str(source.get("split") or "validation"),
            "record_count": len(validated),
            "records": validated,
            "created_at": now,
            "updated_at": now,
        }
        index = self._read()
        if any(existing.get("dataset_id") == item["dataset_id"] for existing in index["items"]):
            raise DrainQualityError("dataset_id 已存在")
        index["items"].append(item)
        atomic_json(self.path, index)
        return item

    @synchronized
    def list(self) -> list[dict[str, Any]]:
        return [dict(item, records=None) for item in self._read()["items"]]

    @synchronized
    def get(self, dataset_id: str) -> dict[str, Any]:
        for item in self._read()["items"]:
            if item.get("dataset_id") == dataset_id:
                return item
        raise DrainQualityError("Dataset 不存在")
