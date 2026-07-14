from __future__ import annotations

import json
import os
import threading
import uuid
from functools import wraps
from pathlib import Path
from typing import Any

from logrisk.drain_eval.schema import DrainQualityError, now_iso, require_object


_STORE_LOCK = threading.RLock()


def synchronized(method):
    @wraps(method)
    def wrapped(*args, **kwargs):
        with _STORE_LOCK:
            return method(*args, **kwargs)
    return wrapped


class AnnotationStore:
    ACTIONS = {"accept", "edit", "split", "merge", "ignore"}

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.annotations_path = self.root / "annotations.jsonl"
        self.reviews_path = self.root / "reviews.jsonl"

    @staticmethod
    def _read(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        try:
            return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, json.JSONDecodeError) as exc:
            raise DrainQualityError(f"标注事件不可读: {exc}") from exc

    @staticmethod
    def _append(path: Path, event: dict[str, Any]) -> None:
        rows = AnnotationStore._read(path)
        rows.append(event)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        os.replace(temporary, path)

    @synchronized
    def append(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        action = source.get("action")
        cluster_id = source.get("cluster_id")
        if action not in self.ACTIONS:
            raise DrainQualityError("标注 action 无效")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise DrainQualityError("cluster_id 不能为空")
        if action in {"split", "merge"} and not source.get("target_cluster_ids"):
            raise DrainQualityError("拆分或合并必须提供 target_cluster_ids")
        event = {
            "schema_version": "drain_annotation_event_v1",
            "annotation_id": f"annotation_{uuid.uuid4().hex}",
            "cluster_id": cluster_id,
            "action": action,
            "template": source.get("template"),
            "target_cluster_ids": list(source.get("target_cluster_ids") or []),
            "reviewer": str(source.get("reviewer") or "local-operator"),
            "note": str(source.get("note") or ""),
            "created_at": now_iso(),
        }
        self._append(self.annotations_path, event)
        return event

    @synchronized
    def review(self, annotation_id: str, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        decision = source.get("decision")
        if decision not in {"approved", "rejected", "changes_requested"}:
            raise DrainQualityError("复核 decision 无效")
        if not any(item.get("annotation_id") == annotation_id for item in self._read(self.annotations_path)):
            raise DrainQualityError("标注事件不存在")
        event = {
            "schema_version": "drain_annotation_review_v1",
            "review_id": f"review_{uuid.uuid4().hex}",
            "annotation_id": annotation_id,
            "decision": decision,
            "reviewer": str(source.get("reviewer") or "local-reviewer"),
            "note": str(source.get("note") or ""),
            "created_at": now_iso(),
        }
        self._append(self.reviews_path, event)
        return event

    @synchronized
    def replay(self) -> dict[str, dict[str, Any]]:
        state: dict[str, dict[str, Any]] = {}
        by_annotation: dict[str, str] = {}
        for event in self._read(self.annotations_path):
            cluster_id = event["cluster_id"]
            current = state.setdefault(cluster_id, {"cluster_id": cluster_id})
            current.update({
                "status": {"accept": "accepted", "edit": "edited", "split": "split", "merge": "merged", "ignore": "ignored"}[event["action"]],
                "annotation_id": event["annotation_id"],
                "updated_at": event["created_at"],
            })
            if event.get("template"):
                current["template"] = event["template"]
            if event.get("target_cluster_ids"):
                current["target_cluster_ids"] = event["target_cluster_ids"]
            by_annotation[event["annotation_id"]] = cluster_id
        for review in self._read(self.reviews_path):
            cluster_id = by_annotation.get(review["annotation_id"])
            if cluster_id:
                state[cluster_id]["review_status"] = review["decision"]
                state[cluster_id]["reviewed_at"] = review["created_at"]
        return state

    @synchronized
    def events(self) -> list[dict[str, Any]]:
        return self._read(self.annotations_path)
