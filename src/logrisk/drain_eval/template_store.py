from __future__ import annotations

import json
import os
import threading
import uuid
from functools import wraps
from pathlib import Path
from typing import Any

from logrisk.drain_eval.dataset import atomic_json
from logrisk.drain_eval.schema import DrainQualityError, now_iso, require_object


_STORE_LOCK = threading.RLock()


def synchronized(method):
    @wraps(method)
    def wrapped(*args, **kwargs):
        with _STORE_LOCK:
            return method(*args, **kwargs)
    return wrapped


class TemplateStore:
    ACTIONS = {"edit", "ignore", "restore", "merge", "delete"}

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.catalog_path = self.root / "template_overrides.json"
        self.events_path = self.root / "template_events.jsonl"

    def _catalog(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            return {"schema_version": "drain_template_catalog_v1", "items": {}}
        try:
            payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DrainQualityError(f"模板目录不可读: {exc}") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), dict):
            raise DrainQualityError("模板目录格式无效")
        payload.setdefault("schema_version", "drain_template_catalog_v1")
        return payload

    def _events(self) -> list[dict[str, Any]]:
        if not self.events_path.exists():
            return []
        try:
            return [json.loads(line) for line in self.events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, json.JSONDecodeError) as exc:
            raise DrainQualityError(f"模板审计记录不可读: {exc}") from exc

    def _append_event(self, event: dict[str, Any]) -> None:
        events = self._events()
        events.append(event)
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.events_path.with_name(f".{self.events_path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in events), encoding="utf-8")
        os.replace(temporary, self.events_path)

    def _record_event(self, template_hash: str, action: str, before: dict[str, Any] | None, after: dict[str, Any], operator: str) -> None:
        self._append_event({
            "schema_version": "drain_template_event_v1",
            "event_id": f"template_event_{uuid.uuid4().hex}",
            "template_hash": template_hash,
            "action": action,
            "operator": operator,
            "before": before,
            "after": after,
            "created_at": now_iso(),
        })

    @synchronized
    def import_templates(self, templates: Any) -> list[dict[str, Any]]:
        if not isinstance(templates, list):
            raise DrainQualityError("templates 必须是数组")
        catalog = self._catalog()
        imported: list[dict[str, Any]] = []
        for raw in templates:
            source = require_object(raw)
            template_hash = source.get("template_hash")
            template = source.get("template")
            if not isinstance(template_hash, str) or not template_hash or not isinstance(template, str) or not template:
                raise DrainQualityError("模板必须包含 template_hash 和 template")
            current = catalog["items"].get(template_hash)
            if current:
                current["count"] = int(source.get("count") or current.get("count") or 0)
                current["risk_levels"] = list(source.get("risk_levels") or current.get("risk_levels") or [])
                current["updated_at"] = now_iso()
                imported.append(current)
                continue
            now = now_iso()
            item = {
                "schema_version": "drain_template_override_v1",
                "template_hash": template_hash,
                "original_template": template,
                "effective_template": template,
                "component": str(source.get("component") or "unknown"),
                "count": int(source.get("count") or 0),
                "risk_levels": list(source.get("risk_levels") or []),
                "status": "active",
                "merged_into": None,
                "version": 1,
                "created_at": now,
                "updated_at": now,
            }
            catalog["items"][template_hash] = item
            imported.append(item)
            self._record_event(template_hash, "import", None, dict(item), "system")
        atomic_json(self.catalog_path, catalog)
        return imported

    @synchronized
    def list_templates(self, *, status: str | None = None, component: str | None = None, query: str | None = None) -> list[dict[str, Any]]:
        items = list(self._catalog()["items"].values())
        if status:
            items = [item for item in items if item.get("status") == status]
        if component:
            items = [item for item in items if item.get("component") == component]
        if query:
            needle = query.lower()
            items = [item for item in items if needle in str(item.get("template_hash", "")).lower() or needle in str(item.get("effective_template", "")).lower()]
        return sorted(items, key=lambda item: (-int(item.get("count") or 0), str(item.get("template_hash"))))

    @synchronized
    def get_template(self, template_hash: str) -> dict[str, Any]:
        item = self._catalog()["items"].get(template_hash)
        if not item:
            raise DrainQualityError("模板不存在")
        return item

    @synchronized
    def change_template(self, template_hash: str, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        if source.get("confirmed") is not True:
            raise DrainQualityError("模板变更需要人工确认")
        action = source.get("action")
        if action not in self.ACTIONS:
            raise DrainQualityError("模板 action 无效")
        catalog = self._catalog()
        current = catalog["items"].get(template_hash)
        if not current:
            raise DrainQualityError("模板不存在")
        if int(source.get("expected_version") or 0) != int(current["version"]):
            raise DrainQualityError("模板版本冲突，请刷新后重试")
        before = dict(current)
        if action == "edit":
            template = source.get("template")
            if not isinstance(template, str) or not template.strip():
                raise DrainQualityError("有效模板不能为空")
            current["effective_template"] = template.strip()
            current["status"] = "active"
        elif action in {"ignore", "delete"}:
            current["status"] = "ignored" if action == "ignore" else "deleted"
        elif action == "restore":
            current["effective_template"] = current["original_template"]
            current["status"] = "active"
            current["merged_into"] = None
        elif action == "merge":
            target = source.get("target_template_hash")
            if not isinstance(target, str) or target == template_hash or target not in catalog["items"]:
                raise DrainQualityError("合并目标模板无效")
            current["status"] = "merged"
            current["merged_into"] = target
            current["effective_template"] = catalog["items"][target]["effective_template"]
        current["version"] = int(current["version"]) + 1
        current["updated_at"] = now_iso()
        atomic_json(self.catalog_path, catalog)
        self._record_event(template_hash, str(action), before, dict(current), str(source.get("operator") or "local-operator"))
        return current

    @synchronized
    def history(self, template_hash: str) -> list[dict[str, Any]]:
        return [event for event in self._events() if event.get("template_hash") == template_hash]

    @synchronized
    def rollback(self, template_hash: str, target_version: int, *, expected_version: int, confirmed: bool, operator: str = "local-operator") -> dict[str, Any]:
        if confirmed is not True:
            raise DrainQualityError("模板回滚需要人工确认")
        catalog = self._catalog()
        current = catalog["items"].get(template_hash)
        if not current:
            raise DrainQualityError("模板不存在")
        if int(current["version"]) != int(expected_version):
            raise DrainQualityError("模板版本冲突，请刷新后重试")
        target = next((event.get("after") for event in self.history(template_hash) if int((event.get("after") or {}).get("version") or 0) == int(target_version)), None)
        if not target:
            raise DrainQualityError("目标模板版本不存在")
        before = dict(current)
        restored = dict(target)
        restored["version"] = int(current["version"]) + 1
        restored["updated_at"] = now_iso()
        catalog["items"][template_hash] = restored
        atomic_json(self.catalog_path, catalog)
        self._record_event(template_hash, "rollback", before, dict(restored), operator)
        return restored

    @synchronized
    def apply_override(self, event: dict[str, Any]) -> dict[str, Any]:
        template_hash = str(event.get("template_hash") or "")
        override = self._catalog()["items"].get(template_hash)
        if not override:
            return dict(event)
        result = dict(event)
        result.update({
            "original_template": event.get("template"),
            "template": override["effective_template"],
            "template_governance_status": override["status"],
            "template_governance_version": override["version"],
            "merged_into": override.get("merged_into"),
        })
        return result
