from __future__ import annotations

import json
import os
import threading
import uuid
from pathlib import Path
from typing import Any

import yaml

from logrisk.semantic.extractor import SemanticExtractor
from logrisk.semantic.schema import SemanticValidationError, validate_dictionary


CORE_CASES = (
    ("HTTP status 503", "access", "nginx", "http_status", 503),
    ("open failed errno=28", "syslog", "kernel", "errno", 28),
    ("container exited with code 137", "system", "containerd", "exit_code", 137),
    ("terminated by signal 9", "system", "containerd", "signal", 9),
    ("NVRM: Xid 79, GPU has fallen off the bus", "syslog", "kernel", "xid_code", 79),
    ("Reason=Evicted", "system", "kubelet", "k8s_reason", "Evicted"),
)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


class SemanticDictionaryStore:
    def __init__(self, root: str | Path, builtin_root: str | Path):
        self.root = Path(root)
        self.builtin_root = Path(builtin_root)
        self.catalog_path = self.root / "catalog.json"
        self.events_path = self.root / "events.jsonl"
        self.validation_root = self.root / "validations"
        self._lock = threading.RLock()
        self._builtins = self._load_builtins()

    def _write_catalog(self, catalog: dict[str, Any]) -> None:
        _atomic_json(self.catalog_path, catalog)

    def _read_version_payload(self, dictionary_id: str, version: int) -> dict[str, Any]:
        path = self._version_path(dictionary_id, version)
        if not path.exists():
            raise SemanticValidationError("语义词典版本不存在")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SemanticValidationError(f"语义词典版本损坏: {exc}") from exc

    def _write_version_payload(self, dictionary_id: str, version: int, payload: dict[str, Any]) -> None:
        _atomic_json(self._version_path(dictionary_id, version), payload)

    def _write_validation(self, dictionary_id: str, version: int, report: dict[str, Any]) -> None:
        _atomic_json(self._validation_path(dictionary_id, version), report)

    def _read_validation(self, dictionary_id: str, version: int) -> dict[str, Any] | None:
        path = self._validation_path(dictionary_id, version)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_builtins(self) -> dict[str, dict[str, Any]]:
        dictionaries: dict[str, dict[str, Any]] = {}
        for path in sorted(self.builtin_root.glob("*.yaml")):
            try:
                payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, yaml.YAMLError) as exc:
                raise SemanticValidationError(f"内置语义词典不可读: {path.name}: {exc}") from exc
            item = validate_dictionary(payload)
            dictionaries[item["dictionary_id"]] = item
        if not dictionaries:
            raise SemanticValidationError("未找到内置语义词典")
        return dictionaries

    def _catalog(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            return {
                "schema_version": "semantic_catalog_v1",
                "items": {
                    dictionary_id: {"latest_version": 1, "active_version": 1, "versions": [1]}
                    for dictionary_id in self._builtins
                },
            }
        try:
            payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SemanticValidationError(f"语义词典目录损坏: {exc}") from exc
        if not isinstance(payload.get("items"), dict):
            raise SemanticValidationError("语义词典目录格式无效")
        for dictionary_id in self._builtins:
            payload["items"].setdefault(dictionary_id, {"latest_version": 1, "active_version": 1, "versions": [1]})
        return payload

    def _version_path(self, dictionary_id: str, version: int) -> Path:
        return self.root / dictionary_id / "versions" / f"{version}.json"

    def _validation_path(self, dictionary_id: str, version: int) -> Path:
        return self.validation_root / dictionary_id / f"{version}.json"

    def _metadata(self, dictionary_id: str) -> dict[str, Any]:
        if dictionary_id not in self._builtins:
            raise SemanticValidationError("语义词典不存在")
        return self._catalog()["items"][dictionary_id]

    def _custom_rules(self, dictionary_id: str, version: int) -> list[dict[str, Any]]:
        if version == 1:
            return []
        payload = self._read_version_payload(dictionary_id, version)
        rules = payload.get("custom_rules")
        if not isinstance(rules, list):
            raise SemanticValidationError("语义词典版本格式无效")
        return rules

    def _snapshot(self, dictionary_id: str, version: int) -> dict[str, Any]:
        metadata = self._metadata(dictionary_id)
        available = sorted({int(item) for item in metadata.get("versions", [1])})
        if version not in available:
            raise SemanticValidationError("语义词典版本不存在")
        builtin = self._builtins[dictionary_id]
        raw_custom = self._custom_rules(dictionary_id, version)
        combined = validate_dictionary({
            "schema_version": "semantic_dictionary_v1",
            "dictionary_id": dictionary_id,
            "name": builtin["name"],
            "version": version,
            "rules": [*builtin["rules"], *raw_custom],
        }, expected_id=dictionary_id)
        custom_ids = {str(item.get("rule_id")) for item in raw_custom}
        active = int(metadata.get("active_version", 1))
        latest = int(metadata.get("latest_version", 1))
        return {
            **combined,
            "status": "published" if version == active else ("candidate" if version == latest else "history"),
            "builtin_read_only": True,
            "builtin_rules": builtin["rules"],
            "custom_rules": [rule for rule in combined["rules"] if rule["rule_id"] in custom_ids],
            "active_version": active,
            "latest_version": latest,
            "available_versions": available,
        }

    def list_dictionaries(self) -> list[dict[str, Any]]:
        catalog = self._catalog()
        return [
            self._snapshot(dictionary_id, int(catalog["items"][dictionary_id]["latest_version"]))
            for dictionary_id in sorted(self._builtins)
        ]

    def get_version(self, dictionary_id: str, version: int) -> dict[str, Any]:
        return self._snapshot(dictionary_id, int(version))

    def create_candidate(self, dictionary_id: str, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise SemanticValidationError("请求体必须是 object")
        with self._lock:
            catalog = self._catalog()
            if dictionary_id not in self._builtins:
                raise SemanticValidationError("语义词典不存在")
            metadata = catalog["items"][dictionary_id]
            version = int(metadata["latest_version"]) + 1
            source_version = int(metadata.get("active_version", 1))
            self._write_version_payload(dictionary_id, version, {
                "schema_version": "semantic_custom_version_v1",
                "dictionary_id": dictionary_id,
                "version": version,
                "custom_rules": self._custom_rules(dictionary_id, source_version),
                "created_by": str(payload.get("operator") or "local-operator"),
            })
            metadata["latest_version"] = version
            metadata.setdefault("versions", [1]).append(version)
            self._write_catalog(catalog)
            return self._snapshot(dictionary_id, version)

    def save_version(self, dictionary_id: str, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise SemanticValidationError("请求体必须是 object")
        with self._lock:
            catalog = self._catalog()
            if dictionary_id not in self._builtins:
                raise SemanticValidationError("语义词典不存在")
            metadata = catalog["items"][dictionary_id]
            latest = int(metadata["latest_version"])
            expected = int(payload.get("expected_version") or 0)
            if expected == 1:
                raise SemanticValidationError("内置语义词典只读，请先创建候选版本")
            if expected != latest:
                raise SemanticValidationError(f"版本冲突：当前版本为 {latest}")
            custom_rules = payload.get("custom_rules")
            if not isinstance(custom_rules, list):
                raise SemanticValidationError("custom_rules 必须是数组")
            version = latest + 1
            validate_dictionary({
                "dictionary_id": dictionary_id,
                "name": self._builtins[dictionary_id]["name"],
                "version": version,
                "rules": [*self._builtins[dictionary_id]["rules"], *custom_rules],
            }, expected_id=dictionary_id)
            self._write_version_payload(dictionary_id, version, {
                "schema_version": "semantic_custom_version_v1",
                "dictionary_id": dictionary_id,
                "version": version,
                "custom_rules": custom_rules,
                "created_by": str(payload.get("operator") or "local-operator"),
            })
            metadata["latest_version"] = version
            metadata.setdefault("versions", [1]).append(version)
            self._write_catalog(catalog)
            return self._snapshot(dictionary_id, version)

    def _bundle(self, override: tuple[str, int] | None = None) -> dict[str, Any]:
        catalog = self._catalog()
        dictionaries = []
        versions: dict[str, Any] = {}
        for dictionary_id in sorted(self._builtins):
            version = override[1] if override and override[0] == dictionary_id else int(catalog["items"][dictionary_id].get("active_version", 1))
            snapshot = self._snapshot(dictionary_id, version)
            dictionaries.append({
                "schema_version": "semantic_dictionary_v1",
                "dictionary_id": dictionary_id,
                "name": snapshot["name"],
                "version": version,
                "rules": snapshot["rules"],
            })
            versions[dictionary_id] = {"version": version, "content_hash": snapshot["content_hash"]}
        return {
            "schema_version": "semantic_snapshot_v1",
            "extractor_version": "1.0.0",
            "dictionaries": dictionaries,
            "versions": versions,
        }

    def active_snapshot(self) -> dict[str, Any]:
        return self._bundle()

    def validate_version(self, dictionary_id: str, version: int) -> dict[str, Any]:
        snapshot = self._snapshot(dictionary_id, int(version))
        bundle = self._bundle((dictionary_id, int(version)))
        extractor = SemanticExtractor.from_snapshot(bundle)
        checks = []
        for message, source_type, component, field, expected in CORE_CASES:
            actual = extractor.extract(message, source_type=source_type, component=component)["semantic_fields"].get(field)
            checks.append({"field": field, "expected": expected, "actual": actual, "passed": actual == expected})
        report = {
            "schema_version": "semantic_validation_v1",
            "dictionary_id": dictionary_id,
            "version": int(version),
            "content_hash": snapshot["content_hash"],
            "valid": all(item["passed"] for item in checks),
            "checks": checks,
            "errors": [f"{item['field']} 核心用例未通过" for item in checks if not item["passed"]],
        }
        self._write_validation(dictionary_id, int(version), report)
        return report

    def _append_event(self, event: dict[str, Any]) -> None:
        items: list[dict[str, Any]] = []
        if self.events_path.exists():
            try:
                items = [json.loads(line) for line in self.events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            except (OSError, json.JSONDecodeError) as exc:
                raise SemanticValidationError(f"语义词典审计记录损坏: {exc}") from exc
        items.append(event)
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.events_path.with_name(f".{self.events_path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in items), encoding="utf-8")
        os.replace(temporary, self.events_path)

    def _activate(self, dictionary_id: str, version: int, payload: Any, action: str) -> dict[str, Any]:
        if not isinstance(payload, dict) or payload.get("confirmed") is not True:
            raise SemanticValidationError("词典变更需要人工确认")
        snapshot = self._snapshot(dictionary_id, int(version))
        if action == "publish":
            validation = self._read_validation(dictionary_id, int(version))
            if validation is None:
                raise SemanticValidationError("候选版本必须先通过校验")
            if validation.get("valid") is not True or validation.get("content_hash") != snapshot["content_hash"]:
                raise SemanticValidationError("候选版本校验未通过或已失效")
        catalog = self._catalog()
        catalog["items"][dictionary_id]["active_version"] = int(version)
        self._write_catalog(catalog)
        self._append_event({
            "schema_version": "semantic_dictionary_event_v1",
            "event_id": f"semantic_event_{uuid.uuid4().hex}",
            "dictionary_id": dictionary_id,
            "version": int(version),
            "content_hash": snapshot["content_hash"],
            "action": action,
            "operator": str(payload.get("operator") or "local-operator"),
        })
        return dict(self._snapshot(dictionary_id, int(version)), status="published")

    def publish(self, dictionary_id: str, version: int, payload: Any) -> dict[str, Any]:
        with self._lock:
            return self._activate(dictionary_id, int(version), payload, "publish")

    def rollback(self, dictionary_id: str, version: int, payload: Any) -> dict[str, Any]:
        with self._lock:
            result = self._activate(dictionary_id, int(version), payload, "rollback")
            return dict(result, active_version=int(version))

    def test_snapshot(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise SemanticValidationError("请求体必须是 object")
        message = payload.get("message_core")
        if not isinstance(message, str) or not message:
            raise SemanticValidationError("message_core 必须是非空字符串")
        snapshot = self.active_snapshot()
        result = SemanticExtractor.from_snapshot(snapshot).extract(
            message,
            source_type=str(payload.get("source_type") or "unknown"),
            component=str(payload.get("component") or "unknown"),
        )
        result["dictionary_versions"] = snapshot["versions"]
        return result
