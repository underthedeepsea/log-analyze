from __future__ import annotations

import configparser
import hashlib
import json
import os
import re
import threading
import uuid
from pathlib import Path
from typing import Any

from logrisk.drain_eval.dataset import atomic_json
from logrisk.drain_eval.schema import DrainQualityError, now_iso, require_object


PARAMETER_RANGES: dict[str, tuple[float, float]] = {
    "sim_th": (0.0, 1.0),
    "depth": (3, 15),
    "max_children": (1, 10000),
    "max_clusters": (1, 1000000),
}


class DrainConfigStore:
    def __init__(self, root: str | Path, baseline_path: str | Path):
        self.root = Path(root)
        self.baseline_path = Path(baseline_path).resolve()
        self.configs_root = self.root / "configs"
        self.catalog_path = self.root / "config_catalog.json"
        self.active_path = self.root / "active_config.json"
        self.events_path = self.root / "config_events.jsonl"
        self._lock = threading.RLock()
        if not self.baseline_path.is_file():
            raise DrainQualityError("Drain3 基线配置不存在")

    def _catalog(self) -> dict[str, Any]:
        if not self.catalog_path.exists():
            return {"schema_version": "drain_config_catalog_v1", "items": {}}
        try:
            payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DrainQualityError(f"Drain3 配置目录损坏: {exc}") from exc
        if not isinstance(payload.get("items"), dict):
            raise DrainQualityError("Drain3 配置目录格式无效")
        return payload

    def _write_catalog(self, catalog: dict[str, Any]) -> None:
        atomic_json(self.catalog_path, catalog)

    def _append_event(self, event: dict[str, Any]) -> None:
        events: list[dict[str, Any]] = []
        if self.events_path.exists():
            try:
                events = [json.loads(line) for line in self.events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            except (OSError, json.JSONDecodeError) as exc:
                raise DrainQualityError(f"Drain3 配置审计记录损坏: {exc}") from exc
        events.append(event)
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.events_path.with_name(f".{self.events_path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in events), encoding="utf-8")
        os.replace(temporary, self.events_path)

    @staticmethod
    def _content_hash(content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse(content: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
        parser = configparser.ConfigParser(interpolation=None)
        try:
            parser.read_string(content)
        except configparser.Error as exc:
            raise DrainQualityError(f"INI 语法无效: {exc}") from exc
        if not parser.has_section("DRAIN"):
            raise DrainQualityError("INI 缺少 DRAIN section")
        drain = parser["DRAIN"]
        parameters: dict[str, Any] = {}
        for name, (minimum, maximum) in PARAMETER_RANGES.items():
            if name not in drain:
                if name == "max_clusters":
                    continue
                raise DrainQualityError(f"DRAIN 缺少参数: {name}")
            try:
                value: float | int = float(drain[name]) if name == "sim_th" else int(drain[name])
            except ValueError as exc:
                raise DrainQualityError(f"{name} 类型无效") from exc
            if not minimum <= value <= maximum:
                raise DrainQualityError(f"{name} 必须在 {minimum:g}–{maximum:g} 之间")
            parameters[name] = value
        try:
            parameters["parametrize_numeric_tokens"] = drain.getboolean("parametrize_numeric_tokens", fallback=True)
        except ValueError as exc:
            raise DrainQualityError("parametrize_numeric_tokens 必须是布尔值") from exc
        raw_delimiters = drain.get("extra_delimiters", fallback='["="]')
        try:
            delimiters = json.loads(raw_delimiters)
        except json.JSONDecodeError as exc:
            raise DrainQualityError("extra_delimiters 必须是 JSON 字符串数组") from exc
        if not isinstance(delimiters, list) or not all(isinstance(item, str) and item for item in delimiters):
            raise DrainQualityError("extra_delimiters 必须是 JSON 字符串数组")
        parameters["extra_delimiters"] = raw_delimiters
        if parser.has_section("SNAPSHOT"):
            parameters["snapshot_interval_minutes"] = parser.getint("SNAPSHOT", "snapshot_interval_minutes", fallback=5)
            parameters["compress_state"] = parser.getboolean("SNAPSHOT", "compress_state", fallback=True)
        if parser.has_section("PROFILING"):
            parameters["profiling_enabled"] = parser.getboolean("PROFILING", "enabled", fallback=False)
            parameters["report_sec"] = parser.getint("PROFILING", "report_sec", fallback=60)

        masking_rules: list[dict[str, str]] = []
        if parser.has_option("MASKING", "masking"):
            try:
                raw_rules = json.loads(parser.get("MASKING", "masking"))
            except json.JSONDecodeError as exc:
                raise DrainQualityError(f"masking JSON 无效: {exc}") from exc
            if not isinstance(raw_rules, list):
                raise DrainQualityError("masking 必须是数组")
            for index, rule in enumerate(raw_rules):
                if not isinstance(rule, dict) or not isinstance(rule.get("regex_pattern"), str) or not isinstance(rule.get("mask_with"), str):
                    raise DrainQualityError(f"masking 规则 {index + 1} 缺少 regex_pattern 或 mask_with")
                try:
                    re.compile(rule["regex_pattern"])
                except re.error as exc:
                    raise DrainQualityError(f"masking 规则 {index + 1} 正则无效: {exc}") from exc
                masking_rules.append({"regex_pattern": rule["regex_pattern"], "mask_with": rule["mask_with"]})
        return parameters, masking_rules

    def _snapshot(self, config_id: str, version: int, path: Path, status: str, metadata: dict[str, Any]) -> dict[str, Any]:
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise DrainQualityError(f"无法读取 Drain3 配置: {exc}") from exc
        parameters, masking_rules = self._parse(content)
        return {
            "schema_version": "drain_config_snapshot_v1",
            "config_id": config_id,
            "version": version,
            "name": metadata.get("name") or config_id,
            "status": status,
            "description": metadata.get("description") or "",
            "content_hash": self._content_hash(content),
            "path": str(path.resolve()),
            "parameters": parameters,
            "masking_rules": masking_rules,
            "ini_content": content,
            "created_at": metadata.get("created_at"),
            "created_by": metadata.get("created_by"),
            "available_versions": sorted(int(item["version"]) for item in metadata.get("versions", [])) or [version],
        }

    def _baseline(self) -> dict[str, Any]:
        return self._snapshot("baseline", 1, self.baseline_path, "baseline", {"name": "drain3_recommended"})

    def list_configs(self) -> list[dict[str, Any]]:
        catalog = self._catalog()
        active = self._active_reference()
        items = [self._baseline()]
        for config_id, metadata in catalog["items"].items():
            version = int(metadata["latest_version"])
            status = "published" if active.get("config_id") == config_id and int(active.get("version", 0)) == version else metadata.get("status", "candidate")
            items.append(self._snapshot(config_id, version, self.configs_root / config_id / f"{version}.ini", status, metadata))
        return items

    def get_version(self, config_id: str, version: int) -> dict[str, Any]:
        if config_id == "baseline":
            if int(version) != 1:
                raise DrainQualityError("基线配置版本不存在")
            return self._baseline()
        catalog = self._catalog()
        metadata = catalog["items"].get(config_id)
        if not metadata:
            raise DrainQualityError("Drain3 配置不存在")
        version = int(version)
        if version < 1 or version > int(metadata["latest_version"]):
            raise DrainQualityError("Drain3 配置版本不存在")
        active = self._active_reference()
        status = "published" if active.get("config_id") == config_id and int(active.get("version", 0)) == version else ("candidate" if version == int(metadata["latest_version"]) else "history")
        version_meta = next((item for item in metadata.get("versions", []) if int(item["version"]) == version), metadata)
        return self._snapshot(config_id, version, self.configs_root / config_id / f"{version}.ini", status, dict(metadata, **version_meta))

    @staticmethod
    def _safe_id(name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:32] or "drain-config"
        return f"{slug}-{uuid.uuid4().hex[:8]}"

    def _write_ini(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, path)

    def create_candidate(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        with self._lock:
            source_id = str(source.get("source_config_id") or "baseline")
            source_version = int(source.get("source_version") or (1 if source_id == "baseline" else self._catalog()["items"].get(source_id, {}).get("latest_version", 0)))
            origin = self.get_version(source_id, source_version)
            name = str(source.get("name") or "Drain3 candidate").strip()
            config_id = self._safe_id(name)
            now = now_iso()
            self._write_ini(self.configs_root / config_id / "1.ini", origin["ini_content"])
            catalog = self._catalog()
            catalog["items"][config_id] = {
                "config_id": config_id,
                "name": name,
                "description": str(source.get("description") or ""),
                "status": "candidate",
                "latest_version": 1,
                "source_config_id": source_id,
                "source_version": source_version,
                "created_at": now,
                "created_by": str(source.get("operator") or "local-operator"),
                "versions": [{"version": 1, "created_at": now, "created_by": str(source.get("operator") or "local-operator")}],
            }
            self._write_catalog(catalog)
            return self.get_version(config_id, 1)

    def save_version(self, config_id: str, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        if config_id == "baseline":
            raise DrainQualityError("系统基线配置只读")
        content = source.get("ini_content")
        if not isinstance(content, str) or not content.strip():
            raise DrainQualityError("ini_content 必须是非空字符串")
        self._parse(content)
        with self._lock:
            catalog = self._catalog()
            metadata = catalog["items"].get(config_id)
            if not metadata:
                raise DrainQualityError("Drain3 配置不存在")
            latest = int(metadata["latest_version"])
            if int(source.get("expected_version") or 0) != latest:
                raise DrainQualityError(f"版本冲突：当前版本为 {latest}")
            version = latest + 1
            now = now_iso()
            self._write_ini(self.configs_root / config_id / f"{version}.ini", content)
            metadata["latest_version"] = version
            metadata["status"] = "candidate"
            metadata.setdefault("versions", []).append({"version": version, "created_at": now, "created_by": str(source.get("operator") or "local-operator")})
            self._write_catalog(catalog)
            return self.get_version(config_id, version)

    def validate_version(self, config_id: str, version: int) -> dict[str, Any]:
        snapshot = self.get_version(config_id, version)
        return {
            "schema_version": "drain_config_validation_v1",
            "valid": True,
            "config_id": config_id,
            "version": int(version),
            "content_hash": snapshot["content_hash"],
            "parameters": snapshot["parameters"],
            "masking_rules": snapshot["masking_rules"],
            "errors": [],
        }

    def _active_reference(self) -> dict[str, Any]:
        if not self.active_path.exists():
            return {"config_id": "baseline", "version": 1}
        try:
            payload = json.loads(self.active_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DrainQualityError(f"活动 Drain3 配置指针损坏: {exc}") from exc
        return payload

    def active_snapshot(self) -> dict[str, Any]:
        reference = self._active_reference()
        snapshot = self.get_version(str(reference.get("config_id") or "baseline"), int(reference.get("version") or 1))
        return dict(snapshot, status="published" if snapshot["config_id"] != "baseline" else "baseline")

    def _activate(self, config_id: str, version: int, payload: dict[str, Any], action: str) -> dict[str, Any]:
        if payload.get("confirmed") is not True:
            raise DrainQualityError("配置变更需要人工确认")
        if config_id == "baseline" and action == "publish":
            raise DrainQualityError("系统基线无需发布")
        snapshot = self.get_version(config_id, version)
        now = now_iso()
        reference = {
            "schema_version": "drain_active_config_v1",
            "config_id": config_id,
            "version": int(version),
            "content_hash": snapshot["content_hash"],
            "updated_at": now,
            "updated_by": str(payload.get("operator") or "local-operator"),
        }
        atomic_json(self.active_path, reference)
        self._append_event({
            "schema_version": "drain_config_event_v1",
            "event_id": f"config_event_{uuid.uuid4().hex}",
            "action": action,
            **reference,
        })
        return dict(snapshot, status="published", updated_at=now)

    def publish(self, config_id: str, version: int, payload: Any) -> dict[str, Any]:
        with self._lock:
            return self._activate(config_id, int(version), require_object(payload), "publish")

    def rollback(self, config_id: str, version: int, payload: Any) -> dict[str, Any]:
        with self._lock:
            return self._activate(config_id, int(version), require_object(payload), "rollback")
