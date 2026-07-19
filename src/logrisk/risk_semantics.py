from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from logrisk.database import SQLiteDatabase


SEVERITIES = {"low", "medium", "high", "critical"}
STATUSES = {"draft", "published", "disabled", "deprecated"}
SOURCES = {"builtin", "user", "imported", "ai_candidate"}
RISK_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _deep_merge(base: dict[str, Any], changes: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in changes.items():
        if key in {"id", "source", "version", "created_at"}:
            continue
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


class RiskSemanticError(ValueError):
    def __init__(self, message: str, *, code: str = "invalid_semantic_rule", status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _expand_catalog(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise RiskSemanticError("内置风险语义目录必须是 object")
    rules = [copy.deepcopy(item) for item in payload.get("rules") or []]
    for item in payload.get("xid_catalog") or []:
        code = int(item["code"])
        severity = str(item["severity"])
        rules.append({
            "id": f"builtin.gpu.xid.{code}",
            "display_name": str(item["display_name"]),
            "description": f"NVIDIA Xid {code} 风险语义；Xid 是排查入口，不等同于唯一根因。",
            "domain": "gpu",
            "category": "xid",
            "risk_type": str(item["risk_type"]),
            "risk_subtype": None,
            "match": {
                "source_types": ["kernel", "syslog", "journal", "unknown"],
                "components": ["kernel", "nvidia", "nvidia-driver"],
                "message_regex": [
                    rf"(?i)NVRM:\s*Xid\s*\((?P<pci_bdf>[^)]+)\):\s*(?P<xid_code>{code})\b",
                    rf"(?i)\bXid\s+(?P<xid_code>{code})\b",
                ],
            },
            "extract": {
                "xid_code": {"type": "integer", "from_group": "xid_code"},
                "pci_bdf": {"type": "string", "from_group": "pci_bdf", "normalize": "pci_bdf", "optional": True},
                "gpu_uuid": {"type": "string", "regex": r"(GPU-[0-9a-fA-F-]+)", "optional": True},
            },
            "classification": {
                "default_severity": severity,
                "base_score": float(item["base_score"]),
                "confidence": 1.0,
                "hard_override": ({"node_level": "critical", "when_status": ["active"]} if severity == "critical" else None),
            },
            "dedup": {"key_fields": ["cluster", "node_id", "risk_type", "pci_bdf"], "window_seconds": 300},
            "lifecycle": {"recovery_mode": "explicit_or_timeout", "recovery_timeout_seconds": 86400, "history_retention_days": 365},
            "recommendation": {"external_reference": "NVIDIA Xid Catalog"},
            "test_samples": {
                "positive": [f"NVRM: Xid (0000:65:00): {code}, GPU diagnostic event"],
                "negative": [f"NVRM: Xid (0000:65:00): {79 if code != 79 else 35}, different event"],
            },
            "tags": ["GPU", "NVIDIA", "Xid", str(code)],
            "priority": 10,
        })
    for item in payload.get("pattern_catalog") or []:
        risk_type = str(item["risk_type"])
        rules.append({
            "id": "builtin." + risk_type,
            "display_name": str(item["display_name"]),
            "description": str(item.get("description") or "基于日志模式识别的确定性风险语义。"),
            "domain": str(item["domain"]),
            "category": str(item["category"]),
            "risk_type": risk_type,
            "risk_subtype": None,
            "match": {
                "source_types": list(item.get("source_types") or []),
                "components": list(item.get("components") or []),
                "message_regex": [str(item["pattern"])],
            },
            "extract": dict(item.get("extract") or {}),
            "classification": {
                "default_severity": str(item["severity"]),
                "base_score": float(item.get("base_score") or {"low": 20, "medium": 45, "high": 75, "critical": 100}[str(item["severity"])]),
                "confidence": float(item.get("confidence") or 0.95),
            },
            "dedup": {"key_fields": list(item.get("dedup_fields") or ["cluster", "node_id", "risk_type"]), "window_seconds": int(item.get("window_seconds") or 300)},
            "lifecycle": {"recovery_mode": "explicit_or_timeout", "recovery_timeout_seconds": int(item.get("recovery_timeout_seconds") or 3600)},
            "recommendation": {"action_code": str(item.get("action_code") or "observe"), "automation_allowed": False},
            "test_samples": {"positive": [str(item["sample"])], "negative": ["normal operation completed"]},
            "tags": [str(item["domain"]), str(item["category"])],
            "priority": int(item.get("priority") or 100),
        })
    return rules


def validate_rule(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise RiskSemanticError("风险语义条目必须是 object")
    rule = copy.deepcopy(raw)
    rule_id = str(rule.get("id") or "")
    if not re.fullmatch(r"[a-z][a-z0-9_.-]{2,127}", rule_id):
        raise RiskSemanticError("语义 ID 无效")
    for field in ("display_name", "domain", "category", "risk_type"):
        if not str(rule.get(field) or "").strip():
            raise RiskSemanticError(f"字段 {field} 不能为空")
    if not RISK_TYPE_RE.fullmatch(str(rule["risk_type"])):
        raise RiskSemanticError("risk_type 格式无效")
    source = str(rule.get("source") or ("builtin" if rule_id.startswith("builtin.") else "user"))
    if source not in SOURCES:
        raise RiskSemanticError("source 无效")
    status = str(rule.get("status") or ("published" if source == "builtin" else "draft"))
    if status not in STATUSES:
        raise RiskSemanticError("status 无效")
    match = rule.get("match")
    patterns = match.get("message_regex") if isinstance(match, dict) else None
    if not isinstance(patterns, list) or not patterns or not all(isinstance(item, str) and item for item in patterns):
        raise RiskSemanticError("match.message_regex 必须是非空字符串数组")
    for pattern in patterns:
        if len(pattern) > 2000:
            raise RiskSemanticError("匹配正则长度超过限制")
        try:
            re.compile(pattern)
        except re.error as exc:
            raise RiskSemanticError(f"匹配正则无效: {exc}") from exc
    classification = rule.get("classification")
    if not isinstance(classification, dict) or classification.get("default_severity") not in SEVERITIES:
        raise RiskSemanticError("classification.default_severity 无效")
    score = float(classification.get("base_score", -1))
    confidence = float(classification.get("confidence", -1))
    if not 0 <= score <= 100 or not 0 <= confidence <= 1:
        raise RiskSemanticError("分类分数或置信度超出范围")
    dedup = rule.get("dedup")
    if not isinstance(dedup, dict) or not isinstance(dedup.get("key_fields"), list) or int(dedup.get("window_seconds") or 0) <= 0:
        raise RiskSemanticError("dedup 配置无效")
    rule.update({
        "schema_version": "risk_semantic_rule_v1",
        "enabled": bool(rule.get("enabled", True)),
        "source": source,
        "status": status,
        "version": int(rule.get("version") or 1),
        "description": str(rule.get("description") or ""),
        "risk_subtype": rule.get("risk_subtype"),
        "extract": dict(rule.get("extract") or {}),
        "lifecycle": dict(rule.get("lifecycle") or {}),
        "recommendation": dict(rule.get("recommendation") or {}),
        "test_samples": dict(rule.get("test_samples") or {}),
        "tags": [str(item) for item in (rule.get("tags") or []) if str(item)][:16],
        "priority": int(rule.get("priority") or 100),
    })
    return rule


class RiskSemanticService:
    def __init__(
        self,
        database: SQLiteDatabase,
        builtin_path: str | Path,
        *,
        clock: Callable[[], str] = _now,
    ) -> None:
        self.database = database
        self.builtin_path = Path(builtin_path)
        self.clock = clock
        self._lock = threading.RLock()
        self._effective: tuple[dict[str, Any], ...] = ()
        self._seed_builtins()
        self.reload()

    def _load_builtins(self) -> list[dict[str, Any]]:
        try:
            payload = yaml.safe_load(self.builtin_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            raise RiskSemanticError(f"内置风险语义目录不可读: {exc}") from exc
        return [validate_rule(item) for item in _expand_catalog(payload)]

    def _seed_builtins(self) -> None:
        now = self.clock()
        with self.database.transaction() as connection:
            for rule in self._load_builtins():
                row = connection.execute(
                    "SELECT current_version, content_hash FROM risk_semantic_rules WHERE rule_id=?", (rule["id"],)
                ).fetchone()
                seed_content = dict(rule, status="published", enabled=True)
                digest = _hash(seed_content)
                if row and row["content_hash"] == digest:
                    continue
                version = int(row["current_version"]) + 1 if row else 1
                snapshot = dict(seed_content, version=version)
                connection.execute(
                    "INSERT INTO risk_semantic_rules(rule_id, source, override_of, status, enabled, current_version, "
                    "content_json, content_hash, created_at, updated_at) VALUES (?, 'builtin', NULL, 'published', 1, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(rule_id) DO UPDATE SET current_version=excluded.current_version, content_json=excluded.content_json, "
                    "content_hash=excluded.content_hash, status='published', enabled=1, updated_at=excluded.updated_at",
                    (snapshot["id"], version, _json(snapshot), digest, now, now),
                )
                connection.execute(
                    "INSERT INTO risk_semantic_rule_versions(rule_id, version, content_json, content_hash, changed_by, "
                    "change_reason, created_at) VALUES (?, ?, ?, ?, 'system-seed', '同步内置风险语义目录', ?)",
                    (snapshot["id"], version, _json(snapshot), digest, now),
                )

    @staticmethod
    def _row(row: Any) -> dict[str, Any]:
        rule = json.loads(row["content_json"])
        rule.update({
            "id": row["rule_id"], "source": row["source"], "override_of": row["override_of"],
            "status": row["status"], "enabled": bool(row["enabled"]), "version": int(row["current_version"]),
            "content_hash": row["content_hash"], "created_at": row["created_at"], "updated_at": row["updated_at"],
        })
        return rule

    def list_rules(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM risk_semantic_rules ORDER BY source, rule_id").fetchall()
        return [self._row(row) for row in rows]

    def get_rule(self, rule_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM risk_semantic_rules WHERE rule_id=?", (rule_id,)).fetchone()
        if row is None:
            raise RiskSemanticError("风险语义不存在", code="semantic_not_found", status_code=404)
        return self._row(row)

    def reload(self) -> dict[str, Any]:
        with self._lock:
            rules = self.list_rules()
            overrides = {
                rule["override_of"]: rule
                for rule in rules
                if rule.get("override_of") and rule["status"] == "published" and rule["enabled"]
            }
            effective = []
            for rule in rules:
                if rule["status"] != "published" or not rule["enabled"]:
                    continue
                if rule["source"] == "builtin" and rule["id"] in overrides:
                    continue
                if rule.get("override_of") or rule["source"] == "builtin":
                    effective.append(validate_rule(rule))
                elif rule["source"] in {"user", "imported"}:
                    effective.append(validate_rule(rule))
            effective.sort(key=lambda item: (int(item.get("priority") or 100), 0 if item.get("override_of") else 1, item["id"]))
            self._effective = tuple(copy.deepcopy(effective))
            version = _hash([(item["id"], item["version"], item["content_hash"]) for item in effective])[:16]
            return {"schema_version": "risk_semantic_registry_v1", "registry_version": version, "rule_count": len(effective)}

    def effective_rules(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(list(self._effective))

    @staticmethod
    def _applies(rule: dict[str, Any], record: dict[str, Any]) -> bool:
        match = rule["match"]
        sources = {str(item).lower() for item in match.get("source_types") or []}
        components = {str(item).lower() for item in match.get("components") or []}
        source = str(record.get("source_type") or "unknown").lower()
        component = str(record.get("component") or "unknown").lower()
        return (not sources or source in sources) and (not components or component in components)

    @staticmethod
    def _fields(rule: dict[str, Any], match: re.Match[str], message: str) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        groups = match.groupdict()
        for name, spec in rule.get("extract", {}).items():
            value = groups.get(str(spec.get("from_group") or ""))
            if value is None and spec.get("regex"):
                found = re.search(str(spec["regex"]), message)
                value = found.group(1) if found else None
            if value is None:
                if not spec.get("optional"):
                    continue
                continue
            if spec.get("type") == "integer":
                value = int(value)
            elif spec.get("normalize") == "pci_bdf":
                value = str(value).strip().lower()
            fields[name] = value
        return fields

    def match(self, record: dict[str, Any]) -> dict[str, Any]:
        message = str(record.get("message_core") or record.get("template") or "")
        with self._lock:
            rules = self._effective
        for rule in rules:
            if not self._applies(rule, record):
                continue
            for pattern in rule["match"]["message_regex"]:
                found = re.search(pattern, message)
                if not found:
                    continue
                classification = rule["classification"]
                return {
                    "schema_version": "semantic_event_v1",
                    "semantic_rule_id": rule["id"],
                    "semantic_rule_version": rule["version"],
                    "domain": rule["domain"],
                    "category": rule["category"],
                    "risk_type": rule["risk_type"],
                    "risk_subtype": rule.get("risk_subtype"),
                    "severity": classification["default_severity"],
                    "base_score": float(classification["base_score"]),
                    "confidence": float(classification["confidence"]),
                    "semantic_fields": self._fields(rule, found, message),
                    "dedup": copy.deepcopy(rule["dedup"]),
                    "lifecycle": copy.deepcopy(rule["lifecycle"]),
                    "hard_override": copy.deepcopy(classification.get("hard_override")),
                    "recommendation": copy.deepcopy(rule.get("recommendation") or {}),
                    "tags": list(rule.get("tags") or []),
                }
        raise RiskSemanticError("未命中风险语义", code="semantic_unclassified", status_code=404)

    def _test(self, rule: dict[str, Any]) -> dict[str, Any]:
        errors = []
        positives = list((rule.get("test_samples") or {}).get("positive") or [])
        negatives = list((rule.get("test_samples") or {}).get("negative") or [])
        if not positives:
            errors.append("至少需要一条正样例")
        patterns = [re.compile(item) for item in rule["match"]["message_regex"]]
        for sample in positives:
            if not any(pattern.search(str(sample)) for pattern in patterns):
                errors.append("正样例未命中")
        for sample in negatives:
            if any(pattern.search(str(sample)) for pattern in patterns):
                errors.append("负样例错误命中")
        return {"schema_version": "risk_semantic_validation_v1", "valid": not errors, "errors": errors}

    def _insert(self, rule: dict[str, Any], *, operator: str, reason: str) -> dict[str, Any]:
        now = self.clock()
        snapshot = validate_rule(rule)
        digest = _hash(snapshot)
        with self.database.transaction() as connection:
            if connection.execute("SELECT 1 FROM risk_semantic_rules WHERE rule_id=?", (snapshot["id"],)).fetchone():
                raise RiskSemanticError("语义 ID 已存在", code="semantic_conflict", status_code=409)
            connection.execute(
                "INSERT INTO risk_semantic_rules(rule_id, source, override_of, status, enabled, current_version, "
                "content_json, content_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)",
                (snapshot["id"], snapshot["source"], snapshot.get("override_of"), snapshot["status"], int(snapshot["enabled"]), _json(snapshot), digest, now, now),
            )
            connection.execute(
                "INSERT INTO risk_semantic_rule_versions(rule_id, version, content_json, content_hash, changed_by, change_reason, created_at) "
                "VALUES (?, 1, ?, ?, ?, ?, ?)",
                (snapshot["id"], _json(snapshot), digest, operator, reason, now),
            )
            self._audit(connection, snapshot["id"], "created", None, 1, None, snapshot, operator, reason, now)
        self.reload()
        return self.get_rule(snapshot["id"])

    def create_rule(self, payload: dict[str, Any], *, operator: str, reason: str) -> dict[str, Any]:
        rule = dict(payload, source=str(payload.get("source") or "user"), status="draft", enabled=True, version=1)
        return self._insert(rule, operator=operator, reason=reason)

    def create_override(self, builtin_id: str, changes: dict[str, Any], *, operator: str, reason: str) -> dict[str, Any]:
        builtin = self.get_rule(builtin_id)
        if builtin["source"] != "builtin":
            raise RiskSemanticError("只能覆盖内置语义")
        rule = _deep_merge(builtin, changes)
        rule.update({
            "id": "user." + builtin_id.removeprefix("builtin."), "source": "user", "override_of": builtin_id,
            "status": "draft", "enabled": True, "version": 1,
        })
        return self._insert(rule, operator=operator, reason=reason)

    @staticmethod
    def _audit(connection: Any, rule_id: str, event_type: str, from_version: int | None, to_version: int | None,
               before: dict[str, Any] | None, after: dict[str, Any] | None, operator: str, reason: str, now: str) -> None:
        connection.execute(
            "INSERT INTO risk_semantic_events(event_id, rule_id, event_type, from_version, to_version, before_json, "
            "after_json, operator, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"semantic-event-{uuid.uuid4().hex}", rule_id, event_type, from_version, to_version,
             _json(before) if before else None, _json(after) if after else None, operator, reason, now),
        )

    def update_rule(self, rule_id: str, changes: dict[str, Any], *, expected_version: int, operator: str, reason: str) -> dict[str, Any]:
        current = self.get_rule(rule_id)
        if current["source"] == "builtin":
            raise RiskSemanticError("内置语义只读，请创建覆盖条目")
        if current["version"] != int(expected_version):
            raise RiskSemanticError("语义版本冲突", code="version_conflict", status_code=409)
        version = current["version"] + 1
        updated = validate_rule(dict(_deep_merge(current, changes), id=rule_id, version=version, status="draft"))
        now = self.clock()
        with self.database.transaction() as connection:
            result = connection.execute(
                "UPDATE risk_semantic_rules SET status='draft', enabled=?, current_version=?, content_json=?, content_hash=?, updated_at=? "
                "WHERE rule_id=? AND current_version=?",
                (int(updated["enabled"]), version, _json(updated), _hash(updated), now, rule_id, expected_version),
            )
            if result.rowcount != 1:
                raise RiskSemanticError("语义版本冲突", code="version_conflict", status_code=409)
            connection.execute(
                "INSERT INTO risk_semantic_rule_versions(rule_id, version, content_json, content_hash, changed_by, change_reason, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)", (rule_id, version, _json(updated), _hash(updated), operator, reason, now),
            )
            self._audit(connection, rule_id, "updated", current["version"], version, current, updated, operator, reason, now)
        self.reload()
        return self.get_rule(rule_id)

    def publish(self, rule_id: str, *, expected_version: int, confirmed: bool, operator: str, reason: str) -> dict[str, Any]:
        if confirmed is not True:
            raise RiskSemanticError("发布需要人工确认", code="confirmation_required", status_code=400)
        current = self.get_rule(rule_id)
        if current["version"] != int(expected_version):
            raise RiskSemanticError("语义版本冲突", code="version_conflict", status_code=409)
        report = self._test(validate_rule(current))
        if not report["valid"]:
            raise RiskSemanticError("发布前验证失败: " + "；".join(report["errors"]))
        now = self.clock()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO risk_semantic_validations(rule_id, version, valid, validation_json, content_hash, created_at) "
                "VALUES (?, ?, 1, ?, ?, ?)", (rule_id, current["version"], _json(report), current["content_hash"], now),
            )
            connection.execute("UPDATE risk_semantic_rules SET status='published', enabled=1, updated_at=? WHERE rule_id=?", (now, rule_id))
            self._audit(connection, rule_id, "published", current["version"], current["version"], current, dict(current, status="published"), operator, reason, now)
        self.reload()
        return self.get_rule(rule_id)

    def restore_default(self, builtin_id: str, *, expected_version: int, confirmed: bool, operator: str, reason: str) -> dict[str, Any]:
        if confirmed is not True:
            raise RiskSemanticError("恢复默认需要人工确认", code="confirmation_required", status_code=400)
        with self.database.connect() as connection:
            row = connection.execute("SELECT rule_id FROM risk_semantic_rules WHERE override_of=? AND status='published'", (builtin_id,)).fetchone()
        if row is None:
            raise RiskSemanticError("没有生效中的覆盖语义", code="semantic_not_found", status_code=404)
        current = self.get_rule(row["rule_id"])
        if current["version"] != int(expected_version):
            raise RiskSemanticError("语义版本冲突", code="version_conflict", status_code=409)
        now = self.clock()
        with self.database.transaction() as connection:
            connection.execute("UPDATE risk_semantic_rules SET status='disabled', enabled=0, updated_at=? WHERE rule_id=?", (now, current["id"]))
            self._audit(connection, current["id"], "restored_default", current["version"], current["version"], current, dict(current, status="disabled", enabled=False), operator, reason, now)
        self.reload()
        return self.get_rule(current["id"])

    def versions(self, rule_id: str) -> list[dict[str, Any]]:
        self.get_rule(rule_id)
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT version, content_json, content_hash, changed_by, change_reason, created_at FROM risk_semantic_rule_versions "
                "WHERE rule_id=? ORDER BY version DESC", (rule_id,),
            ).fetchall()
        return [dict(row) | {"rule": json.loads(row["content_json"])} for row in rows]

    def rollback(self, rule_id: str, *, target_version: int, expected_version: int, confirmed: bool,
                 operator: str, reason: str) -> dict[str, Any]:
        if confirmed is not True:
            raise RiskSemanticError("回滚需要人工确认", code="confirmation_required", status_code=400)
        current = self.get_rule(rule_id)
        if current["version"] != int(expected_version):
            raise RiskSemanticError("语义版本冲突", code="version_conflict", status_code=409)
        target = next((item for item in self.versions(rule_id) if item["version"] == int(target_version)), None)
        if target is None:
            raise RiskSemanticError("目标语义版本不存在", code="version_not_found", status_code=404)
        restored = dict(target["rule"], id=rule_id, version=current["version"] + 1, status="draft")
        return self.update_rule(rule_id, restored, expected_version=current["version"], operator=operator, reason=reason)

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        rule = validate_rule(payload)
        return self._test(rule)

    def test_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        rule = validate_rule(payload.get("rule") or payload)
        samples = [
            (str(sample), expected)
            for expected, key in ((True, "positive_samples"), (False, "negative_samples"))
            for sample in (payload.get(key) or (rule.get("test_samples") or {}).get("positive" if expected else "negative") or [])
        ]
        patterns = [re.compile(item) for item in rule["match"]["message_regex"]]
        results = []
        errors = []
        for sample, expected in samples:
            found = next((pattern.search(sample) for pattern in patterns if pattern.search(sample)), None)
            matched = found is not None
            if matched != expected:
                errors.append(f"样例期望 {'命中' if expected else '不命中'}，实际 {'命中' if matched else '不命中'}")
            results.append({
                "sample": sample,
                "expected": expected,
                "matched": matched,
                "semantic_fields": self._fields(rule, found, sample) if found else {},
                "risk_type": rule["risk_type"] if found else None,
                "severity": rule["classification"]["default_severity"] if found else None,
            })
        return {"schema_version": "risk_semantic_test_v1", "valid": not errors, "compile_errors": [], "errors": errors, "results": results, "conflicts": []}

    def disable(self, rule_id: str, *, expected_version: int, confirmed: bool, operator: str, reason: str) -> dict[str, Any]:
        if confirmed is not True:
            raise RiskSemanticError("停用需要人工确认", code="confirmation_required", status_code=400)
        current = self.get_rule(rule_id)
        if current["source"] == "builtin":
            raise RiskSemanticError("内置语义不可直接停用，请创建覆盖条目")
        if current["version"] != int(expected_version):
            raise RiskSemanticError("语义版本冲突", code="version_conflict", status_code=409)
        now = self.clock()
        with self.database.transaction() as connection:
            connection.execute("UPDATE risk_semantic_rules SET status='disabled', enabled=0, updated_at=? WHERE rule_id=?", (now, rule_id))
            self._audit(connection, rule_id, "disabled", current["version"], current["version"], current, dict(current, status="disabled", enabled=False), operator, reason, now)
        self.reload()
        return self.get_rule(rule_id)

    def delete(self, rule_id: str, *, confirmed: bool, operator: str, reason: str) -> None:
        current = self.get_rule(rule_id)
        if current["source"] == "builtin":
            raise RiskSemanticError("内置语义不可删除")
        if confirmed is not True or not operator.strip() or not reason.strip():
            raise RiskSemanticError("删除需要人工确认、操作人和原因", code="confirmation_required", status_code=400)
        with self.database.transaction() as connection:
            connection.execute("DELETE FROM risk_semantic_rules WHERE rule_id=?", (rule_id,))
        self.reload()

    def export_bundle(self) -> dict[str, Any]:
        return {
            "schema_version": "risk_semantic_bundle_v1",
            "exported_at": self.clock(),
            "rules": [rule for rule in self.list_rules() if rule["source"] != "builtin"],
        }

    def import_bundle(self, payload: dict[str, Any], *, operator: str, reason: str) -> dict[str, Any]:
        items = payload.get("rules") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise RiskSemanticError("导入包缺少 rules 数组")
        created = []
        for item in items:
            rule = dict(item, source="imported", status="draft", enabled=True)
            rule.pop("content_hash", None)
            rule.pop("created_at", None)
            rule.pop("updated_at", None)
            created.append(self._insert(rule, operator=operator, reason=reason))
        return {"schema_version": "risk_semantic_import_v1", "count": len(created), "items": created}

    def list_unclassified(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM risk_semantic_unclassified ORDER BY occurrence_count DESC, last_seen DESC").fetchall()
        return [dict(row) | {"candidate": json.loads(row["candidate_json"])} for row in rows]

    def record_unclassified(self, record: dict[str, Any]) -> dict[str, Any] | None:
        message = str(record.get("message_core") or record.get("template") or "")
        if not re.search(r"(?i)(\bXid\b|\bSXid\b|error|failed|failure|oom|pressure|timeout|notready)", message):
            return None
        typed_message = str(record.get("template") or "")
        if not typed_message:
            return None
        component = str(record.get("component") or "unknown")
        template_hash = str(record.get("template_hash") or "")
        candidate_id = "unclassified-" + hashlib.sha256(f"{component}|{template_hash}|{typed_message}".encode("utf-8")).hexdigest()[:20]
        now = self.clock()
        candidate = {"component": component, "template_hash": template_hash or None, "typed_message": typed_message}
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO risk_semantic_unclassified(candidate_id, component, template_hash, typed_message, occurrence_count, "
                "first_seen, last_seen, status, candidate_json) VALUES (?, ?, ?, ?, 1, ?, ?, 'open', ?) "
                "ON CONFLICT(candidate_id) DO UPDATE SET occurrence_count=occurrence_count+1, last_seen=excluded.last_seen",
                (candidate_id, component, template_hash or None, typed_message, now, now, _json(candidate)),
            )
        return {"candidate_id": candidate_id, **candidate}

    def create_from_unclassified(self, candidate_id: str, payload: dict[str, Any], *, operator: str, reason: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM risk_semantic_unclassified WHERE candidate_id=?", (candidate_id,)).fetchone()
        if row is None:
            raise RiskSemanticError("待补充语义不存在", code="semantic_candidate_not_found", status_code=404)
        rule_payload = payload.get("rule") if isinstance(payload.get("rule"), dict) else payload
        rule_payload = {key: value for key, value in rule_payload.items() if key not in {"operator", "reason"}}
        created = self.create_rule(rule_payload, operator=operator, reason=reason)
        now = self.clock()
        candidate = json.loads(row["candidate_json"])
        candidate["created_rule_id"] = created["id"]
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE risk_semantic_unclassified SET status='converted', candidate_json=?, last_seen=? WHERE candidate_id=?",
                (_json(candidate), now, candidate_id),
            )
        return created
