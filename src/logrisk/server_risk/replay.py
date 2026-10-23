from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from logrisk.server_risk.contracts import ServerRiskError, digest, json_text, typed_fields, utc


@dataclass(frozen=True)
class EvaluationContext:
    """Internal event clock. Public current endpoints never accept this object."""

    mode: str
    as_of: str

    def __post_init__(self) -> None:
        if self.mode not in ("current", "historical"):
            raise ServerRiskError("invalid_evaluation_context", "判断时间模式无效")
        utc(self.as_of)

    @classmethod
    def historical(cls, as_of: str) -> EvaluationContext:
        return cls("historical", as_of)


def isolated_destination(source: Path, destination: Path, allowed_root: Path) -> Path:
    source = source.resolve(strict=True)
    root = allowed_root.resolve(strict=True)
    target = destination.absolute()
    if target.exists() or target.is_symlink() or target.resolve() == source or root not in target.resolve().parents:
        raise ServerRiskError("replay_destination_invalid", "回放目标必须是受控目录中新建数据库")
    current = target
    while current != root:
        if current.is_symlink():
            raise ServerRiskError("replay_destination_invalid", "回放路径不能含符号链接")
        current = current.parent
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def import_history(source: Path, destination: Path, *, allowed_root: Path) -> dict[str, Any]:
    """Copy only whitelisted sanitized history into a new migrated database."""
    from logrisk.database import SQLiteDatabase
    target = isolated_destination(source, destination, allowed_root)
    database = SQLiteDatabase(target)
    manifest = {"evaluation_mode": "historical", "source_readonly": True, "provider_credentials_copied": False, "tables": {}, "timestamp_digest": None}
    times = []
    with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as origin:
        origin.row_factory = sqlite3.Row
        with database.transaction() as conn:
            # Columns are selected from fixed schemas, never arbitrary source SQL.
            tables = {
                "node_risk_events": ("event_id", "cluster", "node_id", "risk_domain", "risk_category", "risk_type", "severity", "base_score", "confidence", "semantic_rule_id", "semantic_rule_version", "dedup_key", "occurrence_count", "first_seen", "last_seen", "window_start", "window_end", "status", "semantic_fields_json", "evidence_refs_json", "created_at", "updated_at", "acknowledged_at", "recovered_at", "current_occurrence_count", "derivation_status"),
                "node_risk_ingestions": ("source_event_fingerprint", "event_id", "occurrence_count", "source_namespace", "source_batch_id", "source_window_start", "source_item_id", "physical_key", "semantic_revision", "is_current", "ingested_at"),
                "node_risk_score_samples": ("sample_id", "cluster", "node_id", "revision", "assessed_at", "overall_score"),
            }
            for table, allowed in tables.items():
                available = {row[1] for row in origin.execute("PRAGMA table_info(" + table + ")")}
                columns = [key for key in allowed if key in available]
                target_columns = {row[1] for row in conn.execute("PRAGMA table_info(" + table + ")")}
                # Supply all required target columns using safe scalar defaults.
                required = [row for row in conn.execute("PRAGMA table_info(" + table + ")") if row[3] and row[4] is None and row[1] not in columns]
                rows = origin.execute("SELECT " + ",".join(columns) + " FROM " + table).fetchall()
                for row in rows:
                    value = dict(row)
                    if "semantic_fields_json" in value:
                        import json
                        value["semantic_fields_json"] = json_text(typed_fields(json.loads(value["semantic_fields_json"] or "{}")))
                    if "evidence_refs_json" in value:
                        value["evidence_refs_json"] = "[]"
                    for item in required:
                        value[item[1]] = 0 if "INT" in item[2].upper() or "REAL" in item[2].upper() else "[]" if item[1].endswith("_json") else "historical-import"
                    names = list(value)
                    conn.execute("INSERT INTO " + table + "(" + ",".join(names) + ") VALUES (" + ",".join("?" for _ in names) + ")", [value[key] for key in names])
                    times.append([table, *(value.get(key) for key in ("first_seen", "last_seen", "window_start", "window_end", "assessed_at"))])
                manifest["tables"][table] = len(rows)
            from logrisk.streaming_state import _stored_payload_hash, _payload_hash
            import json
            commits = origin.execute("SELECT task_id,window_id,cursor_json,summary_json,committed_at,payload_hash FROM streaming_window_commits ORDER BY task_id,window_id").fetchall()
            copied_windows = 0
            source_revisions = []
            for commit in commits:
                cursor, summary = json.loads(commit["cursor_json"]), json.loads(commit["summary_json"])
                try:
                    original_hash, count = _stored_payload_hash(origin, commit["task_id"], commit["window_id"], cursor, summary)
                except Exception:
                    continue
                if original_hash != commit["payload_hash"] or summary.get("template_count") not in (None, count):
                    continue
                windows = []
                for row in origin.execute("SELECT window_json FROM streaming_batch_windows WHERE task_id=? AND window_id=? ORDER BY item_index", (commit["task_id"], commit["window_id"])):
                    window = json.loads(row[0])
                    safe = {key: window[key] for key in ("cluster", "entity_type", "entity_id", "source_namespace", "source_item_id", "source_batch_id", "window_start", "window_end", "first_seen", "last_seen", "count", "template_hash", "component") if key in window and isinstance(window[key], (str, int, float, bool))}
                    semantic = window.get("risk_semantic")
                    if isinstance(semantic, dict):
                        safe["risk_semantic"] = {key: semantic[key] for key in ("risk_type", "severity", "risk_domain", "risk_category", "semantic_rule_id", "semantic_rule_version") if key in semantic and isinstance(semantic[key], (str, int, float, bool))}
                        safe["risk_semantic"]["semantic_fields"] = typed_fields(semantic.get("semantic_fields"))
                    windows.append(safe)
                    times.append(["streaming_batch_windows", *(safe.get(key) for key in ("first_seen", "last_seen", "window_start", "window_end"))])
                safe_cursor = {"kind": "file", "value": {"offset": 0}}
                safe_summary = {"template_count": len(windows), "historical_import": True}
                copied_hash = _payload_hash(safe_cursor, windows, safe_summary)
                task = commit["task_id"]
                conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES (?,'file','{}','historical-import','completed','COMPLETED',?,'{}',?,?) ON CONFLICT(task_id) DO NOTHING", (task, json_text(safe_cursor), commit["committed_at"], commit["committed_at"]))
                conn.execute("INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at,payload_hash) VALUES (?,?,?,?,?,?)", (task, commit["window_id"], json_text(safe_cursor), json_text(safe_summary), commit["committed_at"], copied_hash))
                for index, window in enumerate(windows):
                    conn.execute("INSERT INTO streaming_batch_windows(task_id,window_id,item_index,window_json) VALUES (?,?,?,?)", (task, commit["window_id"], index, json_text(window)))
                copied_windows += len(windows)
                source_revisions.append([task, commit["window_id"], original_hash, copied_hash])
            manifest["tables"]["streaming_window_commits"] = len(source_revisions)
            manifest["tables"]["streaming_batch_windows"] = copied_windows
            manifest["source_verified_revisions_digest"] = digest(source_revisions)
    manifest["timestamp_digest"] = digest(times)
    return manifest
