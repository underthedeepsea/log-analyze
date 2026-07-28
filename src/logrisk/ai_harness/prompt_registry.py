from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from logrisk.database import SQLiteDatabase, utc_now


FEATURE_OUTPUT_FIELDS = (
    "feature_type",
    "title",
    "summary",
    "importance",
    "template_hashes",
    "components",
    "tags",
    "selection_reason",
)


def validate_feature_prompt_contract(content: str) -> None:
    missing = [field for field in FEATURE_OUTPUT_FIELDS if field not in content]
    if missing:
        raise ValueError(f"feature_extract Prompt 缺少必填输出字段: {', '.join(missing)}")
    if "lowercase_snake_case" not in content:
        raise ValueError("feature_extract Prompt 必须要求 feature_type 使用 lowercase_snake_case")
    stripped = content.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        raise ValueError("feature_extract Prompt 不能整体包裹在 Markdown 代码围栏中")


@dataclass(frozen=True)
class PromptTemplate:
    prompt_id: str
    content: str
    sha256: str
    path: str
    display_name: str | None = None
    description: str | None = None
    analysis_type: str = "feature_extract"
    status: str = "active"
    is_default: bool = False
    version: str = "v1"


class PromptRegistry:
    def __init__(self, prompt_dir: str | Path, config_path: str | Path | None = None, history_path: str | Path | None = None):
        self.prompt_dir = Path(prompt_dir)
        self.config_path = Path(config_path) if config_path else None
        self.history_path = Path(history_path) if history_path else None

    def _config(self) -> dict[str, Any]:
        if not self.config_path or not self.config_path.is_file():
            return {}
        return yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}

    def _meta(self, prompt_id: str) -> dict[str, Any]:
        for item in self._config().get("prompts") or []:
            if item.get("prompt_id") == prompt_id:
                return dict(item)
        return {}

    def load(self, prompt_id: str) -> PromptTemplate:
        path = self.prompt_dir / f"{prompt_id}.md"
        if not path.is_file():
            raise FileNotFoundError(f"prompt not found: {prompt_id}")
        raw = path.read_bytes()
        meta = self._meta(prompt_id)
        return PromptTemplate(
            prompt_id=prompt_id,
            content=raw.decode("utf-8"),
            sha256=hashlib.sha256(raw).hexdigest(),
            path=str(path),
            display_name=meta.get("display_name"),
            description=meta.get("description"),
            analysis_type=meta.get("analysis_type") or "feature_extract",
            status=meta.get("status") or "active",
            is_default=bool(meta.get("is_default")),
            version=meta.get("version") or "v1",
        )

    def list_prompts(self) -> list[PromptTemplate]:
        configured = [item.get("prompt_id") for item in self._config().get("prompts") or [] if item.get("prompt_id")]
        prompt_ids = configured or sorted(path.stem for path in self.prompt_dir.glob("*.md"))
        return [self.load(prompt_id) for prompt_id in prompt_ids]

    def get_default(self, analysis_type: str) -> PromptTemplate:
        prompt_id = (self._config().get("defaults") or {}).get(analysis_type)
        if prompt_id:
            return self.load(prompt_id)
        for prompt in self.list_prompts():
            if prompt.analysis_type == analysis_type and prompt.is_default:
                return prompt
        raise FileNotFoundError(f"default prompt not found for analysis_type: {analysis_type}")

    def _history_file(self) -> Path:
        return self.history_path or self.prompt_dir.parent / "state" / "prompt_versions.json"

    def _all_history(self) -> dict[str, list[dict[str, Any]]]:
        path = self._history_file()
        if not path.is_file():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return value if isinstance(value, dict) else {}

    def history(self, prompt_id: str) -> list[dict[str, Any]]:
        return list(self._all_history().get(prompt_id) or [])

    def update(self, prompt_id: str, content: str, note: str = "") -> PromptTemplate:
        if not isinstance(content, str) or not content.strip():
            raise ValueError("prompt content must be non-empty")
        current = self.load(prompt_id)
        if current.analysis_type == "feature_extract":
            validate_feature_prompt_contract(content)
        path = Path(current.path)
        history = self._all_history()
        history.setdefault(prompt_id, []).insert(0, {
            "version_id": current.sha256[:12],
            "content": current.content,
            "sha256": current.sha256,
            "note": note,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        })
        history_path = self._history_file()
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
        path.write_text(content, encoding="utf-8")
        return self.load(prompt_id)


class SQLitePromptRegistry(PromptRegistry):
    def __init__(
        self,
        database: SQLiteDatabase,
        prompt_dir: str | Path,
        config_path: str | Path | None = None,
    ) -> None:
        super().__init__(prompt_dir, config_path)
        self.database = database
        self._seed()

    def _seed(self) -> None:
        config = self._config()
        defaults = set((config.get("defaults") or {}).values())
        configured = [item for item in (config.get("prompts") or []) if item.get("prompt_id")]
        if not configured:
            configured = [{"prompt_id": path.stem} for path in sorted(self.prompt_dir.glob("*.md"))]
        now = utc_now()
        with self.database.transaction() as connection:
            for meta in configured:
                prompt_id = str(meta["prompt_id"])
                path = self.prompt_dir / f"{prompt_id}.md"
                content = path.read_text(encoding="utf-8")
                digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
                analysis_type = str(meta.get("analysis_type") or "feature_extract")
                if analysis_type == "feature_extract":
                    validate_feature_prompt_contract(content)
                current = connection.execute(
                    "SELECT t.current_version, v.content, v.content_sha256 FROM prompt_templates t "
                    "JOIN prompt_versions v ON v.prompt_id=t.prompt_id AND v.version=t.current_version "
                    "WHERE t.prompt_id=?",
                    (prompt_id,),
                ).fetchone()
                if current is None:
                    connection.execute(
                        "INSERT INTO prompt_templates(prompt_id, analysis_type, display_name, description, status, is_default, "
                        "current_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        (
                            prompt_id,
                            analysis_type,
                            meta.get("display_name"),
                            meta.get("description"),
                            meta.get("status") or "active",
                            bool(meta.get("is_default") or prompt_id in defaults),
                            now,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT INTO prompt_versions(prompt_id, version, content, content_sha256, note, created_at) "
                        "VALUES (?, 1, ?, ?, ?, ?)",
                        (prompt_id, content, digest, "seed", now),
                    )
                    continue
                if analysis_type != "feature_extract":
                    continue
                try:
                    validate_feature_prompt_contract(str(current["content"]))
                except ValueError:
                    next_version = int(connection.execute(
                        "SELECT COALESCE(MAX(version), 0) + 1 FROM prompt_versions WHERE prompt_id=?",
                        (prompt_id,),
                    ).fetchone()[0])
                    connection.execute(
                        "INSERT INTO prompt_versions(prompt_id, version, content, content_sha256, note, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            prompt_id,
                            next_version,
                            content,
                            digest,
                            "系统修复：补齐 8 字段输出契约",
                            now,
                        ),
                    )
                    connection.execute(
                        "UPDATE prompt_templates SET current_version=?, updated_at=? WHERE prompt_id=?",
                        (next_version, now, prompt_id),
                    )

    @staticmethod
    def _template(row: Any) -> PromptTemplate:
        return PromptTemplate(
            prompt_id=row["prompt_id"],
            content=row["content"],
            sha256=row["content_sha256"],
            path=f"database:prompt/{row['prompt_id']}/v{row['current_version']}",
            display_name=row["display_name"],
            description=row["description"],
            analysis_type=row["analysis_type"],
            status=row["status"],
            is_default=bool(row["is_default"]),
            version=f"v{row['current_version']}",
        )

    def load(self, prompt_id: str) -> PromptTemplate:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT t.*, v.content, v.content_sha256 FROM prompt_templates t "
                "JOIN prompt_versions v ON v.prompt_id=t.prompt_id AND v.version=t.current_version WHERE t.prompt_id=?",
                (prompt_id,),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"prompt not found: {prompt_id}")
        return self._template(row)

    def load_by_hash(self, prompt_id: str, sha256: str) -> PromptTemplate:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT t.*, v.version AS current_version, v.content, v.content_sha256 "
                "FROM prompt_templates t JOIN prompt_versions v ON v.prompt_id=t.prompt_id "
                "WHERE t.prompt_id=? AND v.content_sha256=?",
                (prompt_id, sha256),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"prompt snapshot not found: {prompt_id}@{sha256}")
        return self._template(row)

    def list_prompts(self) -> list[PromptTemplate]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT t.*, v.content, v.content_sha256 FROM prompt_templates t "
                "JOIN prompt_versions v ON v.prompt_id=t.prompt_id AND v.version=t.current_version ORDER BY t.prompt_id"
            ).fetchall()
        return [self._template(row) for row in rows]

    def get_default(self, analysis_type: str) -> PromptTemplate:
        for prompt in self.list_prompts():
            if prompt.analysis_type == analysis_type and prompt.is_default:
                return prompt
        raise FileNotFoundError(f"default prompt not found for analysis_type: {analysis_type}")

    def history(self, prompt_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            current = connection.execute(
                "SELECT current_version FROM prompt_templates WHERE prompt_id=?", (prompt_id,)
            ).fetchone()
            if current is None:
                return []
            rows = connection.execute(
                "SELECT version, content, content_sha256, note, created_at FROM prompt_versions "
                "WHERE prompt_id=? AND version < ? ORDER BY version DESC",
                (prompt_id, current[0]),
            ).fetchall()
        return [{
            "version_id": row["content_sha256"][:12],
            "version": f"v{row['version']}",
            "content": row["content"],
            "sha256": row["content_sha256"],
            "note": row["note"] or "",
            "saved_at": row["created_at"],
        } for row in rows]

    def update(self, prompt_id: str, content: str, note: str = "") -> PromptTemplate:
        if not isinstance(content, str) or not content.strip():
            raise ValueError("prompt content must be non-empty")
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        now = utc_now()
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT current_version, analysis_type FROM prompt_templates WHERE prompt_id=?", (prompt_id,)
            ).fetchone()
            if row is None:
                raise FileNotFoundError(f"prompt not found: {prompt_id}")
            if row["analysis_type"] == "feature_extract":
                validate_feature_prompt_contract(content)
            version = int(row[0]) + 1
            connection.execute(
                "INSERT INTO prompt_versions(prompt_id, version, content, content_sha256, note, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (prompt_id, version, content, digest, note, now),
            )
            connection.execute(
                "UPDATE prompt_templates SET current_version=?, updated_at=? WHERE prompt_id=?",
                (version, now, prompt_id),
            )
        return self.load(prompt_id)
