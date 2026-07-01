from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


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
