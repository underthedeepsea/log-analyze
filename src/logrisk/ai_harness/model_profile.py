from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import yaml

from logrisk.ai_harness.context_budget import EvidenceBudget
from logrisk.database import SQLiteDatabase


@dataclass(frozen=True)
class ThinkingConfig:
    enabled: bool = False
    provider_option_name: str = "think"
    unsupported_behavior: str = "ignore"


@dataclass(frozen=True)
class ModelProfile:
    profile_id: str
    enabled: bool
    provider: str
    connection_id: str
    model: str
    display_name: str
    parameter_size: str | None
    context_window_tokens: int | None
    recommended_input_tokens: int | None
    max_output_tokens: int | None
    default_prompt_id: str
    structured_output_mode: str
    json_reliability: str | None
    reasoning_capacity: str | None
    thinking: ThinkingConfig
    evidence_budget: EvidenceBudget
    options: dict[str, Any] = field(default_factory=dict)

    def build_model_options(self) -> dict[str, Any]:
        options = dict(self.options or {})
        if self.thinking.provider_option_name:
            options[self.thinking.provider_option_name] = bool(self.thinking.enabled)
        if self.max_output_tokens is not None:
            options.setdefault("num_predict", self.max_output_tokens)
        options["structured_output_mode"] = self.structured_output_mode
        return options

    def public_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "enabled": self.enabled,
            "provider": self.provider,
            "connection_id": self.connection_id,
            "model": self.model,
            "display_name": self.display_name,
            "parameter_size": self.parameter_size,
            "context_window_tokens": self.context_window_tokens,
            "recommended_input_tokens": self.recommended_input_tokens,
            "max_output_tokens": self.max_output_tokens,
            "default_prompt_id": self.default_prompt_id,
            "structured_output_mode": self.structured_output_mode,
            "json_reliability": self.json_reliability,
            "reasoning_capacity": self.reasoning_capacity,
            "thinking_enabled": self.thinking.enabled,
            "evidence_budget": self.evidence_budget.__dict__,
            "options": dict(self.options or {}),
            "runtime_options": self.build_model_options(),
        }


class ModelProfileRegistry:
    def __init__(self, config_path: str | Path, *, database: SQLiteDatabase | None = None):
        self.config_path = Path(config_path)
        self.database = database
        self._profiles: dict[str, ModelProfile] = {}
        self._default_profile_id: str | None = None
        if self.database:
            self._seed_database()
        self.reload()

    @property
    def default_profile_id(self) -> str | None:
        return self._default_profile_id

    def reload(self) -> None:
        if self.database:
            self._reload_database()
            return
        data = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        self._default_profile_id = data.get("default_profile_id")
        self._profiles = {
            profile_id: self._parse(profile_id, raw or {})
            for profile_id, raw in (data.get("profiles") or {}).items()
        }

    def _parse(self, profile_id: str, raw: dict[str, Any]) -> ModelProfile:
        thinking = raw.get("thinking") or {}
        budget = raw.get("evidence_budget") or {}
        return ModelProfile(
            profile_id=profile_id,
            enabled=bool(raw.get("enabled", True)),
            provider=str(raw["provider"]),
            connection_id=str(raw.get("connection_id") or ("ollama-local" if raw.get("provider") == "ollama" else "")),
            model=str(raw["model"]),
            display_name=str(raw.get("display_name") or raw["model"]),
            parameter_size=raw.get("parameter_size"),
            context_window_tokens=raw.get("context_window_tokens"),
            recommended_input_tokens=raw.get("recommended_input_tokens"),
            max_output_tokens=raw.get("max_output_tokens"),
            default_prompt_id=str(raw["default_prompt_id"]),
            structured_output_mode=str(raw.get("structured_output_mode") or "json_schema"),
            json_reliability=raw.get("json_reliability"),
            reasoning_capacity=raw.get("reasoning_capacity"),
            thinking=ThinkingConfig(
                enabled=bool(thinking.get("enabled", False)),
                provider_option_name=str(thinking.get("provider_option_name", "think")),
                unsupported_behavior=str(thinking.get("unsupported_behavior", "ignore")),
            ),
            evidence_budget=EvidenceBudget(
                max_templates=int(budget.get("max_templates", 10)),
                max_template_chars=int(budget.get("max_template_chars", 300)),
                max_affected_entities=int(budget.get("max_affected_entities", 50)),
                max_evidence_chars=int(budget.get("max_evidence_chars", 12000)),
                recommended_input_tokens=raw.get("recommended_input_tokens"),
                max_output_tokens=raw.get("max_output_tokens"),
            ),
            options=dict(raw.get("options") or {}),
        )

    def save(self, raw: dict[str, Any]) -> ModelProfile:
        profile_id = str(raw.get("profile_id") or "").strip()
        if not profile_id.replace("_", "").replace("-", "").isalnum():
            raise ValueError("profile_id 只能包含字母、数字、下划线和短横线")
        if self.database:
            return self._save_database(profile_id, raw)
        payload = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        profiles = payload.setdefault("profiles", {})
        budget = raw.get("evidence_budget") or {}
        profiles[profile_id] = {
            "enabled": bool(raw.get("enabled", True)),
            "provider": str(raw.get("provider") or "ollama"),
            "connection_id": str(raw.get("connection_id") or "ollama-local"),
            "model": str(raw.get("model") or "").strip(),
            "display_name": str(raw.get("display_name") or profile_id),
            "parameter_size": raw.get("parameter_size"),
            "context_window_tokens": raw.get("context_window_tokens"),
            "recommended_input_tokens": raw.get("recommended_input_tokens"),
            "max_output_tokens": raw.get("max_output_tokens"),
            "default_prompt_id": str(raw.get("default_prompt_id") or "feature_extract_v3_compact_strict_json_en"),
            "structured_output_mode": str(raw.get("structured_output_mode") or "json_schema"),
            "json_reliability": raw.get("json_reliability"),
            "reasoning_capacity": raw.get("reasoning_capacity"),
            "thinking": {
                "enabled": bool(raw.get("thinking_enabled", False)),
                "provider_option_name": "think",
                "unsupported_behavior": "ignore",
            },
            "evidence_budget": {
                "max_templates": int(budget.get("max_templates", 10)),
                "max_template_chars": int(budget.get("max_template_chars", 300)),
                "max_affected_entities": int(budget.get("max_affected_entities", 50)),
                "max_evidence_chars": int(budget.get("max_evidence_chars", 12000)),
            },
            "options": self._editable_options(raw.get("options")),
        }
        if not profiles[profile_id]["model"]:
            raise ValueError("model 不能为空")
        self.config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        self.reload()
        return self.get(profile_id)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _editable_options(raw: Any) -> dict[str, Any]:
        options = dict(raw or {"temperature": 0})
        for derived in ("think", "num_predict", "structured_output_mode"):
            options.pop(derived, None)
        return options

    def _seed_database(self) -> None:
        assert self.database is not None
        with self.database.connect() as connection:
            if connection.execute("SELECT 1 FROM model_profiles LIMIT 1").fetchone():
                return
        data = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        default_id = data.get("default_profile_id")
        with self.database.transaction() as connection:
            now = self._now()
            for profile_id, raw in (data.get("profiles") or {}).items():
                raw = dict(raw or {})
                raw["profile_id"] = profile_id
                raw.setdefault("connection_id", "ollama-local" if raw.get("provider", "ollama") == "ollama" else "")
                raw.setdefault("structured_output_mode", "json_schema")
                connection.execute(
                    "INSERT INTO model_profiles(profile_id, connection_id, model, display_name, enabled, "
                    "structured_output_mode, profile_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        profile_id,
                        raw["connection_id"],
                        str(raw.get("model") or ""),
                        str(raw.get("display_name") or raw.get("model") or profile_id),
                        bool(raw.get("enabled", True)),
                        raw["structured_output_mode"],
                        json.dumps(raw, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
            if default_id:
                connection.execute(
                    "INSERT INTO app_settings(setting_key, value_json, updated_at) VALUES ('default_model_profile_id', ?, ?) "
                    "ON CONFLICT(setting_key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
                    (json.dumps(default_id), now),
                )

    def _reload_database(self) -> None:
        assert self.database is not None
        with self.database.connect() as connection:
            setting = connection.execute(
                "SELECT value_json FROM app_settings WHERE setting_key='default_model_profile_id'"
            ).fetchone()
            rows = connection.execute("SELECT profile_json FROM model_profiles ORDER BY profile_id").fetchall()
        self._default_profile_id = json.loads(setting[0]) if setting else None
        self._profiles = {}
        for row in rows:
            raw = json.loads(row[0])
            profile_id = str(raw["profile_id"])
            self._profiles[profile_id] = self._parse(profile_id, raw)

    def _save_database(self, profile_id: str, raw: dict[str, Any]) -> ModelProfile:
        assert self.database is not None
        payload = dict(raw)
        payload["profile_id"] = profile_id
        payload.setdefault("enabled", True)
        payload.setdefault("provider", "ollama")
        payload.setdefault("connection_id", "ollama-local")
        payload.setdefault("display_name", profile_id)
        payload.setdefault("default_prompt_id", "feature_extract_v3_compact_strict_json_en")
        payload.setdefault("structured_output_mode", "json_schema")
        payload["thinking"] = {
            "enabled": bool(payload.get("thinking_enabled", False)),
            "provider_option_name": "think",
            "unsupported_behavior": "ignore",
        }
        payload.setdefault("evidence_budget", {})
        payload["options"] = self._editable_options(payload.get("options"))
        if not str(payload.get("model") or "").strip():
            raise ValueError("model 不能为空")
        if payload["structured_output_mode"] not in {"json_schema", "json_object", "prompt_only"}:
            raise ValueError("structured_output_mode 无效")
        now = self._now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO model_profiles(profile_id, connection_id, model, display_name, enabled, "
                "structured_output_mode, profile_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(profile_id) DO UPDATE SET connection_id=excluded.connection_id, model=excluded.model, "
                "display_name=excluded.display_name, enabled=excluded.enabled, structured_output_mode=excluded.structured_output_mode, "
                "profile_json=excluded.profile_json, updated_at=excluded.updated_at",
                (
                    profile_id,
                    payload["connection_id"],
                    str(payload["model"]).strip(),
                    str(payload["display_name"]),
                    bool(payload["enabled"]),
                    payload["structured_output_mode"],
                    json.dumps(payload, ensure_ascii=False),
                    now,
                    now,
                ),
            )
        self.reload()
        return self.get(profile_id)

    def get(self, profile_id: str | None = None) -> ModelProfile:
        resolved = profile_id or self._default_profile_id
        if not resolved:
            raise KeyError("No model profile id provided and no default_profile_id configured")
        profile = self._profiles[resolved]
        if not profile.enabled:
            raise ValueError(f"Model profile is disabled: {resolved}")
        return profile

    def list_enabled(self) -> list[ModelProfile]:
        return [profile for profile in self._profiles.values() if profile.enabled]

    def from_snapshot(self, raw: dict[str, Any]) -> ModelProfile:
        normalized = dict(raw)
        normalized.setdefault("provider", "ollama")
        normalized.setdefault("connection_id", "ollama-local")
        normalized.setdefault("enabled", True)
        normalized.setdefault("default_prompt_id", "feature_extract_v3_compact_strict_json_en")
        normalized.setdefault("structured_output_mode", "json_schema")
        normalized["thinking"] = {
            "enabled": bool(normalized.get("thinking_enabled", False)),
            "provider_option_name": "think",
            "unsupported_behavior": "ignore",
        }
        return self._parse(str(normalized["profile_id"]), normalized)
