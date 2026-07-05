from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from logrisk.ai_harness.context_budget import EvidenceBudget


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
    model: str
    display_name: str
    parameter_size: str | None
    context_window_tokens: int | None
    recommended_input_tokens: int | None
    max_output_tokens: int | None
    default_prompt_id: str
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
        return options

    def public_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "enabled": self.enabled,
            "provider": self.provider,
            "model": self.model,
            "display_name": self.display_name,
            "parameter_size": self.parameter_size,
            "context_window_tokens": self.context_window_tokens,
            "recommended_input_tokens": self.recommended_input_tokens,
            "max_output_tokens": self.max_output_tokens,
            "default_prompt_id": self.default_prompt_id,
            "json_reliability": self.json_reliability,
            "reasoning_capacity": self.reasoning_capacity,
            "thinking_enabled": self.thinking.enabled,
            "evidence_budget": self.evidence_budget.__dict__,
            "options": self.build_model_options(),
        }


class ModelProfileRegistry:
    def __init__(self, config_path: str | Path):
        self.config_path = Path(config_path)
        self._profiles: dict[str, ModelProfile] = {}
        self._default_profile_id: str | None = None
        self.reload()

    @property
    def default_profile_id(self) -> str | None:
        return self._default_profile_id

    def reload(self) -> None:
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
            model=str(raw["model"]),
            display_name=str(raw.get("display_name") or raw["model"]),
            parameter_size=raw.get("parameter_size"),
            context_window_tokens=raw.get("context_window_tokens"),
            recommended_input_tokens=raw.get("recommended_input_tokens"),
            max_output_tokens=raw.get("max_output_tokens"),
            default_prompt_id=str(raw["default_prompt_id"]),
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
        payload = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        profiles = payload.setdefault("profiles", {})
        budget = raw.get("evidence_budget") or {}
        profiles[profile_id] = {
            "enabled": bool(raw.get("enabled", True)),
            "provider": str(raw.get("provider") or "ollama"),
            "model": str(raw.get("model") or "").strip(),
            "display_name": str(raw.get("display_name") or profile_id),
            "parameter_size": raw.get("parameter_size"),
            "context_window_tokens": raw.get("context_window_tokens"),
            "recommended_input_tokens": raw.get("recommended_input_tokens"),
            "max_output_tokens": raw.get("max_output_tokens"),
            "default_prompt_id": str(raw.get("default_prompt_id") or "feature_extract_v3_compact_strict_json_en"),
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
            "options": dict(raw.get("options") or {"temperature": 0}),
        }
        if not profiles[profile_id]["model"]:
            raise ValueError("model 不能为空")
        self.config_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
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
