from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EvidenceBudget:
    max_templates: int = 10
    max_template_chars: int = 300
    max_affected_entities: int = 50
    max_evidence_chars: int = 12000
    recommended_input_tokens: int | None = None
    max_output_tokens: int | None = None


@dataclass(frozen=True)
class EvidenceBuildMeta:
    model_profile_id: str | None
    max_templates: int
    max_template_chars: int
    max_affected_entities: int
    max_evidence_chars: int
    original_template_count: int
    kept_template_count: int
    original_affected_entity_count: int
    kept_affected_entity_count: int
    evidence_chars: int
    truncated: bool
    truncation_reason: str | None
    estimated_input_tokens: int | None = None


def estimate_tokens_from_chars(char_count: int) -> int:
    return max(1, int(char_count / 1.8))
