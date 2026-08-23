from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


FORBIDDEN_KEYS = frozenset(
    {
        "samples",
        "raw_sample",
        "raw_log",
        "raw_logs",
        "raw_message",
        "message",
        "api_key",
        "token",
        "password",
        "secret",
        "dsn",
        "authorization",
        "cookie",
    }
)

FEEDBACK_SCHEMA_VERSION = "continuous_learning_feedback_v1"
DATASET_SCHEMA_VERSION = "drain_dataset_revision_v1"


class ContinuousLearningError(ValueError):
    """Raised when a continuous-learning persistence request is invalid."""

    def __init__(self, message: str, *, code: str = "invalid_request", status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def reject_forbidden_keys(value: Any) -> None:
    """Reject sensitive fields recursively instead of silently dropping them."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in FORBIDDEN_KEYS:
                raise ContinuousLearningError("输入包含禁止的敏感字段", code="forbidden_field", status_code=422)
            reject_forbidden_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            reject_forbidden_keys(item)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def content_sha256(records: Any) -> str:
    return hashlib.sha256(canonical_json(records).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FeedbackRecord:
    feedback_id: str
    candidate_id: str
    job_id: str
    outcome: str
    reason_code: str
    note: str
    actor: str
    request_id: str
    idempotency_key: str
    created_at: str
    schema_version: str = FEEDBACK_SCHEMA_VERSION


@dataclass(frozen=True)
class DatasetRevision:
    dataset_id: str
    family_id: str
    revision_number: int
    name: str
    description: str
    split: str
    records: tuple[dict[str, Any], ...]
    content_sha256: str
    parent_dataset_id: str | None
    lifecycle_status: str
    actor: str
    request_id: str
    created_at: str
    updated_at: str
    schema_version: str = DATASET_SCHEMA_VERSION
