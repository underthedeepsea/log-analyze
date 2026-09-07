from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any, Mapping


_SUPPORTED_GENERATION_OPTIONS = frozenset({
    "temperature",
    "top_k",
    "top_p",
    "min_p",
    "typical_p",
    "tfs_z",
    "repeat_penalty",
    "repeat_last_n",
    "num_predict",
    "num_ctx",
    "num_keep",
    "seed",
    "stop",
    "think",
    "structured_output_mode",
})


def safe_generation_options(options: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return only supported, scalar provider generation controls.

    Connection credentials belong to a provider connection, never to a Profile's
    generation options.  Building this payload from known controls prevents a
    newly named credential field from reaching the client, Trace, or cache key.
    """
    result: dict[str, Any] = {}
    for key, value in dict(options or {}).items():
        if key not in _SUPPORTED_GENERATION_OPTIONS:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[key] = value
        elif key == "stop" and isinstance(value, (list, tuple)) and all(
            isinstance(item, str) for item in value
        ):
            result[key] = list(value)
    return result


class AICache:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def _read_locked(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def get(self, signature: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._read_locked().get(signature)
            return copy.deepcopy(value) if isinstance(value, dict) else None

    def set(self, signature: str, value: dict[str, Any]) -> None:
        with self._lock:
            payload = self._read_locked()
            payload[signature] = copy.deepcopy(value)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
            temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, self.path)


def cache_signature(
    evidence_hash: str,
    prompt_hash: str,
    provider: str,
    model: str,
    thinking_enabled: bool | None = None,
    generation_options: Mapping[str, Any] | None = None,
    schema_digest: str | None = None,
) -> str:
    raw = json.dumps({
        "version": 2,
        "evidence_hash": evidence_hash,
        "prompt_hash": prompt_hash,
        "provider": provider,
        "model": model,
        "thinking_enabled": thinking_enabled,
        "generation_options": safe_generation_options(generation_options),
        "schema_digest": schema_digest or "",
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
