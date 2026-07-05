from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any


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
) -> str:
    raw = "\x1f".join([evidence_hash, prompt_hash, provider, model, str(thinking_enabled)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
