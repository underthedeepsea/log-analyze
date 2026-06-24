from __future__ import annotations

import json
import os
import threading
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict


class ProcessingMetricsError(RuntimeError):
    """Raised when persisted processing counters are invalid or unavailable."""


_PROCESS_LOCK = threading.RLock()


class ProcessingMetricsStore:
    def __init__(
        self,
        path: str | Path,
        today: Callable[[], date] = date.today,
    ) -> None:
        self.path = Path(path)
        self.today = today

    def _read_locked(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": "1.0", "days": {}}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProcessingMetricsError(f"处理指标无法读取: {exc}") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "1.0"
            or not isinstance(payload.get("days"), dict)
            or not all(isinstance(value, int) and value >= 0 for value in payload["days"].values())
        ):
            raise ProcessingMetricsError("处理指标状态结构无效")
        return payload

    def _write_locked(self, payload: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(temporary, self.path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise ProcessingMetricsError(f"处理指标写入失败: {exc}") from exc

    def today_llm_logs(self) -> int:
        with _PROCESS_LOCK:
            payload = self._read_locked()
            return int(payload["days"].get(self.today().isoformat(), 0))

    def add_llm_logs(self, count: int) -> int:
        value = int(count)
        if value < 0:
            raise ProcessingMetricsError("LLM 关联日志量不能为负数")
        with _PROCESS_LOCK:
            payload = self._read_locked()
            key = self.today().isoformat()
            payload["days"][key] = int(payload["days"].get(key, 0)) + value
            self._write_locked(payload)
            return payload["days"][key]
