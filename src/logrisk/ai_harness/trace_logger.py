from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class AITraceLogger:
    def __init__(self, path: str | Path = "state/ai_traces.jsonl", enabled: bool = True):
        self.path = Path(path)
        self.enabled = enabled

    def append(self, trace: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(trace, ensure_ascii=False, sort_keys=True) + "\n")

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        traces = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                traces.append(value)
        return traces

    def list_traces(
        self,
        *,
        job_id: str | None = None,
        trace_id: str | None = None,
        status: str | None = None,
        prompt_id: str | None = None,
        prompt_hash: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        traces = self._read()
        if job_id:
            traces = [item for item in traces if item.get("job_id") == job_id]
        if trace_id:
            traces = [item for item in traces if item.get("trace_id") == trace_id]
        if status:
            traces = [item for item in traces if item.get("status") == status]
        if prompt_id:
            traces = [item for item in traces if item.get("prompt_id") == prompt_id]
        if prompt_hash:
            traces = [item for item in traces if item.get("prompt_hash") == prompt_hash]
        traces.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return traces[: max(1, min(int(limit), 200))]

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        matches = self.list_traces(trace_id=trace_id, limit=1)
        return matches[0] if matches else None

    def summary_today(self, now: str | None = None) -> dict[str, Any]:
        today = (now or datetime.now(timezone.utc).isoformat())[:10]
        traces = [item for item in self._read() if str(item.get("created_at") or "")[:10] == today]
        model_traces = [item for item in traces if item.get("status") != "cache_hit"]
        calls = len(model_traces)
        successes = sum(item.get("status") == "success" for item in model_traces)
        latencies = [int(item.get("latency_ms") or 0) for item in model_traces]
        return {
            "today_calls": calls,
            "cache_hits": len(traces) - calls,
            "success_rate": round(successes / calls, 3) if calls else 0,
            "avg_latency_ms": round(sum(latencies) / calls) if calls else 0,
        }
