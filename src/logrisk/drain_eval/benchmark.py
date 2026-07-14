from __future__ import annotations

from time import perf_counter
from typing import Any, Callable


def benchmark_records(records: list[dict[str, Any]], runner: Callable[[list[dict[str, Any]]], Any]) -> dict[str, float | int]:
    started = perf_counter()
    runner(records)
    elapsed = max(perf_counter() - started, 0.000001)
    return {
        "record_count": len(records),
        "duration_seconds": round(elapsed, 6),
        "logs_per_second": round(len(records) / elapsed, 3),
    }
