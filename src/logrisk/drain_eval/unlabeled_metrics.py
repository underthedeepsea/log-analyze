from __future__ import annotations

import re
from typing import Any


def evaluate_unlabeled(clusters: list[dict[str, Any]]) -> dict[str, Any]:
    total_logs = sum(max(0, int(item.get("count") or 0)) for item in clusters)
    singleton_clusters = sum(int(item.get("count") or 0) == 1 for item in clusters)
    wildcard_tokens = token_count = 0
    sizes: list[int] = []
    for item in clusters:
        sizes.append(int(item.get("count") or 0))
        tokens = str(item.get("template") or "").split()
        token_count += len(tokens)
        wildcard_tokens += sum(bool(re.fullmatch(r"<[^>]+>", token)) for token in tokens)
    return {
        "cluster_count": len(clusters),
        "total_logs": total_logs,
        "singleton_ratio": round(singleton_clusters / len(clusters), 6) if clusters else 0.0,
        "wildcard_ratio": round(wildcard_tokens / token_count, 6) if token_count else 0.0,
        "cluster_size_distribution": sorted(sizes),
    }
