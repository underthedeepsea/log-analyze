from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from logrisk.database import Database


_PROTECTED_ARTIFACT_TYPES = {"export", "raw", "raw_log", "raw_source", "source", "upload"}
_PROTECTED_FILE_NAMES = {"database_connection.json", "logrisk.sqlite3"}


def parse_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def allowed_path(path: str | Path, roots: Iterable[Path]) -> Path | None:
    candidate = Path(path).expanduser().resolve(strict=False)
    for root in roots:
        safe_root = root.resolve(strict=False)
        try:
            candidate.relative_to(safe_root)
            return candidate
        except ValueError:
            continue
    return None


def protected_runtime_path(path: Path) -> bool:
    name = path.name.lower()
    return name in _PROTECTED_FILE_NAMES or name.endswith((".sqlite3", ".sqlite3-wal", ".sqlite3-shm", "-wal", "-shm"))


def retention_candidates(
    database: Database,
    *,
    roots: Iterable[Path],
    completed_days: int,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=completed_days)
    with database.connect() as connection:
        rows = connection.execute(
            "SELECT artifact_id, owner_type, owner_id, artifact_type, path, size_bytes, created_at FROM artifacts ORDER BY created_at, artifact_id"
        ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        artifact_type = str(row["artifact_type"])
        if artifact_type.lower() in _PROTECTED_ARTIFACT_TYPES:
            continue
        created_at = parse_timestamp(str(row["created_at"]))
        if created_at is None or created_at >= cutoff:
            continue
        path = allowed_path(row["path"], roots)
        if path is None:
            continue
        if protected_runtime_path(path):
            continue
        if _owner_active(database, str(row["owner_type"]), str(row["owner_id"])):
            continue
        items.append(
            {
                "artifact_id": str(row["artifact_id"]),
                "path": str(path),
                "size_bytes": int(row["size_bytes"] or 0),
                "artifact_type": artifact_type,
                "created_at": str(row["created_at"]),
            }
        )
    return items


def _owner_active(database: Database, owner_type: str, owner_id: str) -> bool:
    tables = {
        "feature_job": ("feature_jobs", "job_id"),
        "input_job": ("input_jobs", "input_job_id"),
        "streaming": ("streaming_tasks", "task_id"),
        "benchmark": ("benchmark_runs", "run_id"),
        "replay": ("replay_runs", "replay_id"),
    }
    selected = tables.get(owner_type)
    if selected is None:
        return False
    table, column = selected
    with database.connect() as connection:
        row = connection.execute(f"SELECT status FROM {table} WHERE {column}=?", (owner_id,)).fetchone()
    return bool(row and str(row["status"]) in {"queued", "pending", "running"})


def delete_candidates(candidates: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    deleted_files = 0
    deleted_bytes = 0
    for item in candidates:
        path = Path(str(item["path"]))
        if not path.is_file():
            continue
        size = path.stat().st_size
        path.unlink()
        deleted_files += 1
        deleted_bytes += size
    return {"deleted_files": deleted_files, "deleted_bytes": deleted_bytes}
