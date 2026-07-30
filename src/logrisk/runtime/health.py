from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from logrisk.database import Database


def liveness(database: Database) -> dict[str, Any]:
    try:
        with database.connect() as connection:
            connection.execute("SELECT 1").fetchone()
            connection.execute("SELECT 1 FROM schema_migrations LIMIT 1").fetchone()
            connection.execute("SELECT 1 FROM runtime_audit_events LIMIT 1").fetchone()
    except Exception as exc:
        return {"alive": False, "status": "failed", "error_code": "database_unavailable", "error": str(exc)}
    return {"alive": True, "status": "ok", "storage": database.provider, "migrations": True}


def directory_writable(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=".logrisk-ready-", dir=path)
    try:
        os.close(descriptor)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return True
