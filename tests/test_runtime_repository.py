from __future__ import annotations

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.runtime.repository import RuntimeConflictError, RuntimeRepository


@pytest.fixture
def database(tmp_path):
    return SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")


def test_policy_write_rejects_stale_version(database) -> None:
    repository = RuntimeRepository(database)
    current = repository.save_policy(
        {"retention_days": 30},
        expected_version=0,
        actor="alice",
        request_id="request-1",
    )

    with pytest.raises(RuntimeConflictError):
        repository.save_policy(
            {"retention_days": 14},
            expected_version=0,
            actor="alice",
            request_id="request-2",
        )

    assert current["version"] == 1
    assert repository.get_policy()["policy"] == {"retention_days": 30}


def test_audit_event_drops_credentials_and_raw_log_fields(database) -> None:
    repository = RuntimeRepository(database)
    repository.append_audit(
        "policy.updated",
        "runtime-policy",
        "alice",
        "request-1",
        {"token": "secret", "raw_sample": "line", "retention_days": 30},
    )

    event = repository.list_audits(limit=1)["items"][0]
    assert event["actor"] == "alice"
    assert event["attributes"] == {"retention_days": 30}


def test_maintenance_run_returns_to_caller_after_finish(database) -> None:
    repository = RuntimeRepository(database)
    started = repository.start_maintenance(
        action="retention", mode="dry_run", actor="alice", request_id="request-1"
    )
    completed = repository.finish_maintenance(
        started["run_id"], status="completed", summary={"deleted": 0}
    )

    assert completed["status"] == "completed"
    assert completed["summary"] == {"deleted": 0}
