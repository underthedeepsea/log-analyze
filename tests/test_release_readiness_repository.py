from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.release_readiness.repository import ReleaseReadinessRepository


def _check(check_id: str, status: str, evidence: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "check_id": check_id,
        "title": check_id,
        "status": status,
        "code": check_id + "_code",
        "message": check_id + " message",
        "evidence": evidence or {},
    }


def test_validation_run_is_sanitized_idempotent_and_lists_latest_first(tmp_path) -> None:
    repository = ReleaseReadinessRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    blocked = repository.record_validation(
        target_version="1.30.0",
        idempotency_key="release-130-first",
        status="blocked",
        summary={"blocked": 1, "api_key": "must-not-persist"},
        checks=[_check("database", "passed"), _check("profile", "blocked", {"token": "must-not-persist"})],
    )
    duplicate = repository.record_validation(
        target_version="1.30.0",
        idempotency_key="release-130-first",
        status="passed",
        summary={"blocked": 0},
        checks=[_check("database", "passed")],
    )
    passed = repository.record_validation(
        target_version="1.30.0",
        idempotency_key="release-130-second",
        status="passed",
        summary={"blocked": 0, "warnings": 0},
        checks=[_check("database", "passed")],
    )

    assert duplicate["validation_id"] == blocked["validation_id"]
    assert duplicate["status"] == "blocked"
    assert "api_key" not in blocked["summary"]
    assert "token" not in blocked["checks"][1]["evidence"]
    assert repository.list_history(limit=10)["items"][0]["validation_id"] == passed["validation_id"]


def test_validation_rejects_invalid_status_and_unsanitized_raw_log_fields(tmp_path) -> None:
    repository = ReleaseReadinessRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))

    try:
        repository.record_validation(
            target_version="1.30.0",
            idempotency_key="invalid-status",
            status="running",
            summary={},
            checks=[_check("database", "passed")],
        )
    except ValueError as exc:
        assert "状态" in str(exc)
    else:
        raise AssertionError("无效验证状态必须被拒绝")

    result = repository.record_validation(
        target_version="1.30.0",
        idempotency_key="raw-log-sanitize",
        status="warning",
        summary={"raw_sample": "do-not-store"},
        checks=[_check("runtime", "warning", {"samples": ["do-not-store"]})],
    )
    assert "raw_sample" not in result["summary"]
    assert "samples" not in result["checks"][0]["evidence"]
