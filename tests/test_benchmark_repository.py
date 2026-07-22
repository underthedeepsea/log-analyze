from __future__ import annotations

import pytest

from logrisk.database import SQLiteDatabase


def test_benchmark_repository_persists_suite_run_cases_and_audit(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = BenchmarkRepository(database, clock=lambda: "2026-07-22T00:00:00+00:00")
    suite = repository.create_suite({
        "suite_id": "suite-canonical",
        "name": "Canonical",
        "source_type": "canonical",
        "cases": [{"name": "kernel-error", "input_entity": {"top_templates": []}}],
    })
    run = repository.create_run({
        "run_id": "run-1",
        "suite_id": suite["suite_id"],
        "mode": "fake",
        "idempotency_key": "same-request",
        "snapshot": {"prompt_id": "prompt-a", "model_profile_id": "profile-a"},
    })
    repository.update_run("run-1", status="running", progress_completed=0)
    repository.add_case_result("run-1", {
        "case_id": "kernel-error",
        "passed": True,
        "json_valid": True,
        "schema_valid": True,
        "template_reference_ok": True,
        "duration_ms": 12.5,
        "result": {"features": []},
    })
    completed = repository.update_run(
        "run-1",
        status="completed",
        progress_completed=1,
        metrics={"pass_rate": 1.0},
    )

    restarted = BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    assert restarted.get_suite("suite-canonical")["case_count"] == 1
    assert restarted.get_run("run-1")["status"] == "completed"
    assert restarted.get_run("run-1")["metrics"]["pass_rate"] == 1.0
    assert restarted.list_case_results("run-1")["items"][0]["case_id"] == "kernel-error"
    assert completed["version"] == 3
    assert {item["event_type"] for item in restarted.list_audit_events("run-1")} >= {
        "run_created", "run_status_changed", "case_result_recorded"
    }


def test_run_idempotency_returns_existing_snapshot(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository

    repository = BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    repository.create_suite({"suite_id": "suite-1", "name": "Suite", "source_type": "custom", "cases": []})
    first = repository.create_run({
        "run_id": "run-first", "suite_id": "suite-1", "mode": "fake",
        "idempotency_key": "request-1", "snapshot": {},
    })
    second = repository.create_run({
        "run_id": "run-second", "suite_id": "suite-1", "mode": "fake",
        "idempotency_key": "request-1", "snapshot": {"changed": True},
    })

    assert second["run_id"] == first["run_id"]
    assert second["snapshot"] == {}


def test_repository_rejects_unknown_suite_and_invalid_pagination(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkError, BenchmarkRepository

    repository = BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    with pytest.raises(BenchmarkError) as missing:
        repository.create_run({
            "run_id": "run-1", "suite_id": "missing", "mode": "fake",
            "idempotency_key": "request-1", "snapshot": {},
        })
    with pytest.raises(BenchmarkError) as invalid:
        repository.list_runs(page=0, page_size=10)

    assert missing.value.status_code == 404
    assert invalid.value.status_code == 400


def test_repository_rejects_raw_log_evidence_in_suite(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkError, BenchmarkRepository

    repository = BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    with pytest.raises(BenchmarkError) as error:
        repository.create_suite({
            "suite_id": "unsafe-suite",
            "name": "Unsafe",
            "source_type": "custom",
            "cases": [{"input_entity": {"samples": ["secret raw log"]}}],
        })

    assert error.value.status_code == 422
    assert error.value.code == "raw_evidence_forbidden"


def test_repository_registers_artifact_metadata_without_file_content(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository

    repository = BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    repository.create_suite({"suite_id": "suite-1", "name": "Suite", "source_type": "custom", "cases": []})
    repository.create_run({
        "run_id": "run-1", "suite_id": "suite-1", "mode": "fake",
        "idempotency_key": "request-1", "snapshot": {},
    })

    artifact = repository.add_artifact("run-1", {
        "artifact_type": "json_report",
        "path": "output/benchmarks/run-1/report.json",
        "size_bytes": 42,
        "sha256": "a" * 64,
    })

    assert artifact["artifact_type"] == "json_report"
    assert repository.list_artifacts("run-1")["items"] == [artifact]
