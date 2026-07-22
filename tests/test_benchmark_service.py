from __future__ import annotations

import threading

import pytest

from logrisk.database import SQLiteDatabase


CASES = [
    {
        "name": "kernel-error",
        "input_entity": {
            "entity_type": "node",
            "entity_id": "node-a",
            "top_templates": [{
                "template_hash": "hash-1", "component": "kernel", "category": "kernel_error",
                "severity": "ERROR", "template": "kernel registration failed",
            }],
        },
        "expected": {"must_include_feature_type": ["kernel_error"], "must_reference_hashes": ["hash-1"]},
    },
    {
        "name": "normal-info",
        "input_entity": {
            "entity_type": "node", "entity_id": "node-a",
            "top_templates": [{
                "template_hash": "hash-2", "component": "kernel", "category": "normal",
                "severity": "INFO", "template": "driver registered",
            }],
        },
        "expected": {"expect_empty_features": True},
    },
]


def service(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    return BenchmarkService(BenchmarkRepository(database), canonical_cases=CASES)


def test_fake_run_locks_snapshot_and_completes_with_case_results(tmp_path):
    benchmark = service(tmp_path)
    suite = benchmark.list_suites()["items"][0]
    created = benchmark.create_run({
        "suite_id": suite["suite_id"],
        "mode": "fake",
        "prompt_id": "prompt-a",
        "model_profile_id": "profile-a",
        "idempotency_key": "request-1",
    })

    result = benchmark.execute_run(created["run_id"])
    detail = benchmark.get_run(created["run_id"])

    assert result["status"] == "completed"
    assert result["progress"] == {"completed": 2, "total": 2}
    assert result["metrics"]["pass_rate"] == 1.0
    assert detail["run"]["snapshot"]["prompt_id"] == "prompt-a"
    assert len(detail["cases"]["items"]) == 2


def test_real_run_requires_explicit_budget_confirmation(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkError

    benchmark = service(tmp_path)
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]

    with pytest.raises(BenchmarkError) as error:
        benchmark.create_run({
            "suite_id": suite_id,
            "mode": "real",
            "prompt_id": "prompt-a",
            "model_profile_id": "profile-a",
            "case_limit": 2,
            "timeout_seconds": 30,
            "retry_count": 1,
            "budget_units": 1000,
            "confirmed": False,
        })

    assert error.value.status_code == 422
    assert "确认" in str(error.value)


def test_gate_comparison_is_persisted_without_changing_assets(tmp_path):
    benchmark = service(tmp_path)
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]
    baseline = benchmark.create_run({"suite_id": suite_id, "mode": "fake", "idempotency_key": "base"})
    candidate = benchmark.create_run({"suite_id": suite_id, "mode": "fake", "idempotency_key": "candidate"})
    benchmark.execute_run(baseline["run_id"])
    benchmark.execute_run(candidate["run_id"])

    gate = benchmark.evaluate_gate({
        "baseline_run_id": baseline["run_id"],
        "candidate_run_id": candidate["run_id"],
        "thresholds": {"min_pass_rate": 0.8},
        "operator": "reviewer-a",
    })

    assert gate["decision"] == "passed"
    assert gate["schema_version"] == "benchmark_gate_v1"
    assert benchmark.overview()["gate_counts"]["passed"] == 1


def test_cancelled_real_run_is_not_overwritten_by_late_case_result(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    started = threading.Event()
    release = threading.Event()

    def blocking_extractor(_entity, **_kwargs):
        started.set()
        release.wait(timeout=2)
        return []

    benchmark = BenchmarkService(
        BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")),
        canonical_cases=CASES,
        real_extractor=blocking_extractor,
    )
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]
    run = benchmark.create_run({
        "suite_id": suite_id,
        "mode": "real",
        "prompt_id": "prompt-a",
        "model_profile_id": "profile-a",
        "timeout_seconds": 30,
        "retry_count": 1,
        "budget_units": 1000,
        "confirmed": True,
    })
    worker = threading.Thread(target=benchmark.execute_run, args=(run["run_id"],))
    worker.start()
    assert started.wait(timeout=2)

    benchmark.cancel_run(run["run_id"])
    release.set()
    worker.join(timeout=2)

    assert benchmark.get_run(run["run_id"])["run"]["status"] == "cancelled"


def test_overview_exposes_unified_source_asset_counts(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    benchmark = BenchmarkService(
        BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")),
        canonical_cases=CASES,
        asset_inventory=lambda: {"ai_traces": 3, "model_profiles": 2, "drain_eval_runs": 1},
    )

    assert benchmark.overview()["source_assets"] == {
        "ai_traces": 3,
        "model_profiles": 2,
        "drain_eval_runs": 1,
    }


def test_real_run_uses_immutable_prompt_profile_and_connection_snapshots(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    observed = []
    profile = {"profile_id": "profile-a", "connection_id": "connection-a", "model": "model-a"}
    connection = {"connection_id": "connection-a", "provider": "ollama", "base_url": "http://127.0.0.1:11434", "enabled": True, "api_key": "must-not-persist"}
    prompt = {"prompt_id": "prompt-a", "content": "original", "sha256": "abc123", "version": "v1"}

    def extractor(_entity, **kwargs):
        observed.append(kwargs)
        return []

    benchmark = BenchmarkService(
        BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")),
        canonical_cases=CASES,
        real_extractor=extractor,
        profile_resolver=lambda _profile_id: dict(profile),
        connection_resolver=lambda _connection_id: dict(connection),
        prompt_resolver=lambda _prompt_id: dict(prompt),
    )
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]
    run = benchmark.create_run({
        "suite_id": suite_id,
        "mode": "real",
        "prompt_id": "prompt-a",
        "model_profile_id": "profile-a",
        "case_limit": 1,
        "timeout_seconds": 30,
        "retry_count": 0,
        "budget_units": 10,
        "confirmed": True,
    })
    profile["model"] = "changed-model"
    connection["base_url"] = "https://changed.invalid"
    prompt["content"] = "changed"

    benchmark.execute_run(run["run_id"])

    assert observed[0]["profile_snapshot"]["model"] == "model-a"
    assert observed[0]["connection_snapshot"]["base_url"] == "http://127.0.0.1:11434"
    assert "api_key" not in observed[0]["connection_snapshot"]
    assert observed[0]["prompt_snapshot"]["content"] == "original"


def test_real_run_retries_same_extractor_within_locked_retry_budget(tmp_path):
    from logrisk.ai_eval.runner import mock_extractor
    from logrisk.benchmark_center.repository import BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    calls = []

    def flaky_extractor(entity, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("temporary provider error")
        return mock_extractor(entity)

    benchmark = BenchmarkService(
        BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")),
        canonical_cases=CASES,
        real_extractor=flaky_extractor,
    )
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]
    run = benchmark.create_run({
        "suite_id": suite_id,
        "mode": "real",
        "prompt_id": "prompt-a",
        "model_profile_id": "profile-a",
        "case_limit": 1,
        "timeout_seconds": 30,
        "retry_count": 1,
        "budget_units": 2,
        "confirmed": True,
    })

    result = benchmark.execute_run(run["run_id"])

    assert len(calls) == 2
    assert result["metrics"]["pass_rate"] == 1.0


def test_real_run_rejects_disabled_locked_connection(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkError, BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    benchmark = BenchmarkService(
        BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")),
        canonical_cases=CASES,
        real_extractor=lambda _entity, **_kwargs: [],
        profile_resolver=lambda profile_id: {"profile_id": profile_id, "connection_id": "connection-a", "model": "model-a"},
        connection_resolver=lambda connection_id: {"connection_id": connection_id, "provider": "ollama", "enabled": False},
        prompt_resolver=lambda prompt_id: {"prompt_id": prompt_id, "content": "prompt", "sha256": "a" * 64, "path": "database:test"},
    )
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]

    with pytest.raises(BenchmarkError) as error:
        benchmark.create_run({
            "suite_id": suite_id,
            "mode": "real",
            "prompt_id": "prompt-a",
            "model_profile_id": "profile-a",
            "case_limit": 1,
            "timeout_seconds": 30,
            "retry_count": 0,
            "budget_units": 1,
            "confirmed": True,
        })

    assert error.value.code == "connection_unavailable"


def test_accepted_run_records_executor_setup_failure_instead_of_staying_pending(tmp_path):
    from logrisk.benchmark_center.repository import BenchmarkRepository
    from logrisk.benchmark_center.service import BenchmarkService

    benchmark = BenchmarkService(
        BenchmarkRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")),
        canonical_cases=CASES,
    )
    suite_id = benchmark.list_suites()["items"][0]["suite_id"]
    run = benchmark.create_run({
        "suite_id": suite_id,
        "mode": "real",
        "prompt_id": "prompt-a",
        "model_profile_id": "profile-a",
        "case_limit": 1,
        "timeout_seconds": 30,
        "retry_count": 0,
        "budget_units": 1,
        "confirmed": True,
    })

    result = benchmark.execute_run(run["run_id"])

    assert result["status"] == "failed"
    assert "未配置真实模型评测执行器" in result["error"]
