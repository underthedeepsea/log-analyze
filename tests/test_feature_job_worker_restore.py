from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import threading

import pytest

from logrisk.feature_jobs import FeatureJobFileStore, FeatureJobManager


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_airflow_style_container_does_not_interrupt_a_persisted_queued_job(tmp_path) -> None:
    """A separately started worker must execute the persisted job, not mark it interrupted on restore."""
    from logrisk.application import ApplicationConfig, build_application_container

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
    )
    creator = build_application_container(config)
    job_id = creator.feature_jobs.create_job(
        {"summary": {}, "risk_entities": []},
        model="test-model",
    )

    worker = build_application_container(replace(config, interrupt_feature_jobs=False))

    assert worker.feature_jobs.get_job(job_id)["status"] == "queued"


def test_web_container_refreshes_completed_state_written_by_another_worker(tmp_path) -> None:
    from logrisk.application import ApplicationConfig, build_application_container

    config = replace(
        ApplicationConfig.for_test(project_root=PROJECT_ROOT, state_root=tmp_path / "state"),
        feature_jobs_auto_start=False,
        interrupt_feature_jobs=False,
    )
    creator = build_application_container(config)
    job_id = creator.feature_jobs.create_job({"summary": {}, "risk_entities": []}, model="test-model")
    web = build_application_container(config)
    worker = build_application_container(config)
    cached = web.feature_jobs._jobs[job_id]
    condition = cached["condition"]
    worker.feature_jobs.run_job(job_id)

    web.feature_jobs.refresh_from_persistence(job_id)

    assert web.feature_jobs.get_job(job_id)["status"] == "completed"
    assert web.feature_jobs._jobs[job_id] is cached
    assert cached["condition"] is condition


def test_local_worker_pin_survives_refresh_and_cache_pressure(tmp_path, monkeypatch):
    store = FeatureJobFileStore(tmp_path / "jobs")
    manager = FeatureJobManager(persistence=store, auto_start=False, history_cache_bytes=1)
    job_id = manager.create_job({"risk_entities": []}, model="test-model")
    history_id = manager.create_job({"risk_entities": []}, model="test-model")
    entered = threading.Event()
    release = threading.Event()
    errors = []
    original = manager._run_job

    def blocked_run(target, only_entity_id=None):
        entered.set()
        if not release.wait(5):
            raise RuntimeError("test worker timed out")
        original(target, only_entity_id)

    def run():
        try:
            manager.run_job(job_id)
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(manager, "_run_job", blocked_run)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(5)
        active = manager._jobs[job_id]
        condition = active["condition"]
        persisted = store.load_job(job_id)
        persisted["events"].append({"sequence": len(persisted["events"]), "type": "external_event"})
        store.save(persisted)
        manager.refresh_from_persistence(job_id)
        manager.get_job(history_id)
        assert manager._jobs[job_id] is active
        assert active["condition"] is condition
        assert active["events"][-1]["type"] != "external_event"
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert not errors
    assert not manager._active_job_counts
    assert job_id not in manager._jobs
    assert manager.get_job(job_id)["status"] == "completed"


def test_worker_exception_releases_pin_and_allows_external_recovery(tmp_path, monkeypatch):
    store = FeatureJobFileStore(tmp_path / "jobs")
    manager = FeatureJobManager(persistence=store, auto_start=False, history_cache_bytes=1)
    job_id = manager.create_job({"risk_entities": []}, model="test-model")
    original_emit = manager._emit_locked

    def fail_after_start(job, event_type, **payload):
        original_emit(job, event_type, **payload)
        if event_type == "job_started":
            raise RuntimeError("injected worker failure")

    monkeypatch.setattr(manager, "_emit_locked", fail_after_start)
    with pytest.raises(RuntimeError, match="injected worker failure"):
        manager.run_job(job_id)
    assert not manager._active_job_counts
    assert job_id not in manager._jobs
    assert manager.get_job(job_id)["status"] == "running"
    worker = FeatureJobManager(persistence=store, auto_start=False, interrupt_on_restore=False)
    worker.run_job(job_id)
    manager.refresh_from_persistence(job_id)
    assert manager.get_job(job_id)["status"] == "completed"


def test_passively_restored_running_jobs_do_not_exceed_cache_budget(tmp_path):
    store = FeatureJobFileStore(tmp_path / "jobs")
    creator = FeatureJobManager(persistence=store, auto_start=False)
    job_ids = []
    for _ in range(3):
        job_id = creator.create_job({"risk_entities": []}, model="test-model")
        creator._jobs[job_id]["status"] = "running"
        store.save(creator._jobs[job_id])
        job_ids.append(job_id)
    restored = FeatureJobManager(
        persistence=store, auto_start=False, interrupt_on_restore=False, history_cache_bytes=1,
    )
    assert not restored._jobs
    for job_id in job_ids:
        assert restored.get_job(job_id)["status"] == "running"
        assert list(restored._jobs) == [job_id]


def test_passive_waiter_survives_cache_pressure_and_receives_external_completion(tmp_path):
    store = FeatureJobFileStore(tmp_path / "jobs")
    web = FeatureJobManager(
        persistence=store,
        auto_start=False,
        interrupt_on_restore=False,
        history_cache_bytes=1,
    )
    job_id = web.create_job({"risk_entities": []}, model="test-model")
    history_id = web.create_job({"risk_entities": []}, model="test-model")
    worker = FeatureJobManager(
        persistence=store,
        auto_start=False,
        interrupt_on_restore=False,
    )
    cached = web._jobs[job_id]
    condition = cached["condition"]
    cursor = len(cached["events"])
    entered_wait = threading.Event()
    original_wait = condition.wait

    def observed_wait(timeout=None):
        entered_wait.set()
        return original_wait(timeout)

    condition.wait = observed_wait
    result = []
    errors = []

    def wait_for_completion():
        try:
            result.append(web.wait_for_events(job_id, cursor, timeout=1))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=wait_for_completion)
    thread.start()
    assert entered_wait.wait(1)

    worker.run_job(job_id)
    web.get_job(history_id)
    web.refresh_from_persistence(job_id)
    thread.join(2)

    assert not thread.is_alive()
    assert not errors
    assert cached["status"] == "completed"
    assert cached["condition"] is condition
    assert [event["type"] for event in result[0][0]][-1] == "job_completed"
    assert not web._event_waiter_counts


def test_passive_waiter_pin_releases_after_timeout_and_wait_error(tmp_path):
    store = FeatureJobFileStore(tmp_path / "jobs")
    manager = FeatureJobManager(
        persistence=store,
        auto_start=False,
        interrupt_on_restore=False,
        history_cache_bytes=1,
    )
    timeout_id = manager.create_job({"risk_entities": []}, model="test-model")
    timeout_cursor = len(manager._jobs[timeout_id]["events"])

    assert manager.wait_for_events(timeout_id, timeout_cursor, timeout=0.01) == ([], timeout_cursor)
    assert not manager._event_waiter_counts
    assert timeout_id not in manager._jobs

    error_id = manager.create_job({"risk_entities": []}, model="test-model")
    error_cursor = len(manager._jobs[error_id]["events"])

    def fail_wait(timeout=None):
        raise RuntimeError("injected wait cancellation")

    manager._jobs[error_id]["condition"].wait = fail_wait
    with pytest.raises(RuntimeError, match="injected wait cancellation"):
        manager.wait_for_events(error_id, error_cursor, timeout=1)
    assert not manager._event_waiter_counts
    assert error_id not in manager._jobs
