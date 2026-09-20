from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import Future, wait as futures_wait
from pathlib import Path

import pytest

import logrisk.large_file_pipeline as large_file_pipeline
import logrisk.streaming_drain_pipeline as streaming_drain_pipeline
from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource
from logrisk.streaming_state import StreamingTaskBusyError
from logrisk.streaming_state import StreamingStateRepository


class InlineExecutor:
    instances: list["InlineExecutor"] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.submit_count = 0
        self.shutdown_calls: list[tuple[bool, bool]] = []
        self.__class__.instances.append(self)

    def submit(self, function, /, *args, **kwargs):
        self.submit_count += 1
        future = Future()
        try:
            future.set_result(function(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return future

    def shutdown(self, *, wait=True, cancel_futures=False):
        self.shutdown_calls.append((wait, cancel_futures))


def _write_source(path: Path, nodes: list[str]) -> None:
    path.write_text(
        "".join(
            json.dumps({
                "timestamp": f"2026-09-16T00:00:{index:02d}+00:00",
                "node": node,
                "component": "kernel",
                "severity": "ERROR",
                "message": f"failure {index}",
            }) + "\n"
            for index, node in enumerate(nodes)
        ),
        encoding="utf-8",
    )


def _run_streaming(
    tmp_path: Path,
    nodes: list[str],
    *,
    progress_callback=None,
) -> tuple[dict, StreamingStateRepository]:
    source = tmp_path / "events.jsonl"
    _write_source(source, nodes)
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    result = large_file_pipeline.run_large_file_pipeline(
        input_job_id="executor-lifecycle",
        input_path=source,
        filename=source.name,
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=2,
        max_drain_workers=2,
        streaming_repository=repository,
        stream_batch_records=2,
        progress_callback=progress_callback,
    )
    return result, repository


def test_logical_task_reuses_one_executor_across_batches_and_shuts_it_down(tmp_path, monkeypatch):
    InlineExecutor.instances = []
    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", InlineExecutor)

    result, repository = _run_streaming(tmp_path, ["node-a", "node-b", "node-a", "node-b"])

    assert result["summary"]["streaming_windows_committed"] == 2
    assert len(repository.list_commits(result["summary"]["streaming_task_id"])) == 2
    assert len(InlineExecutor.instances) == 1
    executor = InlineExecutor.instances[0]
    assert executor.submit_count == 4
    assert executor.shutdown_calls == [(True, True)]


def test_spawn_executor_preserves_multi_batch_result_and_commit_order(tmp_path):
    result, repository = _run_streaming(tmp_path, ["node-a", "node-b", "node-a", "node-b"])

    assert result["summary"]["total_raw_logs"] == 4
    assert result["summary"]["streaming_windows_committed"] == 2
    assert result["summary"]["drain3_parallel"] is True
    assert result["summary"]["drain3_worker_count"] == 2
    task_id = result["summary"]["streaming_task_id"]
    assert repository.get_task(task_id)["status"] == "completed"
    assert len(repository.list_commits(task_id)) == 2


def test_single_partition_batches_never_create_or_submit_to_process_pool(tmp_path, monkeypatch):
    creations = 0

    def reject_executor_creation(**kwargs):
        nonlocal creations
        creations += 1
        raise AssertionError("single-partition path must remain serial")

    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", reject_executor_creation)

    result, _ = _run_streaming(tmp_path, ["node-a", "node-a", "node-a", "node-a"])

    assert result["summary"]["drain3_parallel"] is False
    assert creations == 0


def test_executor_setup_failure_marks_task_failed(tmp_path, monkeypatch):
    def fail_executor_setup(**kwargs):
        raise RuntimeError("injected executor setup failure")

    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", fail_executor_setup)

    with pytest.raises(RuntimeError, match="injected executor setup failure"):
        _run_streaming(tmp_path, ["node-a", "node-b"])

    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    task = repository.list_tasks()[0]
    assert task["status"] == "failed"
    assert "executor setup failure" in task["error"]


def test_mining_failure_closes_task_executor_and_cancels_pending_work(tmp_path, monkeypatch):
    InlineExecutor.instances = []
    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", InlineExecutor)

    def fail_partition(*args, **kwargs):
        raise RuntimeError("injected partition failure")

    monkeypatch.setattr(streaming_drain_pipeline, "mine_partition_file", fail_partition)

    with pytest.raises(RuntimeError, match="injected partition failure"):
        _run_streaming(tmp_path, ["node-a", "node-b"])

    assert len(InlineExecutor.instances) == 1
    assert InlineExecutor.instances[0].shutdown_calls == [(True, True)]
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    assert repository.list_tasks()[0]["status"] == "failed"


@pytest.mark.parametrize(
    "interruption",
    [
        KeyboardInterrupt("operator interrupt"),
        asyncio.CancelledError(),
        SystemExit("service exit"),
        GeneratorExit(),
        FutureCancelledError(),
    ],
    ids=["keyboard", "asyncio-cancel", "system-exit", "generator-exit", "future-cancel"],
)
def test_interruption_closes_pool_preserves_exception_and_releases_claim(tmp_path, monkeypatch, interruption):
    InlineExecutor.instances = []
    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", InlineExecutor)

    def interrupt_partition(*args, **kwargs):
        raise interruption

    monkeypatch.setattr(streaming_drain_pipeline, "mine_partition_file", interrupt_partition)

    with pytest.raises(type(interruption)) as caught:
        _run_streaming(tmp_path, ["node-a", "node-b"])

    assert caught.value is interruption
    assert InlineExecutor.instances[0].shutdown_calls == [(True, True)]
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    task = repository.list_tasks()[0]
    assert task["status"] == "interrupted"
    assert repository.claim_task(task["task_id"])["status"] == "running"


def test_business_error_wins_when_executor_shutdown_also_fails(tmp_path, monkeypatch):
    class BrokenShutdownExecutor(InlineExecutor):
        def shutdown(self, *, wait=True, cancel_futures=False):
            super().shutdown(wait=wait, cancel_futures=cancel_futures)
            raise RuntimeError("injected shutdown failure")

    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", BrokenShutdownExecutor)

    original = RuntimeError("injected mining failure")

    def fail_partition(*args, **kwargs):
        raise original

    monkeypatch.setattr(streaming_drain_pipeline, "mine_partition_file", fail_partition)

    with pytest.raises(RuntimeError, match="mining failure") as caught:
        _run_streaming(tmp_path, ["node-a", "node-b"])

    assert caught.value is original
    task = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")).list_tasks()[0]
    assert task["status"] == "failed"
    assert "执行器清理失败" in task["error"]


def test_shutdown_failure_after_success_marks_task_failed(tmp_path, monkeypatch):
    class BrokenShutdownExecutor(InlineExecutor):
        def shutdown(self, *, wait=True, cancel_futures=False):
            super().shutdown(wait=wait, cancel_futures=cancel_futures)
            raise RuntimeError("injected shutdown failure")

    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", BrokenShutdownExecutor)

    with pytest.raises(RuntimeError, match="shutdown failure"):
        _run_streaming(tmp_path, ["node-a", "node-b"])

    task = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")).list_tasks()[0]
    assert task["status"] == "failed"


def test_outer_exception_context_does_not_hide_current_shutdown_failure(tmp_path, monkeypatch):
    class BrokenShutdownExecutor(InlineExecutor):
        def shutdown(self, *, wait=True, cancel_futures=False):
            super().shutdown(wait=wait, cancel_futures=cancel_futures)
            raise RuntimeError("current shutdown failure")

    monkeypatch.setattr(large_file_pipeline, "ProcessPoolExecutor", BrokenShutdownExecutor)

    try:
        raise ValueError("earlier handled error")
    except ValueError:
        with pytest.raises(RuntimeError, match="current shutdown failure"):
            _run_streaming(tmp_path, ["node-a", "node-b"])

    task = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")).list_tasks()[0]
    assert task["status"] == "failed"


def test_completed_notification_failure_does_not_downgrade_task(tmp_path):
    def progress(payload):
        if payload.get("status") == "running" and payload.get("stage") == "completed":
            raise RuntimeError("injected completion notification failure")

    with pytest.raises(RuntimeError, match="notification failure"):
        _run_streaming(tmp_path, ["node-a", "node-b"], progress_callback=progress)

    task = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3")).list_tasks()[0]
    assert task["status"] == "completed"


def test_old_lease_cannot_overwrite_new_claim(tmp_path):
    source = tmp_path / "lease.jsonl"
    _write_source(source, ["node-a"])
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "lease.sqlite3"))
    from logrisk.incremental_sources import FileIncrementalSource

    task = repository.create_or_load(
        descriptor=FileIncrementalSource(source, filename=source.name).descriptor(),
        config_hash="a" * 64,
    )
    first = repository.claim_task(task["task_id"])
    repository.finish_claim(task["task_id"], first["lease_token"], error="first failure")
    second = repository.claim_task(task["task_id"])

    repository.finish_claim(task["task_id"], first["lease_token"], error="late failure")
    with pytest.raises(Exception, match="租约已变化"):
        repository.mark_claim_stage(task["task_id"], first["lease_token"], "MINING")
    with pytest.raises(Exception, match="租约已变化"):
        repository.save_result(task["task_id"], {}, expected_lease_token=first["lease_token"])
    with pytest.raises(Exception, match="租约已变化"):
        repository.complete_claim(task["task_id"], first["lease_token"])

    current = repository.get_task(task["task_id"])
    assert current["status"] == "running"
    assert current["lease_token"] == second["lease_token"]
    assert current["error"] is None


@pytest.mark.parametrize("boundary", ["initialization", "projection"])
def test_claimed_task_errors_outside_batch_loop_release_claim(tmp_path, monkeypatch, boundary):
    original = RuntimeError(f"injected {boundary} failure")
    if boundary == "initialization":
        monkeypatch.setattr(large_file_pipeline, "load_rules", lambda *args, **kwargs: (_ for _ in ()).throw(original))
    else:
        monkeypatch.setattr(
            "logrisk.streaming_results.StreamingResultRepository.build",
            lambda *args, **kwargs: (_ for _ in ()).throw(original),
        )

    with pytest.raises(RuntimeError, match=boundary) as caught:
        _run_streaming(tmp_path, ["node-a", "node-b"])

    assert caught.value is original
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    task = repository.list_tasks()[0]
    assert task["status"] == "failed"
    assert repository.claim_task(task["task_id"])["status"] == "running"


def test_busy_claim_is_not_overwritten_by_unowned_worker(tmp_path):
    source = tmp_path / "busy.jsonl"
    _write_source(source, ["node-a"])
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "busy.sqlite3"))
    task = repository.create_or_load(
        descriptor=FileIncrementalSource(source, filename=source.name).descriptor(),
        config_hash=hashlib.sha256(Path("configs/drain3_recommended.ini").read_bytes()).hexdigest(),
    )
    owner = repository.claim_task(task["task_id"])

    with pytest.raises(StreamingTaskBusyError):
        large_file_pipeline.run_large_file_pipeline(
            input_job_id="busy-worker",
            input_path=source,
            filename=source.name,
            config_path="configs/drain3_recommended.ini",
            rules_path="configs/risk_rules.yaml",
            state_dir=tmp_path / "state",
            streaming_repository=repository,
            resume_task_id=task["task_id"],
        )

    current = repository.get_task(task["task_id"])
    assert current["status"] == "running"
    assert current["lease_token"] == owner["lease_token"]


def test_partition_mining_bounds_inflight_and_consumes_results_in_manifest_order(tmp_path, monkeypatch):
    spool_dir = tmp_path / "spool"
    spool_dir.mkdir()
    partitions = []
    for index in range(7):
        path = spool_dir / f"partition-{index}.jsonl"
        path.write_text("{}\n", encoding="utf-8")
        partitions.append({
            "partition_id": f"partition-{index}",
            "partition_key": ["cluster", f"node-{index}", "source", "component"],
            "path": path.name,
            "record_count": 1,
        })

    class DeferredExecutor:
        def __init__(self):
            self.submitted = 0

        def submit(self, function, /, *args, **kwargs):
            del function, kwargs
            self.submitted += 1
            output_path = Path(args[1])
            index = int(output_path.stem.split("-")[-1])
            output_path.write_text(json.dumps({"partition_index": index}) + "\n", encoding="utf-8")
            future = Future()
            future.partition_index = index
            future.partition_result = {"output_path": str(output_path), "record_count": 1}
            return future

    inflight_sizes: list[int] = []

    def complete_last_partition_first(futures, *, return_when):
        inflight_sizes.append(len(futures))
        selected = max(futures, key=lambda future: future.partition_index)
        selected.set_result(selected.partition_result)
        return futures_wait(futures, return_when=return_when)

    class RecordingAggregator:
        def __init__(self, window_seconds):
            self.events = []

        def add(self, event):
            self.events.append(event)

        def finalize(self):
            return self.events

    monkeypatch.setattr(streaming_drain_pipeline, "wait", complete_last_partition_first)
    monkeypatch.setattr(streaming_drain_pipeline, "TemplateEventAggregator", RecordingAggregator)
    executor = DeferredExecutor()

    windows, metadata = streaming_drain_pipeline.mine_spooled_partitions(
        spool_dir=spool_dir,
        manifest={"partitions": partitions},
        config_path="configs/drain3_recommended.ini",
        state_dir=tmp_path / "drain-state",
        window_seconds=300,
        requested_workers=2,
        max_workers=2,
        executor=executor,
    )

    assert [window["partition_index"] for window in windows] == list(range(7))
    assert metadata["worker_count"] == 2
    assert max(inflight_sizes) <= 4
    assert executor.submitted == 7
