from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import (
    FileIncrementalSource,
    RecomputeSourceIdentityError,
    SourceDescriptor,
)
from logrisk.input_jobs import InputJobConfig, InputJobStore
from logrisk.streaming_state import StreamingStateRepository
from tests.test_b12_bounded_continuation import Classified, source_file


def bind_job(tmp_path, source, *, immutable=False):
    store = InputJobStore(InputJobConfig(tmp_path / "output"))
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    old = store.create(upload_id="upload", filename=source.name, source_path=str(source))
    descriptor = FileIncrementalSource(source, immutable_identity=immutable).descriptor()
    task = repository.create_or_load(
        descriptor=descriptor,
        config_hash=hashlib.sha256(open("configs/drain3_recommended.ini", "rb").read()).hexdigest(),
    )
    old["streaming_task_id"] = task["task_id"]
    store.write_job(old["input_job_id"], old)
    return store, repository, old


def test_small_file_recompute_uses_controlled_snapshot_and_isolates_effects(tmp_path):
    source = tmp_path / "small.log"
    source.write_bytes(b"safe\n" * 8)
    store, repository, old = bind_job(tmp_path, source)
    fresh = store.create_recompute(old["input_job_id"], streaming_repository=repository)
    assert fresh["status"] == "queued"
    assert fresh["side_effect_policy"] == "isolated_recompute"
    assert fresh["recompute_source_verification"] == "complete_head_tail_coverage"
    assert store.resolve_source_path(fresh).read_bytes() == source.read_bytes()
    assert repository.get_task(fresh["streaming_task_id"])["cursor"]["value"] == {}


def registered_recompute_fixture(tmp_path, monkeypatch):
    from logrisk.application.container import ApplicationConfig, build_application_container

    project_root = Path(__file__).resolve().parents[1]
    config = replace(
        ApplicationConfig.for_test(project_root=project_root, state_root=tmp_path / "state"),
        output_root=tmp_path / "output", feature_jobs_auto_start=False,
    )
    container = build_application_container(config)
    # Exercise the ordinary-file snapshot branch through the real registered closures.
    container.input_jobs = InputJobStore(InputJobConfig(tmp_path / "input_jobs"))
    container.risk_semantics = Classified()
    source = source_file(tmp_path)
    old = container.input_jobs.create(
        upload_id="fixture", filename=source.name, source_path=str(source),
    )
    repository = container.streaming_state
    task = repository.create_or_load(
        descriptor=FileIncrementalSource(source).descriptor(),
        config_hash=hashlib.sha256((project_root / "configs/drain3_recommended.ini").read_bytes()).hexdigest(),
    )
    repository.attach_input_job(task["task_id"], old["input_job_id"])
    old["streaming_task_id"] = task["task_id"]
    container.input_jobs.write_job(old["input_job_id"], old)
    old_evidence = (repository.get_task(task["task_id"]), repository.list_commits(task["task_id"]))
    effect_calls = []

    def forbidden(*args, **kwargs):
        effect_calls.append((args, kwargs))
        raise AssertionError("isolated recompute must not deliver global effects")

    monkeypatch.setattr(container.node_risks, "ingest_batch", forbidden)
    monkeypatch.setattr(container.multi_source, "ingest_risk_entities", forbidden)
    fresh = container.create_recompute_input_job(old["input_job_id"])
    snapshot = container.input_jobs.resolve_source_path(fresh)
    descriptor = repository.get_task(fresh["streaming_task_id"])["source"]
    assert snapshot != source
    assert descriptor["identity"]["path"] == str(snapshot.resolve())
    assert descriptor["identity"]["identity_digest"] == hashlib.sha256(snapshot.read_bytes()).hexdigest()
    assert fresh["side_effect_policy"] == "isolated_recompute"
    return container, old, old_evidence, fresh, snapshot, effect_calls


def test_registered_recompute_executes_bound_snapshot_without_global_effects(tmp_path, monkeypatch):
    container, old, old_evidence, fresh, snapshot, effect_calls = registered_recompute_fixture(tmp_path, monkeypatch)
    reads = []
    original_read = FileIncrementalSource.read

    def observe_read(self, cursor):
        reads.append(self.path)
        yield from original_read(self, cursor)

    monkeypatch.setattr(FileIncrementalSource, "read", observe_read)
    container.run_input_job(fresh["input_job_id"])

    assert reads == [snapshot]
    assert container.input_jobs.get_job(fresh["input_job_id"])["status"] == "completed"
    assert container.streaming_state.get_task(fresh["streaming_task_id"])["status"] == "completed"
    result = container.input_jobs.get_result(fresh["input_job_id"])
    assert result["summary"]["total_raw_logs"] == 12
    assert result["summary"]["risk_semantic_matches"] == 12
    assert effect_calls == []
    assert container.input_jobs.get_job(old["input_job_id"]) == old
    assert container.streaming_state.get_task(old["streaming_task_id"]) == old_evidence[0]
    assert container.streaming_state.list_commits(old["streaming_task_id"]) == old_evidence[1]


def test_registered_recompute_rejects_snapshot_rewrite_before_dispatch(tmp_path, monkeypatch):
    container, old, old_evidence, fresh, snapshot, effect_calls = registered_recompute_fixture(tmp_path, monkeypatch)
    stat = snapshot.stat()
    original = snapshot.read_bytes()
    changed = original.replace(b"GPU", b"CPU", 1)
    assert changed != original and len(changed) == len(original)
    snapshot.write_bytes(changed)
    os.utime(snapshot, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    reads = []

    def forbidden_read(*args, **kwargs):
        reads.append(True)
        raise AssertionError("changed snapshot must fail before reading records")

    monkeypatch.setattr(FileIncrementalSource, "read", forbidden_read)
    container.run_input_job(fresh["input_job_id"])

    saved = container.input_jobs.get_job(fresh["input_job_id"])
    assert saved["status"] == "failed"
    assert "输入文件完整内容已变化" in saved["error"]
    stream_task = container.streaming_state.get_task(fresh["streaming_task_id"])
    assert stream_task["status"] == "conflict"
    assert stream_task["stage"] == "CONFLICT"
    assert "输入文件完整内容已变化" in stream_task["error"]
    assert container.streaming_state.list_commits(fresh["streaming_task_id"]) == []
    assert not container.input_jobs.result_path(fresh["input_job_id"]).exists()
    assert reads == []
    assert effect_calls == []
    assert container.input_jobs.get_job(old["input_job_id"]) == old
    assert container.streaming_state.get_task(old["streaming_task_id"]) == old_evidence[0]
    assert container.streaming_state.list_commits(old["streaming_task_id"]) == old_evidence[1]


def test_large_weak_identity_middle_rewrite_is_unverifiable(tmp_path):
    source = tmp_path / "large.log"
    source.write_bytes(b"a" * (192 * 1024))
    store, repository, old = bind_job(tmp_path, source)
    stat = source.stat()
    with source.open("r+b") as stream:
        stream.seek(96 * 1024)
        stream.write(b"b")
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(RecomputeSourceIdentityError) as caught:
        store.create_recompute(old["input_job_id"], streaming_repository=repository)
    assert caught.value.code == "RECOMPUTE_SOURCE_IDENTITY_UNVERIFIABLE"


def test_kafka_recompute_rejects_without_source_or_broker_behavior(tmp_path):
    store = InputJobStore(InputJobConfig(tmp_path / "output"))
    repository = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    old = store.create(upload_id="upload", filename="kafka", source_path=str(tmp_path / "absent"))
    task = repository.create_or_load(
        descriptor=SourceDescriptor("kafka", {}, {"topic": "logs"}), config_hash="c" * 64,
    )
    old["streaming_task_id"] = task["task_id"]
    store.write_job(old["input_job_id"], old)
    with pytest.raises(ValueError, match="不支持 Kafka"):
        store.create_recompute(old["input_job_id"], streaming_repository=repository)
