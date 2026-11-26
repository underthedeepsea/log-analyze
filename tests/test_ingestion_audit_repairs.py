from __future__ import annotations

import gzip
import json
from dataclasses import replace
from pathlib import Path

import pytest

from logrisk.application.container import ApplicationConfig, build_application_container
from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.kafka_adapter import KafkaPythonConsumerAdapter
from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.miner_generations import MinerGenerationWorkspace
from logrisk.node_risk import NodeRiskService
from logrisk.operational_ledgers import OperationalLedgerRepository, source_identity
from logrisk.risk_semantics import RiskSemanticService
from logrisk.streaming_observability.continuous import ContinuousKafka
from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError
from logrisk.streaming_observability.repository import ObservabilityRepository
from logrisk.streaming_state import StreamingStateRepository
from tests.test_kafka_observability import Clock, FakeBroker, SETTINGS


ROOT = Path(__file__).resolve().parents[1]
DRAIN = ROOT / "configs" / "drain3_recommended.ini"
RULES = ROOT / "configs" / "risk_rules.yaml"


def _gpu_row(code: int, device: str) -> dict:
    return {
        "node": "node-a", "cluster": "fixture", "component": "kernel",
        "timestamp": "2026-11-17T06:00:00+00:00", "severity": "ERROR",
        "message": f"NVRM: Xid ({device}): {code}, GPU diagnostic event",
        "_line_no": 1, "_byte_offset_end": 0,
    }


def _upload(container, payload: bytes, filename: str = "events.jsonl") -> tuple[dict, dict]:
    manifest = container.upload_store.create(filename=filename, size_bytes=len(payload))
    upload_id = manifest["upload_id"]
    container.upload_store.append_chunk(upload_id=upload_id, index=0, data=payload)
    manifest = container.upload_store.complete(upload_id=upload_id)
    job = container.input_jobs.create(
        upload_id=upload_id, filename=filename, source_path=str(container.upload_store.source_path(upload_id)),
    )
    return job, manifest


def _risks(windows: list[dict]) -> dict[tuple[int, str], int]:
    return {
        (int(semantic["semantic_fields"]["xid_code"]), semantic["semantic_fields"]["pci_bdf"]): window["count"]
        for window in windows if (semantic := window.get("risk_semantic")) and semantic.get("category") == "xid"
    }


def test_normal_upload_binds_verified_digest_counts_physical_lines_and_isolates_recompute(tmp_path, monkeypatch):
    container = build_application_container(ApplicationConfig.for_test(project_root=ROOT, state_root=tmp_path / "state"))
    rows = [_gpu_row(35, "0000:65:00"), _gpu_row(79, "0000:65:00"), _gpu_row(79, "0000:66:00")]
    small = container.input_analyzer(rows)
    expected = {(35, "0000:65:00"): 1, (79, "0000:65:00"): 1, (79, "0000:66:00"): 1}
    assert _risks(small["top_templates"]) == expected
    data = b" \n" * 66000 + ("\n" + "\n\n".join(map(json.dumps, rows)) + "\n").encode()
    job, manifest = _upload(container, data)
    full_reads = []
    original_digest = FileIncrementalSource.content_digest

    def count_initial_hash(source):
        full_reads.append(source.path)
        return original_digest(source)

    monkeypatch.setattr(FileIncrementalSource, "content_digest", count_initial_hash)
    job_id = job["input_job_id"]
    container.run_input_job(job_id)
    persisted = container.input_jobs.get_job(job_id)
    assert persisted["status"] == "completed", persisted.get("error")
    task = container.streaming_state.get_task(persisted["streaming_task_id"])
    assert task["source"]["identity"]["identity_digest"] == manifest["sha256"]
    assert len(full_reads) == 1
    result = container.input_jobs.get_result(job_id)
    assert _risks(result["top_templates"]) == expected
    assert result["summary"]["risk_semantic_matches"] == 3
    assert result["summary"]["provenance"] == "verified"
    ledger = OperationalLedgerRepository(container.database)
    receipts = ledger.list_ingestion_batches(job_id)["items"]
    assert len(receipts) == 1 and receipts[0]["newly_ingested_count"] == 3
    assert receipts[0]["ranges"] == [
        {"partition_key": "", "start": n, "end": n + 1} for n in (66002, 66004, 66006)
    ]
    with container.database.connect() as connection:
        baseline = connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0]

    isolated_id = container.create_recompute_input_job(job_id)["input_job_id"]
    container.run_input_job(isolated_id)
    isolated = container.input_jobs.get_job(isolated_id)
    assert isolated["status"] == "completed", isolated.get("error")
    assert _risks(container.input_jobs.get_result(isolated_id)["top_templates"]) == expected
    assert ledger.list_ingestion_batches(isolated_id)["items"] == []
    with container.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == baseline


def test_normal_upload_refuses_changed_middle_even_with_same_size_and_ends(tmp_path):
    container = build_application_container(ApplicationConfig.for_test(project_root=ROOT, state_root=tmp_path / "state"))
    original = b"\n" * 65536 + b"middle\n" + b"\n" * 65536
    job, _ = _upload(container, original)
    container.upload_store.source_path(job["upload_id"]).write_bytes(original.replace(b"middle", b"edited"))
    container.run_input_job(job["input_job_id"])
    persisted = container.input_jobs.get_job(job["input_job_id"])
    assert persisted["status"] == "failed"
    assert "SHA-256" in persisted["error"]
    assert container.streaming_state.list_tasks() == []


def test_gzip_checkpoint_replays_prefix_under_limits_and_counts_only_new_lines(tmp_path):
    source = tmp_path / "events.jsonl.gz"
    raw = b"\n" + b"\n\n".join(json.dumps(_gpu_row(code, "0000:65:00")).encode() for code in (35, 79)) + b"\n"
    source.write_bytes(gzip.compress(raw))
    database = SQLiteDatabase(tmp_path / "database.sqlite3")
    ledger = OperationalLedgerRepository(database)

    class StopSecondCommit(StreamingStateRepository):
        def commit_window(self, task_id, *, cursor, **kwargs):
            if cursor.value.get("line", 0) >= 5:
                raise OSError("synthetic interrupted write")
            return super().commit_window(task_id, cursor=cursor, **kwargs)

    interrupted = StopSecondCommit(database, ledger)
    options = dict(
        input_job_id="gzip-recovery", input_path=source, filename=source.name, config_path=DRAIN,
        rules_path=RULES, state_dir=tmp_path / "state", worker_count=1, stream_batch_records=1,
        ledger_repository=ledger,
        risk_semantics=RiskSemanticService(database, ROOT / "configs/risk_semantics/builtin.yaml"),
        node_risks=NodeRiskService(database, ROOT / "configs/node_risk.yaml"),
    )
    with pytest.raises(OSError, match="synthetic"):
        run_large_file_pipeline(streaming_repository=interrupted, **options)
    task = interrupted.list_tasks()[0]
    cursor = SourceCursor.from_dict(task["cursor"])
    assert cursor.value["line"] == 3
    bounded = FileIncrementalSource(source, immutable_identity=True, max_decompressed_bytes=cursor.value["offset"] - 1)
    with pytest.raises(ValueError, match="decompressed_size_exceeded"):
        next(bounded.read(cursor))
    result = run_large_file_pipeline(
        streaming_repository=StreamingStateRepository(database, ledger), resume_task_id=task["task_id"], **options,
    )
    assert result["summary"]["total_raw_logs"] == 2
    assert result["summary"]["invocation"]["records_parsed"] == 1
    assert result["summary"]["risk_semantic_matches"] == 2
    assert result["summary"]["node_risk_ingestions"] == 2
    receipts = ledger.list_ingestion_batches("gzip-recovery")["items"]
    assert sorted(item["ranges"][0]["start"] for item in receipts) == [2, 4]
    assert sum(item["newly_ingested_count"] for item in receipts) == 2


def test_legacy_line_hint_is_reconstructed_from_reader_byte_boundary(tmp_path):
    source = tmp_path / "events.jsonl"
    first = json.dumps(_gpu_row(35, "0000:65:00")).encode() + b"\n"
    source.write_bytes(first + b"\n" + json.dumps(_gpu_row(79, "0000:65:00")).encode() + b"\n")
    old_frontier = SourceCursor("file", {"offset": len(first), "line": 100000})
    item = next(FileIncrementalSource(source).read(old_frontier))
    assert item.record["_line_no"] == 1
    assert item.metadata == {"partition_key": "", "start": 3, "end": 4}
    assert item.next_cursor.value["line"] == 4


def test_normal_kafka_counts_real_offsets_and_classifies_device_after_broker_retry(tmp_path, monkeypatch):
    monkeypatch.setenv(SETTINGS["bootstrap_env"], "fixture.invalid:9092")
    config = replace(ApplicationConfig.for_test(project_root=ROOT, state_root=tmp_path / "state"), kafka_enabled=True)
    container = build_application_container(config)
    clock = Clock()
    broker = FakeBroker(clock)
    rows = [_gpu_row(35, "0000:65:00"), _gpu_row(79, "0000:65:00"), _gpu_row(79, "0000:66:00")]
    broker.end = {0: 6}
    broker.sparse = {(0, 3)}

    def consumer_factory(**options):
        consumer = broker.consumer(**options)
        original_poll = consumer.poll

        def poll(**kwargs):
            records = original_poll(**kwargs)
            for partition_records in records.values():
                for record in partition_records:
                    payload = rows[record.offset % len(rows)]
                    if record.offset == 5:
                        payload = {"labels": "private-malformed-labels"}
                    record.value = json.dumps(payload).encode()
            return records

        consumer.poll = poll
        return consumer

    adapter = KafkaPythonConsumerAdapter(consumer_factory, offset_reader=broker)
    container.kafka_adapters[adapter.adapter_id] = adapter
    observations = ObservabilityRepository(container.database, clock)
    worker = ContinuousKafka(
        observations, container.streaming_state, source_factory=container.kafka_source,
        run_task=container.run_kafka_task, config_snapshot=lambda: container.drain_quality.configs.active_snapshot(),
    )
    ledger = OperationalLedgerRepository(container.database)

    def register(group):
        configuration = dict(SETTINGS, consumer_group=group)
        source_id = observations.register(
            container.kafka_source(configuration).descriptor(), initial_offset="earliest", exclusive_group=True,
        )
        observations.set_enabled(source_id, True)
        return source_id

    first_source = register("fixture-first-group")
    broker.failed = True
    with pytest.raises(KafkaBoundaryError):
        worker.step(first_source)
    task_id = observations.get(first_source)["active_task_id"]
    assert worker.step(first_source) == task_id
    result = container.streaming_state.get_task(task_id)["result"]
    assert result["summary"]["node_risk_ingestions"] == result["summary"]["risk_semantic_matches"] == 4
    committed = container.streaming_state.iter_committed_windows(task_id)
    assert _risks(committed) == {(35, "0000:65:00"): 1, (79, "0000:65:00"): 2, (79, "0000:66:00"): 1}
    receipts = ledger.list_ingestion_batches(task_id)["items"]
    assert sum(item["newly_ingested_count"] for item in receipts) == 4
    assert receipts[0]["ranges"] == [
        {"partition_key": "0", "start": 0, "end": 3}, {"partition_key": "0", "start": 4, "end": 5},
    ]
    assert broker.group == {0: 6}
    with container.database.connect() as connection:
        persisted = connection.execute(
            "SELECT summary_json FROM streaming_window_commits WHERE task_id=?", (task_id,),
        ).fetchone()[0]
    assert json.loads(persisted)["rejected_records"] == [
        {"partition": 0, "offset": 5, "reason_code": "record_shape_invalid"}
    ]
    assert "private-malformed-labels" not in persisted

    broker.group = {0: None}
    second_task = worker.step(register("fixture-replay-group"))
    replay = ledger.list_ingestion_batches(second_task)["items"][0]
    assert replay["actual_count"] == 4 and replay["newly_ingested_count"] == 0

    broker.group = {0: None}
    broker.identity = "fixture-new-topic-uuid"
    new_epoch_task = worker.step(register("fixture-new-epoch-group"))
    new_epoch = ledger.list_ingestion_batches(new_epoch_task)["items"][0]
    assert new_epoch["newly_ingested_count"] == 4
    assert new_epoch["source_id"] != replay["source_id"]


def test_ledger_intersection_uses_indexed_bounds_for_monotonic_ingestion(tmp_path):
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    ledger = OperationalLedgerRepository(database)
    source = {"environment": "test", "scope_key": "default", "source_kind": "file", "identity_digest": "fixture"}
    source["source_id"] = source_identity(**source)
    with database.transaction() as connection:
        ledger._lock_source(connection, source)
        connection.executemany(
            "INSERT INTO operational_source_ranges(range_id,source_id,kind,partition_key,start_position,end_position,origin_id) "
            "VALUES (?,?,'ingested','',?,?,?)",
            [(f"old-{i}", source["source_id"], i * 2, i * 2 + 1, "old-batch") for i in range(10000)],
        )
        progress = []

        def count_steps():
            progress.append(1)
            return 0

        connection.set_progress_handler(count_steps, 100)
        receipt = ledger.record_ingestion_batch(
            batch_id="latest", source=source, input_job_id="latest", checkpoint_key="ckp", parser_version="v2",
            ranges=[{"partition_key": "", "start": 19998, "end": 20001}],
            actual_count=3, connection=connection,
        )
        connection.set_progress_handler(None, 0)
    assert receipt["newly_ingested_count"] == 2  # overlap with 19998 only
    assert len(progress) < 10


def test_rolling_workspace_recovers_precommit_orphan_and_postcommit_frontier(tmp_path):
    initial_names = {f"node-{i}.bin" for i in range(64)}
    config = "c" * 64
    with MinerGenerationWorkspace(tmp_path, config, None, initial_names) as first:
        for name in initial_names:
            (first.work / name).write_bytes(name.encode())
        first_manifest = first.seal()
        first.activate(first_manifest)
    blob_names = {item.name for item in first.root.glob("shard-*.bin")}
    assert len(blob_names) == 64

    names = {"node-0.bin"}
    with MinerGenerationWorkspace(tmp_path, config, first_manifest, names) as orphan:
        assert {item.name for item in orphan.work.iterdir()} == names
        (orphan.work / "node-0.bin").write_bytes(b"rollback")
        orphan.seal()
    with MinerGenerationWorkspace(tmp_path, config, first_manifest, names) as committed:
        assert (committed.work / "node-0.bin").read_bytes() == b"node-0.bin"
        (committed.work / "node-0.bin").write_bytes(b"committed")
        manifest = committed.seal()
        assert set(manifest["files"]) == names
    # The DB pointer advanced, then the process stopped before updating the FS.
    with MinerGenerationWorkspace(tmp_path, config, manifest, names) as resumed:
        assert (resumed.work / "node-0.bin").read_bytes() == b"committed"
        stable = resumed.seal()
        resumed.activate(stable)
    current_blobs = {item.name for item in resumed.root.glob("shard-*.bin")}
    assert len(current_blobs) == 64
    assert len(current_blobs & blob_names) == 63
    assert len(list(resumed.root.glob("index-*.json"))) == 1
    assert list(resumed.root.glob("work-*")) == []
