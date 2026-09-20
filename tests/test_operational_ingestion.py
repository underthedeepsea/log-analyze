from __future__ import annotations

import copy
from pathlib import Path

import pytest

from logrisk.analysis_accounting import AnalysisAccounting, AnalysisAccountingError
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.kafka_adapter import KafkaPythonConsumerAdapter
from logrisk.large_file_pipeline import _source_record_position, run_large_file_pipeline
from logrisk.operational_ledgers import (
    OperationalLedgerRepository,
    canonical_ranges,
    ingestion_batch_id,
    source_identity,
)
from logrisk.streaming_state import StreamingStateRepository


def _source(*, scope_key: str = "default", digest: str = "source-bytes") -> dict[str, str]:
    source = {
        "environment": "local-test",
        "scope_key": scope_key,
        "source_kind": "file",
        "identity_digest": digest,
    }
    source["source_id"] = source_identity(**source)
    return source


def _document(count: int = 1000) -> dict:
    return {
        "summary": {
            "records_parsed": count,
            "total_raw_logs": count,
            "total_normalized_logs": count,
        },
        "risk_entities": [{
            "window_start": "2026-09-16T00:00:00+00:00",
            "window_end": "2026-09-16T00:05:00+00:00",
            "cluster": "test",
            "entity_type": "node",
            "entity_id": "node-a",
            "risk_score": 90,
            "risk_level": "high",
            "top_templates": [{"template_hash": "hash-a", "count": count}],
            "affected_entities": [],
        }],
    }


class _InputJobs:
    def __init__(self, input_job_id: str, document: dict, status: str = "completed") -> None:
        self.input_job_id = input_job_id
        self.job = {"input_job_id": input_job_id, "status": status}
        self.document = copy.deepcopy(document)

    def get_job(self, input_job_id: str) -> dict:
        assert input_job_id == self.input_job_id
        return copy.deepcopy(self.job)

    def get_result(self, input_job_id: str) -> dict:
        assert input_job_id == self.input_job_id
        return copy.deepcopy(self.document)


def _record_input(repository: OperationalLedgerRepository, source: dict[str, str], input_job_id: str) -> None:
    for checkpoint, start, end in (("window-1", 1, 501), ("window-2", 501, 1001)):
        repository.record_ingestion_batch(
            batch_id=ingestion_batch_id(input_job_id, checkpoint),
            source=source,
            input_job_id=input_job_id,
            checkpoint_key=checkpoint,
            parser_version="parser-v1",
            ranges=[{"partition_key": "", "start": start, "end": end}],
            actual_count=end - start,
        )


def test_analysis_accounting_resolves_server_receipts_and_reanalysis(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    input_job_id = "input-1000"
    document = _document()
    _record_input(repository, source, input_job_id)
    accounting = AnalysisAccounting(repository, _InputJobs(input_job_id, document))

    first = accounting.begin(
        input_job_id=input_job_id,
        document=document,
        request_key="analysis-1",
        environment="local-test",
        expected_members=2,
    )
    assert first["provenance"] == "verified"
    assert first["input_count"] == 1000
    assert len(first["member_ids"]) == 2
    assert first["ranges"] == [{
        "source_id": source["source_id"],
        "partition_key": "",
        "start": 1,
        "end": 1001,
    }]
    replay = accounting.begin(
        input_job_id=input_job_id,
        document=document,
        request_key="analysis-1",
        environment="local-test",
        expected_members=2,
    )
    assert replay["analysis_run_id"] == first["analysis_run_id"]
    assert replay["member_ids"] == first["member_ids"]
    attached = accounting.attach(
        analysis_run_id=first["analysis_run_id"],
        member_id=first["member_ids"][0],
        input_job_id=input_job_id,
        document=document,
    )
    assert attached["member"]["status"] == "pending"

    for member_id in first["member_ids"]:
        repository.finish_analysis_member(first["analysis_run_id"], member_id, "completed")
    settled = repository.get_analysis_run(first["analysis_run_id"])
    assert settled["status"] == "completed"

    second = accounting.begin(
        request_key="analysis-2",
        environment="local-test",
        reanalysis_of=first["analysis_run_id"],
    )
    assert second["provenance"] == "verified"
    assert second["input_count"] == 1000
    repository.finish_analysis_member(second["analysis_run_id"], second["member_ids"][0], "completed")
    repository.complete_analysis_run(second["analysis_run_id"])

    with database.connect() as connection:
        totals = {
            row["kind"]: int(row["records"])
            for row in connection.execute(
                "SELECT kind, SUM(end_position - start_position) AS records "
                "FROM operational_source_ranges GROUP BY kind"
            ).fetchall()
        }
        workload = connection.execute(
            "SELECT SUM(input_count) FROM operational_analysis_runs WHERE status='completed'"
        ).fetchone()[0]
    assert totals == {"covered": 1000, "ingested": 1000}
    assert workload == 2000
    repository.complete_analysis_run(first["analysis_run_id"])
    assert repository.get_analysis_run(first["analysis_run_id"])["settled_at"] == settled["settled_at"]


def test_analysis_accounting_keeps_legacy_document_reported_and_rejects_missing_receipts(tmp_path: Path) -> None:
    repository = OperationalLedgerRepository(SQLiteDatabase(tmp_path / "ledger.sqlite3"))
    accounting = AnalysisAccounting(repository, _InputJobs("input-empty", _document()))
    reported = accounting.begin(
        document=_document(12),
        request_key="legacy-1",
        environment="local-test",
    )
    assert reported["provenance"] == "unverified-input"
    assert reported["input_count"] is None
    assert reported["reported_input_count"] == 12
    with pytest.raises(AnalysisAccountingError, match="receipt"):
        accounting.begin(
            input_job_id="input-empty",
            request_key="missing-receipt",
            environment="local-test",
        )


def test_two_feature_members_retry_without_multiplying_workload(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    input_job_id = "input-1000"
    document = _document()
    _record_input(repository, source, input_job_id)
    accounting = AnalysisAccounting(repository, _InputJobs(input_job_id, document))
    operation = accounting.begin(
        input_job_id=input_job_id,
        request_key="two-experts",
        environment="local-test",
        expected_members=2,
    )

    calls = 0

    def extractor(entity: dict, **kwargs: object) -> list[dict]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("synthetic provider failure")
        return [{
            "candidate_id": f"candidate-{entity['entity_id']}",
            "status": "pending",
            "feature_type": "log_pattern",
            "title": "candidate",
            "summary": "summary",
            "importance": "high",
            "occurrence_count": 1000,
            "template_hashes": ["hash-a"],
            "components": ["kernel"],
            "tags": [],
            "affected_entities": [],
        }]

    managers = []
    jobs = []
    for member_id in operation["member_ids"]:
        manager = FeatureJobManager(
            extractor=extractor,
            auto_start=False,
            ledger_repository=repository,
            environment="local-test",
            analysis_run_id=operation["analysis_run_id"],
            analysis_member_id=member_id,
        )
        managers.append(manager)
        jobs.append(manager.create_job(document, model="fake", retry_count=1))
    for manager, job_id in zip(managers, jobs):
        manager.run_job(job_id)

    root = repository.get_analysis_run(operation["analysis_run_id"])
    assert calls == 3
    assert root["status"] == "completed"
    assert all(member["status"] == "completed" for member in root["members"])
    assert sum(int(root["input_count"] or 0) for _ in [root]) == 1000


def test_no_eligible_entities_still_settle_the_full_verified_input(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    repository = OperationalLedgerRepository(database)
    source = _source()
    input_job_id = "input-no-risk"
    _record_input(repository, source, input_job_id)
    document = _document()
    document["risk_entities"][0]["risk_score"] = 10
    accounting = AnalysisAccounting(repository, _InputJobs(input_job_id, document))
    operation = accounting.begin(
        input_job_id=input_job_id,
        request_key="no-eligible",
        environment="local-test",
    )

    def should_not_call(entity: dict, **kwargs: object) -> list[dict]:
        raise AssertionError("skipped entities must not invoke an extractor")

    manager = FeatureJobManager(
        extractor=should_not_call,
        auto_start=False,
        ledger_repository=repository,
        environment="local-test",
        analysis_run_id=operation["analysis_run_id"],
        analysis_member_id=operation["member_ids"][0],
    )
    job_id = manager.create_job(document, model="fake", min_score=40)
    manager.run_job(job_id)
    root = repository.get_analysis_run(operation["analysis_run_id"])
    assert root["status"] == "completed"
    assert root["input_count"] == 1000
    assert root["members"][0]["status"] == "completed"
    with database.connect() as connection:
        covered = connection.execute(
            "SELECT SUM(end_position - start_position) FROM operational_source_ranges WHERE kind='covered'"
        ).fetchone()[0]
    assert covered == 1000


def test_streaming_receipt_and_checkpoint_roll_back_together(tmp_path: Path) -> None:
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    ledger = OperationalLedgerRepository(database)
    streaming = StreamingStateRepository(database, ledger)
    source_path = tmp_path / "input.log"
    source_path.write_text("one\n", encoding="utf-8")
    task = streaming.create_or_load(
        descriptor=FileIncrementalSource(source_path).descriptor(),
        config_hash="config-v1",
    )
    source = _source()
    original_append = streaming._append_event

    def fail_window_event(connection, task_id, event_type, payload, created_at):
        if event_type == "window_committed":
            raise RuntimeError("injected checkpoint failure")
        return original_append(connection, task_id, event_type, payload, created_at)

    streaming._append_event = fail_window_event  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="checkpoint failure"):
        streaming.commit_window(
            task["task_id"],
            window_id="file-cursor:1",
            cursor=SourceCursor("file", {"offset": 4, "line": 2}),
            templates=[],
            summary={"record_count": 1},
            ledger_batch={
                "batch_id": ingestion_batch_id("input-rollback", "file-cursor:1"),
                "source": source,
                "input_job_id": "input-rollback",
                "checkpoint_key": "file-cursor:1",
                "parser_version": "parser-v1",
                "ranges": [{"partition_key": "", "start": 1, "end": 2}],
                "actual_count": 1,
            },
        )
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM operational_ingestion_batches").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM streaming_window_commits").fetchone()[0] == 0
    assert streaming.get_task(task["task_id"])["cursor"] == {"kind": "", "value": {}}


def test_registered_kafka_adapter_exposes_duplicate_offsets_and_holes(tmp_path: Path, monkeypatch) -> None:
    class Message:
        def __init__(self, partition: int, offset: int) -> None:
            self.partition = partition
            self.offset = offset
            self.value = b'{"message":"event"}'

    class FakeConsumer:
        def __init__(self, **kwargs) -> None:
            self.partition = None
            self.polled = False

        def partitions_for_topic(self, topic):
            return {0}

        def assign(self, partitions):
            self.partition = tuple(partitions)[0]

        def seek_to_beginning(self, partition):
            return None

        def seek(self, partition, offset):
            return None

        def end_offsets(self, partitions):
            return {self.partition: 10}

        def poll(self, **kwargs):
            if self.polled:
                return {}
            self.polled = True
            return {self.partition: [
                Message(0, 1), Message(0, 2), Message(0, 2), Message(0, 9),
            ]}

        def position(self, partition):
            return 10

        def close(self, **kwargs):
            return None

    monkeypatch.setenv("LOGRISK_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    adapter = KafkaPythonConsumerAdapter(FakeConsumer)
    configuration = {
        "topic": "logs",
        "consumer_group": "test",
        "bootstrap_env": "LOGRISK_KAFKA_BOOTSTRAP",
    }
    records = list(adapter.read(configuration, SourceCursor.empty()))
    positions = [
        _source_record_position(record, "kafka")
        for record in records
    ]
    assert positions == [
        {"partition_key": "0", "start": 1, "end": 2},
        {"partition_key": "0", "start": 2, "end": 3},
        {"partition_key": "0", "start": 2, "end": 3},
        {"partition_key": "0", "start": 9, "end": 10},
    ]
    assert canonical_ranges(positions) == [
        {"partition_key": "0", "start": 1, "end": 3},
        {"partition_key": "0", "start": 9, "end": 10},
    ]


def test_file_pipeline_records_1000_immutable_positions(tmp_path: Path) -> None:
    source_path = tmp_path / "messages.log"
    source_path.write_text("event %04d\n" * 1000 % tuple(range(1, 1001)), encoding="utf-8")
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    ledger = OperationalLedgerRepository(database)
    streaming = StreamingStateRepository(database, ledger)

    result = run_large_file_pipeline(
        input_job_id="input-file-1000",
        input_path=source_path,
        filename="messages.log",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        max_drain_workers=1,
        streaming_repository=streaming,
        stream_batch_records=250,
        ledger_repository=ledger,
        environment="local-test",
    )

    assert result["summary"]["provenance"] == "verified"
    assert result["summary"]["total_raw_logs"] == 1000
    with database.connect() as connection:
        receipt_count = connection.execute(
            "SELECT COUNT(*) FROM operational_ingestion_batches WHERE input_job_id='input-file-1000'"
        ).fetchone()[0]
        ingested = connection.execute(
            "SELECT SUM(end_position - start_position) FROM operational_source_ranges WHERE kind='ingested'"
        ).fetchone()[0]
    assert receipt_count == 4
    assert ingested == 1000


def test_empty_file_pipeline_records_verified_zero_receipt(tmp_path: Path) -> None:
    source_path = tmp_path / "empty.log"
    source_path.write_bytes(b"")
    database = SQLiteDatabase(tmp_path / "ledger.sqlite3")
    ledger = OperationalLedgerRepository(database)
    streaming = StreamingStateRepository(database, ledger)

    result = run_large_file_pipeline(
        input_job_id="input-empty-file",
        input_path=source_path,
        filename="empty.log",
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "state",
        worker_count=1,
        max_drain_workers=1,
        streaming_repository=streaming,
        ledger_repository=ledger,
        environment="local-test",
    )

    assert result["summary"]["total_raw_logs"] == 0
    assert result["summary"]["provenance"] == "verified"
    with database.connect() as connection:
        receipt = connection.execute(
            "SELECT actual_count, ranges_json, provenance FROM operational_ingestion_batches "
            "WHERE input_job_id='input-empty-file'"
        ).fetchone()
        ranges = connection.execute(
            "SELECT COUNT(*) FROM operational_source_ranges WHERE kind='ingested'"
        ).fetchone()[0]
    assert tuple(receipt) == (0, "[]", "verified")
    assert ranges == 0
