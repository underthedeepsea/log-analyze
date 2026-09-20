from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.input_jobs import InputJobConfig, InputJobStore
from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.multi_source.repository import MultiSourceRepository
from logrisk.multi_source.service import MultiSourceService
from logrisk.node_risk import NodeRiskError, NodeRiskService
from logrisk.streaming_results import StreamingResultRepository
from logrisk.streaming_state import StreamingIncompleteError, StreamingStateRepository


class Classified:
    def match(self, window):
        return {"domain": "hardware", "category": "gpu", "risk_type": "gpu.fallen_off_bus",
                "severity": "critical", "base_score": 95, "confidence": 1.0,
                "semantic_rule_id": "test-xid", "semantic_rule_version": 1,
                "semantic_fields": {}, "dedup": {"key_fields": ["cluster", "node_id", "risk_type"]}}


class BoundaryRepository(StreamingStateRepository):
    def __init__(self, database, boundary):
        super().__init__(database)
        self.boundary = boundary
        self.calls = 0
        self.failed = False

    def trip(self, boundary):
        if not self.failed and self.boundary == boundary:
            self.failed = True
            raise RuntimeError("B12 injected " + boundary)

    def commit_window(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 2:
            self.trip("sealed_before_db")
        result = super().commit_window(*args, **kwargs)
        if self.calls == 1:
            self.trip("after_db")
        if self.calls == 3:
            self.trip("after_last_batch")
        return result

    def clear_pending_external_commit(self, *args, **kwargs):
        self.trip("after_external_commit")
        return super().clear_pending_external_commit(*args, **kwargs)

    def save_result(self, *args, **kwargs):
        self.trip("after_effects_before_finalize")
        return super().save_result(*args, **kwargs)


def source_file(tmp_path):
    source = tmp_path / "records.jsonl"
    source.write_text("".join(json.dumps({
        "timestamp": "2026-07-19T10:00:00+00:00", "node": "node-a", "cluster": "prod-a",
        "component": "kernel", "severity": "ERROR", "message": "GPU fallen off bus",
    }) + "\n" for _ in range(12)), encoding="utf-8")
    return source


def arguments(source, root, repository):
    return dict(input_job_id="input_b12", input_path=source, filename=source.name,
                config_path="configs/drain3_recommended.ini", rules_path="configs/risk_rules.yaml",
                state_dir=root / "state", worker_count=1, streaming_repository=repository,
                stream_batch_records=4, risk_semantics=Classified(),
                node_risks=NodeRiskService(repository.database, "configs/node_risk.yaml", clock=lambda: "2026-07-19T12:00:00+00:00"),
                multi_source=MultiSourceService(MultiSourceRepository(repository.database), aliases={}, rules=[]))


@pytest.mark.parametrize("boundary", ["sealed_before_db", "after_db", "after_external_commit",
                                       "after_last_batch", "after_effects_before_finalize"])
def test_three_batches_resume_matches_baseline_and_replays_effects_once(tmp_path, boundary):
    source = source_file(tmp_path)
    baseline_repo = StreamingStateRepository(SQLiteDatabase(tmp_path / "baseline.sqlite3"))
    baseline = run_large_file_pipeline(**arguments(source, tmp_path / "baseline", baseline_repo))
    repository = BoundaryRepository(SQLiteDatabase(tmp_path / "resumed.sqlite3"), boundary)
    kwargs = arguments(source, tmp_path / "resumed", repository)
    with pytest.raises(RuntimeError, match="B12 injected"):
        run_large_file_pipeline(**kwargs)
    task_id = repository.list_tasks()[0]["task_id"]
    before = repository.get_task(task_id)
    assert before["status"] == "failed"
    assert not repository.list_unknown_templates(task_id=task_id)
    result = run_large_file_pipeline(**kwargs, resume_task_id=task_id)
    fields = ("total_raw_logs", "total_normalized_logs", "total_template_events", "total_template_windows",
              "risk_semantic_matches", "node_risk_ingestions", "drain3_partition_count", "drain3_worker_count",
              "drain3_parallel", "unknown_template_count", "total_risk_entities", "critical_entities",
              "high_entities", "streaming_result_completeness", "streaming_summary_completeness")
    assert {key: result["summary"][key] for key in fields} == {key: baseline["summary"][key] for key in fields}
    assert result["summary"]["total_raw_logs"] == result["summary"]["risk_semantic_matches"] == 12
    assert result["summary"]["node_risk_ingestions"] == 12
    assert result["top_templates"] == baseline["top_templates"]
    assert result["risk_entities"] == baseline["risk_entities"]
    assert repository.get_task(task_id)["status"] == "completed"
    assert len(repository.list_commits(task_id)) == 3
    with repository.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == 12
        resumed_observations = connection.execute("SELECT COUNT(*) FROM source_observations").fetchone()[0]
        manifests = connection.execute("SELECT state_manifest_json FROM streaming_window_commits ORDER BY committed_at, window_id").fetchall()
    with baseline_repo.database.connect() as connection:
        assert resumed_observations == connection.execute("SELECT COUNT(*) FROM source_observations").fetchone()[0]
    assert len({json.loads(row[0])["generation"] for row in manifests}) == 3
    if boundary in {"after_last_batch", "after_effects_before_finalize"}:
        assert result["summary"]["invocation"]["records_parsed"] == 0


def test_manifest_binding_duplicate_receipt_and_transaction_rollback(tmp_path):
    source = source_file(tmp_path)
    repo = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = repo.create_or_load(descriptor=FileIncrementalSource(source).descriptor(), config_hash="c")
    task_id = task["task_id"]
    def commit(name, offset, manifest=None):
        return repo.commit_window(task_id, window_id=name, cursor=SourceCursor("file", {"offset": offset}),
                                  templates=[], windows=[], summary={"record_count": 0}, miner_generation=manifest)
    commit("empty", 0)
    first = {"generation": "one", "files": {"miner.bin": "abc"}}
    second = {"generation": "two", "files": {"miner.bin": "def"}}
    commit("one", 10, first)
    commit("receipt", 20)
    commit("two", 30, second)
    assert commit("one", 10, second) is False
    before = repo.get_task(task_id)
    with repo.database.transaction() as connection:
        connection.execute("CREATE TRIGGER fail_commit BEFORE INSERT ON streaming_task_events WHEN NEW.event_type='window_committed' BEGIN SELECT RAISE(ABORT, 'rollback'); END")
    with pytest.raises(sqlite3.IntegrityError, match="rollback"):
        commit("rollback", 40, {"generation": "orphan", "files": {}})
    assert repo.get_task(task_id) == before
    with repo.database.connect() as connection:
        rows = connection.execute("SELECT window_id,cursor_json,state_manifest_json FROM streaming_window_commits").fetchall()
    bindings = {row[0]: (json.loads(row[1]), json.loads(row[2]) if row[2] else None) for row in rows}
    assert "rollback" not in bindings
    assert bindings["empty"][1] is None
    assert bindings["one"] == (SourceCursor("file", {"offset": 10}).to_dict(), first)
    assert bindings["receipt"][1] == first
    assert bindings["two"][1] == second


def test_upgrade_keeps_historical_manifest_unknown(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for path in Path("database/migrations").glob("*.sql"):
        if path.name < "0031":
            shutil.copy(path, migrations / path.name)
    database = SQLiteDatabase(tmp_path / "upgrade.sqlite3", migrations_dir=migrations)
    repo = StreamingStateRepository(database)
    task = repo.create_or_load(descriptor=FileIncrementalSource(source_file(tmp_path)).descriptor(), config_hash="c")
    with database.transaction() as connection:
        connection.execute("INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at) VALUES (?, 'legacy', '{}', '{}', 'now')", (task["task_id"],))
    upgraded = SQLiteDatabase(tmp_path / "upgrade.sqlite3")
    with upgraded.connect() as connection:
        assert connection.execute("SELECT state_manifest_json FROM streaming_window_commits").fetchone()[0] is None


def test_summary_reduces_sum_max_any_and_marks_missing_fields(tmp_path):
    repo = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = repo.create_or_load(descriptor=FileIncrementalSource(source_file(tmp_path)).descriptor(), config_hash="c")
    for index, workers in enumerate((2, 1, 3)):
        repo.commit_window(task["task_id"], window_id=str(index), cursor=SourceCursor("file", {"offset": index + 1}), templates=[], windows=[],
                           summary={"record_count": 4, "template_count": 1, "unknown_template_count": 0,
                                    "risk_semantic_matches": 4, "template_event_count": 4, "partition_count": 2,
                                    "worker_count": workers, "parallel": workers > 1, "node_risk_enabled": False})
    summary = repo.committed_summary(task["task_id"])
    assert (summary["record_count"], summary["partition_count"], summary["worker_count"], summary["parallel"]) == (12, 6, 3, True)
    repo.commit_window(task["task_id"], window_id="old", cursor=SourceCursor("file", {"offset": 4}), templates=[], windows=[], summary={"record_count": 1})
    summary = repo.committed_summary(task["task_id"])
    assert summary["record_count"] == 13
    assert summary["partition_count"] is None
    assert summary["completeness"] == "legacy_partial"


def test_legacy_fails_before_read_effects_projection_and_explicit_recompute(tmp_path, monkeypatch):
    source = source_file(tmp_path)
    repo = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    store = InputJobStore(InputJobConfig(tmp_path / "output"))
    old = store.create(upload_id="upload", filename=source.name, source_path=str(source))
    task = repo.create_or_load(descriptor=FileIncrementalSource(source).descriptor(), config_hash=hashlib.sha256(Path("configs/drain3_recommended.ini").read_bytes()).hexdigest())
    old["streaming_task_id"] = task["task_id"]
    store.write_job(old["input_job_id"], old)
    repo.attach_input_job(task["task_id"], old["input_job_id"])
    repo.commit_window(task["task_id"], window_id="legacy", cursor=SourceCursor("file", {"offset": 20}), templates=[], summary={"record_count": 4})
    with repo.database.transaction() as connection:
        connection.execute("UPDATE streaming_window_commits SET payload_hash=NULL WHERE task_id=?", (task["task_id"],))
    repo.mark_failed(task["task_id"], "old crash")
    with monkeypatch.context() as patch:
        def forbidden(*args, **kwargs):
            raise AssertionError("legacy prefix must fail before reading/effects")
        patch.setattr(FileIncrementalSource, "read", forbidden)
        patch.setattr(NodeRiskService, "ingest_batch", forbidden)
        with pytest.raises(StreamingIncompleteError, match="legacy_partial"):
            run_large_file_pipeline(**arguments(source, tmp_path, repo), resume_task_id=task["task_id"])
    assert repo.get_task(task["task_id"])["status"] == "failed"
    with pytest.raises(StreamingIncompleteError):
        StreamingResultRepository(repo.database).build(task["task_id"], {})
    fresh = store.create_recompute(old["input_job_id"], streaming_repository=repo)
    assert fresh["input_job_id"] != old["input_job_id"]
    new_task = repo.get_task(fresh["streaming_task_id"])
    assert new_task["cursor"] == SourceCursor.empty().to_dict()
    assert "miner_generation" not in new_task
    result = run_large_file_pipeline(**dict(arguments(source, tmp_path / "fresh", repo), input_job_id=fresh["input_job_id"]), resume_task_id=fresh["streaming_task_id"])
    assert result["summary"]["total_raw_logs"] == 12
    assert repo.list_commits(task["task_id"]) == ["legacy"]
    assert store.get_job(old["input_job_id"]) == old
    source.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError):
        store.create_recompute(old["input_job_id"], streaming_repository=repo)
    source.unlink()
    with pytest.raises(FileNotFoundError):
        store.create_recompute(old["input_job_id"], streaming_repository=repo)


def test_node_count_reports_successful_rows_after_partial_effect_failure(tmp_path, monkeypatch):
    source = source_file(tmp_path)
    repo = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    kwargs = arguments(source, tmp_path, repo)
    subject = kwargs["node_risks"]
    original = subject.ingest_batch
    calls = 0
    def partial(contributions):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise NodeRiskError("injected unsuccessful effect")
        return original(contributions)
    monkeypatch.setattr(subject, "ingest_batch", partial)
    result = run_large_file_pipeline(**kwargs)
    assert result["summary"]["risk_semantic_matches"] == 12
    assert result["summary"]["node_risk_ingestions"] == 8
    with repo.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == 8
