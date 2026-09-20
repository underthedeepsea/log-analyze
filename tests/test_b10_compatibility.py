from __future__ import annotations

import json
from contextlib import contextmanager
from itertools import count
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobFileStore, FeatureJobManager
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.input_jobs import InputJobConfig, InputJobStore
from logrisk.legacy_import import LegacyStateImporter
from logrisk.sqlite_stores import (
    SQLiteApprovedRuleStore,
    SQLiteFeatureJobStore,
    SQLiteInputJobStore,
    SQLiteUploadSessionStore,
)
from logrisk.streaming_results import StreamingResultRepository
from logrisk.streaming_state import StreamingStateRepository
from logrisk.upload_sessions import UploadConfig
from tests.test_approval_deduplication import candidate, entity
from tests.test_dashboard_server import dashboard_server, request_json
from tests.test_streaming_result_projection import seeded


def reference_manager(tmp_path):
    path = tmp_path / "source.log"
    path.write_text("sanitized fixture\n", encoding="utf-8")
    database = SQLiteDatabase(tmp_path / "reference.sqlite3")
    state = StreamingStateRepository(database)
    task = state.create_or_load(
        descriptor=FileIncrementalSource(path, filename=path.name).descriptor(),
        config_hash="a" * 64,
    )
    windows = []
    for node in ("node-a", "node-b"):
        source = entity(node, "2026-06-22T10:00:00+08:00")
        windows.append({
            **{key: value for key, value in source.items() if key != "top_templates"},
            **source["top_templates"][0], "severity": "ERROR",
        })
    state.commit_window(
        task["task_id"], window_id="batch-1", cursor=SourceCursor("file", {"offset": 1}),
        templates=[], windows=windows, summary={"record_count": 6},
    )
    reference = StreamingResultRepository(database).build(task["task_id"], {})
    sequence = count()
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{next(sequence):04d}")],
        auto_start=False, interrupt_on_restore=False,
        persistence=SQLiteFeatureJobStore(database), rule_store=SQLiteApprovedRuleStore(database),
    )
    return database, manager, {"result_ref": reference, "risk_entities": []}


@pytest.mark.parametrize("status", ["approved", "rejected"])
def test_reference_identity_review_keeps_durable_events_and_restart_state(tmp_path, status):
    database, manager, document = reference_manager(tmp_path)
    job_ids = [manager.create_job(document, model="fake", min_score=0) for _ in range(2)]
    for job_id in job_ids:
        manager.run_job(job_id)
    first = manager.get_job(job_ids[0])["features"][0]
    manager.update_feature(
        job_ids[0], first["candidate_id"], {"status": status, "review_scope": "approval_identity"},
    )
    store = manager.persistence
    assert len(store.list_candidates()) == 4
    assert {item["status"] for item in store.list_candidates()} == {status}
    event_type = "pending_candidate_reconciled" if status == "approved" else "candidate_group_rejected"
    for job_id in job_ids:
        events = manager.list_events(job_id, limit=200)
        assert any(event["type"] == event_type for event in events)
        assert [event["sequence"] for event in events] == list(range(len(events)))
        assert manager._jobs[job_id]["events"].pending == []
    restored = FeatureJobManager(
        auto_start=False, interrupt_on_restore=False, persistence=SQLiteFeatureJobStore(database),
        rule_store=SQLiteApprovedRuleStore(database),
    )
    for job_id in job_ids:
        assert {item["status"] for item in restored.get_job(job_id)["features"]} == {status}
        if status == "approved":
            assert len(restored.export_approved(job_id)["approved_features"]) == 2


def test_reference_identity_review_rollback_does_not_leave_sibling_cache_changes(tmp_path, monkeypatch):
    database, manager, document = reference_manager(tmp_path)
    job_ids = [manager.create_job(document, model="fake", min_score=0) for _ in range(2)]
    for job_id in job_ids:
        manager.run_job(job_id)
    first = manager.get_job(job_ids[0])["features"][0]
    before = manager.persistence.list_candidates()
    before_events = {job_id: manager.list_events(job_id) for job_id in job_ids}
    original = database.transaction

    @contextmanager
    def fail_after_decision():
        with original() as connection:
            yield connection
            if connection.execute("SELECT COUNT(*) FROM approval_decisions").fetchone()[0]:
                raise RuntimeError("injected paged approval rollback")

    with monkeypatch.context() as context:
        context.setattr(database, "transaction", fail_after_decision)
        with pytest.raises(RuntimeError, match="paged approval rollback"):
            manager.update_feature(job_ids[0], first["candidate_id"], {"status": "approved"})
    assert manager.persistence.list_candidates() == before
    assert manager.rule_store.list_rules() == []
    for job_id in job_ids:
        assert {item["status"] for item in manager._jobs[job_id]["features"].values()} == {"pending"}
        assert manager.list_events(job_id) == before_events[job_id]


def test_reference_agent_candidate_keeps_entity_link_after_restart(tmp_path):
    database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    source = manager._record_source(manager._jobs[job_id]["entities"][0])
    feature = candidate(source, "unused")
    first = manager.register_agent_candidate(job_id, source["entity_id"], feature, run_id="agent-fixture")
    restored = FeatureJobManager(
        auto_start=False, interrupt_on_restore=False, persistence=SQLiteFeatureJobStore(database),
        rule_store=SQLiteApprovedRuleStore(database),
    )
    second = restored.register_agent_candidate(job_id, source["entity_id"], feature, run_id="agent-fixture")
    detail = restored.get_job(job_id)
    record = next(item for item in detail["entities"] if item["entity_id"] == source["entity_id"])
    assert first["candidate_id"] == second["candidate_id"]
    assert record["feature_ids"] == [first["candidate_id"]]
    assert len([event for event in restored.list_events(job_id) if event["type"] == "agent_candidate_registered"]) == 1
    assert second["status"] == "pending"


def test_file_job_import_retains_small_result_candidate_and_event_contract(tmp_path):
    state_root = tmp_path / "legacy"
    file_store = FeatureJobFileStore(state_root / "feature_jobs")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, "legacy-candidate")],
        auto_start=False, persistence=file_store,
    )
    job_id = manager.create_job({"summary": {}, "risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")]}, model="fake")
    manager.run_job(job_id)
    before = manager.get_job(job_id)
    events = manager.list_events(job_id)
    database = SQLiteDatabase(tmp_path / "imported.sqlite3")
    importer = LegacyStateImporter(database, state_root)
    assert importer.run()["files_imported"] == 1
    assert importer.run()["files_imported"] == 0
    restored = FeatureJobManager(auto_start=False, persistence=SQLiteFeatureJobStore(database), interrupt_on_restore=False)
    after = restored.get_job(job_id)
    assert after["entities"] == before["entities"]
    assert [item["candidate_id"] for item in after["features"]] == ["legacy-candidate"]
    assert restored.list_events(job_id) == events
    assert "preview" not in after and "next_cursor" not in after
    assert isinstance(restored.persistence.load_job(job_id)["entities"], list)


def test_file_input_small_result_page_and_export_keep_complete_document(tmp_path):
    store = InputJobStore(InputJobConfig(tmp_path / "output"))
    job = store.create(upload_id="fixture", filename="source.log", source_path="fixture.log")
    document = {"summary": {"total_raw_logs": 3}, "risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")], "top_templates": []}
    store.write_result(job["input_job_id"], document)
    assert store.get_result_page(job["input_job_id"]) == document
    assert json.loads(store.export_complete_result(job["input_job_id"]).read_text()) == document


def test_reference_input_result_web_pages_do_not_skip_entities(tmp_path, monkeypatch, dashboard_server):
    from tests.test_django_upload_api import Client
    from logrisk_django.views import uploads

    database, _state, task = seeded(tmp_path, 150)
    repository = StreamingResultRepository(database)
    reference = repository.build(task["task_id"], {})
    store = SQLiteInputJobStore(InputJobConfig(tmp_path / "input-output"), database)
    upload_store = SQLiteUploadSessionStore(UploadConfig(tmp_path / "input-uploads"), database)
    upload = upload_store.create(filename="source.log", size_bytes=2)
    job = store.create(upload_id=upload["upload_id"], filename="source.log", source_path=str(tmp_path / "source.log"))
    job_id = job["input_job_id"]
    store.write_result(job_id, {"result_ref": reference, "complete": False, "summary": repository.summary(reference), "risk_entities": []})
    base_url, _manager, server = dashboard_server
    server.input_jobs = store
    monkeypatch.setattr(uploads, "get_container", lambda: SimpleNamespace(input_jobs=store))
    for read in (lambda path: request_json(base_url + path)[1], lambda path: Client().get(path).json()):
        path = f"/api/input-jobs/{job_id}/result"
        first = read(path)["result"]
        second = read(path + "?" + urlencode({"cursor": first["next_cursor"]}))["result"]
        ids = [item["entity_id"] for item in first["risk_entities"] + second["risk_entities"]]
        assert [len(first["risk_entities"]), len(second["risk_entities"])] == [100, 50]
        assert len(ids) == len(set(ids)) == 150
        assert second["next_cursor"] is None
        assert first["complete"] is False
        assert len(json.dumps(first).encode()) < 2 * 1024 * 1024
