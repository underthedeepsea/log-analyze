from __future__ import annotations

import pytest
import json
from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource,SourceCursor
from logrisk.streaming_state import StreamingStateRepository
from logrisk.streaming_results import StreamingResultRepository
from logrisk.feature_jobs import FeatureJobManager
from logrisk.sqlite_stores import SQLiteFeatureJobStore
from tests.test_dashboard_server import dashboard_server


def test_safe_result_keeps_reference_without_raw_fields():
    from logrisk.large_file_pipeline import _safe_streaming_result
    reference = {"task_id": "task", "generation": "generation"}
    result = _safe_streaming_result({"result_ref": reference, "complete": False, "next_cursor": "cursor", "samples": ["secret"]})
    assert result["result_ref"] == reference
    assert result["complete"] is False
    assert "samples" not in result


def seeded(tmp_path,count=150):
    path=tmp_path / "source.log"; path.write_text("x\n")
    database=SQLiteDatabase(tmp_path / "db.sqlite3")
    state=StreamingStateRepository(database)
    task=state.create_or_load(descriptor=FileIncrementalSource(path,filename="source.log").descriptor(),config_hash="a"*64)
    for batch in range(3):
        windows=[{"window_start":"2026-01-01T00:00:00+00:00","window_end":"2026-01-01T00:05:00+00:00","cluster":"c","entity_type":"node","entity_id":f"n-{index:04d}","component":"kernel","template_hash":f"t-{index}","template":"error <*>","severity":"ERROR","count":batch+1,"semantic_fields":{"errno":[{"value":batch,"count":batch+1}]}} for index in range(count)]
        state.commit_window(task["task_id"],window_id=f"batch-{batch}",cursor=SourceCursor("file",{"offset":batch+1}),templates=[],windows=windows,summary={"record_count":count*(batch+1)})
    return database,state,task


def test_projection_pages_are_complete_and_feature_job_consumes_reference(tmp_path):
    database,state,task=seeded(tmp_path)
    repository=StreamingResultRepository(database)
    reference=repository.build(task["task_id"],{})
    assert repository.summary(reference)["records"] == 900
    first=repository.entities(reference,limit=70)
    second=repository.entities(reference,after=first["next_key"],limit=70)
    third=repository.entities(reference,after=second["next_key"],limit=70)
    assert [len(page["items"]) for page in (first,second,third)] == [70,70,10]
    assert third["next_key"] is None
    template=first["items"][0]["top_templates"][0]
    assert template["count"] == 6
    assert {value["value"]:value["count"] for value in template["semantic_fields"]["errno"]} == {0:1,1:2,2:3}
    manager=FeatureJobManager(auto_start=False,persistence=SQLiteFeatureJobStore(database))
    job_id=manager.create_job({"result_ref":reference,"risk_entities":first["items"]},model="fake",min_score=0)
    job=manager._jobs[job_id]
    assert len(job["entities"]) == 150
    assert all("_source_ref" in record["source"] for record in job["entities"])
    assert manager._record_source(job["entities"][-1])["top_templates"][0]["count"] == 6


def test_generation_resume_after_page_failure_does_not_double_count(tmp_path,monkeypatch):
    database,state,task=seeded(tmp_path,110)
    repository=StreamingResultRepository(database)
    original=repository._split; calls=0
    def fail(window):
        nonlocal calls
        calls+=1
        if calls==105:
            raise RuntimeError("injected page crash")
        return original(window)
    monkeypatch.setattr(repository,"_split",fail)
    with pytest.raises(RuntimeError,match="page crash"):
        repository.build(task["task_id"],{})
    monkeypatch.setattr(repository,"_split",original)
    reference=repository.build(task["task_id"],{})
    assert repository.summary(reference)["records"] == 660
    assert repository.build(task["task_id"],{}) == reference


def test_result_manifest_cursor_and_complete_export(tmp_path):
    from logrisk.input_jobs import InputJobConfig
    from logrisk.sqlite_stores import SQLiteInputJobStore, SQLiteUploadSessionStore
    from logrisk.upload_sessions import UploadConfig
    database, state, task = seeded(tmp_path)
    repository = StreamingResultRepository(database)
    reference = repository.build(task["task_id"], {})
    store = SQLiteInputJobStore(InputJobConfig(tmp_path / "output"), database)
    uploads = SQLiteUploadSessionStore(UploadConfig(tmp_path / "uploads", chunk_size_bytes=4), database)
    upload = uploads.create(filename="source.log", size_bytes=2)
    job = store.create(upload_id=upload["upload_id"], filename="source.log", source_path=str(tmp_path / "source.log"))
    job_id = job["input_job_id"]
    store.write_result(job_id, {"result_ref": reference, "complete": False, "summary": repository.summary(reference), "risk_entities": repository.entities(reference)["items"]})
    assert store.get_result(job_id)["risk_entities"] == []
    first = store.get_result_page(job_id, limit=80)
    second = store.get_result_page(job_id, limit=80, cursor=first["next_cursor"])
    assert len(first["risk_entities"]) == 80
    assert len(second["risk_entities"]) == 70
    assert second["next_cursor"] is None
    with pytest.raises(ValueError, match="游标"):
        store.get_result_page(job_id, cursor=first["next_cursor"], collection="windows")
    exported = json.loads(store.export_complete_result(job_id).read_text())
    assert exported["risk_entities"] == first["risk_entities"] + second["risk_entities"]


def test_projection_matches_existing_scoring_and_merge_for_small_fixture(tmp_path):
    from logrisk.large_file_pipeline import _merge_template_windows, _safe_streaming_result
    from logrisk.risk_engine import score_risk_entities
    database, state, task = seeded(tmp_path, 4)
    windows = list(state.iter_committed_windows(task["task_id"]))
    baseline = score_risk_entities(_merge_template_windows(windows), {})
    repository = StreamingResultRepository(database)
    reference = repository.build(task["task_id"], {})
    actual = repository.entities(reference)["items"]
    expected = _safe_streaming_result({"risk_entities": baseline})["risk_entities"]
    result = _safe_streaming_result({"risk_entities": actual})["risk_entities"]
    assert result == expected


def test_reference_feature_job_save_and_load_keep_entities_paged(tmp_path):
    database, _state, task = seeded(tmp_path)
    repository = StreamingResultRepository(database)
    reference = repository.build(task["task_id"], {})
    store = SQLiteFeatureJobStore(database)
    manager = FeatureJobManager(auto_start=False, persistence=store)

    job_id = manager.create_job(
        {"result_ref": reference, "risk_entities": []},
        model="fake",
        min_score=0,
    )

    live_entities = manager._jobs[job_id]["entities"]
    restored = store.load_job(job_id)
    assert getattr(live_entities, "paged", False) is True
    assert restored is not None
    assert getattr(restored["entities"], "paged", False) is True
    assert len(restored["entities"]) == 150
    assert restored["entities"][-1]["entity_id"] == "n-0149"
    with database.connect() as connection:
        snapshot = json.loads(connection.execute(
            "SELECT job_json FROM feature_jobs WHERE job_id=?", (job_id,)
        ).fetchone()[0])
    assert "entities" not in snapshot


def test_reference_worker_is_lazy_durable_and_detail_is_paged(tmp_path, monkeypatch):
    database, _state, task = seeded(tmp_path, 120)
    reference = StreamingResultRepository(database).build(task["task_id"], {})
    store = SQLiteFeatureJobStore(database)
    calls = []
    manager = FeatureJobManager(extractor=lambda source, **kwargs: calls.append(source["entity_id"]) or [],
                                auto_start=False, persistence=store, interrupt_on_restore=False)
    job_id = manager.create_job({"result_ref": reference, "risk_entities": []}, model="fake", min_score=0)
    original_page = store.load_entity_page

    def checked_page(*args, **kwargs):
        if kwargs.get("after"):
            assert calls, "Worker read its second page before extracting the first page"
        return original_page(*args, **kwargs)

    monkeypatch.setattr(store, "load_entity_page", checked_page)
    manager.run_job(job_id)
    assert len(calls) == 120
    monkeypatch.setattr(store, "load_entity_page", original_page)
    first = manager.get_job(job_id)
    second = manager.get_job(job_id, cursor=first["next_cursor"])
    assert first["entities_total"] == first["progress"]["completed"] == 120
    assert len(first["entities"]) == 100
    assert len(second["entities"]) == 20
    assert second["next_cursor"] is None
    assert {item["entity_id"] for item in first["entities"]}.isdisjoint(item["entity_id"] for item in second["entities"])
    assert all(item["status"] == "completed" for item in store.load_job(job_id)["entities"])
    assert len(manager._jobs[job_id]["processed_samples"]) <= 61
    cursor, sequences = 0, []
    while True:
        events, next_cursor = manager.wait_for_events(job_id, cursor, timeout=0)
        if not events:
            break
        assert len(events) <= 200
        sequences.extend(item["sequence"] for item in events)
        cursor = next_cursor
    assert sequences == list(range(len(store.load_job(job_id)["events"])))


def test_reference_worker_resume_skips_committed_entities(tmp_path):
    database, _state, task = seeded(tmp_path, 6)
    reference = StreamingResultRepository(database).build(task["task_id"], {})
    store = SQLiteFeatureJobStore(database)
    calls = []

    def interrupt(source, **kwargs):
        calls.append(source["entity_id"])
        if len(calls) == 3:
            raise KeyboardInterrupt("worker terminated")
        return []

    manager = FeatureJobManager(extractor=interrupt, auto_start=False, persistence=store, interrupt_on_restore=False)
    job_id = manager.create_job({"result_ref": reference, "risk_entities": []}, model="fake", min_score=0)
    with pytest.raises(KeyboardInterrupt):
        manager.run_job(job_id)
    resumed_calls = []
    resumed = FeatureJobManager(extractor=lambda source, **kwargs: resumed_calls.append(source["entity_id"]) or [],
                                auto_start=False, persistence=store, interrupt_on_restore=False)
    resumed.run_job(job_id)
    assert resumed_calls == ["n-0002", "n-0003", "n-0004", "n-0005"]
    assert resumed.get_job(job_id)["progress"]["completed"] == 6


def test_reference_candidates_remain_complete_and_reviewable(tmp_path):
    from tests.test_feature_jobs import candidate
    from logrisk.sqlite_stores import SQLiteApprovedRuleStore

    database, _state, task = seeded(tmp_path, 4)
    reference = StreamingResultRepository(database).build(task["task_id"], {})
    store = SQLiteFeatureJobStore(database)
    rules = SQLiteApprovedRuleStore(database)

    def extract(source, **kwargs):
        value = candidate(source)
        value["template_hashes"] = [source["top_templates"][0]["template_hash"]]
        value["source_templates"] = source["top_templates"]
        return [value]

    manager = FeatureJobManager(extractor=extract, auto_start=False, persistence=store,
                                rule_store=rules, interrupt_on_restore=False)
    job_id = manager.create_job({"result_ref": reference, "risk_entities": []}, model="fake", min_score=0)
    manager.run_job(job_id)
    detail = manager.get_job(job_id)
    assert detail["status"] == "completed"
    assert detail["features_total"] == 4
    assert len(store.load_job(job_id)["features"]) == 4
    assert not manager._jobs[job_id]["features"].dirty
    reviewed = manager.update_feature(job_id, detail["features"][0]["candidate_id"], {"status": "approved"})
    assert reviewed["status"] == "approved"
    assert store.load_candidate(reviewed["candidate_id"])["status"] == "approved"
    assert manager.export_approved(job_id)["approved_features"]


def test_reference_detail_cursor_is_scoped_and_web_pages_are_complete(tmp_path, monkeypatch, dashboard_server):
    from types import SimpleNamespace
    from urllib.parse import urlencode
    from tests.test_dashboard_server import request_json
    from tests.test_django_job_api import Client
    from logrisk.application.api import ApiFacade
    from logrisk.feature_jobs import FeatureJobError
    from logrisk_django.views import jobs

    database, _state, task = seeded(tmp_path, 150)
    reference = StreamingResultRepository(database).build(task["task_id"], {})
    manager = FeatureJobManager(auto_start=False, persistence=SQLiteFeatureJobStore(database),
                                interrupt_on_restore=False)
    job_id = manager.create_job({"result_ref": reference, "risk_entities": []}, model="fake", min_score=0)
    first = manager.get_job(job_id)
    other_id = manager.create_job({"result_ref": reference, "risk_entities": []}, model="fake", min_score=0)
    with pytest.raises(FeatureJobError, match="游标"):
        manager.get_job(other_id, cursor=first["next_cursor"])
    with pytest.raises(FeatureJobError, match="游标"):
        manager.get_job(job_id, cursor="invalid")

    base_url, _original_manager, server = dashboard_server
    server.manager = manager
    server.feature_jobs = manager
    facade = ApiFacade(SimpleNamespace(feature_jobs=manager), version="test")
    monkeypatch.setattr(jobs, "get_facade", lambda: facade)
    for read in (
        lambda path: request_json(base_url + path)[1],
        lambda path: Client().get(path).json(),
    ):
        page = read(f"/api/jobs/{job_id}")
        following = read(f"/api/jobs/{job_id}?" + urlencode({"cursor": page["next_cursor"]}))
        ids = [item["entity_id"] for item in page["entities"] + following["entities"]]
        assert len(page["entities"]) == 100
        assert len(following["entities"]) == 50
        assert len(set(ids)) == len(ids) == page["entities_total"] == 150
        assert following["next_cursor"] is None
        assert len(json.dumps(page).encode()) < 1024 * 1024
