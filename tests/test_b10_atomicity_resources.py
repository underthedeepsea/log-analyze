from __future__ import annotations

import base64
import copy
import json
import os
import subprocess
import sys
from contextlib import contextmanager

import pytest

from logrisk.database import PostgresCursor, SQLiteDatabase
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.sqlite_stores import SQLiteApprovedRuleStore, SQLiteFeatureJobStore
from logrisk.streaming_results import ResultBudgetError, StreamingResultRepository
from tests.test_approval_deduplication import candidate, entity
from tests.test_b10_compatibility import reference_manager


REFERENCE = {"task_id": "fixture", "generation": "ready"}


def test_reference_build_failure_never_publishes_partial_input(tmp_path, monkeypatch):
    database = SQLiteDatabase(tmp_path / "build.sqlite3")
    store = SQLiteFeatureJobStore(database)
    manager = FeatureJobManager(auto_start=False, persistence=store)

    def sources(_self, _reference):
        for index in range(100):
            yield entity(f"node-{index:04d}", "2026-06-22T10:00:00+08:00")
        raise ResultBudgetError("entity 101 budget")

    monkeypatch.setattr(StreamingResultRepository, "feature_sources", sources)
    with pytest.raises(ResultBudgetError, match="entity 101"):
        manager.create_job({"result_ref": REFERENCE, "risk_entities": []}, model="fake")
    jobs = store.load()
    assert len(jobs) == 1
    job = jobs[0]
    assert job["status"] == "failed"
    assert job["input_build"]["status"] == "failed"
    assert job["source_summary"]["feature_job_entity_count"] == 100
    assert len(job["entities"]) == 100
    assert store.list_candidates() == []
    with pytest.raises(FeatureJobError, match="完整"):
        manager.run_job(job["job_id"])
    with pytest.raises(FeatureJobError, match="完整"):
        manager.retry_entity(job["job_id"], "node-0000", start=False)


def test_reference_build_process_death_is_not_a_runnable_job(tmp_path):
    path = tmp_path / "crash-build.sqlite3"
    database = SQLiteDatabase(path)
    script = '''
import os, sys
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager
from logrisk.sqlite_stores import SQLiteFeatureJobStore
from logrisk.streaming_results import StreamingResultRepository
def sources(self, reference):
    for index in range(100):
        yield {"entity_id":f"node-{index:04d}","entity_type":"node","risk_score":80,"top_templates":[]}
    os._exit(71)
StreamingResultRepository.feature_sources = sources
manager=FeatureJobManager(auto_start=False,persistence=SQLiteFeatureJobStore(SQLiteDatabase(sys.argv[1])))
manager.create_job({"result_ref":{"task_id":"fixture","generation":"ready"},"risk_entities":[]},model="fake")
'''
    result = subprocess.run([sys.executable, "-c", script, str(path)], env={**os.environ, "PYTHONPATH": "src"}, check=False)
    assert result.returncode == 71
    store = SQLiteFeatureJobStore(database)
    job = store.load()[0]
    assert job["status"] == "building"
    assert job["input_build"]["status"] == "building"
    assert job["source_summary"]["feature_job_entity_count"] == 100
    restored = FeatureJobManager(auto_start=False, persistence=store)
    with pytest.raises(FeatureJobError, match="完整"):
        restored.run_job(job["job_id"])
    assert store.list_candidates() == []


@pytest.mark.parametrize("provider", ["sqlite", "postgres"])
@pytest.mark.parametrize("collection", ["entities", "candidates"])
@pytest.mark.parametrize("oversized", [False, True])
def test_feature_pages_budget_before_payload_fetch_and_decode(tmp_path, monkeypatch, provider, collection, oversized):
    database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    manager.run_job(job_id)
    table, key, payload = (("feature_job_entities", "entity_id", "entity_json") if collection == "entities"
                           else ("feature_candidates", "candidate_id", "candidate_json"))
    with database.transaction() as connection:
        rows = connection.execute(f"SELECT {key},{payload} FROM {table} WHERE job_id=? ORDER BY {key}", (job_id,)).fetchall()
        for row in rows:
            value = json.loads(row[1])
            value["title"] = "汉" * (400000 if oversized else 300000)
            connection.execute(f"UPDATE {table} SET {payload}=? WHERE job_id=? AND {key}=?", (json.dumps(value, ensure_ascii=False), job_id, row[0]))
    queries = []
    decoded = []

    class GuardCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def fetchall(self):
            raise AssertionError("page must not fetchall JSON or metadata")

        def fetchone(self):
            row = self.cursor.fetchone()
            return dict(row) if row is not None else None

        def __iter__(self):
            return (dict(row) for row in self.cursor)

    class GuardConnection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql, parameters=()):
            queries.append(sql)
            # PostgreSQL client's payload buffer must itself be bounded: a
            # fetchone over an unconstrained SELECT would still buffer all rows.
            if sql.startswith(f"SELECT {payload} "):
                assert f"AND {key}=?" in sql or "LIMIT 1" in sql
                assert "length(" in sql.lower(), "concurrent growth needs a SQL length guard"
            cursor = GuardCursor(self.connection.execute(sql, parameters))
            return PostgresCursor(cursor)

    @contextmanager
    def guarded_connect():
        with database.connect() as connection:
            connection.create_function("octet_length", 1, lambda value: len(value.encode("utf-8")))
            yield GuardConnection(connection)

    store = SQLiteFeatureJobStore(database)
    original_decode = store._decode_json
    monkeypatch.setattr(store, "_connect", guarded_connect)
    monkeypatch.setattr(store, "database", type("Provider", (), {"provider": provider})())
    monkeypatch.setattr(store, "_decode_json", lambda value, default: decoded.append(len(value.encode("utf-8"))) or original_decode(value, default))
    reader = store.load_entity_page if collection == "entities" else store.candidate_page
    if oversized:
        with pytest.raises(FeatureJobError, match="byte budget"):
            reader(job_id)
        assert decoded == []
    else:
        page = reader(job_id)
        assert len(page) == 1
        assert len(decoded) == 1 and decoded[0] < 1024 * 1024
    expected_length = (
        f"octet_length(CAST({payload} AS TEXT))"
        if provider == "postgres"
        else f"length(CAST({payload} AS BLOB))"
    )
    assert any(expected_length in query for query in queries)


@pytest.mark.parametrize("paged", [False, True])
@pytest.mark.parametrize("status", ["approved", "rejected"])
def test_identity_failure_restores_every_cached_sibling_in_place(tmp_path, monkeypatch, paged, status):
    database, manager, document = reference_manager(tmp_path)
    if not paged:
        document = {"risk_entities": [entity("node-a", "2026-06-22T10:00:00+08:00")]}
    ids = [manager.create_job(document, model="fake", min_score=0) for _ in range(2)]
    for job_id in ids:
        manager.run_job(job_id)
    target = manager.get_job(ids[0])["features"][0]
    sibling = manager._jobs[ids[1]]
    condition = sibling["condition"]
    records = sibling["entities"]
    active = records[0]
    if paged:
        sibling["_active_record"] = active
        for feature in manager.persistence.candidate_page(ids[1]):
            sibling["features"][feature["candidate_id"]] = copy.deepcopy(feature)
    before = {job_id: {"features": copy.deepcopy(dict(manager._jobs[job_id]["features"])),
                       "events": manager.list_events(job_id), "record": copy.deepcopy(active)} for job_id in ids}
    dirty_before = copy.deepcopy(sibling["features"].dirty) if paged else None
    original = database.transaction

    @contextmanager
    def fail_commit():
        with original() as connection:
            yield connection
            if connection.execute("SELECT COUNT(*) FROM approval_decisions").fetchone()[0]:
                raise RuntimeError("identity commit fault")

    with monkeypatch.context() as context:
        context.setattr(database, "transaction", fail_commit)
        with pytest.raises(RuntimeError, match="identity commit fault"):
            manager.update_feature(ids[0], target["candidate_id"], {"status": status, "review_scope": "approval_identity"})
    assert manager._jobs[ids[1]] is sibling
    assert sibling["condition"] is condition
    assert sibling["entities"] is records
    assert active == before[ids[1]]["record"]
    if paged:
        assert sibling["_active_record"] is active
        assert sibling["features"].dirty == dirty_before
    for job_id in ids:
        assert dict(manager._jobs[job_id]["features"]) == before[job_id]["features"]
        assert manager.list_events(job_id) == before[job_id]["events"]
    assert {feature["status"] for feature in manager.persistence.list_candidates()} == {"pending"}


@pytest.mark.parametrize("boundary", ["candidate", "completion"])
def test_worker_hard_death_cannot_commit_an_orphan_candidate(tmp_path, boundary):
    database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    script = '''
import os, sys
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager
from logrisk.sqlite_stores import SQLiteFeatureJobStore
from tests.test_approval_deduplication import candidate
store=SQLiteFeatureJobStore(SQLiteDatabase(sys.argv[1]))
manager=FeatureJobManager(extractor=lambda source,**kwargs:[candidate(source,"hard-death-"+source["entity_id"])],auto_start=False,interrupt_on_restore=False,persistence=store)
if sys.argv[3]=="candidate":
    original=manager._register_feature_group_locked
    def crash(*args,**kwargs):
        value=original(*args,**kwargs)
        os._exit(72)
    manager._register_feature_group_locked=crash
else:
    original=manager._emit_locked
    def crash(job,event_type,**payload):
        original(job,event_type,**payload)
        if event_type=="entity_completed": os._exit(72)
    manager._emit_locked=crash
manager.run_job(sys.argv[2])
'''
    result = subprocess.run([sys.executable, "-c", script, str(database.path), job_id, boundary], env={**os.environ, "PYTHONPATH": "src:."}, check=False)
    assert result.returncode == 72
    store = SQLiteFeatureJobStore(database)
    assert store.list_candidates() == []
    assert store.load_entity_page(job_id)[0]["status"] == "running"
    assert store.load_entity_page(job_id)[0]["feature_ids"] == []
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM approval_group_candidates").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM approval_candidate_projection").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM feature_job_events WHERE event_type='entity_completed'").fetchone()[0] == 0
    resumed = FeatureJobManager(extractor=lambda source, **kwargs: [candidate(source, "hard-death-" + source["entity_id"])],
                                auto_start=False, interrupt_on_restore=False, persistence=store)
    resumed.run_job(job_id)
    assert resumed.get_job(job_id)["progress"]["completed"] == 2
    assert len(store.list_candidates()) == 2
    for record in store.load_entity_page(job_id):
        assert record["feature_ids"] == ["hard-death-" + record["entity_id"]]


def test_job_cursor_is_scoped_seek_not_a_signed_permission(tmp_path):
    _database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    def cursor(**changes):
        value = {"version": 1, "job_id": job_id, "entity": "", "feature": "", **changes}
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()
    assert len(manager.get_job(job_id, cursor=cursor(entity="node-a"))["entities"]) == 1
    assert manager.get_job(job_id, cursor=cursor(entity="zzzz", feature="zzzz"))["entities"] == []
    for changes in ({"job_id": "other"}, {"version": 9}, {"entity": []}, {"feature": None}):
        with pytest.raises(FeatureJobError, match="游标"):
            manager.get_job(job_id, cursor=cursor(**changes))


def test_input_cursor_validates_scope_filters_and_key_type(tmp_path):
    from logrisk.input_jobs import InputJobStore

    _database, manager, document = reference_manager(tmp_path)
    store = object.__new__(InputJobStore)
    store.database = manager.persistence.database
    store.get_result = lambda _job_id: {**document, "complete": False}
    job_id = "input-fixture"
    def token(**changes):
        value = {"reference": document["result_ref"], "input_job_id": job_id,
                 "after": "zzzz", "collection": "entities", "window_key": None, **changes}
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()
    assert store.get_result_page(job_id, cursor=token())["risk_entities"] == []
    for changes in ({"reference": REFERENCE}, {"input_job_id": "other"}, {"after": []},
                    {"collection": "windows"}, {"window_key": "other"}):
        with pytest.raises(ValueError, match="游标"):
            store.get_result_page(job_id, cursor=token(**changes))


def test_approval_cursor_validates_generation_status_and_key_type(tmp_path):
    database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    manager.run_job(job_id)
    with database.connect() as connection:
        generation = connection.execute("SELECT generation FROM approval_projection_state WHERE singleton=1").fetchone()[0]
    def token(**changes):
        value = {"v": 1, "generation": generation, "status": "pending", "after": "zzzz", **changes}
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()
    assert manager.persistence.approval_page(status="pending", page_size=10, cursor=token())["items"] == []
    for changes in ({"generation": "stale"}, {"status": "approved"}, {"after": []}, {"v": 9}):
        with pytest.raises(ValueError, match="invalid_cursor"):
            manager.persistence.approval_page(status="pending", page_size=10, cursor=token(**changes))


def test_reference_build_publishes_all_entities_before_rule_effects(tmp_path):
    _database, manager, document = reference_manager(tmp_path)
    manager.rule_store.upsert_feature(candidate(entity("node-a", "2026-06-22T10:00:00+08:00"), "seed-rule"))
    job_id = manager.create_job(document, model="fake", min_score=0)
    assert manager.persistence.load_job(job_id)["input_build"]["status"] == "ready"
    assert manager.persistence.entity_count(job_id) == 2
    assert manager.persistence.list_candidates() == []
    manager.run_job(job_id)
    assert {record["status"] for record in manager.persistence.load_entity_page(job_id)} == {"rule_matched"}
    assert len(manager.export_approved(job_id)["approved_features"]) == 2


def test_evicted_reference_job_approval_restores_unbound_paged_store(tmp_path):
    _database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    manager.run_job(job_id)
    candidate_id = manager.get_job(job_id)["features"][0]["candidate_id"]

    # Force review() to hydrate the paged job while its stores are bound to
    # the approval transaction, matching a real history-cache eviction.
    manager._jobs.pop(job_id)
    approved = manager.update_feature(job_id, candidate_id, {"status": "approved"})
    assert approved["status"] == "approved"

    detail = manager.get_job(job_id)
    assert {item["status"] for item in detail["features"]} == {"approved"}
    assert manager.list_events(job_id)
    assert len(manager.export_approved(job_id)["approved_features"]) == 2


@pytest.mark.parametrize("effect", ["rule", "agent", "worker"])
def test_candidate_effect_commit_failure_restores_links_and_events(tmp_path, monkeypatch, effect):
    database, manager, document = reference_manager(tmp_path)
    job_id = manager.create_job(document, model="fake", min_score=0)
    source = manager._record_source(manager._jobs[job_id]["entities"][0])
    if effect == "rule":
        manager.rule_store.upsert_feature(candidate(source, "seed-rule"))
    original = database.transaction

    @contextmanager
    def fail_commit():
        with original() as connection:
            yield connection
            if connection.execute("SELECT COUNT(*) FROM feature_candidates").fetchone()[0]:
                raise RuntimeError("candidate effect fault")

    with monkeypatch.context() as context:
        context.setattr(database, "transaction", fail_commit)
        if effect == "agent":
            with pytest.raises(RuntimeError, match="candidate effect fault"):
                manager.register_agent_candidate(job_id, source["entity_id"], candidate(source, "unused"), run_id="fixture")
        elif effect == "rule":
            with pytest.raises(RuntimeError, match="candidate effect fault"):
                manager.run_job(job_id)
        else:
            manager.run_job(job_id)
            assert manager.get_job(job_id)["status"] == "completed_with_errors"
    assert manager.persistence.list_candidates() == []
    assert all(record["feature_ids"] == [] for record in manager.persistence.load_entity_page(job_id))
    assert not ({event["type"] for event in manager.list_events(job_id)}
                & {"entity_completed", "entity_rule_matched", "agent_candidate_registered"})
    assert manager._jobs[job_id]["features"].dirty == {}
    assert manager._jobs[job_id]["events"].pending == []
