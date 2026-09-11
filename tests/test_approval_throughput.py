from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest

from logrisk.approval_dedup import approval_identity, group_id_for_key
from logrisk.application.api import ApiFacade
from logrisk.database import PostgresDatabase, SQLiteDatabase
from logrisk.feature_jobs import FeatureJobManager
from logrisk.sqlite_stores import SQLiteApprovalGroupStore, SQLiteApprovedRuleStore, SQLiteFeatureJobStore


@pytest.fixture(params=["sqlite", "postgres"])
def review_database(request, tmp_path):
    if request.param == "sqlite":
        yield SQLiteDatabase(tmp_path / "review.sqlite3")
        return
    url = os.environ.get("LOGRISK_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("LOGRISK_TEST_POSTGRES_URL is not configured")
    admin = PostgresDatabase(url, state_root=tmp_path, migrate=False)
    schema = "review_perf_" + uuid.uuid4().hex
    with admin.transaction() as connection:
        connection.execute(f'CREATE SCHEMA "{schema}"')
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query["options"] = f"-csearch_path={schema}"
    scoped = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    try:
        yield PostgresDatabase(scoped, state_root=tmp_path)
    finally:
        with admin.transaction() as connection:
            connection.execute(f'DROP SCHEMA "{schema}" CASCADE')


def seed_candidates(database, *, count=1200, distinct=False):
    store = SQLiteFeatureJobStore(database)
    features = {}
    for index in range(count):
        template = f"Unclassified failure family_{index:05d}" if distinct else "CNI no enough IPs while creating pod sandbox"
        feature = {
            "candidate_id": f"candidate-{index:05d}", "job_id": "job-scale", "status": "pending",
            "entity": {"type": "node", "id": f"node-{index}"}, "entity_id": f"node-{index}",
            "feature_type": "network_failure", "title": "网络故障", "summary": "脱敏聚合证据",
            "importance": "high", "tags": [], "components": ["kubelet"], "reviewer_note": "",
            "source_templates": [{"template_hash": f"template-{index}", "component": "kubelet", "template": template, "count": 3}],
            "template_hashes": [f"template-{index}"], "risk_score": 90,
        }
        identity = approval_identity(feature)
        feature.update({"approval_key": identity["approval_key"], "problem_code": identity["problem_code"],
                        "approval_group_id": group_id_for_key(identity["approval_key"])})
        features[feature["candidate_id"]] = feature
    store.save({"job_id": "job-scale", "status": "completed", "model": "test-model", "provider": "test",
                "created_at": "2026-09-01T00:00:00+00:00", "features": features, "entities": [], "events": []})
    if not distinct:
        first = next(iter(features.values()))
        group = {"approval_group_id": first["approval_group_id"], "approval_key": first["approval_key"],
                 "candidate_ids": list(features), "candidate_count": count, "status": "pending", "entity_keys": []}
        SQLiteApprovalGroupStore(database).save(group)
        with database.transaction() as connection:
            connection.executemany(
                "INSERT INTO approval_group_candidates(approval_group_id, candidate_id, job_id, entity_id, created_at) VALUES (?, ?, ?, ?, ?)",
                [(first["approval_group_id"], item["candidate_id"], "job-scale", item["entity_id"], "2026-09-01T00:00:00+00:00") for item in features.values()],
            )
    return store


@pytest.mark.parametrize("status", ["approved", "rejected"])
def test_thousand_candidate_review_uses_bounded_transactions_and_append_only_events(review_database, monkeypatch, status):
    database = review_database
    # Create the manager before seeding to exercise loading a job from another worker.
    store = SQLiteFeatureJobStore(database)
    manager = FeatureJobManager(auto_start=False, persistence=store, rule_store=SQLiteApprovedRuleStore(database),
                                approval_group_store=SQLiteApprovalGroupStore(database))
    seed_candidates(database)
    with database.connect() as connection:
        original_snapshot = connection.execute("SELECT job_json FROM feature_jobs WHERE job_id='job-scale'").fetchone()[0]
    connections = []
    connect = database.connect
    monkeypatch.setattr(database, "connect", lambda: (connections.append(1), connect())[1])
    monkeypatch.setattr(store, "save", lambda job: pytest.fail("An approval must not rewrite the whole job"))
    started = perf_counter()
    reviewed = manager.update_feature("job-scale", "candidate-00000", {"status": status, "review_scope": "approval_identity"})
    elapsed = perf_counter() - started
    transaction_count = len(connections)
    assert transaction_count < 40
    assert reviewed["status"] == status
    persisted = store.load_job("job-scale")
    assert len(persisted["features"]) == 1200
    assert {item["status"] for item in persisted["features"].values()} == {status}
    assert len(persisted["events"]) == 1200
    assert [item["sequence"] for item in persisted["events"]] == list(range(1200))
    assert SQLiteApprovalGroupStore(database).list_groups()[0]["status"] == status
    with database.connect() as connection:
        assert connection.execute("SELECT job_json FROM feature_jobs WHERE job_id='job-scale'").fetchone()[0] == original_snapshot
    print(f"review_scale provider={database.provider} status={status} candidates=1200 seconds={elapsed:.3f} connections={transaction_count}")


def test_keyset_queue_does_not_skip_groups_after_review(review_database):
    store = seed_candidates(review_database, distinct=True)
    api = ApiFacade(SimpleNamespace(feature_jobs=SimpleNamespace(list_persisted_candidates=store.list_candidates)), version="test")
    started = perf_counter()
    page = api.feature_approvals({"page_size": "100"}).body
    elapsed = perf_counter() - started
    assert page["total_candidates"] == page["total_groups"] == 1200
    assert len(page["items"]) == 100
    first = page["items"][0]["representative"]
    assert first["model"] == "test-model"
    focused_key = api.feature_approvals({"page_size": "500"}).body["items"][400]["review_key"]
    focused = api.feature_approvals({"page_size": "100", "review_key": focused_key}).body
    assert focused["selected_group"]["review_key"] == focused_key
    assert focused_key not in {item["review_key"] for item in focused["items"]}
    store.update_candidate_review_state(first["candidate_id"], {"status": "rejected"}, expected_status="pending")
    second = api.feature_approvals({"page_size": "100", "after": page["next_review_key"]}).body
    assert len(second["items"]) == 100
    assert second["items"][0]["review_key"] > page["next_review_key"]
    assert not set(item["review_key"] for item in page["items"]) & set(item["review_key"] for item in second["items"])
    all_groups = api.feature_approvals({"page_size": "500"}).body["items"]
    assert second["items"][0]["review_key"] == all_groups[99]["review_key"]
    print(f"queue_scale provider={review_database.provider} groups=1200 first_page_seconds={elapsed:.3f}")


def test_batch_resolution_rolls_back_candidates_groups_and_events(review_database, monkeypatch):
    store = seed_candidates(review_database, count=12)
    selected = store.list_candidates()[0]
    monkeypatch.setattr(store, "_append_review_events", lambda *args: (_ for _ in ()).throw(RuntimeError("injected failure")))
    with pytest.raises(RuntimeError, match="injected failure"):
        store.resolve_pending_identity(selected, status="rejected")
    assert len(store.list_candidates(status="pending")) == 12
    assert SQLiteApprovalGroupStore(review_database).list_groups()[0]["status"] == "pending"
    assert store.load_job("job-scale")["events"] == []


def test_concurrent_batch_review_never_reopens_terminal_candidates(review_database):
    store = seed_candidates(review_database, count=12)
    selected = store.list_candidates()[0]
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: SQLiteFeatureJobStore(review_database).resolve_pending_identity(selected, status="rejected"), range(2)))
    assert sum(len(item["candidates"]) for item in results) == 12
    assert len(store.load_job("job-scale")["events"]) == 12
    assert {item["status"] for item in store.list_candidates()} == {"rejected"}


def test_corrupt_candidate_returns_not_found_instead_of_index_error(review_database):
    from logrisk.feature_jobs import FeatureJobError

    store = seed_candidates(review_database, count=1)
    with review_database.transaction() as connection:
        connection.execute("UPDATE feature_candidates SET candidate_json='[]' WHERE candidate_id='candidate-00000'")
    assert store.load_candidate("candidate-00000") is None
    with pytest.raises(FeatureJobError) as failure:
        store.update_candidate_review_state("candidate-00000", {"status": "rejected"}, expected_status="pending")
    assert failure.value.code == "candidate_not_found"
