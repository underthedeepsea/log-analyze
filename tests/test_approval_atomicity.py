from __future__ import annotations

import copy
import multiprocessing
from contextlib import contextmanager

import pytest

from logrisk.approved_rules import ApprovedRuleStore
from logrisk.database import PostgresDatabase, SQLiteDatabase
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.sqlite_stores import SQLiteApprovalGroupStore, SQLiteApprovedRuleStore, SQLiteFeatureJobStore
from tests.test_approval_throughput import review_database, seed_candidates


def manager_for(database):
    return FeatureJobManager(
        auto_start=False, interrupt_on_restore=False,
        persistence=SQLiteFeatureJobStore(database),
        rule_store=SQLiteApprovedRuleStore(database),
        approval_group_store=SQLiteApprovalGroupStore(database),
    )


def test_review_idempotency_and_explicit_version(review_database):
    store = seed_candidates(review_database, count=2)
    manager = manager_for(review_database)
    before = store.load_candidate("candidate-00000")
    kwargs = {"expected_updated_at": before["updated_at"], "request_key": "review-1", "actor_scope": "reviewer-a"}
    first = manager.update_feature("job-scale", before["candidate_id"], {"status": "approved"}, **kwargs)
    replay = manager_for(review_database).update_feature("job-scale", before["candidate_id"], {"status": "approved"}, **kwargs)
    assert replay["decision_id"] == first["decision_id"]
    assert replay["idempotent_replay"] is True
    with pytest.raises(FeatureJobError) as conflict:
        manager.update_feature("job-scale", before["candidate_id"], {"status": "rejected"}, **kwargs)
    assert conflict.value.status_code == 409
    assert store.load_candidate(before["candidate_id"])["status"] == "approved"


def test_failure_after_rule_write_rolls_back_whole_approval(review_database, monkeypatch):
    store = seed_candidates(review_database, count=2)
    manager = manager_for(review_database)
    before = copy.deepcopy(store.list_candidates())
    original = SQLiteApprovedRuleStore.upsert_feature

    def fail_after_write(self, feature):
        original(self, feature)
        raise RuntimeError("injected post-rule failure")

    monkeypatch.setattr(SQLiteApprovedRuleStore, "upsert_feature", fail_after_write)
    with pytest.raises(RuntimeError, match="post-rule failure"):
        manager.update_feature("job-scale", "candidate-00000", {"status": "approved"})
    assert store.list_candidates() == before
    assert manager.rule_store.list_rules() == []
    assert store.load_job("job-scale")["events"] == []


def test_legacy_upsert_reuses_only_exact_scope(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    feature = {"schema_version": "approved_rule_v1", "feature_type": "network_failure", "title": "故障",
               "summary": "故障", "importance": "high", "components": ["kubelet"],
               "source_templates": [{"template_hash": "hash-a", "category": "network", "component": "kubelet"}]}
    legacy = {**feature, "rule_id": "legacy-a", "signature": "legacy-signature", "status": "active",
              "template_signatures": feature["source_templates"], "current_version": 1}
    rules._write_locked([legacy])
    assert rules.upsert_feature(feature)["rule_id"] == "legacy-a"
    changed = {**feature, "components": ["containerd"]}
    assert rules.match_feature(changed) == []
    incomplete = {**feature, "template_hashes": ["hash-a", "hash-missing"]}
    assert rules.match_feature(incomplete) == []


def _approval_process(provider, location, state_root, fail, ready, start, entered, attempted, release, results):
    """Each spawned worker owns its database, connections, manager, and Python locks."""
    try:
        database = (SQLiteDatabase(location, migrate=False) if provider == "sqlite" else
                    PostgresDatabase(location, state_root=state_root, migrate=False))
        manager = manager_for(database)
        if fail:
            class FailingRules(SQLiteApprovedRuleStore):
                def upsert_feature(self, feature):
                    super().upsert_feature(feature)
                    entered.set()
                    if not release.wait(15):
                        raise AssertionError("test coordinator timed out")
                    raise RuntimeError("injected transaction failure")
            manager.rule_store = FailingRules(database)
        ready.put("ready")
        if not start.wait(15):
            raise AssertionError("test start timed out")
        if not fail:
            if not entered.wait(15):
                raise AssertionError("first approval did not enter")
            attempted.set()
        result = manager.update_feature("job-scale", "candidate-00000", {"status": "approved", "title": "新审批"},
                                        request_key="failed" if fail else "winner", actor_scope="reviewer")
        results.put(("approved", result["decision_id"]))
    except Exception as exc:
        results.put(("failed", type(exc).__name__, str(exc)))


def test_independent_process_approval_survives_older_transaction_failure(review_database):
    database = review_database
    store = seed_candidates(database, count=2)
    context = multiprocessing.get_context("spawn")
    ready, results = context.Queue(), context.Queue()
    start, entered, attempted, release = (context.Event() for _ in range(4))
    location = str(database.path) if database.provider == "sqlite" else database.database_url
    workers = [context.Process(target=_approval_process, args=(
        database.provider, location, str(database.state_root), fail, ready, start, entered, attempted, release, results,
    )) for fail in (True, False)]
    try:
        for worker in workers:
            worker.start()
        assert [ready.get(timeout=20) for _ in workers] == ["ready", "ready"]
        start.set()
        assert entered.wait(15)
        assert attempted.wait(15)
        # The second process may wait for the first transaction. No barrier is
        # placed inside a database lock that both writers must own.
        release.set()
        outcomes = [results.get(timeout=20) for _ in workers]
        assert sorted(item[0] for item in outcomes) == ["approved", "failed"]
        assert [item[1:] for item in outcomes if item[0] == "failed"] == [("RuntimeError", "injected transaction failure")]
        for worker in workers:
            worker.join(timeout=10)
            assert worker.exitcode == 0
    finally:
        release.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
    saved = store.load_candidate("candidate-00000")
    assert saved["status"] == "approved"
    assert saved["title"] == "新审批"
    assert saved["resolved_rule_id"] == SQLiteApprovedRuleStore(database).list_rules()[0]["rule_id"]
    assert store.load_candidate("candidate-00001")["status"] == "approved"
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM approval_decisions").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM rule_versions").fetchone()[0] == 1


@pytest.mark.parametrize("entrypoint", ["register", "save_generated_candidate", "save"])
def test_registration_rechecks_rule_after_another_connection_approves(review_database, entrypoint):
    database = review_database
    store = seed_candidates(database, count=1)
    registering = manager_for(database)
    raw = store.load_candidate("candidate-00000")
    raw.update({"candidate_id": "candidate-new", "status": "pending"})
    assert registering.rule_store.match_feature(raw) == []
    approved = manager_for(database).update_feature("job-scale", "candidate-00000", {"status": "approved"})
    if entrypoint == "register":
        registering.approval_service.register(raw, {}, job=registering._job("job-scale"), record={"source": {}})
    elif entrypoint == "save_generated_candidate":
        store.save_generated_candidate("job-scale", raw)
    else:
        stale_job = registering._job("job-scale")
        stale_job["features"][raw["candidate_id"]] = raw
        store.save(stale_job)
    actual = store.load_candidate("candidate-new")
    assert actual["status"] == "approved"
    assert actual["resolved_rule_id"] == approved["resolved_rule_id"]
    assert store.load_candidate("candidate-00000")["status"] == "approved"


def test_repeat_approval_with_a_new_key_does_not_publish_a_new_rule_version(review_database):
    store = seed_candidates(review_database, count=1)
    manager = manager_for(review_database)
    first = manager.update_feature("job-scale", "candidate-00000", {"status": "approved"}, request_key="first")
    form = {field: first.get(field, "") for field in ("title", "summary", "importance", "tags", "reviewer_note")}
    form.update({"status": "approved", "review_scope": "approval_identity"})
    form["title"] = " " + form["title"] + " "
    second = manager.update_feature("job-scale", "candidate-00000", form, request_key="second",
                                    expected_updated_at=first["updated_at"])
    assert second["resolved_rule_id"] == first["resolved_rule_id"]
    assert second["updated_at"] == first["updated_at"]
    assert store.load_candidate("candidate-00000")["status"] == "approved"
    with review_database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM rule_versions").fetchone()[0] == 1


def test_disabled_rule_and_incomplete_evidence_remain_pending(review_database):
    from logrisk.rule_governance import RuleGovernanceRepository, RuleGovernanceService

    database = review_database
    store = seed_candidates(database, count=1)
    manager = manager_for(database)
    raw = store.load_candidate("candidate-00000")
    approved = manager.update_feature("job-scale", "candidate-00000", {"status": "approved"})
    incomplete = {**raw, "candidate_id": "incomplete", "template_hashes": ["template-0", "missing"]}
    assert store.save_generated_candidate("job-scale", incomplete)["status"] == "pending"
    RuleGovernanceService(RuleGovernanceRepository(database)).change_status(
        approved["resolved_rule_id"], "disabled", expected_version=1, reason="人工停用", operator="reviewer",
    )
    fresh = {**raw, "candidate_id": "after-disabled"}
    assert store.save_generated_candidate("job-scale", fresh)["status"] == "pending"
    previously_matched = {**fresh, "candidate_id": "stale-match", "status": "approved",
                          "rule_id": approved["resolved_rule_id"], "resolution_type": "rule_matched"}
    saved, _ = manager.approval_service.register(previously_matched, {}, job=manager._job("job-scale"), record={"source": {}})
    assert saved["status"] == "pending"


def test_worker_commit_failure_restores_live_references_without_reinserting_candidate(tmp_path, monkeypatch):
    from tests.test_feature_jobs import candidate, entity

    database = SQLiteDatabase(tmp_path / "worker.sqlite3")
    manager = manager_for(database)
    manager.extractor = lambda source, **kwargs: [candidate(source)]
    job_id = manager.create_job({"summary": {}, "risk_entities": [entity("node-a", 90)]}, model="test")
    job = manager._job(job_id)
    record = job["entities"][0]
    original = database.transaction
    failures = []

    @contextmanager
    def fail_registration_commit():
        with original() as connection:
            yield connection
            if not failures and connection.execute("SELECT COUNT(*) FROM approval_group_candidates").fetchone()[0]:
                failures.append(True)
                raise RuntimeError("injected registration commit failure")

    monkeypatch.setattr(database, "transaction", fail_registration_commit)
    manager.run_job(job_id)
    assert failures == [True]
    assert manager._job(job_id) is job
    assert job["entities"][0] is record
    assert record["status"] == "failed"
    assert job["status"] == "completed_with_errors"
    assert job["features"] == {}
    assert manager.persistence.load_job(job_id)["features"] == {}
    assert manager.approval_group_store.list_groups() == []


def test_rule_update_does_not_rewrite_unrelated_stale_snapshots(review_database, monkeypatch):
    from logrisk.rule_governance import RuleGovernanceRepository, RuleGovernanceService

    database = review_database
    store = seed_candidates(database, count=2, distinct=True)
    rules = SQLiteApprovedRuleStore(database)
    first = rules.upsert_feature(store.load_candidate("candidate-00000"))
    second = rules.upsert_feature(store.load_candidate("candidate-00001"))
    stale = rules._read_locked()
    RuleGovernanceService(RuleGovernanceRepository(database)).change_status(
        first["rule_id"], "disabled", expected_version=1, reason="人工停用", operator="reviewer",
    )
    monkeypatch.setattr(rules, "_read_locked", lambda: copy.deepcopy(stale))
    rules.upsert_feature({**store.load_candidate("candidate-00001"), "title": "更新第二条规则"})
    current = {rule["rule_id"]: rule for rule in SQLiteApprovedRuleStore(database).list_rules()}
    assert current[first["rule_id"]]["status"] == "disabled"
    assert current[first["rule_id"]]["current_version"] == 2
    assert current[second["rule_id"]]["title"] == "更新第二条规则"


def test_approval_observations_are_written_after_commit_once(review_database):
    from logrisk.observability import ObservabilityRepository, SpanRecorder

    seed_candidates(review_database, count=2)
    manager = manager_for(review_database)
    recorder = SpanRecorder(ObservabilityRepository(review_database))
    manager.observability = recorder
    manager.update_feature("job-scale", "candidate-00000", {"status": "approved"}, request_key="observed")
    manager.update_feature("job-scale", "candidate-00000", {"status": "approved"}, request_key="observed")
    with review_database.connect() as connection:
        spans = connection.execute("SELECT name, status FROM observability_spans WHERE stage='approval'").fetchall()
    assert sorted((row["name"], row["status"]) for row in spans) == [
        ("manual-review", "success"), ("pending-candidate-reconciled", "success"),
    ]
    assert recorder.failure_count == 0


def test_rolled_back_approval_discards_deferred_observations(review_database, monkeypatch):
    from logrisk.observability import ObservabilityRepository, SpanRecorder

    store = seed_candidates(review_database, count=2)
    manager = manager_for(review_database)
    recorder = SpanRecorder(ObservabilityRepository(review_database))
    manager.observability = recorder
    original = review_database.transaction

    @contextmanager
    def fail_after_decision():
        with original() as connection:
            yield connection
            if connection.execute("SELECT COUNT(*) FROM approval_decisions").fetchone()[0]:
                raise RuntimeError("injected approval commit failure")

    with monkeypatch.context() as context:
        context.setattr(review_database, "transaction", fail_after_decision)
        with pytest.raises(RuntimeError, match="approval commit failure"):
            manager.update_feature("job-scale", "candidate-00000", {"status": "approved"}, request_key="retried")
    with review_database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM observability_spans").fetchone()[0] == 0
    assert store.load_candidate("candidate-00000")["status"] == "pending"
    assert recorder.failure_count == 0
    manager.update_feature("job-scale", "candidate-00000", {"status": "approved"}, request_key="retried")
    with review_database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM observability_spans WHERE name='manual-review'").fetchone()[0] == 1


def test_legacy_file_rule_failure_restores_candidate(tmp_path):
    from logrisk.feature_jobs import FeatureJobFileStore
    from tests.test_feature_jobs import candidate, entity

    class FailingRules(ApprovedRuleStore):
        def upsert_feature(self, feature):
            raise RuntimeError("injected file rule failure")

    store = FeatureJobFileStore(tmp_path / "jobs")
    manager = FeatureJobManager(auto_start=False, persistence=store, rule_store=FailingRules(tmp_path / "rules.json"),
                                extractor=lambda source, **kwargs: [candidate(source)])
    job_id = manager.create_job({"summary": {}, "risk_entities": [entity("node-a", 90)]}, model="test")
    manager.run_job(job_id)
    before = store.load_candidate("feature-node-a")
    with pytest.raises(RuntimeError, match="file rule failure"):
        manager.update_feature(job_id, "feature-node-a", {"status": "approved", "title": "失败请求标题"})
    after = store.load_candidate("feature-node-a")
    assert after["status"] == "pending"
    assert after["title"] == before["title"]
    assert after.get("resolved_rule_id") is None


def test_legacy_file_failure_cannot_revert_a_newer_successful_approval(tmp_path):
    from logrisk.feature_jobs import FeatureJobFileStore
    from tests.test_feature_jobs import candidate, entity

    winner = None

    class InterleavedFailure(ApprovedRuleStore):
        def upsert_feature(self, feature):
            winner.update_feature(feature["job_id"], feature["candidate_id"],
                                  {"status": "approved", "title": "新审批胜者"})
            raise RuntimeError("injected older file failure")

    store = FeatureJobFileStore(tmp_path / "jobs")
    rules_path = tmp_path / "rules.json"
    manager = FeatureJobManager(auto_start=False, persistence=store, rule_store=InterleavedFailure(rules_path),
                                extractor=lambda source, **kwargs: [candidate(source)])
    job_id = manager.create_job({"summary": {}, "risk_entities": [entity("node-a", 90)]}, model="test")
    manager.run_job(job_id)
    winner = FeatureJobManager(auto_start=False, persistence=FeatureJobFileStore(store.root), rule_store=ApprovedRuleStore(rules_path))
    with pytest.raises(RuntimeError, match="older file failure"):
        manager.update_feature(job_id, "feature-node-a", {"status": "approved", "title": "旧请求标题"})
    saved = store.load_candidate("feature-node-a")
    assert saved["status"] == "approved"
    assert saved["title"] == "新审批胜者"
    assert saved["resolved_rule_id"] == winner.rule_store.list_rules()[0]["rule_id"]
    assert manager._job(job_id)["features"]["feature-node-a"]["title"] == "新审批胜者"
    with pytest.raises(FeatureJobError) as conflict:
        store.rollback_candidate_review_state("feature-node-a", {"status": "pending"}, expected_updated_at=None)
    assert conflict.value.code == "candidate_state_conflict"
    assert store.load_candidate("feature-node-a")["status"] == "approved"
