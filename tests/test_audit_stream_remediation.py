from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.multi_source import repository as multi_source
from logrisk.runtime import service as runtime_service
from logrisk.runtime.config import RuntimeConfig
from logrisk.server_risk.contracts import ServerRiskError
from tests.server_risk_fixtures import batch, domain_services, observation


@pytest.mark.parametrize(("mixed", "committed"), [(False, False), (True, False), (True, True)])
def test_high_cardinality_risk_observations_admit_and_continue_current_recovery(tmp_path, mixed, committed):
    service, now, _, policies, _ = domain_services(tmp_path)
    if committed:
        policies.sources["fixture-source"]["node_ids"].append("node-alias")
        alias = observation(10_003, now["value"])
        alias["identity"].update(node_id="node-alias", node_uid="node-01")
        assert service.observations.submit(batch("alias", alias), "fixture-actor")["projection_complete"]
    for start in range(0, 10_000, 500):
        service.observations.submit(
            batch(f"seed-{start}", *(
                observation(seq, now["value"], kind="recovery_candidate" if mixed and seq % 2 else "adverse")
                for seq in range(start, start + 500)
            )),
            "fixture-actor", defer_persist=True,
            physical_keys=[f"position-{seq}" for seq in range(start, start + 500)] if committed else None,
        )
    payload = batch("next", observation(10_000, now["value"]))
    receipt = service.observations.submit(payload, "fixture-actor")
    assert receipt["accepted_count"] == 1 and receipt["projection_complete"]
    replay = service.observations.submit(payload, "fixture-actor")
    assert replay["idempotent_replay"] and replay["projection_complete"]
    check = service.assessment("prod-a", "node-01")["checks"][0]
    assert check["status"] == "present" and check["evidence_count"] == 10_001
    assert check["evidence_refs_bounded"] and len(check["evidence_refs"]) <= 3
    with pytest.raises(ServerRiskError, match="资源上限"):
        service.repository.observations("prod-a", "node-01")

    now["value"] = (datetime.fromisoformat(now["value"]) + timedelta(seconds=5)).isoformat()
    positive = batch("recovering", observation(10_001, now["value"], kind="state_snapshot"))
    assert service.observations.submit(positive, "fixture-actor")["projection_complete"]
    assert service.assessment("prod-a", "node-01")["checks"][0]["status"] == "recovering"
    first_episode = service.assessment("prod-a", "node-01")["episodes"][0]["episode_id"]
    now["value"] = (datetime.fromisoformat(now["value"]) + timedelta(seconds=10)).isoformat()
    assert service.observations.submit(
        batch("clear", observation(10_002, now["value"], kind="state_snapshot")), "fixture-actor",
    )["projection_complete"]
    recovered = service.assessment("prod-a", "node-01")
    assert recovered["checks"][0]["status"] == "clear"
    assert recovered["episodes"][0]["episode_id"] == first_episode
    assert recovered["episodes"][0]["resolved_at"] == now["value"]
    with service.repository.database.connect() as conn:
        saved = conn.execute("SELECT lifecycle,resolved_at FROM server_risk_episodes WHERE episode_id=?", (first_episode,)).fetchone()
        assert dict(saved) == {"lifecycle": "resolved", "resolved_at": now["value"]}
        if committed:
            assert conn.execute(
                "SELECT lifecycle FROM server_risk_episodes WHERE cluster='prod-a' AND node_id='node-alias'"
            ).fetchone()[0] == "open"
            assert conn.execute(
                "SELECT COUNT(*) FROM server_risk_projection_state WHERE input_namespace LIKE 'assessment:%'"
            ).fetchone()[0] == 2


def test_correlation_reads_only_bounded_projected_candidate_rows(tmp_path, monkeypatch):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repo = multi_source.MultiSourceRepository(database)
    before = datetime(2026, 11, 24, 5, tzinfo=timezone.utc)
    record = {"cluster": "prod", "source_type": "kernel", "source_family": "kernel",
              "component": "kernel", "template_hash": "sha-fixture", "template": "safe template",
              "count": 1, "risk_score": 90.0, "risk_level": "high", "entity_keys": ["node-a"]}
    for position, when in enumerate(before + timedelta(minutes=delta) for delta in (-120, -5, -1, 0, 1, 5, 120)):
        repo.save_observation(dict(record, observation_id=str(position), window_start=when.isoformat(), window_end=when.isoformat()))

    def reject_decoding(_value):
        pytest.fail("candidate cap must be reached before deserialization")

    sample = dict(cluster="prod", since=before - timedelta(minutes=1), through=before + timedelta(minutes=1),
                  families={"kernel"}, min_risk=20.0, min_count=1)
    with monkeypatch.context() as patch:
        patch.setattr(multi_source, "_array", reject_decoding)
        assert repo.correlation_observations(**sample, limit=2) is None
    assert [row["observation_id"] for row in repo.correlation_observations(**sample, limit=3)] == ["2", "3", "4"]


def test_run_center_decodes_only_the_requested_page(tmp_path, monkeypatch):
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    runtime = runtime_service.RuntimeService(
        database, state_root=tmp_path / "state", output_root=tmp_path / "output",
        config=RuntimeConfig.from_mapping({}),
    )
    with database.transaction() as conn:
        conn.executemany(
            "INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,"
            "stage,cursor_json,task_json,created_at,updated_at) VALUES (?,'file','{}','fixture',?,"
            "'AGGREGATING','{}','{\"progress\":71}','2026-11-24T00:00:00+00:00','2026-11-24T01:00:00+00:00')",
            [(f"task-{index:05}", "failed" if index % 2 else "running") for index in range(1_000)],
        )
    decode = runtime_service._object
    seen = []

    def traced(value):
        seen.append(value)
        return decode(value)

    monkeypatch.setattr(runtime_service, "_object", traced)
    result = runtime.list_tasks(kind="streaming", status="failed", page=2, page_size=3)
    assert result["total"] == 500
    assert [row["task_id"] for row in result["items"]] == ["task-00993", "task-00991", "task-00989"]
    assert all(row["actions"] == ["view", "resume"] for row in result["items"])
    assert len(seen) == 3


def test_capacity_view_reuses_recent_measurement_while_admission_reads_fresh(tmp_path, monkeypatch):
    runtime = runtime_service.RuntimeService(
        SQLiteDatabase(tmp_path / "logrisk.sqlite3"), state_root=tmp_path / "state",
        output_root=tmp_path / "output",
        config=RuntimeConfig.from_mapping({"quota": {"soft_limit_bytes": 4, "hard_limit_bytes": 8}}),
    )
    counter = [0.0]
    monkeypatch.setattr(runtime_service, "monotonic", lambda: counter[0])
    assert runtime.storage_usage()["total_bytes"] == 0
    (runtime.state_root / "fixture").write_bytes(b"123456789")
    assert runtime.storage_usage()["total_bytes"] == 0
    with pytest.raises(runtime_service.RuntimeQuotaError):
        runtime.require_capacity("fixture operation")
    counter[0] += runtime_service.STORAGE_READ_TTL_SECONDS
    assert runtime.storage_usage()["total_bytes"] == 9


def test_risk_overview_scopes_same_node_legacy_events_by_overlapping_date(tmp_path):
    service, *_ = domain_services(tmp_path)
    old = "2026-01-01T00:00:00+00:00"
    recent = "2026-08-31T06:00:00+00:00"
    with service.repository.database.transaction() as conn:
        sql = (
            "INSERT INTO node_risk_events(event_id,cluster,node_id,risk_domain,risk_category,risk_type,severity,"
            "base_score,confidence,semantic_rule_id,semantic_rule_version,dedup_key,occurrence_count,first_seen,"
            "last_seen,window_start,window_end,status,semantic_fields_json,evidence_refs_json,created_at,updated_at) "
            "VALUES (?,'prod-a','node-01','service','service','service.failed','high',60,.8,'fixture',1,"
            "?,1,?,?,?,?,'active','{}','[]',?,?)"
        )
        conn.executemany(sql, [(f"event-{i}", f"key-{i}", *([old] * 6)) for i in range(10_000)])
        conn.execute(sql, ("new", "new", *([recent] * 6)))
        conn.execute(sql, ("overlap", "overlap", old, recent, old, recent, old, recent))

    daily = service.overview_daily_summaries(["2026-08-31"], today_limit=10)[0]
    overview = service.overview("2026-08-31", "2026-08-31", "Asia/Shanghai")
    assert daily["summary"] == overview["summary"]
    assert overview["summary"]["host_count"] == 1
    assert overview["summary"]["known_occurrence_count"] == 1
    assert "boundary_ambiguous" in overview["quality"]["missing_reasons"]
    assert len(service.overview_detail("prod-a", "node-01", "2026-08-31", "2026-08-31")["groups"]) == 2
    with pytest.raises(ServerRiskError, match="资源上限"):
        service.repository.facts("prod-a", "node-01")


def test_paged_subject_replay_preserves_reported_node_scope_with_a_shared_uid(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["node_ids"].append("node-alias")
    first = observation(1, now["value"])
    first["identity"]["node_uid"] = "stable-node-uid"
    alias = observation(2, now["value"])
    alias["identity"].update(node_id="node-alias", node_uid="stable-node-uid")
    for key, item in (("node-01", first), ("node-alias", alias)):
        assert service.observations.submit(batch(key, item), "fixture-actor")["projection_complete"]
    original = service.repository.observations("prod-a", "node-01")[0]
    with service.repository.current_snapshot() as conn:
        check, _ = service.assessments.current_subject(
            "prod-a", "node-01", original["subject_key"], datetime.fromisoformat(now["value"]), conn, paged=True,
        )
        episodes = conn.execute(
            "SELECT node_id,lifecycle,subject_key FROM server_risk_episodes ORDER BY node_id"
        ).fetchall()
        assert [(row["node_id"], row["lifecycle"], row["subject_key"]) for row in episodes] == [
            ("node-01", "open", original["subject_key"]), ("node-alias", "open", original["subject_key"]),
        ]
        assert conn.execute(
            "SELECT COUNT(*) FROM server_risk_projection_state WHERE input_namespace LIKE 'assessment:%'"
        ).fetchone()[0] == 2
    assert check["status"] == "present"
    assert check["evidence_count"] == 1
    assert check["evidence_refs"] == [original["observation_id"]]
    first_decision = service.repository.decisions("prod-a", "node-01")
    alias_decision = service.repository.decisions("prod-a", "node-alias")
    assert [decision["decision_ref"] for decision in first_decision] == [check["decision_ref"]]
    assert len(alias_decision) == 1 and alias_decision[0]["superseded_by"] is None
    assert alias_decision[0]["decision_ref"] != check["decision_ref"]


def test_scoped_risk_migration_preserves_legacy_ids_and_requires_consistent_binding(tmp_path, monkeypatch):
    from pathlib import Path
    from shutil import copyfile

    from logrisk.server_risk.contracts import digest, json_text
    from logrisk import database as database_module

    migrations = tmp_path / "migrations"
    migrations.mkdir()
    source = Path("database/migrations")
    for path in source.glob("*.sql"):
        if path.name[:4] < "0035":
            copyfile(path, migrations / path.name)
    monkeypatch.setattr(database_module, "SQLiteDatabase", lambda path: SQLiteDatabase(path, migrations_dir=migrations))
    service, now, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["node_ids"].append("node-alias")
    first = observation(1, now["value"])
    first["identity"]["node_uid"] = "stable-node-uid"
    alias = observation(2, now["value"])
    alias["identity"].update(node_id="node-alias", node_uid="stable-node-uid")
    repository = service.repository
    payload = batch("legacy", first, alias)
    receipt_digest = digest(payload)
    registration = policies.source("fixture-source")
    # Populate the frozen pre-0035 layout directly, including its admitted
    # batch proof. There is no current-assessment checkpoint table yet.
    with repository.database.transaction() as conn:
        conn.execute(
            "INSERT INTO server_risk_projection_state(input_namespace,input_revision,status,updated_at) "
            "VALUES (?,?,'complete',?)", ("batch:" + digest(["fixture-source", "legacy"]), receipt_digest, now["value"]),
        )
        for row in (first, alias):
            item = service.observations.validate(row, payload, registration, now["value"])
            conn.execute(
                "INSERT INTO server_risk_observations(observation_id,source_fact_key,source_id,source_epoch,"
                "config_revision,semantic_revision,subject_key,kind,interval_start,interval_end,received_at,mode,"
                "quality,sanitized_payload_json,payload_digest,batch_id,idempotency_key,batch_digest) "
                "VALUES (?,?,'fixture-source','epoch-1','config-1','config-1',?,?,?,?,?,'current',"
                "'observed_interval',?,?,'legacy','legacy',?)",
                (item["observation_id"], item["source_fact_key"], item["subject_key"], row["kind"],
                 item["start"], item["end"], now["value"], json_text(item["payload"]), item["payload_digest"], receipt_digest),
            )
    a = repository.observations("prod-a", "node-01")[0]
    b = repository.observations("prod-a", "node-alias")[0]
    subject = a["subject_key"]
    a_ref, b_ref = a["observation_id"], b["observation_id"]
    episode_id = digest([subject, a_ref])
    with repository.database.transaction() as conn:
        conn.execute(
            "INSERT INTO server_risk_episodes(episode_id,subject_key,risk_type,object_identity_json,"
            "first_adverse,last_adverse,lifecycle,latest_revision) VALUES (?,?,?,?,?,?,'open','legacy')",
            (episode_id, subject, "service.failed", json_text(first["identity"]), now["value"], now["value"]),
        )
        rows = [
            ("episode-evidence", episode_id, json_text([a_ref])),
            ("episode-alone", episode_id, "[]"),
            ("alias-evidence", None, json_text([b_ref])),
            ("mixed-evidence", episode_id, json_text([a_ref, b_ref])),
            ("mismatched", episode_id, json_text([b_ref])),
            ("incomplete-evidence", None, json_text([a_ref, "missing"])),
            ("invalid-evidence", episode_id, "[invalid"),
        ]
        conn.executemany(
            "INSERT INTO server_risk_decisions(decision_id,episode_id,subject_key,revision,effective_at,"
            "decided_at,state,reason_codes_json,evidence_refs_json,policy_version) "
            "VALUES (?,?,?,?,?,?,'present','[]',?,'legacy')",
            [(key, ep, subject, key, now["value"], now["value"], evidence) for key, ep, evidence in rows],
        )
        conn.execute(
            "UPDATE server_risk_decisions SET superseded_by='alias-evidence' WHERE decision_id='episode-evidence'"
        )
        old_rows = [tuple(row) for row in conn.execute(
            "SELECT decision_id,episode_id,subject_key,evidence_refs_json,superseded_by FROM server_risk_decisions ORDER BY decision_id"
        )]
        old_facts = [tuple(row) for row in conn.execute("SELECT * FROM server_risk_observations ORDER BY observation_id")]
    migration = "0035_scoped_runtime_reads.sql"
    copyfile(source / migration, migrations / migration)
    repository.database.migrate()
    repository.database.migrate()
    with repository.database.connect() as conn:
        assert [tuple(row) for row in conn.execute("SELECT * FROM server_risk_observations ORDER BY observation_id")] == old_facts
        assert [tuple(row) for row in conn.execute(
            "SELECT decision_id,episode_id,subject_key,evidence_refs_json,superseded_by FROM server_risk_decisions ORDER BY decision_id"
        )] == old_rows
        assert {row[0] for row in conn.execute("SELECT decision_id FROM server_risk_decisions WHERE node_id IS NULL")} == {
            "mixed-evidence", "mismatched", "incomplete-evidence", "invalid-evidence",
        }
    migrated = repository.decisions("prod-a", "node-01")
    assert {row["decision_ref"] for row in migrated} == {"episode-evidence", "episode-alone"}
    assert all(row["superseded_by"] is None for row in migrated)
    assert [row["decision_ref"] for row in repository.decisions("prod-a", "node-alias")] == ["alias-evidence"]
    new = observation(3, now["value"])
    new["identity"].update(node_id="node-alias", node_uid="stable-node-uid")
    assert service.observations.submit(batch("after-upgrade", new), "fixture-actor")["projection_complete"]
    with repository.database.connect() as conn:
        assert conn.execute("SELECT lifecycle FROM server_risk_episodes WHERE episode_id=?", (episode_id,)).fetchone()[0] == "open"
        assert {row["node_id"] for row in conn.execute("SELECT node_id FROM server_risk_episodes WHERE lifecycle='open'")} == {
            "node-01", "node-alias",
        }


def test_current_risk_checkpoint_uses_suffix_and_rebuilds_for_late_facts(tmp_path, monkeypatch):
    from logrisk.server_risk import assessment, repository

    monkeypatch.setattr(repository, "MAX_SCOPE_ROWS", 5)
    monkeypatch.setattr(assessment, "MAX_SCOPE_ROWS", 5)
    service, clock, *_ = domain_services(tmp_path)
    noon = datetime.fromisoformat(clock["value"])

    def instant(seconds):
        return (noon + timedelta(seconds=seconds)).isoformat()

    seed = [observation(index, instant(-30 + index), kind="adverse" if index % 2 == 0 else "recovery_candidate") for index in range(6)]
    future = observation(6, instant(-10), kind="recovery_candidate")
    del future["occurred_at"]
    future.update(interval_start=instant(-10), interval_end=instant(20))
    service.observations.submit(batch("seed", *seed, future), "fixture-actor", defer_persist=True)
    original = service.repository.iter_subject_observations
    emitted = []

    def monitor(*args, **kwargs):
        for row in original(*args, **kwargs):
            emitted.append(row["observation_id"])
            yield row

    monkeypatch.setattr(service.repository, "iter_subject_observations", monitor)

    def submit(label, seq, at, kind="adverse", *, event_at=None):
        clock["value"] = instant(at)
        emitted.clear()
        item = observation(seq, instant(event_at if event_at is not None else at), kind=kind)
        assert service.observations.submit(batch(label, item), "fixture-actor")["projection_complete"]

    submit("cold", 7, 0)
    assert len(emitted) == 7
    submit("warm", 8, 1)
    assert len(emitted) == 1
    submit("future-crossed", 9, 21)
    assert len(emitted) == 10
    submit("recovering", 10, 22, "state_snapshot")
    assert len(emitted) == 1
    submit("clear", 11, 32, "state_snapshot")
    assert len(emitted) == 1
    assert service.assessment("prod-a", "node-01")["checks"][0]["status"] == "clear"
    submit("late", 12, 33, event_at=26)
    assert len(emitted) > 10
    check = service.assessment("prod-a", "node-01")["checks"][0]
    assert check["status"] == "recovering"
    assert check["evidence_count"] == 13
    with service.repository.database.connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM server_risk_decisions WHERE decision_id=? AND superseded_by IS NULL",
            (check["decision_ref"],),
        ).fetchone()[0] == 1


def test_scoped_risk_overview_uses_point_lookup_for_older_physical_contributions(tmp_path, monkeypatch):
    from tests.server_risk_fixtures import committed_window, seed_committed
    from tests.test_risk_overview import seed_event

    service, *_ = domain_services(tmp_path)
    repository = service.repository
    database = repository.database
    old, new = "2026-01-01T00:00:00+00:00", "2026-08-31T06:00:00+00:00"
    old_window, new_window = committed_window(0, old), committed_window(1, new)
    seed_committed(database, "old-linked", [old_window])
    seed_committed(database, "new-linked", [new_window])
    seed_committed(database, "unrelated-history", [
        committed_window(index, old, entity_id="unrelated") for index in range(1_000, 2_000)
    ])
    seed_event(service, "recurring", first=old, last=new, count=2)
    keys = [repository._physical({"window_id": "fixture"}, item) for item in (old_window, new_window)]
    with database.transaction() as conn:
        conn.executemany(
            "INSERT INTO node_risk_ingestions(source_event_fingerprint,event_id,occurrence_count,ingested_at,physical_key) "
            "VALUES (?,'recurring',1,?,?)",
            [("old-link", old, keys[0]), ("new-link", new, keys[1])],
        )
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT window_json FROM streaming_batch_windows "
            "WHERE logrisk_window_physical(window_json,window_id)=?", (keys[0],),
        ).fetchall()
        assert any("idx_streaming_windows_physical_lookup" in row[3] for row in plan)
    old_read = repository._window_pages
    by_key = repository._physical_window_pages
    visited = []

    def dated_only(**kwargs):
        assert kwargs.get("bounds") is not None
        return old_read(**kwargs)

    def point_lookup(keys):
        for row in by_key(keys):
            visited.append(row["window_id"])
            yield row

    monkeypatch.setattr(repository, "_window_pages", dated_only)
    monkeypatch.setattr(repository, "_physical_window_pages", point_lookup)
    summary = service.overview_daily_summaries(["2026-08-31"], today_limit=10)[0]["summary"]
    assert summary["host_count"] == summary["known_occurrence_count"] == 1
    assert visited == ["old-linked"]


@pytest.mark.parametrize("existing", [False, True])
def test_future_receipt_retries_after_maturity_without_read_side_effect_or_duplicates(tmp_path, monkeypatch, existing):
    from concurrent.futures import ThreadPoolExecutor

    from logrisk.server_risk import assessment, repository

    service, clock, *_ = domain_services(tmp_path)
    start = datetime.fromisoformat(clock["value"])
    if existing:
        monkeypatch.setattr(repository, "MAX_SCOPE_ROWS", 5)
        monkeypatch.setattr(assessment, "MAX_SCOPE_ROWS", 5)
        items = [observation(i, (start - timedelta(seconds=10 - i)).isoformat()) for i in range(6)]
        service.observations.submit(batch("seed", *items), "fixture-actor", defer_persist=True)
        assert service.observations.submit(batch("cold", observation(6, clock["value"])), "fixture-actor")["projection_complete"]
    future = observation(7, (start + timedelta(seconds=10)).isoformat(), typed_fields={"severity": "critical"})
    payload = batch("future", future)
    first = service.observations.submit(payload, "fixture-actor")
    assert first["accepted_count"] == 1 and not first["projection_complete"]

    def unexpected_revalidation(*_args):
        pytest.fail("already proven facts must be resumed without running intake validation")

    monkeypatch.setattr(service.observations, "validate", unexpected_revalidation)
    clock["value"] = (start + timedelta(seconds=9)).isoformat()
    early = service.observations.submit(payload, "fixture-actor")
    assert early["idempotent_replay"] and not early["projection_complete"]
    clock["value"] = future["occurred_at"]
    # As-of GET can already describe the eligible evidence and must not write.
    check = service.assessment("prod-a", "node-01")["checks"][0]
    assert check["status"] == "present" and check["severity"] == "critical"
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT 1 FROM server_risk_decisions WHERE decision_id=?", (check["decision_ref"],)).fetchone() is None

    def retry(_index):
        return service.observations.submit(payload, "fixture-actor")

    with ThreadPoolExecutor(max_workers=2) as pool:
        retries = list(pool.map(retry, range(2)))
    assert all(receipt["idempotent_replay"] and receipt["projection_complete"] for receipt in retries)
    assert all("accepted_count" not in receipt for receipt in retries)
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM server_risk_observations").fetchone()[0] == (8 if existing else 1)
        assert conn.execute("SELECT COUNT(*) FROM server_risk_decisions WHERE decision_id=?", (check["decision_ref"],)).fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM server_risk_projection_state WHERE status='pending'").fetchone()[0] == 0


def test_committed_due_receipt_recovers_before_normal_scan_wrap_and_cancellation_is_atomic(tmp_path):
    from tests.server_risk_fixtures import commit_source, committed_window, seed_committed

    service, clock, *_ = domain_services(tmp_path)
    adapter, _ = commit_source(service)
    future = (datetime.fromisoformat(clock["value"]) + timedelta(seconds=10)).isoformat()
    seed_committed(service.repository.database, "future", [committed_window(1, future)])
    first = adapter.tick()
    assert len(first) == 1 and first[0]["accepted_count"] == 1 and not first[0]["projection_complete"]
    early = adapter.advance("fixture-source", "fixture-task", "future")
    assert early["idempotent_replay"] and not early["projection_complete"]
    clock["value"] = future
    stopped = False

    def cancel_during_projection(stage, _details):
        nonlocal stopped
        if stage == "deterministic_recovery_and_expiry":
            stopped = True

    adapter.stage_hook = cancel_during_projection
    adapter.cancelled = lambda: stopped
    with pytest.raises(ServerRiskError) as exc:
        adapter.advance("fixture-source", "fixture-task", "future")
    assert exc.value.code == "commit_cancelled"
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM server_risk_decisions").fetchone()[0] == 0
    adapter.cancelled = lambda: False
    adapter.stage_hook = None
    result = adapter.tick()
    assert result == [{"commit_ref": "future", "projection_complete": True, "idempotent_replay": True}]
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM server_risk_decisions").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM server_risk_observations").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM server_risk_projection_state WHERE status='pending'").fetchone()[0] == 0
