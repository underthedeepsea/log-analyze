from __future__ import annotations

import base64
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from logrisk.agentic.analysis_view import entity_analysis_view
from logrisk.agentic.errors import AgenticError
from logrisk.agentic.models import AgentRunRequest, scoped_evidence_summary
from logrisk.agentic.repository import AgentRepository
from logrisk.agentic.tool_registry import AgentToolContext
from logrisk.agentic.tools import build_agent_tool_registry
from logrisk.application import ApplicationConfig, build_application_container
from logrisk.application.api import ApiFacade
from logrisk.database import SQLiteDatabase
from logrisk.entity_identity import entity_key
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.sqlite_stores import SQLiteApprovedRuleStore, SQLiteFeatureJobStore
from logrisk.streaming_results import StreamingResultRepository
from logrisk.streaming_state import StreamingStateRepository
from tests.test_approval_deduplication import candidate, entity


def source(name: str, cluster: str, kind: str, count: int, hour: int = 10) -> dict:
    window = entity(name, f"2026-06-22T{hour}:00:00+08:00")
    window.update(cluster=cluster, entity_type=kind)
    window["top_templates"] = [{
        "template_hash": "common",
        "template": "CNI no enough IPs while creating pod sandbox",
        "component": "kubelet",
        "count": count,
    }]
    return window


def result_document(tmp_path, database, sources, paged):
    if not paged:
        return {"risk_entities": sources}
    path = tmp_path / "scope-fixture.log"
    path.write_text("fixture", encoding="utf-8")
    state = StreamingStateRepository(database)
    task = state.create_or_load(
        descriptor=FileIncrementalSource(path, filename=path.name).descriptor(), config_hash="a" * 64,
    )
    state.commit_window(
        task["task_id"], window_id="batch-1", cursor=SourceCursor("file", {"offset": 1}),
        templates=[], windows=[
            {**{key: value for key, value in row.items() if key != "top_templates"},
             **row["top_templates"][0], "severity": "ERROR"} for row in sources
        ], summary={"record_count": sum(item["top_templates"][0]["count"] for item in sources)},
    )
    ref = StreamingResultRepository(database).build(task["task_id"], {})
    return {"result_ref": ref, "risk_entities": []}


@pytest.mark.parametrize("paged", [False, True])
def test_entity_scope_survives_evidence_retry_approval_and_reload(tmp_path, paged):
    database = SQLiteDatabase(tmp_path / "scopes.sqlite")
    east, east_later = source("重复名称", "east", "node", 4), source("重复名称", "east", "node", 2, 11)
    west, pod = source("重复名称", "west", "node", 9), source("重复名称", "east", "pod", 10)

    def extract(row, **_kwargs):
        if row["cluster"] == "west":
            raise RuntimeError("fixture extraction failure")
        return [candidate(row, f"candidate-{row['cluster']}-{row['entity_type']}")]

    def manager(extractor=extract):
        return FeatureJobManager(
            extractor=extractor, auto_start=False, interrupt_on_restore=False,
            persistence=SQLiteFeatureJobStore(database), rule_store=SQLiteApprovedRuleStore(database),
        )

    jobs = manager()
    document = result_document(tmp_path, database, [east, west, east_later, pod], paged)
    job_id = jobs.create_job(document, model="fake", min_score=0)
    jobs.run_job(job_id)
    jobs = manager(lambda row, **_: [candidate(row, "candidate-west-node")])
    details = jobs.get_job(job_id)
    assert {item["entity_key"]: item["log_count"] for item in details["entities"]} == {
        entity_key(east): 6, entity_key(west): 9, entity_key(pod): 10,
    }

    with pytest.raises(FeatureJobError) as exc:
        jobs.get_agent_evidence(job_id, east["entity_id"])
    assert exc.value.status_code == 409 and exc.value.code == "ambiguous_entity"
    with pytest.raises(FeatureJobError, match="同名"):
        jobs.retry_entity(job_id, east["entity_id"], start=False)

    evidence = jobs.get_agent_evidence(job_id, west["entity_id"], entity_scope_key=entity_key(west))
    assert evidence["entity"] == {"id": west["entity_id"], "type": "node", "cluster": "west"}
    assert [item["count"] for item in evidence["templates"]] == [9]
    tools = build_agent_tool_registry(jobs, None, None)
    context = AgentToolContext(
        "run", job_id, west["entity_id"], frozenset({"get_sanitized_evidence"}), "fixture", "request",
        entity_scope_key=entity_key(west),
    )
    assert tools.execute("get_sanitized_evidence", {"job_id": job_id, "entity_id": west["entity_id"]}, context) == evidence
    jobs.retry_entity(job_id, west["entity_id"], start=False, entity_scope_key=entity_key(west))
    jobs.run_job(job_id, west["entity_id"], entity_scope_key=entity_key(west))
    details = jobs.get_job(job_id)
    assert {item["status"] for item in details["entities"]} == {"completed"}
    assert {item["candidate_id"] for item in details["features"]} == {
        "candidate-east-node", "candidate-east-pod", "candidate-west-node",
    }
    assert {item["status"] for item in details["features"]} == {"pending"}
    approved = jobs.update_feature(job_id, "candidate-west-node", {"status": "approved"})
    assert approved["entity"] == {"type": "node", "id": west["entity_id"]}
    assert approved["cluster"] == "west"
    assert [item["count"] for item in approved["source_templates"]] == [9]


def test_scoped_reference_paginates_same_name_and_legacy_ambiguous_reference_is_refused(tmp_path):
    database = SQLiteDatabase(tmp_path / "pages.sqlite")
    east, west = source("same", "east", "node", 4), source("same", "west", "node", 9)
    rows = [source(f"other-{index:03d}", "east", "node", 1) for index in range(99)] + [east, west]
    document = result_document(tmp_path, database, rows, paged=True)
    store = SQLiteFeatureJobStore(database)
    manager = FeatureJobManager(auto_start=False, persistence=store)
    job_id = manager.create_job(document, model="fake")
    first = manager.get_job(job_id)
    second = manager.get_job(job_id, cursor=first["next_cursor"])
    selected = [item for page in (first, second) for item in page["entities"] if item["entity_id"] == "same"]
    assert {item["entity_key"] for item in selected} == {entity_key(east), entity_key(west)}
    assert len(first["entities"]) == 100 and len(second["entities"]) == 1
    repo = StreamingResultRepository(database)
    ref = document["result_ref"]
    unambiguous = next(item for item in repo.feature_sources(ref) if item["entity_id"] == "other-000")
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO streaming_feature_entities(task_id,generation,entity_id,source_json) VALUES (?,?,?,?)",
            (ref["task_id"], ref["generation"], unambiguous["entity_id"], json.dumps(unambiguous)),
        )
    assert repo.feature_source(dict(ref, entity_id="other-000"))["cluster"] == "east"
    assert repo.feature_source(dict(ref, entity_id="other-001"))["cluster"] == "east"
    with pytest.raises(FeatureJobError, match="重新创建"):
        repo.feature_source(dict(ref, entity_id="same"))
    v1 = base64.urlsafe_b64encode(json.dumps({
        "version": 1, "job_id": job_id, "entity": "other-098", "feature": "",
    }).encode()).decode()
    assert {item["entity_key"] for item in manager.get_job(job_id, cursor=v1)["entities"]} == {
        entity_key(east), entity_key(west),
    }


def test_agent_run_scope_and_analysis_route_keep_same_name_separate(tmp_path):
    app = build_application_container(ApplicationConfig.for_test(project_root=Path.cwd(), state_root=tmp_path))
    jobs = app.feature_jobs
    east, west = source("same", "east", "node", 4), source("same", "west", "node", 9)
    jobs.auto_start = False
    job_id = jobs.create_job({"risk_entities": [east, west]}, model="fake")
    api = ApiFacade(app, version="fixture")
    assert api.entity_analysis(job_id, "same").status == 409
    detail = api.entity_analysis(job_id, "same", entity_scope_key=entity_key(west))
    assert detail.status == 200
    assert detail.body["facts"]["entity"]["cluster"] == "west"

    repository = AgentRepository(app.database)
    run_request = AgentRunRequest(
        source_job_id=job_id, entity_id="same", entity_type="node", model_profile_id=app.model_profiles.default_profile_id,
        prompt_id="agent_plan_v1", max_steps=4, max_tool_calls=6, timeout_seconds=120, allowed_tools=("get_sanitized_evidence",),
        idempotency_key="scope-test", actor="fixture", roles=(), request_id="request",
    )
    def snapshot(row):
        evidence = jobs.get_agent_evidence(job_id, "same", entity_scope_key=entity_key(row))
        return {"evidence_summary": scoped_evidence_summary(evidence)}
    first = repository.create_run(run_request, locked_snapshot=snapshot(east))
    with pytest.raises(AgenticError, match="跨实体"):
        repository.create_run(run_request, locked_snapshot=snapshot(west))
    with pytest.raises(AgenticError, match="跨任务/实体"):
        entity_analysis_view(jobs, SimpleNamespace(repository=repository), job_id, "same", first["run_id"],
                             entity_scope_key=entity_key(west))


@pytest.mark.parametrize("has_template_scope", [False, True])
def test_sqlite_migration_preserves_historical_approvals_and_refuses_mixed_evidence(tmp_path, has_template_scope):
    legacy_migrations = tmp_path / "legacy-migrations"
    legacy_migrations.mkdir()
    for path in Path("database/migrations").glob("*.sql"):
        if path.name[:4] <= "0033":
            shutil.copyfile(path, legacy_migrations / path.name)
    path = tmp_path / "version-33.sqlite"
    legacy = SQLiteDatabase(path, migrations_dir=legacy_migrations)
    east = source("same-host", "cluster-east", "node", 4)
    west = source("same-host", "cluster-west", "node", 9)
    for item in (east, west):
        cluster = item["cluster"]
        item["top_templates"].append({
            "template_hash": cluster, "template": "only-" + cluster, "component": "kernel", "count": 1,
            **({"cluster": cluster} if has_template_scope else {}),
        })
    # Output of the 1.48.2 collapse by bare name: the higher-risk west row
    # owns both exclusive templates and the shared hash's combined count.
    combined = dict(west["top_templates"][0], count=13)
    mixed = dict(west, top_templates=[combined, west["top_templates"][1], east["top_templates"][1]])
    row = {key: value for key, value in west.items() if key != "top_templates"}
    row.update(source=mixed, log_count=15, status="interrupted", feature_ids=["prior-reviewed", "prior-pending"])
    timestamp = "2026-06-22T10:00:00+08:00"
    reviewed = dict(candidate(west, "prior-reviewed"), status="approved")
    pending = candidate(west, "prior-pending")
    with legacy.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id,status,job_json,created_at,updated_at) VALUES (?,?,?,?,?)",
            ("history", "completed", "{}", timestamp, timestamp),
        )
        connection.execute(
            "INSERT INTO feature_job_entities(job_id,entity_id,status,entity_json,updated_at) VALUES (?,?,?,?,?)",
            ("history", west["entity_id"], "interrupted", json.dumps(row), timestamp),
        )
        connection.executemany(
            "INSERT INTO feature_candidates(candidate_id,job_id,entity_id,status,candidate_json,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            [(feature["candidate_id"], "history", west["entity_id"], feature["status"],
              json.dumps(feature), timestamp, timestamp) for feature in (reviewed, pending)],
        )

    database = SQLiteDatabase(path)
    with database.connect() as connection:
        stored = connection.execute(
            "SELECT entity_key,scope_ambiguous,scope_verified,entity_json FROM feature_job_entities"
        ).fetchone()
        decision = connection.execute(
            "SELECT status,candidate_json FROM feature_candidates WHERE candidate_id='prior-reviewed'"
        ).fetchone()
    assert stored["entity_key"] == entity_key(west)
    assert stored["scope_ambiguous"] == has_template_scope and stored["scope_verified"] == 0
    assert json.loads(stored["entity_json"]) == row
    assert decision["status"] == "approved" and json.loads(decision["candidate_json"]) == reviewed

    manager = FeatureJobManager(auto_start=False, persistence=SQLiteFeatureJobStore(database), interrupt_on_restore=False)
    for key in (None, entity_key(west)):
        with pytest.raises(FeatureJobError, match="重新创建") as refused:
            manager.get_agent_evidence("history", west["entity_id"], entity_scope_key=key)
        assert refused.value.status_code == 409
    with pytest.raises(FeatureJobError, match="重新创建"):
        manager.retry_entity("history", west["entity_id"], start=False, entity_scope_key=entity_key(west))
    with pytest.raises(FeatureJobError, match="重新创建"):
        manager.update_feature("history", "prior-pending", {"status": "approved"})
    assert manager.persistence.load_candidate("prior-pending")["status"] == "pending"
    assert manager.persistence.load_candidate("prior-reviewed")["status"] == "approved"

    rebuilt = manager.create_job({"risk_entities": [east, west]}, model="fake")
    for original, expected in ((east, 4), (west, 9)):
        evidence = manager.get_agent_evidence(rebuilt, original["entity_id"], entity_scope_key=entity_key(original))
        assert {item["template_hash"]: item["count"] for item in evidence["templates"]} == {
            "common": expected, original["cluster"]: 1,
        }


def test_agent_evidence_keeps_two_typed_risks_from_one_drain_template(tmp_path):
    from logrisk.agentic.evidence_context import evidence_facts

    xid35 = source("one-node", "gpu", "node", 4)
    xid79 = source("one-node", "gpu", "node", 9)
    for window, code, risk_type in (
        (xid35, 35, "gpu.video_processor_exception"),
        (xid79, 79, "gpu.fallen_off_bus"),
    ):
        window["top_templates"][0].update(
            semantic_rule_revision="pinned", risk_semantic={
                "semantic_rule_id": f"builtin.gpu.xid.{code}", "semantic_rule_version": 1,
                "risk_type": risk_type, "semantic_fields": {"xid_code": code},
            },
        )
    manager = FeatureJobManager(auto_start=False)
    job = manager.create_job({"risk_entities": [xid35, xid79]}, model="fake")
    evidence = manager.get_agent_evidence(job, xid35["entity_id"])
    facts = evidence_facts(evidence)
    assert {item["anomaly_type"]: item["log_occurrence_count"] for item in facts} == {
        "gpu.video_processor_exception": 4, "gpu.fallen_off_bus": 9,
    }
    assert {item["base_template_hash"] for item in evidence["templates"]} == {"common"}
    assert len({item["template_hash"] for item in evidence["templates"]}) == 2


def test_orphaned_legacy_candidates_allow_notes_but_cannot_approve_match_or_export(tmp_path):
    from logrisk.approval_projection import update_projection

    legacy_migrations = tmp_path / "legacy-orphan-migrations"
    legacy_migrations.mkdir()
    for migration in Path("database/migrations").glob("*.sql"):
        if migration.name[:4] <= "0033":
            shutil.copyfile(migration, legacy_migrations / migration.name)
    path = tmp_path / "old-orphan.sqlite3"
    legacy = SQLiteDatabase(path, migrations_dir=legacy_migrations)
    original = source("same-host", "cluster-west", "node", 9)
    timestamp = "2026-06-22T10:00:00+08:00"
    with legacy.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id,status,job_json,created_at,updated_at) VALUES (?,?,?,?,?)",
            ("history", "completed", "{}", timestamp, timestamp),
        )
        for name, status in (("orphan-pending", "pending"), ("orphan-reviewed", "approved")):
            feature = dict(candidate(original, name), status=status, job_id="history")
            connection.execute(
                "INSERT INTO feature_candidates(candidate_id,job_id,entity_id,status,candidate_json,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (name, "history", original["entity_id"], status, json.dumps(feature), timestamp, timestamp),
            )
            update_projection(connection, feature)

    database = SQLiteDatabase(path)
    store = SQLiteFeatureJobStore(database)
    rules = SQLiteApprovedRuleStore(database)
    manager = FeatureJobManager(
        persistence=store, rule_store=rules, auto_start=False, interrupt_on_restore=False,
        extractor=lambda record, **kwargs: [candidate(record, "current-candidate")],
    )
    note = manager.update_feature("history", "orphan-pending", {"reviewer_note": "保留旧任务供查阅"})
    assert note["reviewer_note"] == "保留旧任务供查阅" and note["status"] == "pending"
    with pytest.raises(FeatureJobError) as refused:
        manager.update_feature("history", "orphan-pending", {"status": "approved"})
    assert refused.value.code == "entity_scope_unverified" and rules.list_rules() == []
    with pytest.raises(FeatureJobError, match="缺少可验证"):
        manager.export_approved("history")
    assert store.load_candidate("orphan-reviewed")["status"] == "approved"

    active_source = manager.create_job({"risk_entities": [original]}, model="fake")
    manager.run_job(active_source)
    approved = manager.update_feature(active_source, "current-candidate", {"status": "approved"})
    assert approved["status"] == "approved"
    assert store.load_candidate("orphan-pending")["status"] == "pending"
    assert store.save_generated_candidate("history", dict(note, candidate_id="another-orphan"))["status"] == "pending"
    assert len(rules.list_rules()) == 1
