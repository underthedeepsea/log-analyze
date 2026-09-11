from __future__ import annotations

import copy
import json
import sqlite3

import pytest

from logrisk.approval_dedup import approval_identity
from logrisk.approval_repair import reclassify_pending_candidate, repair_pending_candidates
from logrisk.approved_rules import ApprovedRuleStore, RuleFormat, classify_rule
from logrisk.database import SQLiteDatabase
from logrisk.feature_extractor_ollama import _attach_source_facts
from logrisk.feature_jobs import FeatureJobManager
from logrisk.feature_semantic_partition import partition_feature_by_semantics
from logrisk.problem_resolver import resolve_selected_template
from logrisk.rule_governance import RuleGovernanceError, RuleGovernanceRepository
from logrisk.sqlite_stores import SQLiteApprovalGroupStore, SQLiteApprovedRuleStore, SQLiteFeatureJobStore


@pytest.mark.parametrize("template,code", [
    ('StopContainer "<ID>" failed: operation timeout context deadline exceeded', "kubernetes.runtime.container_stop_timeout"),
    ('Container "<ID>" termination failed with gracePeriod <NUM>: context deadline exceeded', "kubernetes.runtime.container_stop_timeout"),
    ('container start failed CreateContainerError: Error response from daemon Minimum memory limit allowed is <BYTES>', "kubernetes.runtime.memory_limit_invalid"),
    ('[cpumanager] failed to update container "<ID>" failed to write "<NUM>" write <K8S_CGROUP> device or resource busy', "kubernetes.runtime.resource_update_busy"),
    ('RemoveContainer "<ID>" failed: removal of container <ID> is already in progress', "kubernetes.runtime.container_removal_in_progress"),
    ('Failed to stop sandbox {"docker" "<ID>"}', "kubernetes.runtime.sandbox_stop_failure"),
    ('Unable to read config path "/etc/kubernetes/manifests" path does not exist, ignoring', "kubernetes.kubelet.manifest_path_missing"),
    ('preStop hook for container "<ID>" failed command exited with <NUM>', "kubernetes.pod.prestop_hook_failure"),
    ('readString Failed to read "<K8S_CGROUP>" read <K8S_CGROUP> no such device', "linux.cgroup.device_missing"),
])
def test_explicit_failure_variants_have_deterministic_presentations(template, code):
    source = {"template_hash": "t", "component": "kubelet", "template": template}
    resolution = resolve_selected_template(source)
    assert resolution.problem_code == code
    assert resolution.semantic_safe
    child, = partition_feature_by_semantics({"top_templates": [source]}, {
        "template_hashes": ["t"], "importance": "high",
    })
    assert child["feature_type"] != "unresolved_template_evidence"


@pytest.mark.parametrize("template", [
    "Minimum memory limit allowed is <BYTES>",
    "StopContainer succeeded before context deadline exceeded in another operation",
    "failed to read config file: unrelated path does not exist",
    "UpdateContainerResources succeeded; device or resource busy in unrelated IO",
    "preStop hook for container succeeded", "ImagePullBackOff",
])
def test_weak_evidence_does_not_acquire_concrete_approval_identity(template):
    assert not resolve_selected_template({"template": template}).semantic_safe


def source():
    return {
        "entity_id": "node-a", "entity_type": "node", "cluster": "cluster-a",
        "window_start": "2026-09-09T01:00:00+00:00", "window_end": "2026-09-09T02:00:00+00:00",
        "risk_score": 80, "risk_level": "high",
        "top_templates": [
            {"template_hash": "cache", "component": "kubelet", "count": 3,
             "template": "RecentStats unable to find data in memory cache"},
            {"template_hash": "volume", "component": "kubelet", "count": 2,
             "template": "error cleaning subPath mounts for volume <*>"},
        ],
    }


def historical_candidate(entity):
    value = _attach_source_facts(entity, {
        "feature_type": "unresolved_template_evidence", "title": "未完全解析的日志证据（待复核）",
        "summary": "kubelet 的 2 个所选模板尚不能确定单一异常类别，需人工复核。",
        "tags": ["日志证据", "待复核"], "template_hashes": ["cache", "volume"],
        "components": ["kubelet"], "importance": "high", "selection_reason": "所选模板需复核",
    }, "fake", "fake")
    value["evaluator_result"] = {"passed": True, "errors": [], "warnings": ["历史待复核"]}
    value["trace_id"] = "historical-trace"
    return value


def seeded_manager(tmp_path):
    database = SQLiteDatabase(tmp_path / "audit.sqlite3")
    persistence = SQLiteFeatureJobStore(database)
    rules = SQLiteApprovedRuleStore(database)
    groups = SQLiteApprovalGroupStore(database)
    manager = FeatureJobManager(extractor=lambda current, **_: [historical_candidate(current)],
                                persistence=persistence, rule_store=rules, approval_group_store=groups, auto_start=False)
    job_id = manager.create_job({"summary": {}, "risk_entities": [source()]}, model="fake")
    manager.run_job(job_id)
    return database, manager, job_id


@pytest.mark.parametrize("changes", [
    {"status": "approved"}, {"status": "rejected"}, {"reviewer_note": "人工备注"},
    {"title": "人工标题"}, {"summary": "人工摘要"}, {"evaluator_result": {"passed": False}},
    {"template_hashes": ["cache", "missing"]},
])
def test_reclassification_preserves_reviewed_edited_or_invalid_candidates(changes):
    candidate = historical_candidate(source())
    candidate.update(changes)
    original = copy.deepcopy(candidate)
    assert reclassify_pending_candidate(candidate) == []
    assert candidate == original


def table_snapshot(database):
    with database.connect() as connection:
        return {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                for table in ("feature_jobs", "feature_job_entities", "feature_candidates", "approval_groups",
                              "approval_group_candidates", "feature_job_events")}


def test_reclassification_is_atomic_idempotent_and_repeats_reuse_after_restart(tmp_path):
    database, manager, job_id = seeded_manager(tmp_path)
    original = manager.get_job(job_id)["features"][0]
    before = table_snapshot(database)
    preview = repair_pending_candidates(database)
    assert preview["candidates_reclassified"] == 1 and preview["candidates_added"] == 1
    assert table_snapshot(database) == before
    assert repair_pending_candidates(database, apply=True)["candidates_reclassified"] == 1
    assert repair_pending_candidates(database, apply=True)["candidates_reclassified"] == 0
    persistence = SQLiteFeatureJobStore(database)
    repaired = persistence.load_job(job_id)["features"]
    assert original["candidate_id"] in repaired
    assert {item["status"] for item in repaired.values()} == {"pending"}
    assert sum(item["occurrence_count"] for item in repaired.values()) == 5
    assert {h for item in repaired.values() for h in item["template_hashes"]} == {"cache", "volume"}
    assert all(item["trace_id"] == "historical-trace" and item["evaluator_result"]["passed"] for item in repaired.values())
    with database.connect() as connection:
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()
        assert connection.execute("SELECT COUNT(*) FROM approval_group_candidates").fetchone()[0] == 2
    rules = SQLiteApprovedRuleStore(database)
    groups = SQLiteApprovalGroupStore(database)
    def extractor(current, **_):
        return [_attach_source_facts(current, child, "fake", "fake") for child in
                partition_feature_by_semantics(current, historical_candidate(current))]
    manager = FeatureJobManager(extractor=extractor, persistence=persistence, rule_store=rules,
                                approval_group_store=groups, auto_start=False)
    for cid in repaired:
        manager.update_feature(job_id, cid, {"status": "approved"})
    manager = FeatureJobManager(extractor=extractor, persistence=persistence, rule_store=rules,
                                approval_group_store=groups, auto_start=False)
    repeated = source()
    repeated["entity_id"] = "another-node"
    repeat_id = manager.create_job({"summary": {}, "risk_entities": [repeated]}, model="fake")
    manager.run_job(repeat_id)
    assert {item["status"] for item in manager.get_job(repeat_id)["features"]} == {"approved"}
    assert len(rules.list_rules()) == 2


def test_reclassification_rolls_back_on_child_id_collision(tmp_path):
    database, manager, job_id = seeded_manager(tmp_path)
    original = manager.get_job(job_id)["features"][0]
    child = reclassify_pending_candidate(original)[1]
    SQLiteFeatureJobStore(database).save_generated_candidate(job_id, child)
    before = table_snapshot(database)
    with pytest.raises(sqlite3.IntegrityError):
        repair_pending_candidates(database, apply=True)
    assert table_snapshot(database) == before


def test_reclassification_refuses_running_jobs(tmp_path):
    database, _, _ = seeded_manager(tmp_path)
    with database.transaction() as connection:
        connection.execute("UPDATE feature_jobs SET status='running'")
    before = table_snapshot(database)
    with pytest.raises(ValueError, match="运行中的特征任务"):
        repair_pending_candidates(database, apply=True)
    assert table_snapshot(database) == before


@pytest.mark.parametrize("text", ["Unclassified operation failed", "CNI failed: no enough ips"])
def test_modern_candidate_with_historical_physical_key_reuses_current_rule(tmp_path, text):
    value = historical_candidate(source())
    value.update({"template_hashes": ["t"], "source_templates": [
        {"template_hash": "t", "component": "kubelet", "template": text},
    ], "anchor_signatures": []})
    value.update(approval_identity(value))
    value["approval_key"] = "historical-physical-key"
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    rule = rules.upsert_feature(value)
    assert rules.match_feature(value)[0]["rule_id"] == rule["rule_id"]
    invalid = {**value, "template_hashes": ["missing"]}
    assert rules.match_feature(invalid) == []


def imported_rule(tmp_path):
    database = SQLiteDatabase(tmp_path / "rules.sqlite3")
    store = SQLiteApprovedRuleStore(database)
    candidate = historical_candidate(source())
    rule = store.upsert_feature(candidate)
    for key in ("problem_code", "approval_key", "match_mode", "anchor_signatures", "supporting_signatures"):
        rule.pop(key, None)
    snapshot = json.dumps(rule)
    with database.transaction() as connection:
        connection.execute("UPDATE approved_rules SET rule_json=?, problem_code=NULL, approval_key=NULL", (snapshot,))
        connection.execute("UPDATE rule_versions SET rule_json=?, change_type='legacy_import', operator='system-migration'", (snapshot,))
    return database, store, rule, snapshot


def test_proven_legacy_format_repair_preserves_approval_scope_and_history(tmp_path):
    database, store, original, historical_json = imported_rule(tmp_path)
    repository = RuleGovernanceRepository(database)
    repaired = repository.repair_legacy_import(original["rule_id"], operator="maintenance")
    assert repaired["schema_version"] == "approved_rule_v1"
    assert repaired["current_version"] == 2
    for key in ("rule_id", "signature", "approved_at", "status", "template_signatures", "feature_type"):
        assert repaired[key] == original[key]
    assert classify_rule(store.list_rules()[0]).kind == RuleFormat.LEGACY_V1
    candidate = historical_candidate(source())
    candidate["schema_version"] = "approved_rule_v1"
    candidate.pop("anchor_signatures")
    assert store.match_feature(candidate)[0]["rule_id"] == original["rule_id"]
    assert not store.match_feature({**candidate, "feature_type": "different"})
    with database.connect() as connection:
        assert connection.execute("SELECT rule_json FROM rule_versions WHERE version=1").fetchone()[0] == historical_json
        assert connection.execute("SELECT COUNT(*) FROM rule_audit_events WHERE event_type='legacy_format_repaired'").fetchone()[0] == 1


@pytest.mark.parametrize("damage", ["untrusted_import", "changed_templates", "modern_identity", "newer_version"])
def test_legacy_repair_requires_unchanged_migration_provenance(tmp_path, damage):
    database, store, original, _ = imported_rule(tmp_path)
    with database.transaction() as connection:
        if damage == "untrusted_import":
            connection.execute("UPDATE rule_versions SET operator='someone'")
        elif damage == "changed_templates":
            original["template_signatures"] = [{"template_hash": "other"}]
            connection.execute("UPDATE approved_rules SET rule_json=?", (json.dumps(original),))
        elif damage == "modern_identity":
            original["match_mode"] = "template_set"
            connection.execute("UPDATE approved_rules SET rule_json=?", (json.dumps(original),))
        else:
            connection.execute("UPDATE approved_rules SET current_version=2")
    with pytest.raises(RuleGovernanceError, match="没有匹配的原始迁移记录"):
        RuleGovernanceRepository(database).repair_legacy_import(original["rule_id"], operator="maintenance")
    assert classify_rule(store.list_rules()[0]).kind == RuleFormat.MALFORMED_V2
