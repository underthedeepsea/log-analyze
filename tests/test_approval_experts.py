from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from logrisk.application.api import ApiFacade
from logrisk.approval_experts import ApprovalExpertReader
from logrisk.database import SQLiteDatabase


@pytest.fixture
def database(tmp_path):
    db = SQLiteDatabase(tmp_path / "experts.sqlite3", migrate=False)
    with db.transaction() as connection:
        connection.executescript("""
            CREATE TABLE feature_candidates(job_id TEXT, candidate_id TEXT, entity_id TEXT, candidate_json TEXT);
            CREATE TABLE agent_runs(run_id TEXT, source_job_id TEXT, entity_id TEXT, locked_snapshot_json TEXT);
            CREATE TABLE agent_workflow_runs(workflow_run_id TEXT, source_job_id TEXT, entity_id TEXT);
            CREATE TABLE agent_workflow_nodes(workflow_run_id TEXT, node_id TEXT, role_id TEXT, child_agent_run_id TEXT);
            CREATE TABLE agent_artifacts(artifact_id TEXT, run_id TEXT, artifact_type TEXT, payload_json TEXT, fingerprint TEXT, created_at TEXT);
            CREATE TABLE agent_tool_calls(run_id TEXT, tool_call_id TEXT, tool_name TEXT, status TEXT, result_summary_json TEXT);
        """)
        connection.execute("INSERT INTO feature_candidates VALUES ('job', 'candidate', 'node', ?)",
                           (json.dumps({"agent_run_id": "feature", "model": "fixture-model"}),))
        snapshot = {"workflow_run_id": "workflow", "connection_snapshot": {"api_key": "private-secret"},
                    "dependency_artifact_refs": [{"node_id": "e", "child_agent_run_id": "e-old"},
                                                 {"node_id": "r", "child_agent_run_id": "r-old"}]}
        for run_id in ("feature", "e-old", "r-old", "e-new", "feature-new"):
            connection.execute("INSERT INTO agent_runs VALUES (?, 'job', 'node', ?)",
                               (run_id, json.dumps(snapshot if run_id == "feature" else {})))
        connection.execute("INSERT INTO agent_workflow_runs VALUES ('workflow', 'job', 'node')")
        for node, role, run_id in [("e", "evidence_specialist", "e-new"), ("r", "rule_specialist", "r-old")]:
            connection.execute("INSERT INTO agent_workflow_nodes VALUES ('workflow', ?, ?, ?)", (node, role, run_id))
    return db


def artifact(database, identity, run_id, kind, payload, fingerprint=None, time="2026-10-01"):
    with database.transaction() as connection:
        connection.execute("INSERT INTO agent_artifacts VALUES (?, ?, ?, ?, ?, ?)",
                           (identity, run_id, kind, json.dumps(payload), fingerprint, time))


def assessment(run_id, tool, output):
    return {"producer_run_id": run_id, "source_tool": tool,
            "evidence_refs": [{"source_job_id": "job", "entity_id": "node", "tool_call_id": "call"}],
            "safe_payload": output, "requires_explicit_read": False}


def populate(database):
    artifact(database, "evidence", "e-old", "evidence_assessment_v1", assessment(
        "e-old", "get_sanitized_evidence", {"templates": [{"component": "kernel", "severity": "ERROR",
        "template": "Out of memory <NUM>", "template_hash": "hash", "count": 7, "samples": ["private-secret"]}]}))
    artifact(database, "new-evidence", "e-new", "evidence_assessment_v1", assessment(
        "e-new", "get_sanitized_evidence", {"templates": [{"component": "wrong-new-run", "count": 999}]}))
    artifact(database, "rules", "r-old", "rule_match_assessment_v1", assessment(
        "r-old", "find_approved_rules", {"matched": 0, "items": [], "truncated": False}))
    feature = {"candidate_id": "candidate", "title": "内存异常", "summary": "内存异常模板", "components": ["kernel"],
               "template_hashes": ["hash"], "selection_reason": "模板表明内存异常", "problem_resolution": {"semantic_safe": True}}
    artifact(database, "eval", "feature", "evaluation", {"passed": True, "rule_results": []}, "fp")
    artifact(database, "candidate", "feature", "candidate", feature, "fp", "2026-10-02")
    artifact(database, "other-eval", "feature", "evaluation", {"passed": False, "errors": ["wrong-evaluation"]}, "other")
    artifact(database, "future-eval", "feature", "evaluation", {"passed": False, "errors": ["later-evaluation"]}, "fp", "2026-10-03")


def test_reads_original_dependency_and_candidate_evaluation_without_private_fields(database):
    populate(database)
    result = ApprovalExpertReader(database).read("job", "candidate")
    assert result
    evidence, rules, feature = result["opinions"]
    assert "7 次" in evidence["basis"]
    assert evidence["records"][0]["run_id"] == "e-old"
    assert rules["conclusion"] == "本次未匹配到批准规则"
    assert feature["conclusion"] == "候选已通过结构校验"
    serialized = json.dumps(result)
    for forbidden in ("private-secret", "samples", "connection_snapshot", "wrong-evaluation", "later-evaluation", "wrong-new-run"):
        assert forbidden not in serialized


def test_scope_missing_and_facade_route(database):
    facade = ApiFacade(SimpleNamespace(database=database), version="test")
    response = facade.dispatch_read("/api/jobs/job/features/candidate/expert-opinions")
    assert response and response.status == 200
    assert all(item["state"] == "missing" for item in response.body["opinions"])
    assert facade.feature_expert_opinions("other-job", "candidate").status == 404
    with database.transaction() as connection:
        connection.execute("UPDATE agent_runs SET entity_id='other-node' WHERE run_id='feature'")
    assert facade.feature_expert_opinions("job", "candidate").body["agent_run_id"] is None


def test_partial_rule_lookup_and_failed_or_unknown_evaluation_are_explicit(database):
    populate(database)
    artifact(database, "partial", "r-old", "rule_match_assessment_v1", assessment(
        "r-old", "find_approved_rules", {"matched": 0, "items": [], "truncated": True}), time="2026-10-02")
    with database.transaction() as connection:
        connection.execute("UPDATE agent_artifacts SET payload_json=? WHERE artifact_id='eval'", (json.dumps({"passed": False}),))
    result = ApprovalExpertReader(database).read("job", "candidate")
    assert result["opinions"][1]["state"] == "attention"
    assert "不完整" in result["opinions"][1]["conclusion"]
    assert result["opinions"][2]["conclusion"] == "候选校验未通过"
    with database.transaction() as connection:
        connection.execute("UPDATE agent_artifacts SET payload_json='[]' WHERE artifact_id='eval'")
    assert "不完整" in ApprovalExpertReader(database).read("job", "candidate")["opinions"][2]["conclusion"]


def test_large_assessment_resolves_exact_recorded_tool_call(database):
    value = assessment("e-old", "get_sanitized_evidence", {})
    value["requires_explicit_read"] = True
    artifact(database, "large", "e-old", "evidence_assessment_v1", value)
    with database.transaction() as connection:
        connection.execute("INSERT INTO agent_tool_calls VALUES ('e-old', 'call', 'get_sanitized_evidence', 'completed', ?)",
                           (json.dumps({"templates": [{"component": "kernel", "count": 2}]}),))
    assert "2 次" in ApprovalExpertReader(database).read("job", "candidate")["opinions"][0]["basis"]
