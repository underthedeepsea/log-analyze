from __future__ import annotations

import json

import pytest
from logrisk.database import SQLiteDatabase
from logrisk.sqlite_stores import SQLiteApprovedRuleStore


NOW = "2026-07-18T08:00:00+00:00"


def feature(title: str = "内核错误") -> dict[str, object]:
    return {
        "feature_type": "kernel_error",
        "title": title,
        "summary": "检测到内核安全注册失败",
        "importance": "high",
        "tags": ["内核", "注册失败"],
        "selection_reason": "kernel ERROR 模板具备异常证据价值",
        "components": ["kernel"],
        "source_templates": [{"template_hash": "hash-1", "category": "kernel"}],
    }


def entity() -> dict[str, object]:
    return {
        "entity_id": "node-a",
        "cluster": "prod-a",
        "top_templates": [{"template_hash": "hash-1", "category": "kernel"}],
    }


def governed_rule(tmp_path):
    from logrisk.rule_governance import RuleGovernanceRepository, RuleGovernanceService

    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    store = SQLiteApprovedRuleStore(database, clock=lambda: NOW)
    rule = store.upsert_feature(feature())
    service = RuleGovernanceService(RuleGovernanceRepository(database), clock=lambda: NOW)
    return database, store, service, rule


def test_status_change_is_versioned_and_disabled_rule_stops_matching(tmp_path):
    database, store, service, rule = governed_rule(tmp_path)

    result = service.change_status(
        rule["rule_id"],
        status="disabled",
        expected_version=1,
        operator="reviewer-a",
        reason="误报复审后停用",
    )

    assert result["request_id"].startswith("request-")
    assert result["rule_id"] == rule["rule_id"]
    assert result["version"] == 2
    assert result["rule"]["status"] == "disabled"
    assert store.match_entity(entity()) == []
    with database.connect() as connection:
        versions = connection.execute(
            "SELECT version, change_type FROM rule_versions WHERE rule_id=? ORDER BY version",
            (rule["rule_id"],),
        ).fetchall()
    assert [tuple(item) for item in versions] == [(1, "rule_created"), (2, "status_changed")]


def test_v1_status_changes_preserve_rule_format_and_null_projection(tmp_path):
    database, store, service, rule = governed_rule(tmp_path)
    legacy = store.list_rules()[0]
    legacy["schema_version"] = "approved_rule_v1"
    store._write_locked([legacy])

    disabled = service.change_status(rule["rule_id"], "disabled", 1, "reviewer-a", "停用")
    enabled = service.change_status(rule["rule_id"], "active", 2, "reviewer-a", "恢复")

    assert disabled["rule"]["schema_version"] == "approved_rule_v1"
    assert enabled["rule"]["schema_version"] == "approved_rule_v1"
    with database.connect() as connection:
        row = connection.execute(
            "SELECT schema_version, problem_code, approval_key FROM approved_rules WHERE rule_id=?",
            (rule["rule_id"],),
        ).fetchone()
    assert tuple(row) == ("approved_rule_v1", None, None)


def test_rollback_to_v1_snapshot_preserves_v1_format(tmp_path):
    database, store, service, rule = governed_rule(tmp_path)
    v1_snapshot = dict(rule, schema_version="approved_rule_v1")
    current_v2 = dict(rule, current_version=2, updated_at=NOW)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE rule_versions SET rule_json=? WHERE rule_id=? AND version=1",
            (json.dumps(v1_snapshot, ensure_ascii=False, separators=(",", ":")), rule["rule_id"]),
        )
        connection.execute(
            "UPDATE approved_rules SET rule_json=?, current_version=2, schema_version='approved_rule_v2', problem_code=?, approval_key=? WHERE rule_id=?",
            (json.dumps(current_v2, ensure_ascii=False, separators=(",", ":")), current_v2["problem_code"], current_v2["approval_key"], rule["rule_id"]),
        )

    result = service.rollback(
        rule["rule_id"], target_version=1, expected_version=2, confirmed=True,
        operator="reviewer-a", reason="恢复历史 V1 规则",
    )

    assert result["rule"]["schema_version"] == "approved_rule_v1"
    with database.connect() as connection:
        row = connection.execute(
            "SELECT schema_version, problem_code, approval_key FROM approved_rules WHERE rule_id=?",
            (rule["rule_id"],),
        ).fetchone()
    assert tuple(row) == ("approved_rule_v1", None, None)


def test_rollback_trusted_legacy_snapshot_normalizes_to_v1(tmp_path):
    database, store, service, rule = governed_rule(tmp_path)
    legacy_snapshot = {
        "rule_id": rule["rule_id"],
        "signature": rule["signature"],
        "feature_type": rule["feature_type"],
        "title": rule["title"],
        "summary": rule["summary"],
        "components": rule["components"],
        "template_signatures": rule["template_signatures"],
        "status": "active",
        "current_version": 1,
        "approved_at": rule["approved_at"],
        "created_at": rule["created_at"],
        "next_review_at": rule["next_review_at"],
    }
    current_v2 = dict(rule, current_version=2, updated_at=NOW)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE rule_versions SET rule_json=? WHERE rule_id=? AND version=1",
            (json.dumps(legacy_snapshot, ensure_ascii=False, separators=(",", ":")), rule["rule_id"]),
        )
        connection.execute(
            "UPDATE approved_rules SET rule_json=?, current_version=2, schema_version='approved_rule_v2', problem_code=?, approval_key=? WHERE rule_id=?",
            (json.dumps(current_v2, ensure_ascii=False, separators=(",", ":")), current_v2["problem_code"], current_v2["approval_key"], rule["rule_id"]),
        )

    result = service.rollback(
        rule["rule_id"], target_version=1, expected_version=2, confirmed=True,
        operator="reviewer-a", reason="恢复未版本化历史规则",
    )

    assert result["rule"]["schema_version"] == "approved_rule_v1"
    with database.connect() as connection:
        row = connection.execute(
            "SELECT schema_version, problem_code, approval_key FROM approved_rules WHERE rule_id=?",
            (rule["rule_id"],),
        ).fetchone()
    assert tuple(row) == ("approved_rule_v1", None, None)


def test_status_change_rejects_stale_expected_version(tmp_path):
    from logrisk.rule_governance import RuleGovernanceError

    _, _, service, rule = governed_rule(tmp_path)
    service.change_status(rule["rule_id"], "disabled", 1, "reviewer-a", "停用")

    with pytest.raises(RuleGovernanceError) as error:
        service.change_status(rule["rule_id"], "active", 1, "reviewer-b", "恢复")

    assert error.value.status_code == 409
    assert error.value.code == "version_conflict"


def test_feedback_updates_health_and_enters_review_queue(tmp_path):
    _, store, service, rule = governed_rule(tmp_path)
    store.record_reuse(rule["rule_id"], job_id="job-1", entity_id="node-a", cluster="prod-a")
    store.record_reuse(rule["rule_id"], job_id="job-2", entity_id="node-b", cluster="prod-b")
    service.record_feedback(
        rule["rule_id"],
        outcome="confirmed",
        operator="reviewer-a",
        cluster="prod-a",
        note="命中有效",
    )
    service.record_feedback(
        rule["rule_id"],
        outcome="false_positive",
        operator="reviewer-b",
        cluster="prod-b",
        note="该集群为预期行为",
    )

    detail = service.get_rule(rule["rule_id"])
    queue = service.review_queue()

    assert detail["health"]["hits_7d"] == 2
    assert detail["health"]["hits_30d"] == 2
    assert detail["health"]["cluster_count_30d"] == 2
    assert detail["health"]["false_positive_rate_30d"] == 0.5
    assert queue["items"][0]["rule_id"] == rule["rule_id"]
    assert "存在误报反馈" in queue["items"][0]["review_reasons"]


def test_rollback_restores_historical_snapshot_as_new_version(tmp_path):
    _, store, service, first = governed_rule(tmp_path)
    second = store.upsert_feature(feature(title="更新后的标题"))
    assert second["current_version"] == 2

    result = service.rollback(
        first["rule_id"],
        target_version=1,
        expected_version=2,
        confirmed=True,
        operator="reviewer-a",
        reason="恢复首次审批版本",
    )

    assert result["version"] == 3
    assert result["rule"]["title"] == "内核错误"
    detail = service.get_rule(first["rule_id"])
    assert [item["version"] for item in detail["versions"]] == [3, 2, 1]
    assert detail["audit_events"][0]["event_type"] == "rolled_back"
