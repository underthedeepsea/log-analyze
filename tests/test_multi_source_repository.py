from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.multi_source.repository import MultiSourceRepository


def test_repository_persists_sanitized_observation_and_idempotent_correlation(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = MultiSourceRepository(database)
    observation = {
        "observation_id": "obs-1",
        "source_job_id": "job-1",
        "cluster": "prod-a",
        "source_type": "syslog",
        "source_family": "kernel",
        "component": "kernel",
        "severity": "ERROR",
        "template_hash": "hash-1",
        "template": "NVRM Xid <*>",
        "count": 3,
        "risk_score": 72.0,
        "risk_level": "high",
        "window_start": "2026-07-30T10:00:00+00:00",
        "window_end": "2026-07-30T10:05:00+00:00",
        "entity_keys": ["prod-a/node/node-01"],
        "relations": [],
    }

    repository.save_observation(observation)
    repository.save_observation(observation)
    repository.seed_rules([{
        "rule_id": "rule-1",
        "display_name": "测试规则",
        "enabled": True,
        "source_pairs": [["kernel", "kubelet"]],
        "max_gap_seconds": 300,
        "min_risk_score": 40,
        "min_count": 1,
        "confidence": 0.9,
    }])
    repository.save_correlation({
        "correlation_id": "corr-1",
        "rule_id": "rule-1",
        "rule_version": 1,
        "cluster": "prod-a",
        "primary_entity_key": "prod-a/node/node-01",
        "window_start": observation["window_start"],
        "window_end": observation["window_end"],
        "confidence": 0.9,
        "risk_score": 72.0,
        "source_families": ["kernel", "kubelet"],
        "observation_ids": ["obs-1"],
    })
    repository.save_correlation({
        "correlation_id": "corr-1",
        "rule_id": "rule-1",
        "rule_version": 1,
        "cluster": "prod-a",
        "primary_entity_key": "prod-a/node/node-01",
        "window_start": observation["window_start"],
        "window_end": observation["window_end"],
        "confidence": 0.9,
        "risk_score": 72.0,
        "source_families": ["kernel", "kubelet"],
        "observation_ids": ["obs-1"],
    })

    timeline = repository.entity_timeline("prod-a/node/node-01")
    detail = repository.get_correlation("corr-1")
    assert len(timeline["items"]) == 1
    assert timeline["items"][0]["template"] == "NVRM Xid <*>"
    assert "raw_sample" not in timeline["items"][0]
    assert detail["observation_ids"] == ["obs-1"]


def test_repository_updates_rule_with_optimistic_version_and_appends_audit(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = MultiSourceRepository(database)
    repository.seed_rules([{
        "rule_id": "rule-1",
        "display_name": "同实体跨来源",
        "enabled": True,
        "source_pairs": [["kernel", "kubelet"]],
        "max_gap_seconds": 300,
        "min_risk_score": 40,
        "min_count": 1,
        "confidence": 0.9,
    }])

    updated = repository.update_rule(
        "rule-1",
        {"enabled": False, "expected_version": 1},
        actor="alice",
        request_id="request-1",
    )

    assert updated["enabled"] is False
    assert updated["version"] == 2
    assert repository.list_audits(limit=10)["items"][0]["actor"] == "alice"


def test_repository_persists_complete_validated_rule_definition(tmp_path) -> None:
    database = SQLiteDatabase(tmp_path / "logrisk.sqlite3")
    repository = MultiSourceRepository(database)
    repository.seed_rules([{
        "rule_id": "rule-1",
        "display_name": "同实体跨来源",
        "enabled": True,
        "source_pairs": [["kernel", "kubelet"]],
        "max_gap_seconds": 300,
        "min_risk_score": 40,
        "min_count": 1,
        "confidence": 0.9,
    }])

    updated = repository.update_rule(
        "rule-1",
        {
            "display_name": "节点运行时异常",
            "enabled": True,
            "source_pairs": [["kernel", "containerd"], ["kubelet", "podlog"]],
            "max_gap_seconds": 120,
            "min_risk_score": 55,
            "min_count": 2,
            "confidence": 0.86,
            "expected_version": 1,
        },
        actor="alice",
        request_id="request-2",
    )

    assert updated == {
        **updated,
        "rule_id": "rule-1",
        "display_name": "节点运行时异常",
        "enabled": True,
        "source_pairs": [["kernel", "containerd"], ["kubelet", "podlog"]],
        "max_gap_seconds": 120,
        "min_risk_score": 55.0,
        "min_count": 2,
        "confidence": 0.86,
        "version": 2,
    }
    reloaded = repository.list_rules()["items"][0]
    assert reloaded["source_pairs"] == [["kernel", "containerd"], ["kubelet", "podlog"]]
    assert reloaded["max_gap_seconds"] == 120
