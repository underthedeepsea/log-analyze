from __future__ import annotations

from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.risk_semantics import RiskSemanticError, RiskSemanticService


BUILTINS = Path("configs/risk_semantics/builtin.yaml")


def service(tmp_path) -> RiskSemanticService:
    return RiskSemanticService(SQLiteDatabase(tmp_path / "logrisk.sqlite3"), BUILTINS)


def record(message: str) -> dict:
    return {
        "message_core": message,
        "source_type": "syslog",
        "component": "kernel",
        "cluster": "prod-a",
        "node": "gpu-node-01",
        "timestamp": "2026-07-19T10:00:00+00:00",
    }


def test_xid_35_and_79_share_structure_but_keep_distinct_risk_semantics(tmp_path):
    subject = service(tmp_path)

    xid35 = subject.match(record("NVRM: Xid (0000:65:00): 35, Video processor exception"))
    xid79 = subject.match(record("NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus"))

    assert xid35["risk_type"] == "gpu.video_processor_exception"
    assert xid35["severity"] == "medium"
    assert xid35["semantic_fields"] == {"xid_code": 35, "pci_bdf": "0000:65:00"}
    assert xid79["risk_type"] == "gpu.fallen_off_bus"
    assert xid79["severity"] == "critical"
    assert xid79["semantic_fields"] == {"xid_code": 79, "pci_bdf": "0000:65:00"}
    assert xid35["semantic_rule_id"] != xid79["semantic_rule_id"]


def test_builtin_edit_creates_override_and_restore_reactivates_default(tmp_path):
    subject = service(tmp_path)
    builtin = subject.get_rule("builtin.gpu.xid.35")

    override = subject.create_override(
        builtin["id"],
        {"classification": {**builtin["classification"], "default_severity": "high", "base_score": 75}},
        operator="reviewer-a",
        reason="本地旧驱动环境需要提高关注等级",
    )
    published = subject.publish(
        override["id"], expected_version=1, confirmed=True, operator="reviewer-a", reason="正负样例通过"
    )

    assert override["override_of"] == builtin["id"]
    assert published["status"] == "published"
    assert subject.match(record("NVRM: Xid (0000:65:00): 35, Video processor exception"))["severity"] == "high"
    restored = subject.restore_default(
        builtin["id"], expected_version=1, confirmed=True, operator="reviewer-a", reason="恢复系统默认"
    )
    assert restored["status"] == "disabled"
    assert subject.match(record("NVRM: Xid (0000:65:00): 35, Video processor exception"))["severity"] == "medium"


def test_invalid_regex_and_negative_sample_block_publish(tmp_path):
    subject = service(tmp_path)
    with pytest.raises(RiskSemanticError, match="正则"):
        subject.create_rule({
            "id": "user.invalid.regex",
            "display_name": "invalid",
            "domain": "test",
            "category": "test",
            "risk_type": "test.invalid_regex",
            "match": {"message_regex": ["("], "source_types": [], "components": []},
            "classification": {"default_severity": "low", "base_score": 20, "confidence": 1.0},
            "dedup": {"key_fields": ["cluster", "node_id", "risk_type"], "window_seconds": 300},
            "lifecycle": {"recovery_mode": "timeout", "recovery_timeout_seconds": 300},
            "test_samples": {"positive": ["error"], "negative": []},
        }, operator="qa", reason="invalid")


def test_rollback_appends_version_and_preserves_history(tmp_path):
    subject = service(tmp_path)
    created = subject.create_rule({
        "id": "user.linux.custom_failure",
        "display_name": "自定义失败",
        "description": "测试语义",
        "domain": "linux",
        "category": "process",
        "risk_type": "linux.process.custom_failure",
        "match": {"message_regex": [r"custom failure (?P<error_code>\d+)"], "source_types": [], "components": []},
        "extract": {"error_code": {"type": "integer", "from_group": "error_code"}},
        "classification": {"default_severity": "medium", "base_score": 45, "confidence": 1.0},
        "dedup": {"key_fields": ["cluster", "node_id", "risk_type", "error_code"], "window_seconds": 300},
        "lifecycle": {"recovery_mode": "timeout", "recovery_timeout_seconds": 300},
        "test_samples": {"positive": ["custom failure 7"], "negative": ["normal start"]},
    }, operator="qa", reason="新建")
    second = subject.update_rule(
        created["id"], {"display_name": "自定义进程失败"}, expected_version=1, operator="qa", reason="调整名称"
    )
    rolled = subject.rollback(
        created["id"], target_version=1, expected_version=2, confirmed=True, operator="qa", reason="撤销名称调整"
    )

    assert second["version"] == 2
    assert rolled["version"] == 3
    assert rolled["display_name"] == "自定义失败"
    assert [item["version"] for item in subject.versions(created["id"])] == [3, 2, 1]
