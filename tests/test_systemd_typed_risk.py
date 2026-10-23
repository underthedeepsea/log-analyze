from __future__ import annotations

import json
from pathlib import Path

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource
from logrisk.large_file_pipeline import run_incremental_pipeline
from logrisk.risk_semantics import RiskSemanticError, RiskSemanticService
from logrisk.streaming_state import StreamingStateRepository


@pytest.fixture
def semantics(tmp_path):
    return RiskSemanticService(SQLiteDatabase(tmp_path / "rules.sqlite3"), "configs/risk_semantics/builtin.yaml")


def test_raw_and_typed_unit_match_same_builtin_and_preserve_identity(semantics):
    raw = semantics.match({"component": "systemd", "message_core": "alpha.service failed"})
    typed = semantics.match({"component": "systemd", "template": "<SYSTEMD_UNIT> failed", "count": 2,
                             "semantic_fields": {"unit": [{"value": "alpha.service", "count": 2}]}})
    assert raw == typed
    assert typed["semantic_rule_id"] == "builtin.systemd.unit.failed"
    assert typed["semantic_fields"] == {"unit": "alpha.service"}


@pytest.mark.parametrize("record", [
    {"component": "systemd", "message_core": "operation failed"},
    {"component": "systemd", "message_core": "alpha.service failedness"},
    {"component": "app", "message_core": "alpha.service failed"},
    {"component": "systemd", "template": "<SYSTEMD_UNIT> failed"},
    {"component": "systemd", "template": "<SYSTEMD_UNIT> failed", "semantic_fields": {"unit": "not-a-unit"}},
    {"component": "systemd", "template": "<SYSTEMD_UNIT> failed", "count": 2,
     "semantic_fields": {"unit": [{"value": "alpha.service", "count": 1}, {"value": "beta.service", "count": 1}]}},
])
def test_unrelated_or_ambiguous_failed_never_becomes_unit_risk(semantics, record):
    with pytest.raises(RiskSemanticError) as error:
        semantics.match(record)
    assert error.value.code == "semantic_unclassified"


def test_actual_raw_file_separates_unit_host_cluster_and_keeps_cgroup(tmp_path):
    rows = []
    expected = {("a", "host1", "alpha.service"): 2, ("a", "host1", "beta.service"): 1,
                ("a", "host2", "alpha.service"): 1, ("b", "host1", "alpha.service"): 1}
    for (cluster, host, unit), count in expected.items():
        for _ in range(count):
            rows.append({"timestamp": "2026-08-31T12:00:00+00:00", "cluster": cluster, "host": host,
                         "component": "systemd", "message": unit + " failed"})
    rows.append({"timestamp": "2026-08-31T12:00:00+00:00", "cluster": "a", "node": "host1",
                 "component": "kernel", "message": "memory cgroup out of memory"})
    path = tmp_path / "raw.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    database = SQLiteDatabase(tmp_path / "pipeline.sqlite3")
    service = RiskSemanticService(database, "configs/risk_semantics/builtin.yaml")
    result = run_incremental_pipeline(input_job_id="systemd-regression", source=FileIncrementalSource(path),
        source_name=str(path), config_path="configs/drain3_recommended.ini", rules_path="configs/risk_rules.yaml",
        state_dir=tmp_path / "drain", streaming_repository=StreamingStateRepository(database), worker_count=1,
        risk_semantics=service, stream_batch_records=10)
    with database.connect() as connection:
        windows = [json.loads(row[0]) for row in connection.execute("SELECT window_json FROM streaming_batch_windows")]
    systemd = [w for w in windows if (w.get("risk_semantic") or {}).get("risk_type") == "systemd.unit.failed"]
    actual = {(w["cluster"], w["entity_id"], w["risk_semantic"]["semantic_fields"]["unit"]): w["count"] for w in systemd}
    assert actual == expected
    assert len({(w["source_namespace"], w["source_item_id"]) for w in systemd}) == len(expected)
    assert sum(w["count"] for w in windows if (w.get("risk_semantic") or {}).get("risk_type") == "linux.memory.cgroup_oom") == 1
    assert result["summary"]["risk_semantic_matches"] == len(rows)
    assert all(w["template"] == "<SYSTEMD_UNIT> failed" for w in systemd)
