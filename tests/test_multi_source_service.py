from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.multi_source.repository import MultiSourceRepository
from logrisk.multi_source.service import MultiSourceService


def test_service_builds_sanitized_cross_source_evidence_from_risk_entities(tmp_path) -> None:
    repository = MultiSourceRepository(SQLiteDatabase(tmp_path / "logrisk.sqlite3"))
    service = MultiSourceService(
        repository,
        aliases={},
        rules=[{
            "rule_id": "same-node",
            "display_name": "同节点跨来源",
            "enabled": True,
            "source_pairs": [["kernel", "kubelet"]],
            "max_gap_seconds": 300,
            "min_risk_score": 40,
            "min_count": 1,
            "confidence": 0.9,
        }],
    )
    risk_entities = [
        {
            "cluster": "prod-a",
            "entity_type": "node",
            "entity_id": "node-01",
            "risk_score": 72,
            "risk_level": "high",
            "window_start": "2026-07-30T10:00:00+00:00",
            "window_end": "2026-07-30T10:05:00+00:00",
            "top_templates": [
                {
                    "cluster": "prod-a",
                    "node": "node-01",
                    "source_type": "syslog",
                    "component": "kernel",
                    "severity": "ERROR",
                    "template_hash": "kernel-hash",
                    "template": "NVRM Xid <*>",
                    "count": 2,
                    "window_start": "2026-07-30T10:00:00+00:00",
                    "window_end": "2026-07-30T10:05:00+00:00",
                    "samples": ["secret raw line"],
                },
                {
                    "cluster": "prod-a",
                    "node": "node-01",
                    "source_type": "kubelet",
                    "component": "kubelet",
                    "severity": "ERROR",
                    "template_hash": "kubelet-hash",
                    "template": "pod sandbox changed",
                    "count": 1,
                    "window_start": "2026-07-30T10:02:00+00:00",
                    "window_end": "2026-07-30T10:05:00+00:00",
                    "raw_sample": "secret raw line",
                },
            ],
        }
    ]

    result = service.ingest_risk_entities(risk_entities, source_job_id="job-1")

    assert result == {"observations": 2, "correlations": 1, "unroutable": 0}
    timeline = service.entity_timeline("node", "node-01", cluster="prod-a")
    assert len(timeline["items"]) == 2
    assert len(timeline["correlations"]) == 1
    assert timeline["correlations"][0]["source_families"] == ["kernel", "kubelet"]
    assert all("samples" not in item and "raw_sample" not in item for item in timeline["items"])
    detail = service.entity_detail("node", "node-01", cluster="prod-a")
    assert detail["entity"]["entity_key"] == "prod-a/node/node-01"
    assert detail["timeline_count"] == 2
    assert detail["correlation_count"] == 1
    assert repository.summary()["source_coverage"] == [
        {"source_family": "kernel", "count": 1},
        {"source_family": "kubelet", "count": 1},
    ]
