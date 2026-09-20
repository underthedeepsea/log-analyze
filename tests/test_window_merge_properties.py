from __future__ import annotations

from logrisk.large_file_pipeline import _merge_template_windows


def test_cross_batch_merge_preserves_counts_severity_time_and_semantic_distributions():
    base = {
        "window_start": "2026-01-01T00:00:00+00:00", "window_end": "2026-01-01T00:05:00+00:00",
        "cluster": "c", "entity_type": "node", "entity_id": "n", "component": "kernel",
        "template_hash": "h", "risk_semantic": {"risk_type": "gpu"},
    }
    ten = dict(base, count=10, severity="INFO", first_seen="2026-01-01T00:00:20+00:00", last_seen="2026-01-01T00:01:00+00:00", semantic_fields={"errno": [{"value": 5, "count": 10}]})
    twenty = dict(base, count=20, severity="ERROR", first_seen="2026-01-01T00:00:10+00:00", last_seen="2026-01-01T00:02:00+00:00", semantic_fields={"errno": [{"value": 13, "count": 20}]})

    first = _merge_template_windows([ten, twenty])[0]
    second = _merge_template_windows([twenty, ten])[0]

    assert first["count"] == 30
    assert first["severity"] == "ERROR"
    assert first["first_seen"] == "2026-01-01T00:00:10+00:00"
    assert first["last_seen"] == "2026-01-01T00:02:00+00:00"
    assert {entry["value"]: entry["count"] for entry in first["semantic_fields"]["errno"]} == {5: 10, 13: 20}
    assert second == first
