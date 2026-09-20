from __future__ import annotations

from logrisk.aggregator import resolve_event_time


def test_invalid_or_missing_event_time_stays_unknown_not_processing_now():
    assert resolve_event_time({"timestamp": "bad"}, {}).value is None
    assert resolve_event_time({}, {}).value is None
    assert resolve_event_time({"timestamp": "bad"}, {}).quality == "unknown"


def test_trusted_window_boundary_is_used_when_record_timestamp_missing():
    resolved = resolve_event_time({}, {"window_start": "2020-01-01T00:00:00+00:00"})
    assert resolved.value.isoformat() == "2020-01-01T00:00:00+00:00"
    assert resolved.quality == "window"
