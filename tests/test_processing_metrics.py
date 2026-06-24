from datetime import date

import pytest

from logrisk.processing_metrics import ProcessingMetricsError, ProcessingMetricsStore


def test_daily_llm_volume_survives_reload(tmp_path):
    path = tmp_path / "metrics.json"
    today = lambda: date(2026, 6, 23)

    ProcessingMetricsStore(path, today=today).add_llm_logs(120)

    assert ProcessingMetricsStore(path, today=today).today_llm_logs() == 120


def test_metrics_are_kept_separate_by_local_day(tmp_path):
    path = tmp_path / "metrics.json"
    current = {"value": date(2026, 6, 23)}
    store = ProcessingMetricsStore(path, today=lambda: current["value"])
    store.add_llm_logs(10)
    current["value"] = date(2026, 6, 24)

    assert store.today_llm_logs() == 0
    assert store.add_llm_logs(4) == 4


def test_negative_volume_is_rejected(tmp_path):
    store = ProcessingMetricsStore(tmp_path / "metrics.json")

    with pytest.raises(ProcessingMetricsError, match="不能为负数"):
        store.add_llm_logs(-1)


def test_malformed_metrics_state_is_rejected(tmp_path):
    path = tmp_path / "metrics.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ProcessingMetricsError, match="指标"):
        ProcessingMetricsStore(path).today_llm_logs()
