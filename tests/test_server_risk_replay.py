from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from logrisk.server_risk.contracts import ServerRiskError, day_bounds, digest
from logrisk.server_risk.replay import EvaluationContext, import_history, isolated_destination
from tests.server_risk_fixtures import batch, domain_services, observation, fixture_server, request_http


def test_explicit_historical_context_preserves_end_without_current_green(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["objects"] = [{"object_kind": "unit", "object_id": "A"}]
    items = [observation(1, "2026-08-31T15:59:39+00:00"), observation(2, "2026-08-31T15:59:49+00:00", kind="state_snapshot"), observation(3, "2026-08-31T15:59:59+00:00", kind="state_snapshot")]
    now["value"] = "2026-10-10T12:00:00+00:00"
    service.observations.submit(batch("history", *items, mode="historical"), "fixture-actor")
    historical = service.assessments.evaluate("prod-a", "node-01", context=EvaluationContext.historical("2026-08-31T16:00:00+00:00"))
    assert historical["status"] == "clear" and historical["evaluation_mode"] == "historical"
    current = service.assessment("prod-a", "node-01")
    assert current["status"] == "unknown" and current["evaluation_mode"] == "current"
    assert current["episodes"][0]["resolved_at"] == items[-1]["occurred_at"]
    assert current["episodes"][0]["lifecycle"] == "resolved"
    assert service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["summary"]["end_of_day_assessment"] == "clear"
    assert [row["interval_end"] for row in service.repository.observations("prod-a", "node-01")] == [item["occurred_at"] for item in items]


def test_readonly_import_has_only_whitelisted_history_no_credentials(tmp_path):
    service, now, *_ = domain_services(tmp_path / "source")
    service.observations.submit(batch("seed", observation(1, now["value"])), "fixture-actor")
    source = service.repository.database.path
    # Source's secret-shaped opaque tables are intentionally excluded.
    with service.repository.database.transaction() as conn:
        conn.execute("CREATE TABLE fake_provider_secrets(secret TEXT)")
        conn.execute("INSERT INTO fake_provider_secrets VALUES ('must-not-copy')")
    before = source.read_bytes()
    manifest = import_history(source, tmp_path / "target.sqlite3", allowed_root=tmp_path)
    assert source.read_bytes() == before and manifest["source_readonly"]
    copied = sqlite3.connect(tmp_path / "target.sqlite3")
    names = {row[0] for row in copied.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "fake_provider_secrets" not in names
    assert copied.execute("SELECT COUNT(*) FROM server_risk_observations").fetchone()[0] == 0
    assert "must-not-copy" not in str(copied.execute("SELECT * FROM provider_connections").fetchall())
    copied.close()
    for target in (source, tmp_path / "target.sqlite3", tmp_path.parent / "outside.sqlite3"):
        with pytest.raises(ServerRiskError) as caught:
            isolated_destination(source, target, tmp_path)
        assert caught.value.code == "replay_destination_invalid"
    link = tmp_path / "link"
    link.symlink_to(tmp_path / "source", target_is_directory=True)
    with pytest.raises(ServerRiskError):
        isolated_destination(source, link / "escape.sqlite3", tmp_path)


@pytest.mark.parametrize("day", ["9999-12-31", "0001-01-01", "2026-02-30"])
def test_real_http_date_bounds_and_current_clock_injection_have_safe_error(tmp_path, day):
    server, _, base, thread = fixture_server(tmp_path)
    hits = []
    server.server_risk.reports.stage_hook = lambda stage, report_id: hits.append(stage)
    try:
        status, error, _ = request_http(base, "/api/node-risks/prod-a/node-01/days/" + day + "?timezone=Asia%2FShanghai")
        assert status == 422 and error["code"] == "invalid_date_or_timezone"
        assert "query_validation" in hits
        for normal in ("2026-08-31", "0001-01-02", "9999-12-30"):
            assert request_http(base, "/api/node-risks/prod-a/node-01/days/" + normal + "?timezone=Asia%2FShanghai")[0] == 200
        status, error, _ = request_http(base, "/api/node-risks/prod-a/node-01/assessment?as_of=2026-08-31&mode=historical")
        assert status == 422 and error["code"] == "invalid_schema"
        assert request_http(base, "/api/node-risks/prod-a/node-01/assessment")[1]["evaluation_mode"] == "current"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_historical_decision_keeps_its_clock_mode_when_next_positive_arrives(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["objects"] = [{"object_kind": "unit", "object_id": "A"}]
    now["value"] = "2026-10-10T12:00:00+00:00"
    first = [observation(1, "2026-08-31T15:59:39+00:00"), observation(2, "2026-08-31T15:59:49+00:00", kind="state_snapshot"), observation(3, "2026-08-31T15:59:59+00:00", kind="state_snapshot")]
    context = EvaluationContext.historical(first[-1]["occurred_at"])
    service.observations.submit(batch("initial-history", *first, mode="historical"), "fixture-actor", context=context)
    saved = next(decision for decision in service.repository.decisions("prod-a", "node-01") if decision["state"] == "clear")
    later = observation(4, "2026-08-31T16:00:05+00:00", kind="state_snapshot")
    service.observations.submit(batch("later-history", later, mode="historical"), "fixture-actor", context=EvaluationContext.historical(later["occurred_at"]))
    previous = next(decision for decision in service.repository.decisions("prod-a", "node-01") if decision["decision_ref"] == saved["decision_ref"])
    assert previous["evaluation_mode"] == "historical" and previous["superseded_by"] is None
    assert service.assessment("prod-a", "node-01")["status"] == "unknown"
