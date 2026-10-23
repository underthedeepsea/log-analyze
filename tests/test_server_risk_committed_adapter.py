from __future__ import annotations

import copy
import json
import time

import pytest

from logrisk.server_risk.committed_adapter import CommittedObservationWorker
from logrisk.server_risk.contracts import ServerRiskError, json_text
from tests.server_risk_fixtures import commit_source, committed_window, domain_services, seed_committed


def test_independent_worker_automatically_consumes_three_commits_and_whole_frontier(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    adapter, stages = commit_source(service)
    worker = CommittedObservationWorker(adapter, interval=.25)
    assert worker.start() and not worker.start()
    states = []
    try:
        for index, (timestamp, expected) in enumerate((("2026-08-31T12:00:00+00:00", "present"), ("2026-08-31T12:00:10+00:00", "recovering"), ("2026-08-31T12:00:20+00:00", "clear"))):
            now["value"] = timestamp
            seed_committed(service.repository.database, "auto-" + str(index), [committed_window(index, timestamp, positive=index > 0)])
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if any(stage["stage"] == "commit_frontier_complete" and stage["commit_ref"] == "auto-" + str(index) for stage in stages):
                    break
                time.sleep(.025)
            assert any(stage["stage"] == "commit_frontier_complete" and stage["commit_ref"] == "auto-" + str(index) for stage in stages)
            result = service.assessment("prod-a", "node-01")
            states.append(result["status"])
            assert result["status"] == expected
        check = result["checks"][0]
        assert check["observed_seconds"] == 10 and check["remaining_seconds"] == 0
        assert check["policy_version"] and check["decision_ref"] and len(check["evidence_refs"]) == 3
        assert result["episodes"][0]["resolved_at"] == now["value"]
        assert service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["summary"]["known_occurrence_count"] == 1
        assert adapter.advance("fixture-source", "fixture-task", "auto-2")["idempotent_replay"]
        assert len(service.repository.observations("prod-a", "node-01")) == 3
    finally:
        worker.close()
    assert not worker.thread.is_alive() and states == ["present", "recovering", "clear"]


@pytest.mark.parametrize("fault,code", [("absent", "commit_not_found"), ("partial", "commit_integrity_invalid"), ("hash", "commit_integrity_invalid"), ("namespace", "commit_namespace_mismatch"), ("node", "commit_scope_mismatch"), ("identity", "commit_identity_invalid"), ("duplicate", "commit_contribution_conflict")])
def test_commit_integrity_negatives_reach_target_and_pair_normal(tmp_path, fault, code):
    service, now, *_ = domain_services(tmp_path)
    adapter, stages = commit_source(service)
    window = committed_window(1, now["value"])
    windows = [window]
    if fault == "namespace":
        window["source_namespace"] = "forged"
    if fault == "node":
        window["entity_id"] = "other"
    if fault == "identity":
        window["risk_semantic"]["semantic_fields"].pop("unit")
    if fault == "duplicate":
        windows.append(copy.deepcopy(window))
    if fault != "absent":
        seed_committed(service.repository.database, "bad", windows)
        with service.repository.database.transaction() as conn:
            if fault == "partial":
                conn.execute("DELETE FROM streaming_batch_windows WHERE window_id='bad'")
            if fault == "hash":
                conn.execute("UPDATE streaming_window_commits SET payload_hash='wrong' WHERE window_id='bad'")
    with pytest.raises(ServerRiskError) as caught:
        adapter.advance("fixture-source", "fixture-task", "bad")
    assert caught.value.code == code
    assert any(stage["stage"] == "commit_integrity_and_all_parts" for stage in stages)
    with service.repository.database.connect() as conn:
        assert not conn.execute("SELECT 1 FROM server_risk_observations").fetchone()
        assert not conn.execute("SELECT 1 FROM server_risk_projection_state WHERE status='complete'").fetchone()
    seed_committed(service.repository.database, "normal", [committed_window(2, now["value"])])
    assert adapter.advance("fixture-source", "fixture-task", "normal")["projection_complete"]
    assert service.assessment("prod-a", "node-01")["status"] == "present"


def test_default_off_unregistered_forged_trust_raw_fields_and_whole_parts_rollback(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    from logrisk.server_risk.committed_adapter import CommittedObservationAdapter
    off = CommittedObservationAdapter(service.repository, service.observations, policies, service.observations.clock)
    worker = CommittedObservationWorker(off)
    assert not worker.start() and worker.thread is None
    with pytest.raises(ServerRiskError) as error:
        off.advance("fixture-source", "fixture-task", "never")
    assert error.value.code == "commit_adapter_unregistered"
    adapter, stages = commit_source(service)
    windows = [committed_window(index, now["value"]) for index in range(501)]
    windows[-1]["risk_semantic"]["semantic_fields"]["ready"] = "forged-positive"
    # Wrong signal type is in the second part, after first part really inserted.
    windows[-1]["risk_semantic"]["risk_type"] = "fixture.healthy"
    windows[0]["samples"] = ["secret-raw"]
    windows[0]["risk_semantic"]["semantic_fields"]["message"] = "secret-raw"
    seed_committed(service.repository.database, "rollback", windows)
    with pytest.raises(ServerRiskError) as error:
        adapter.advance("fixture-source", "fixture-task", "rollback")
    assert error.value.code == "observation_fields_invalid"
    assert any(stage["stage"] == "commit_part_complete" and stage["part"] == 0 for stage in stages)
    assert service.repository.observations("prod-a", "node-01") == []
    control = [committed_window(index + 1000, now["value"]) for index in range(501)]
    control[0]["samples"] = ["secret-raw"]
    seed_committed(service.repository.database, "normal-parts", control)
    result = adapter.advance("fixture-source", "fixture-task", "normal-parts")
    assert result["accepted_count"] == 501
    with service.repository.database.connect() as conn:
        text = str(conn.execute("SELECT sanitized_payload_json FROM server_risk_observations").fetchall())
        assert "secret-raw" not in text and "samples" not in text
    assert {stage["part"] for stage in stages if stage["stage"] == "commit_part_complete"} == {0, 1}


def test_conflicting_physical_replay_cannot_replace_or_double_count(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    adapter, stages = commit_source(service)
    original = committed_window(1, now["value"])
    seed_committed(service.repository.database, "first", [original])
    adapter.advance("fixture-source", "fixture-task", "first")
    changed = copy.deepcopy(original)
    changed["count"] = 8
    seed_committed(service.repository.database, "conflicting", [changed])
    with pytest.raises(ServerRiskError) as error:
        adapter.advance("fixture-source", "fixture-task", "conflicting")
    assert error.value.code == "observation_fact_conflict"
    assert len(service.repository.observations("prod-a", "node-01")) == 1
    assert service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["summary"]["known_occurrence_count"] == 1
    assert any(stage["stage"] == "sanitized_observation_projection" for stage in stages)
