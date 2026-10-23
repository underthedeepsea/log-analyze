from __future__ import annotations

from tests.server_risk_fixtures import batch, domain_services, observation


def submit(service, key, *items):
    return service.observations.submit(batch(key, *items), "fixture-actor")


def check(service, obj="A"):
    return next(c for c in service.assessment("prod-a", "node-01")["checks"] if c["identity"]["object_id"] == obj)


def test_object_identity_hold_gap_expiry_and_recurrence(tmp_path):
    service, now, config, policies, _ = domain_services(tmp_path)
    submit(service, "a", observation(1, "2026-08-31T12:00:00+00:00"))
    assert check(service)["status"] == "present"
    now["value"] = "2026-08-31T12:00:05+00:00"
    submit(service, "b", observation(2, now["value"], kind="state_snapshot", obj="B"))
    assert check(service)["status"] == "present"
    submit(service, "candidate", observation(3, now["value"], kind="recovery_candidate"))
    assert check(service)["status"] == "unknown"
    now["value"] = "2026-08-31T12:00:10+00:00"
    submit(service, "positive-1", observation(4, now["value"], kind="state_snapshot"))
    assert check(service)["status"] == "recovering"
    now["value"] = "2026-08-31T12:00:30+00:00"
    submit(service, "gap", observation(5, now["value"], kind="state_snapshot"))
    assert check(service)["status"] == "recovering"
    now["value"] = "2026-08-31T12:00:40+00:00"
    submit(service, "positive-2", observation(6, now["value"], kind="state_snapshot"))
    assert check(service)["status"] == "clear"
    first = service.assessment("prod-a", "node-01")["episodes"][0]
    assert first["resolved_at"] == now["value"]
    now["value"] = "2026-08-31T12:00:45+00:00"
    submit(service, "positive-3", observation(7, now["value"], kind="state_snapshot"))
    assert service.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"] == first["resolved_at"]
    now["value"] = "2026-08-31T12:03:00+00:00"
    assert check(service)["status"] == "unknown"
    assert service.assessment("prod-a", "node-01")["episodes"][0]["lifecycle"] == "resolved"
    submit(service, "recurrence", observation(8, now["value"]))
    rounds = service.assessment("prod-a", "node-01")["episodes"]
    assert len(rounds) == 2 and rounds[1]["previous_episode_id"] == rounds[0]["episode_id"]


def test_late_adverse_supersedes_clear_without_rewriting_old_decision(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-08-31T12:00:30+00:00"
    submit(service, "initial", observation(1, "2026-08-31T12:00:00+00:00"), observation(2, "2026-08-31T12:00:10+00:00", kind="state_snapshot"), observation(4, "2026-08-31T12:00:20+00:00", kind="state_snapshot"))
    assert check(service)["status"] == "clear"
    submit(service, "late", observation(3, "2026-08-31T12:00:15+00:00"))
    assert check(service)["status"] == "recovering"
    decisions = service.repository.decisions("prod-a", "node-01")
    assert any(d["state"] == "clear" and d["superseded_by"] for d in decisions)
    assert service.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"] is None


def test_boot_change_does_not_clear_unresolved_risk(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-08-31T12:00:20+00:00"
    submit(service, "boot", observation(1, "2026-08-31T12:00:00+00:00"), observation(2, "2026-08-31T12:00:10+00:00", kind="state_snapshot", boot="boot-2"), observation(3, now["value"], kind="state_snapshot", boot="boot-2"))
    assert check(service)["status"] == "unknown"
    assert "boot_incompatible" in check(service)["reasons"]


def test_hardware_requires_matching_verified_device_and_config_epoch(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    source = policies.sources["fixture-source"]
    source["signals"]["unit_expected_state"]["kind"] = "recovery_candidate"
    source["signals"]["gpu_recovery_verified"] = {"kind": "state_snapshot", "risk_type": "service.failed", "field_types": {"verified": "boolean"}}
    policies.rules["service.failed"] = {"mode": "hardware", "recovery_signal": "gpu_recovery_verified", "positive_field": "verified", "positive_value": True, "hold_seconds": 10}
    now["value"] = "2026-08-31T12:00:20+00:00"
    submit(service, "hardware", observation(1, "2026-08-31T12:00:00+00:00"))
    submit(service, "ordinary", observation(2, "2026-08-31T12:00:05+00:00", kind="recovery_candidate"))
    assert check(service)["status"] == "unknown" and not service.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"]
    one = observation(3, "2026-08-31T12:00:10+00:00", kind="state_snapshot", signal_code="gpu_recovery_verified", typed_fields={"verified": True})
    two = observation(4, now["value"], kind="state_snapshot", signal_code="gpu_recovery_verified", typed_fields={"verified": True})
    submit(service, "verified", one, two)
    assert check(service)["status"] == "clear"
    source["config_revision"] = "config-2"
    assert check(service)["status"] == "unknown"
    assert service.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"] is None


def test_historical_positive_and_activity_gap_never_clear(tmp_path):
    from tests.test_server_risk_daily import stream_service, coverage
    service, now, policies = stream_service(tmp_path)
    policies.rules["service.failed"]["requires_activity"] = True
    now["value"] = "2026-08-31T12:00:20+00:00"
    gap = coverage(2, "2026-08-31T12:00:00+00:00", now["value"], "gap", gaps=[{"start": "2026-08-31T12:00:05+00:00", "end": "2026-08-31T12:00:06+00:00"}])
    submit(service, "gap", observation(1, "2026-08-31T12:00:00+00:00"), gap)
    assert check(service)["status"] == "unknown"
    cover = coverage(3, "2026-08-31T12:00:00+00:00", now["value"], "noactivity")
    submit(service, "noactivity", cover)
    assert check(service)["status"] == "unknown"
    cover = coverage(4, "2026-08-31T12:00:00+00:00", now["value"], "history")
    cover["typed_fields"] = {"activity": True, "success": True}
    service.observations.submit(batch("history", cover, mode="historical"), "fixture-actor")
    assert check(service)["status"] == "unknown"


def test_positive_boot_boundary_restarts_hold_without_adverse(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["objects"] = [{"object_kind": "unit", "object_id": "A"}]
    now["value"] = "2026-08-31T12:00:10+00:00"
    submit(service, "first", observation(1, "2026-08-31T12:00:00+00:00", kind="state_snapshot"), observation(2, now["value"], kind="state_snapshot", boot="boot-2"))
    assert check(service)["status"] == "recovering"
    now["value"] = "2026-08-31T12:00:20+00:00"
    submit(service, "second", observation(3, now["value"], kind="state_snapshot", boot="boot-2"))
    assert check(service)["status"] == "clear"
    control, clock, *_ = domain_services(tmp_path / "positive")
    clock["value"] = now["value"]
    submit(control, "same-boot", observation(1, "2026-08-31T12:00:00+00:00", kind="state_snapshot"), observation(2, "2026-08-31T12:00:10+00:00", kind="state_snapshot"))
    assert check(control)["status"] == "clear"


def test_hardware_new_boot_verified_same_device_ends_only_its_round(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    source = policies.sources["fixture-source"]
    source["signals"]["gpu_recovery_verified"] = {"kind": "state_snapshot", "risk_type": "service.failed", "field_types": {"verified": "boolean"}}
    policies.rules["service.failed"] = {"mode": "hardware", "recovery_signal": "gpu_recovery_verified", "positive_field": "verified", "positive_value": True, "hold_seconds": 10}
    now["value"] = "2026-08-31T12:00:30+00:00"
    submit(service, "failed", observation(1, "2026-08-31T12:00:00+00:00"))
    submit(service, "ready", observation(2, "2026-08-31T12:00:05+00:00", kind="state_snapshot", boot="boot-2"))
    assert check(service)["status"] == "unknown"
    def verified(seq, time, obj="A"):
        return observation(seq, time, obj=obj, boot="boot-2", kind="state_snapshot", signal_code="gpu_recovery_verified", typed_fields={"verified": True})
    submit(service, "other-device", verified(3, "2026-08-31T12:00:10+00:00", "B"), verified(4, "2026-08-31T12:00:20+00:00", "B"))
    assert not service.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"]
    submit(service, "matched", verified(5, "2026-08-31T12:00:20+00:00"), verified(6, now["value"]))
    assert check(service)["status"] == "clear"
    assert service.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"] == now["value"]


def test_required_activity_complete_quiet_window_clears_but_gap_restarts_observation(tmp_path):
    from tests.test_server_risk_daily import stream_service, coverage
    def configured(path):
        service, now, policies = stream_service(path)
        policies.rules["service.failed"]["requires_activity"] = True
        now["value"] = "2026-08-31T12:00:20+00:00"
        submit(service, "adverse", observation(1, "2026-08-31T12:00:00+00:00"))
        return service, now
    normal, _ = configured(tmp_path / "normal")
    positive = coverage(2, "2026-08-31T12:00:00+00:00", "2026-08-31T12:00:20+00:00", "activity")
    positive["typed_fields"] = {"activity": True, "success": True}
    submit(normal, "activity", positive)
    assert check(normal)["status"] == "clear" and normal.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"]
    gap, clock = configured(tmp_path / "gap")
    incomplete = coverage(2, "2026-08-31T12:00:00+00:00", clock["value"], "gap", gaps=[{"start": "2026-08-31T12:00:05+00:00", "end": "2026-08-31T12:00:06+00:00"}])
    incomplete["typed_fields"] = {"activity": True, "success": True}
    submit(gap, "gap", incomplete)
    assert check(gap)["status"] == "unknown" and gap.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"] is None
    clock["value"] = "2026-08-31T12:00:25+00:00"
    restarted = coverage(3, "2026-08-31T12:00:20+00:00", clock["value"], "restart")
    restarted["typed_fields"] = {"activity": True, "success": True}
    submit(gap, "restart", restarted)
    assert check(gap)["status"] == "recovering"
    clock["value"] = "2026-08-31T12:00:30+00:00"
    completed = coverage(4, "2026-08-31T12:00:25+00:00", clock["value"], "complete")
    completed["typed_fields"] = {"activity": True, "success": True}
    submit(gap, "complete", completed)
    assert check(gap)["status"] == "clear"
    assert gap.assessment("prod-a", "node-01")["episodes"][0]["resolved_at"] == clock["value"]


def test_sorted_and_late_arrival_independent_fixtures_have_same_final_semantics(tmp_path):
    ordered, clock, *_ = domain_services(tmp_path / "ordered")
    late, late_clock, *_ = domain_services(tmp_path / "late")
    clock["value"] = late_clock["value"] = "2026-08-31T12:00:30+00:00"
    facts = [observation(1, "2026-08-31T12:00:00+00:00"), observation(2, "2026-08-31T12:00:10+00:00", kind="state_snapshot"), observation(3, "2026-08-31T12:00:15+00:00"), observation(4, "2026-08-31T12:00:20+00:00", kind="state_snapshot"), observation(5, "2026-08-31T12:00:30+00:00", kind="state_snapshot")]
    submit(ordered, "all", *facts)
    submit(late, "first", facts[4], facts[1], facts[0], facts[3])
    assert check(late)["status"] == "clear"
    submit(late, "late", facts[2])
    first = ordered.assessment("prod-a", "node-01")
    second = late.assessment("prod-a", "node-01")
    keys = ("status", "severity", "evidence_at", "valid_until", "last_known_state", "reasons", "coverage", "revision", "checks", "episodes", "projection_complete")
    assert {key: first[key] for key in keys} == {key: second[key] for key in keys}
    assert first["checks"][0]["status"] == "clear" and first["episodes"][0]["resolved_at"] == clock["value"]
    assert any(decision["effective_at"] == clock["value"] and not decision["superseded_by"] for decision in late.repository.decisions("prod-a", "node-01") if decision["state"] == "clear")
