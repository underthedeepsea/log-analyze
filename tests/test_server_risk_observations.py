from __future__ import annotations

import copy
import pytest
from logrisk.server_risk.contracts import ServerRiskError
from tests.server_risk_fixtures import batch, domain_services, observation


def test_source_schema_actor_time_and_idempotency_are_strict(tmp_path):
    service, now, config, policies, fake = domain_services(tmp_path)
    good = batch("first", observation(1, now["value"]))
    for mutate, code in ((lambda p: p.update(source_id="unknown"), "observation_source_unregistered"), (lambda p: p["observations"][0].update(message="healthy"), "invalid_schema"), (lambda p: p["observations"][0]["typed_fields"].update(raw_sample="secret"), "invalid_schema"), (lambda p: p["observations"][0].update(occurred_at="2026-08-31T15:00:00+00:00"), "observation_clock_skew"), (lambda p: p["observations"][0].update(evidence_refs=["/etc/passwd"]), "observation_evidence_ref_invalid")):
        invalid = copy.deepcopy(good)
        mutate(invalid)
        with pytest.raises(ServerRiskError) as exc:
            service.observations.submit(invalid, "fixture-actor")
        assert exc.value.code == code
    with pytest.raises(ServerRiskError) as exc:
        service.observations.submit(good, "other-actor")
    assert exc.value.code == "observation_actor_forbidden"
    assert service.observations.submit(good, "fixture-actor")["accepted_count"] == 1
    assert service.observations.submit(good, "fixture-actor")["idempotent_replay"]
    changed = copy.deepcopy(good)
    changed["observations"][0]["count"] = 2
    with pytest.raises(ServerRiskError) as exc:
        service.observations.submit(changed, "fixture-actor")
    assert exc.value.code == "observation_idempotency_conflict"
    assert len(service.repository.observations("prod-a", "node-01")) == 1
    assert not fake.calls


def test_transaction_rolls_back_conflicting_physical_fact(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    service.observations.submit(batch("one", observation(1, now["value"])), "fixture-actor")
    conflicting = observation(1, now["value"], count=9)
    with pytest.raises(ServerRiskError):
        service.observations.submit(batch("two", observation(2, now["value"]), conflicting), "fixture-actor")
    assert len(service.repository.observations("prod-a", "node-01")) == 1


def test_semantic_revision_replaces_current_contribution_without_deleting_audit(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    item = observation(1, now["value"], count=2)
    service.observations.submit(batch("old", item), "fixture-actor")
    policies.sources["fixture-source"]["config_revision"] = "config-2"
    newer = batch("new", dict(item, count=3)); newer["config_revision"] = "config-2"
    service.observations.submit(newer, "fixture-actor")
    assert service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["summary"]["known_occurrence_count"] == 3
    assert len(service.repository.observations("prod-a", "node-01")) == 1
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM server_risk_observations").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM server_risk_decisions WHERE superseded_by IS NOT NULL").fetchone()[0] >= 1


def test_error_only_cannot_forge_authority_or_coverage_and_batch_is_bounded(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["capability"] = "error_only"
    with pytest.raises(ServerRiskError) as exc:
        service.observations.submit(batch("forged", observation(1, now["value"], kind="state_snapshot")), "fixture-actor")
    assert exc.value.code == "observation_authority_required"
    with pytest.raises(ServerRiskError) as exc:
        service.observations.submit(batch("oversized", *[observation(i, now["value"]) for i in range(501)]), "fixture-actor")
    assert exc.value.code == "observation_batch_limit"
    with pytest.raises(ServerRiskError):
        service.observations.submit(batch("nan", observation(1, now["value"], typed_fields={"severity": float("nan")})), "fixture-actor")
    assert not service.repository.nodes()


def test_observation_concurrent_receipts_and_physical_conflicts_are_atomic(tmp_path):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    service, now, *_ = domain_services(tmp_path)
    def race(left, right):
        barrier = threading.Barrier(2)
        def submit(payload):
            barrier.wait(3)
            try:
                return service.observations.submit(payload, "fixture-actor")
            except ServerRiskError as exc:
                return exc.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(submit, [left, right]))
    same = batch("same", observation(1, now["value"]))
    results = race(same, same)
    assert all(isinstance(r, dict) and r["projection_complete"] for r in results)
    assert sorted(r["idempotent_replay"] for r in results) == [False, True]
    assert len(service.repository.observations("prod-a", "node-01")) == 1
    results = race(batch("key-conflict", observation(2, now["value"], count=2)), batch("key-conflict", observation(2, now["value"], count=3)))
    assert "observation_idempotency_conflict" in results and sum(isinstance(r, dict) for r in results) == 1
    results = race(batch("fact-left", observation(3, now["value"], count=2)), batch("fact-right", observation(3, now["value"], count=3)))
    assert "observation_fact_conflict" in results and sum(isinstance(r, dict) for r in results) == 1
    with service.repository.database.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM server_risk_projection_state WHERE input_namespace LIKE 'batch:%'").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM server_risk_observations").fetchone()[0] == 3
    results = race(batch("copy-left", observation(4, now["value"])), batch("copy-right", observation(4, now["value"])))
    assert all(isinstance(r, dict) and r["projection_complete"] for r in results)
    assert len(service.repository.observations("prod-a", "node-01")) == 4
    assert service.observations.submit(batch("after", observation(5, now["value"])), "fixture-actor")["projection_complete"]
