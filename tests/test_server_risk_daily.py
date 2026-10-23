from __future__ import annotations

import copy
import json
import pytest
from logrisk.server_risk.contracts import ServerRiskError, day_bounds
from tests.server_risk_fixtures import batch, domain_services, observation


def coverage(seq, start, end, key, **changes):
    item = observation(seq, end, kind="coverage", typed_fields={})
    item.pop("occurred_at")
    item.update(interval_start=start, interval_end=end, coverage={"signal_codes": ["failed"], "covered_start": start, "covered_end": end, "complete_through": end, "gaps": [], "dropped_count": 0, "parse_failure_count": 0, "input_frontier_ref": "batch:" + key})
    item["coverage"].update(changes)
    return item


def stream_service(tmp_path):
    service, now, config, policies, fake = domain_services(tmp_path)
    source = policies.sources["fixture-source"]
    source["capability"] = "complete_event_stream"
    source["objects"] = [{"object_kind": "unit", "object_id": "A"}]
    source["signals"] = {"failed": {"kind": "adverse", "risk_type": "service.failed"}, "coverage": {"kind": "coverage", "risk_type": "service.failed"}}
    policies.rules["service.failed"] = {"mode": "quiet", "required_signals": ["failed"], "hold_seconds": 10}
    return service, now, policies


def test_local_midnight_dst_missing_days_and_complete_zero(tmp_path):
    service, now, policies = stream_service(tmp_path)
    a, b = day_bounds("2026-03-08", "America/New_York")
    assert (b - a).total_seconds() == 23 * 3600
    a, b = day_bounds("2026-11-01", "America/New_York")
    assert (b - a).total_seconds() == 25 * 3600
    now["value"] = "2026-09-01T16:00:00+00:00"
    service.observations.submit(batch("day", coverage(1, "2026-08-31T16:00:00+00:00", "2026-09-01T16:00:00+00:00", "day")), "fixture-actor")
    series = service.daily("prod-a", "node-01", "2026-08-31", "2026-09-02", "Asia/Shanghai")
    assert len(series["items"]) == 3
    assert series["items"][0]["occurrence_count"] is None
    assert series["items"][1]["occurrence_count"] == 0 and series["items"][1]["coverage"] == "complete"
    assert series["items"][2]["occurrence_count"] is None and series["items"][2]["coverage"] == "unknown"


def test_midnight_exclusive_intervals_and_full_pagination(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-09-02T00:00:00+00:00"
    interval = observation(1, now["value"], count=5)
    interval.pop("occurred_at")
    interval.update(interval_start="2026-08-31T15:00:00+00:00", interval_end="2026-08-31T16:00:00+00:00")
    items = [interval] + [observation(i + 2, "2026-08-31T15:30:00+00:00") for i in range(125)]
    service.observations.submit(batch("many", *items), "fixture-actor")
    first = service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai", limit=50)
    assert first["total"] == 126 and first["summary"]["known_occurrence_count"] == 130
    assert "boundary_ambiguous" not in first["summary"]["missing_reasons"]
    refs = [f["fact_ref"] for f in first["items"]]
    page = first
    while page["next_cursor"]:
        page = service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai", cursor=page["next_cursor"], limit=50)
        refs += [f["fact_ref"] for f in page["items"]]
    assert len(refs) == len(set(refs)) == 126
    next_day = service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")
    assert next_day["total"] == 0
    with pytest.raises(ServerRiskError):
        service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai", cursor=first["next_cursor"])


def test_error_only_no_event_cannot_clear_and_partial_frontier_stays_unknown(tmp_path):
    service, now, policies = stream_service(tmp_path)
    now["value"] = "2026-08-31T12:00:20+00:00"
    service.observations.submit(batch("cover", observation(1, "2026-08-31T12:00:00+00:00"), coverage(2, "2026-08-31T12:00:00+00:00", now["value"], "cover")), "fixture-actor")
    assert service.assessment("prod-a", "node-01")["status"] == "clear"
    with service.repository.database.transaction() as conn:
        conn.execute("UPDATE server_risk_projection_state SET status='partial'")
    assert service.assessment("prod-a", "node-01")["status"] == "unknown"
    policies.sources["fixture-source"]["capability"] = "error_only"
    assert service.assessment("prod-a", "node-01")["status"] != "clear"


def test_cross_day_aggregate_not_distributed_and_scores_use_assessed_day(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-09-02T00:00:00+00:00"
    interval = observation(1, now["value"], count=11)
    interval.pop("occurred_at")
    interval.update(interval_start="2026-08-31T15:59:00+00:00", interval_end="2026-08-31T16:01:00+00:00")
    service.observations.submit(batch("cross", interval), "fixture-actor")
    with service.repository.database.transaction() as conn:
        conn.execute("INSERT INTO node_risk_score_samples(sample_id,cluster,node_id,revision,assessed_at,overall_score) VALUES ('s','prod-a','node-01',1,'2026-09-01T20:00:00+00:00',90)")
    series = service.daily("prod-a", "node-01", "2026-08-31", "2026-09-02", "Asia/Shanghai")
    assert [p["known_occurrence_count"] for p in series["items"]] == [0, 0, 0]
    assert "boundary_ambiguous" in series["items"][0]["missing_reasons"]
    assert [p["assessed_score_peak"] for p in series["items"]] == [None, None, 90]


def test_committed_legacy_physical_contribution_uses_source_batch_and_item_fallback(tmp_path):
    from logrisk.server_risk.contracts import digest, json_text
    from tests.server_risk_fixtures import legacy_digest
    service, now, *_ = domain_services(tmp_path)
    db = service.repository.database
    with db.transaction() as conn:
        conn.execute("INSERT INTO node_risk_events(event_id,cluster,node_id,risk_domain,risk_category,risk_type,severity,base_score,confidence,semantic_rule_id,semantic_rule_version,dedup_key,occurrence_count,first_seen,last_seen,window_start,window_end,status,semantic_fields_json,evidence_refs_json,created_at,updated_at) VALUES ('legacy','prod-a','node-01','service','service','service.failed','high',60,.8,'rule',1,'dedup',9,'2026-08-31T01:00:00+00:00','2026-09-01T01:00:00+00:00','2026-08-31T01:00:00+00:00','2026-09-01T01:05:00+00:00','recovered','{}','[]','2026-08-31T01:00:00+00:00','2026-09-01T01:00:00+00:00')")
        conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES ('task','file','{}','config','completed','COMPLETED','{}','{}','2026-09-01T01:00:00Z','2026-09-01T01:00:00Z')")
        conn.execute("INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at) VALUES ('task','window','{}','{}','2026-09-01T01:00:00Z')")
        for index, item_field in enumerate(("source_item_id", "raw_log_id", "template_instance_hash")):
            window = {"source_namespace": "ns", "source_batch_id": "batch-stable", "window_start": "2026-09-01T01:00:00+00:00", item_field: "item-" + str(index), "first_seen": "2026-09-01T01:00:00+00:00", "last_seen": "2026-09-01T01:01:00+00:00"}
            physical = digest(["ns", "batch-stable", window["window_start"], "item-" + str(index)])
            conn.execute("INSERT INTO streaming_batch_windows(task_id,window_id,item_index,window_json) VALUES ('task','window',?,?)", (index, json_text(window)))
            conn.execute("INSERT INTO node_risk_ingestions(source_event_fingerprint,event_id,occurrence_count,ingested_at,physical_key,semantic_revision,is_current) VALUES (?,'legacy',2,'2026-09-01T01:00:00Z',?,'r1',1)", ("fingerprint-" + str(index), physical))
        conn.execute("INSERT INTO node_risk_ingestions(source_event_fingerprint,event_id,occurrence_count,ingested_at,physical_key,semantic_revision,is_current) VALUES ('old','legacy',100,'2026-09-01T01:00:00Z','old','r0',0)")
    before = legacy_digest(db)
    result = service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")
    assert result["summary"]["known_occurrence_count"] == 6 and result["total"] == 3
    assert all(f["provenance"] == "committed_window" and f["first_seen"] == "2026-09-01T01:00:00+00:00" for f in result["items"])
    assert service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["total"] == 0
    assert service.assessment("prod-a", "node-01")["status"] == "unknown"
    assert legacy_digest(db) == before


def test_midnight_point_is_next_day_and_incomplete_episode_total_unknown(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-08-31T16:00:00+00:00"
    service.observations.submit(batch("midnight", observation(1, now["value"])), "fixture-actor")
    previous = service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")
    following = service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")
    assert previous["total"] == 0 and previous["summary"]["end_of_day_assessment"] == "unknown"
    assert following["total"] == 1 and following["summary"]["known_episode_started_count"] == 1
    assert following["summary"]["episode_started_count"] is None


def test_adjacent_coverage_union_requires_compatible_object_epoch_and_no_gap(tmp_path):
    service, now, policies = stream_service(tmp_path)
    now["value"] = "2026-09-01T16:00:00+00:00"
    start, middle, end = "2026-08-31T16:00:00+00:00", "2026-09-01T04:00:00+00:00", now["value"]
    service.observations.submit(batch("first-half", coverage(1, start, middle, "first-half")), "fixture-actor")
    second = coverage(2, middle, end, "second-half")
    second["identity"]["boot_id"] = "boot-2"
    service.observations.submit(batch("second-half", second), "fixture-actor")
    point = service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")["summary"]
    assert point["occurrence_count"] is None and point["coverage"] != "complete"
    service.observations.submit(batch("matching", coverage(3, middle, end, "matching")), "fixture-actor")
    point = service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")["summary"]
    assert point["occurrence_count"] == 0 and point["coverage"] == "complete"
    gap, clock, _ = stream_service(tmp_path / "gap")
    clock["value"] = end
    gap.observations.submit(batch("g1", coverage(1, start, middle, "g1")), "fixture-actor")
    gap.observations.submit(batch("g2", coverage(2, "2026-09-01T04:00:01+00:00", end, "g2")), "fixture-actor")
    assert gap.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")["summary"]["occurrence_count"] is None


def test_severity_occurrences_are_weighted_and_exclude_boundary_aggregate(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-09-02T00:00:00+00:00"
    ambiguous = observation(3, now["value"], count=11)
    ambiguous.pop("occurred_at")
    ambiguous.update(interval_start="2026-08-31T15:59:00+00:00", interval_end="2026-08-31T16:01:00+00:00")
    service.observations.submit(batch("weights", observation(1, "2026-08-31T12:00:00+00:00", count=100), observation(2, "2026-08-31T12:00:00+00:00", count=1, typed_fields={"severity": "low"}), ambiguous), "fixture-actor")
    summary = service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["summary"]
    assert summary["known_occurrence_count"] == 101
    assert summary["severity_counts"] == {"high": 100, "low": 1}
    assert summary["severity_record_counts"] == {"high": 2, "low": 1}
    assert "boundary_ambiguous" in summary["missing_reasons"]
    control = service.day("prod-a", "node-01", "2026-09-01", "Asia/Shanghai")["summary"]
    assert control["known_occurrence_count"] == 0 and control["severity_counts"] == {}


def test_unrelated_10001_observations_decisions_windows_do_not_block_scoped_query_or_submit(tmp_path):
    from logrisk.server_risk.contracts import digest, json_text
    service, now, *_ = domain_services(tmp_path)
    service.observations.submit(batch("A", observation(1, now["value"])), "fixture-actor")
    db = service.repository.database
    with db.transaction() as conn:
        original = dict(conn.execute("SELECT * FROM server_risk_observations LIMIT 1").fetchone())
        payload = json.loads(original["sanitized_payload_json"])
        payload["identity"]["node_id"] = "NODE-01"
        columns = list(original)
        query = "INSERT INTO server_risk_observations(" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")"
        values = []
        for index in range(10001):
            item = dict(original, observation_id="other-o-" + str(index), source_fact_key="other-f-" + str(index), subject_key="other-subject", sanitized_payload_json=json_text(payload))
            values.append([item[k] for k in columns])
        conn.executemany(query, values)
        conn.executemany("INSERT INTO server_risk_decisions(decision_id,subject_key,revision,effective_at,decided_at,state,reason_codes_json,evidence_refs_json,policy_version) VALUES (?,?,?,?,?,'unknown','[]','[]','fixture')", [("other-d-" + str(i), "other-subject", str(i), now["value"], now["value"]) for i in range(10001)])
        conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES ('other','file','{}','config','completed','COMPLETED','{}','{}',?,?)", (now["value"], now["value"]))
        conn.execute("INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at) VALUES ('other','commit','{}','{}',?)", (now["value"],))
        conn.executemany("INSERT INTO streaming_batch_windows(task_id,window_id,item_index,window_json) VALUES ('other','commit',?,?)", [(i, json_text({"cluster": "prod-a", "entity_type": "node", "entity_id": "NODE-01", "template_hash": "other", "window_start": now["value"]})) for i in range(10001)])
    assert service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")["summary"]["known_occurrence_count"] == 1
    assert service.observations.submit(batch("A-next", observation(2, now["value"])), "fixture-actor")["projection_complete"]
    assert len(service.repository.observations("prod-a", "node-01")) == 2
    assert service.repository.decisions("prod-a", "node-01")
    assert len(service.repository.nodes()) == 2


def test_real_committed_window_pages_recover_best_effort_and_detect_missing_page(tmp_path):
    from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
    from logrisk.streaming_state import StreamingStateRepository
    from logrisk.node_risk import NodeRiskService
    from tests.server_risk_fixtures import legacy_digest
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-08-31T12:01:00+00:00"
    empty_source = tmp_path / "empty-source"
    empty_source.touch()
    state = StreamingStateRepository(service.repository.database)
    task = state.create_or_load(descriptor=FileIncrementalSource(empty_source, filename="empty-source").descriptor(), config_hash="a" * 64)
    semantic = {"schema_version": "semantic_event_v1", "domain": "service", "category": "service", "risk_type": "service.failed", "severity": "high", "base_score": 60, "confidence": .8, "semantic_rule_id": "fixture", "semantic_rule_version": 1, "semantic_fields": {"unit": "A"}, "dedup": {"window_seconds": 300, "key_fields": ["cluster", "node", "unit"]}, "lifecycle": {}}
    windows = [{"template_hash": "template-" + str(i), "cluster": "prod-a", "entity_type": "node", "entity_id": "node-01", "count": 1, "source_namespace": "fixture-source", "source_batch_id": "commit", "source_item_id": "item-" + str(i), "window_start": "2026-08-31T12:00:00+00:00", "first_seen": "2026-08-31T12:00:00+00:00", "last_seen": "2026-08-31T12:00:01+00:00", "risk_semantic": semantic} for i in range(501)]
    state.commit_window(task["task_id"], window_id="commit", cursor=SourceCursor("file", {"offset": 501}), templates=[], windows=windows, summary={"record_count": 501, "template_count": 501})
    old = NodeRiskService(service.repository.database, "configs/node_risk.yaml", clock=lambda: now["value"])
    old.ingest(semantic, source_record=dict(windows[0], node="node-01"), source_job_id="fixture-job", occurrence_count=1)
    succeeded = old.committed_ingestion_count(windows, semantic_revision=None)
    assert succeeded == 1 and succeeded < sum(w["count"] for w in windows)
    before = legacy_digest(service.repository.database)
    projection = service.repository.historical_projection("prod-a", "node-01")
    assert projection["projection_complete"] and len(projection["windows"]) == 501
    day = service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai", limit=100)
    assert day["summary"]["known_occurrence_count"] == 501 and day["total"] == 501 and day["summary"]["projection_complete"]
    with service.repository.database.transaction() as conn:
        conn.execute("DELETE FROM streaming_batch_windows WHERE task_id=? AND window_id='commit' AND item_index>=250", (task["task_id"],))
    projection = service.repository.historical_projection("prod-a", "node-01")
    assert not projection["projection_complete"] and len(projection["windows"]) == 250
    missing = service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")
    assert not missing["summary"]["projection_complete"] and "projection_pending" in missing["summary"]["missing_reasons"]
    assert service.assessment("prod-a", "node-01")["status"] == "unknown"
    assert legacy_digest(service.repository.database) == before


def test_current_node_resource_limit_fails_explicitly_and_other_node_remains_readable(tmp_path):
    from logrisk.server_risk.contracts import json_text
    service, now, *_ = domain_services(tmp_path)
    service.observations.submit(batch("seed", observation(1, now["value"])), "fixture-actor")
    with service.repository.database.transaction() as conn:
        original = dict(conn.execute("SELECT * FROM server_risk_observations LIMIT 1").fetchone())
        columns = list(original)
        query = "INSERT INTO server_risk_observations(" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")"
        copies = []
        for index in range(10000):
            item = dict(original, observation_id="scope-o-" + str(index), source_fact_key="scope-f-" + str(index))
            copies.append([item[key] for key in columns])
        conn.executemany(query, copies)
        other = dict(original, observation_id="other-o", source_fact_key="other-f", subject_key="other-s")
        payload = json.loads(other["sanitized_payload_json"])
        payload["identity"]["node_id"] = "node-02"
        other["sanitized_payload_json"] = json_text(payload)
        conn.execute(query, [other[key] for key in columns])
    with pytest.raises(ServerRiskError) as caught:
        service.day("prod-a", "node-01", "2026-08-31", "Asia/Shanghai")
    assert caught.value.code == "query_resource_limit"
    assert len(service.repository.observations("prod-a", "node-02")) == 1
