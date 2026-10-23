from __future__ import annotations

import copy
from pathlib import Path

import pytest

from logrisk.server_risk.contracts import ServerRiskError, digest, json_text
from tests.server_risk_fixtures import batch, domain_services, observation


def seed_event(service, key, *, cluster="prod-a", node="node-01", obj="A", risk_type="service.failed", level="high", first="2026-08-31T01:00:00+00:00", last=None, count=7, status="active", source_job_id=None, rule_id="fixture-rule", rule_version=1):
    last = last or first
    with service.repository.database.transaction() as conn:
        conn.execute("INSERT INTO node_risk_events(event_id,cluster,node_id,risk_domain,risk_category,risk_type,severity,base_score,confidence,semantic_rule_id,semantic_rule_version,dedup_key,occurrence_count,first_seen,last_seen,window_start,window_end,status,semantic_fields_json,evidence_refs_json,source_job_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (key, cluster, node, "service", "service", risk_type, level, 60, .8, rule_id, rule_version, key, count, first, last, first, last, status, json_text({"unit": obj}), "[]", source_job_id, first, last))


def overview(service, **changes):
    return service.overview("2026-08-31", "2026-08-31", **changes)


def detail(service, cluster="prod-a", node="node-01", **changes):
    return service.overview_detail(cluster, node, "2026-08-31", "2026-08-31", **changes)


def database_digest(service):
    with service.repository.database.connect() as conn:
        return digest({table: [dict(row) for row in conn.execute("SELECT * FROM " + table).fetchall()] for table in ("node_risk_events", "node_risk_ingestions", "server_risk_observations", "server_risk_episodes", "server_risk_decisions", "server_risk_projection_state", "server_risk_reports")})


def test_date_beijing_midnight_half_open_and_unsplit_cross_day_counts(tmp_path):
    service, *_ = domain_services(tmp_path)
    seed_event(service, "before", obj="before", first="2026-08-30T15:59:59+00:00")
    seed_event(service, "start", obj="start", first="2026-08-30T16:00:00+00:00", count=2)
    seed_event(service, "end", obj="end", first="2026-08-31T16:00:00+00:00")
    seed_event(service, "cross", obj="cross", first="2026-08-30T15:00:00+00:00", last="2026-08-30T17:00:00+00:00", count=19)
    groups = {group["object"]["id"]: group for group in detail(service)["groups"]}
    assert set(groups) == {"start", "cross"}
    assert groups["start"]["known_occurrence_count"] == 2
    assert groups["cross"]["known_occurrence_count"] == 0
    assert groups["cross"]["occurrence_count"] is None
    assert "boundary_ambiguous" in groups["cross"]["quality"]["missing_reasons"]
    combined = service.overview_detail("prod-a", "node-01", "2026-08-30", "2026-08-31")
    assert next(group for group in combined["groups"] if group["object"]["id"] == "cross")["known_occurrence_count"] == 19


@pytest.mark.parametrize("first,last", [("2026-02-30", "2026-03-01"), ("9999-12-31", "9999-12-31"), ("0001-01-01", "0001-01-01"), ("2026-08-31", "2026-08-30"), ("2026-01-01", "2026-04-01")])
def test_date_invalid_and_extreme_ranges_fail_safely(tmp_path, first, last):
    service, *_ = domain_services(tmp_path)
    with pytest.raises(ServerRiskError):
        service.overview(first, last)


@pytest.mark.parametrize("first,last", [("0001-01-02", "0001-01-02"), ("9999-12-30", "9999-12-30"), ("2026-01-01", "2026-03-31")])
def test_date_representable_extremes_and_ninety_days_remain_legal(tmp_path, first, last):
    service, *_ = domain_services(tmp_path)
    result = service.overview(first, last)
    assert result["items"] == [] and result["summary"]["host_count"] == 0
    assert result["query"]["timezone"] == "Asia/Shanghai"


def test_date_registered_interval_ending_at_start_is_not_recounted(tmp_path):
    service, *_ = domain_services(tmp_path)
    item = observation(1, "2026-08-30T16:00:00+00:00")
    del item["occurred_at"]
    item.update(interval_start="2026-08-30T15:00:00+00:00", interval_end="2026-08-30T16:00:00+00:00", count=9)
    service.observations.submit(batch("interval", item), "fixture-actor")
    assert overview(service)["items"] == []
    previous = service.overview_detail("prod-a", "node-01", "2026-08-30", "2026-08-30")
    assert previous["groups"][0]["known_occurrence_count"] == 9


def test_unknown_legacy_active_and_manual_recovered_do_not_prove_current_state(tmp_path):
    service, *_ = domain_services(tmp_path)
    seed_event(service, "old-active")
    seed_event(service, "manual", obj="B", status="recovered")
    result = detail(service)
    assert len(result["groups"]) == 2
    assert {g["recovery_state"] for g in result["groups"]} == {"unknown"}
    assert all(g["occurrence_count"] is None and g["known_occurrence_count"] == 7 for g in result["groups"])
    assert all(g["analysis_status"] == "not_analyzed" for g in result["groups"])
    assert overview(service, recovery="unrecovered")["items"] == []
    assert overview(service, recovery="unknown")["summary"]["recovery_counts"] == {"unrecovered": 0, "recovered": 0, "unknown": 2}


def test_unknown_expired_registered_state_and_live_positive_pair(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    service.observations.submit(batch("fresh", observation(1, "2026-08-31T11:59:59+00:00")), "fixture-actor")
    assert detail(service)["groups"][0]["recovery_state"] == "unrecovered"
    now["value"] = "2026-08-31T12:02:00+00:00"
    assert detail(service)["groups"][0]["recovery_state"] == "unknown"
    assert overview(service, recovery="unrecovered")["items"] == []


def test_isolation_recovery_is_per_object_and_evidence_stays_on_host(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    now["value"] = "2026-08-31T12:00:20+00:00"
    service.observations.submit(batch("objects", observation(1, "2026-08-31T11:59:59+00:00"), observation(2, "2026-08-31T12:00:00+00:00", kind="state_snapshot"), observation(3, "2026-08-31T12:00:10+00:00", kind="state_snapshot"), observation(4, "2026-08-31T12:00:19+00:00", obj="B")), "fixture-actor")
    seed_event(service, "other-host", cluster="prod-b", obj="A", risk_type="network.cni.failed")
    result = detail(service)
    groups = {g["object"]["id"]: g for g in result["groups"]}
    assert groups["A"]["recovery_state"] == "recovered"
    assert groups["B"]["recovery_state"] == "unrecovered"
    assert set(groups["A"]["evidence_refs"]).isdisjoint(groups["B"]["evidence_refs"])
    assert "other-host" not in json_text(result)
    assert groups["A"]["association"]["status"] == "unverified"


def test_isolation_synthetic_source_is_excluded_without_host_name_heuristics(tmp_path):
    service, _, _, policies, _ = domain_services(tmp_path)
    synthetic = copy.deepcopy(policies.sources["fixture-source"])
    synthetic.update(source_id="synthetic-source", environment="synthetic", cluster="prod-b", node_ids=["ordinary-host"])
    policies.sources[synthetic["source_id"]] = synthetic
    item = observation(1, "2026-08-31T11:59:59+00:00")
    item["identity"].update(cluster="prod-b", node_id="ordinary-host")
    payload = dict(batch("synthetic", item), source_id="synthetic-source")
    service.observations.submit(payload, "fixture-actor")
    seed_event(service, "real-named-demo", node="DEMO-02", risk_type="network.cni.failed")
    result = overview(service)
    assert [(item["cluster"], item["node_id"]) for item in result["items"]] == [("prod-a", "DEMO-02")]
    assert detail(service, "prod-b", "ordinary-host")["groups"] == []
    assert "synthetic" not in json_text(result["items"])


def test_isolation_real_and_synthetic_same_host_do_not_merge_counts_or_recovery(tmp_path):
    service, _, _, policies, _ = domain_services(tmp_path)
    service.observations.submit(batch("real", observation(1, "2026-08-31T11:59:59+00:00")), "fixture-actor")
    synthetic = copy.deepcopy(policies.sources["fixture-source"])
    synthetic.update(source_id="synthetic-source", environment="synthetic")
    policies.sources[synthetic["source_id"]] = synthetic
    item = observation(20, "2026-08-31T11:59:59+00:00", count=900)
    service.observations.submit(dict(batch("synthetic", item), source_id="synthetic-source"), "fixture-actor")
    groups = detail(service)["groups"]
    assert len(groups) == 1 and groups[0]["known_occurrence_count"] == 1


def test_pagination_filters_groups_before_hosts_and_summary_covers_every_page(tmp_path):
    service, *_ = domain_services(tmp_path)
    for index in range(121):
        seed_event(service, "high-" + str(index), node="node-" + str(index).zfill(3), count=2)
        seed_event(service, "low-" + str(index), node="node-" + str(index).zfill(3), obj="B", level="low", count=3)
    page = overview(service, level="high", page=7, page_size=20)
    assert len(page["items"]) == 1 and page["items"][0]["node_id"] == "node-120"
    assert page["pagination"] == {"page": 7, "page_size": 20, "total": 121, "total_pages": 7}
    assert page["summary"]["host_count"] == page["summary"]["high_host_count"] == 121
    assert page["summary"]["risk_count"] == 121
    assert page["summary"]["known_occurrence_count"] == 242
    assert page["summary"]["unknown_occurrence_groups"] == 121
    assert all(item["risk_types"] == ["service.failed"] and item["risk_count"] == 1 for item in page["items"])
    assert overview(service, q="NODE-120")["summary"]["risk_count"] == 2
    assert overview(service, page=99)["items"] == []


@pytest.mark.parametrize("changes", [{"page": 0}, {"page": True}, {"page_size": 0}, {"page_size": 101}, {"level": "critical"}, {"recovery": "active"}, {"q": "a" * 257}, {"zone": "UTC"}])
def test_pagination_invalid_inputs_have_legal_positive_pair(tmp_path, changes):
    service, *_ = domain_services(tmp_path)
    with pytest.raises(ServerRiskError):
        overview(service, **changes)
    assert overview(service, page=1, page_size=100)["items"] == []


def test_pagination_approved_risk_then_recency_sort_precedes_page_boundary(tmp_path):
    service, *_ = domain_services(tmp_path)
    seed_event(service, "low-new", node="node-a", level="low", first="2026-08-31T10:00:00+00:00")
    seed_event(service, "high-old", node="node-z", first="2026-08-31T01:00:00+00:00")
    seed_event(service, "high-new", node="node-y", first="2026-08-31T09:00:00+00:00")
    seed_event(service, "high-tie", node="node-x", first="2026-08-31T09:00:00+00:00")
    assert [item["node_id"] for item in overview(service, page_size=2)["items"]] == ["node-x", "node-y"]
    assert [item["node_id"] for item in overview(service, page=2, page_size=2)["items"]] == ["node-z", "node-a"]
    assert overview(service, page=2, page_size=2)["summary"]["high_host_count"] == 3


def test_metamorphic_unrelated_hosts_objects_and_row_order_do_not_change_selected_evidence(tmp_path):
    service, *_ = domain_services(tmp_path)
    seed_event(service, "selected", risk_type="network.cni.failed", obj="cni0")
    initial = detail(service)
    seed_event(service, "other-node", node="node-other", obj="cni0", count=999)
    seed_event(service, "other-cluster", cluster="prod-b", obj="cni0", count=888)
    seed_event(service, "other-object", obj="worker.service", risk_type="service.failed", count=777)
    current = detail(service)
    selected = next(g for g in current["groups"] if g["object"]["id"] == "cni0")
    assert selected == initial["groups"][0]
    assert selected["association"]["evidence_refs"] == []
    assert "777" not in json_text(selected) and "worker.service" not in json_text(selected)


def test_unknown_partial_source_count_is_lower_bound_and_reads_never_mutate_business_state(tmp_path):
    service, *_ = domain_services(tmp_path)
    service.observations.submit(batch("single", observation(1, "2026-08-31T11:59:59+00:00", count=4)), "fixture-actor")
    before = database_digest(service)
    result = overview(service)
    item = result["items"][0]
    assert item["known_occurrence_count"] == 4 and item["occurrence_count"] is None
    assert result["quality"]["complete"] is False
    assert result["analysis_status"] == "not_analyzed"
    detail(service)
    assert database_digest(service) == before


def test_isolation_matched_rule_version_supplies_readable_dotted_type_summary(tmp_path):
    service, *_ = domain_services(tmp_path)
    rule = {"risk_type": "runtime.cni_setup_failed", "display_name": "CNI 配置失败", "description": "命中对象的网络配置失败规则", "consequence": "规则提示该对象的网络配置未完成", "recommendation": {"text": "核对该对象的 CNI 配置与恢复观测"}}
    encoded = json_text(rule)
    with service.repository.database.transaction() as conn:
        conn.execute("INSERT INTO risk_semantic_rules(rule_id,source,status,enabled,current_version,content_json,content_hash,created_at,updated_at) VALUES ('fixture.cni','builtin','published',1,1,?,?,?,?)", (encoded, digest(rule), "2026-08-31", "2026-08-31"))
        conn.execute("INSERT INTO risk_semantic_rule_versions(rule_id,version,content_json,content_hash,changed_by,change_reason,created_at) VALUES ('fixture.cni',1,?,?,'fixture','fixture','2026-08-31')", (encoded, digest(rule)))
    seed_event(service, "cni", risk_type=rule["risk_type"], obj="cni0", rule_id="fixture.cni")
    item = overview(service)["items"][0]
    assert item["risk_types"] == ["runtime.cni_setup_failed"]
    assert item["risk_summaries"] == [{"risk_type": "runtime.cni_setup_failed", "title": "CNI 配置失败"}]
    group = detail(service)["groups"][0]
    assert group["title"] == "CNI 配置失败" and group["consequence"] == rule["consequence"]
    assert group["recommendation"] == rule["recommendation"]["text"]
    assert group["analysis_status"] == "not_analyzed"


def test_isolation_committed_source_environment_excludes_synthetic_not_demo_named_real(tmp_path):
    service, *_ = domain_services(tmp_path)
    with service.repository.database.transaction() as conn:
        for key, environment in (("synthetic-job", "synthetic"), ("real-job", "production")):
            descriptor = json_text({"kind": "kafka", "identity": {"environment": environment}, "configuration": {}})
            conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES (?,'kafka',?,'fixture','completed','COMPLETED','{}','{}','2026-08-31','2026-08-31')", (key, descriptor))
    seed_event(service, "synthetic", node="ordinary-host", source_job_id="synthetic-job")
    seed_event(service, "real", node="DEMO-01", source_job_id="real-job")
    assert [item["node_id"] for item in overview(service)["items"]] == ["DEMO-01"]


def test_isolation_association_requires_actual_interval_overlap_not_aggregate_envelope(tmp_path):
    service, *_ = domain_services(tmp_path)
    seed_event(service, "a-early", obj="same", first="2026-08-31T01:00:00+00:00")
    seed_event(service, "a-late", obj="same", first="2026-08-31T09:00:00+00:00")
    seed_event(service, "b-gap", obj="same", risk_type="network.cni.failed", first="2026-08-31T05:00:00+00:00")
    assert all(group["association"]["status"] == "unverified" for group in detail(service)["groups"])
    seed_event(service, "b-overlap", obj="same", risk_type="network.cni.failed", first="2026-08-31T09:00:00+00:00")
    assert all(group["association"]["status"] == "confirmed" for group in detail(service)["groups"])


def complete_stream(service, policies, now, *, coverage_boot="boot-1"):
    now["value"] = "2026-09-01T12:00:00+00:00"
    source = policies.sources["fixture-source"]
    source["capability"] = "complete_event_stream"
    source["signals"]["coverage"] = {"kind": "coverage", "risk_type": "service.failed"}
    policies.rules["service.failed"]["required_signals"] = ["failed"]
    covered = observation(2, "2026-08-31T16:00:00+00:00", kind="coverage", boot=coverage_boot)
    del covered["occurred_at"]
    covered.update(interval_start="2026-08-30T16:00:00+00:00", interval_end="2026-08-31T16:00:00+00:00", coverage={"signal_codes": ["failed"], "covered_start": "2026-08-30T16:00:00+00:00", "covered_end": "2026-08-31T16:00:00+00:00", "complete_through": "2026-08-31T16:00:00+00:00", "gaps": [], "dropped_count": 0, "parse_failure_count": 0, "input_frontier_ref": "batch:complete"})
    service.observations.submit(batch("complete", observation(1, "2026-08-31T08:00:00+00:00", count=3), covered, mode="historical"), "fixture-actor")


def test_unknown_full_coverage_has_legal_exact_count_positive_pair(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    complete_stream(service, policies, now)
    assert detail(service)["groups"][0]["occurrence_count"] == 3
    assert overview(service)["items"][0]["occurrence_count"] == 3


def test_unknown_coverage_from_another_boot_cannot_make_count_complete(tmp_path):
    service, now, _, policies, _ = domain_services(tmp_path)
    complete_stream(service, policies, now, coverage_boot="different-boot")
    group = detail(service)["groups"][0]
    assert group["known_occurrence_count"] == 3 and group["occurrence_count"] is None


def test_isolation_metamorphic_demo_pair_same_cni_type_keeps_each_entity_rule_version_and_evidence(tmp_path):
    service, *_ = domain_services(tmp_path)
    risk_type = "runtime.cni_setup_failed"
    rules = {version: {"risk_type": risk_type, "display_name": f"DEMO-0{version} CNI 规则", "description": f"DEMO-0{version} 独立规则说明", "consequence": f"DEMO-0{version} 已登记规则影响说明", "recommendation": {"text": f"DEMO-0{version} 本对象核对建议"}} for version in (1, 2)}
    with service.repository.database.transaction() as conn:
        conn.execute("INSERT INTO risk_semantic_rules(rule_id,source,status,enabled,current_version,content_json,content_hash,created_at,updated_at) VALUES ('fixture.same.cni','builtin','published',1,2,?,?,?,?)", (json_text(rules[2]), digest(rules[2]), "2026-08-31", "2026-08-31"))
        for version, rule in rules.items():
            conn.execute("INSERT INTO risk_semantic_rule_versions(rule_id,version,content_json,content_hash,changed_by,change_reason,created_at) VALUES ('fixture.same.cni',?,?,?,'fixture','fixture','2026-08-31')", (version, json_text(rule), digest(rule)))
            source = json_text({"kind": "kafka", "identity": {"environment": "production"}, "configuration": {"cluster_id": "prod-a"}})
            conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES (?,'kafka',?,'fixture','completed','COMPLETED','{}','{}','2026-08-31','2026-08-31')", (f"real-source-{version}", source))
    seed_event(service, "cni-two", node="DEMO-02", obj="pod-two", risk_type=risk_type, level="medium", first="2026-08-31T11:00:00+00:00", last="2026-08-31T11:03:00+00:00", count=4, source_job_id="real-source-2", rule_id="fixture.same.cni", rule_version=2)
    before = detail(service, node="DEMO-02")
    seed_event(service, "cni-one", node="DEMO-01", obj="pod-one", risk_type=risk_type, first="2026-08-31T01:00:00+00:00", last="2026-08-31T01:05:00+00:00", count=900, source_job_id="real-source-1", rule_id="fixture.same.cni", rule_version=1)
    after = detail(service, node="DEMO-02")
    assert after == before
    group = after["groups"][0]
    assert group["object"] == {"kind": "legacy", "id": "pod-two", "label": "pod-two"}
    assert group["level"] == "medium" and group["known_occurrence_count"] == 4
    assert group["first_seen"] == "2026-08-31T11:00:00+00:00" and group["last_seen"] == "2026-08-31T11:03:00+00:00"
    assert group["evidence_refs"] == ["legacy:cni-two"]
    assert group["narrative_source"]["rule_refs"] == [{"rule_id": "fixture.same.cni", "version": 2}]
    assert group["title"] == rules[2]["display_name"] and group["consequence"] == rules[2]["consequence"]
    assert group["recommendation"] == rules[2]["recommendation"]["text"]
    assert group["association"]["status"] == "unverified" and group["association"]["evidence_refs"] == []
    assert all(other not in json_text(after) for other in ("DEMO-01", "pod-one", "legacy:cni-one"))
    assert group["known_occurrence_count"] != 900 and group["occurrence_count"] is None
    assert overview(service)["summary"]["host_count"] == 2


@pytest.mark.parametrize("risk_type,meaning,recommendation", [("runtime.cni_setup_failed", "CNI 网络配置", "节点网络"), ("k8s.pod.image_pull_backoff", "镜像拉取", "仓库访问与认证")])
def test_isolation_builtin_explanation_is_possible_impact_with_matched_rule_provenance(tmp_path, risk_type, meaning, recommendation):
    from logrisk.risk_semantics import RiskSemanticService
    service, _, _, policies, _ = domain_services(tmp_path)
    policies.sources["fixture-source"]["environment"] = "production"
    RiskSemanticService(service.repository.database, Path("configs/risk_semantics/builtin.yaml"))
    seed_event(service, "builtin-only", risk_type=risk_type, rule_id="builtin." + risk_type, obj=None, count=2)
    group = detail(service)["groups"][0]
    assert meaning in group["description"]
    assert "一般可能影响" in group["consequence"] and "当前业务影响尚未确认" in group["consequence"]
    assert recommendation in group["recommendation"]
    assert group["object"]["label"] == "未标识对象" and group["recommendation"].startswith("先从本机异常记录定位")
    assert group["narrative_source"]["kind"] == "builtin_rule_explanation"
    assert group["narrative_source"]["explanation_version"] == "node_risk_builtin_explanation_v1"
    assert group["narrative_source"]["rule_refs"] == [{"rule_id": "builtin." + risk_type, "version": 1}]
    assert group["evidence_refs"] == ["legacy:builtin-only"] and group["known_occurrence_count"] == 2
    assert group["analysis_status"] == "not_analyzed"


def test_unknown_unmapped_rule_keeps_impact_and_ai_unconfirmed(tmp_path):
    service, *_ = domain_services(tmp_path)
    seed_event(service, "unmapped", risk_type="custom.unmapped")
    group = detail(service)["groups"][0]
    assert group["consequence"] == "已记录该对象的异常，尚无本实体业务影响的确认记录。"
    assert group["narrative_source"]["kind"] == "scoped_facts" and group["analysis_status"] == "not_analyzed"
