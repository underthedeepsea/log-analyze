import pytest

from logrisk.approved_rules import ApprovedRuleStore
from logrisk.database import SQLiteDatabase
from logrisk.feature_jobs import FeatureJobError, FeatureJobFileStore, FeatureJobManager, validate_result_document
from logrisk.processing_metrics import ProcessingMetricsStore
from logrisk.sqlite_stores import SQLiteApprovalGroupStore, SQLiteApprovedRuleStore, SQLiteFeatureJobStore


def entity(entity_id, score, log_count=2, entity_type="node"):
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": entity_type,
        "entity_id": entity_id,
        "risk_score": score,
        "risk_level": "high" if score >= 70 else "medium",
        "top_templates": [{"template_hash": f"hash-{entity_id}", "count": log_count}],
        "affected_entities": [],
    }


def candidate(source, title="候选特征"):
    return {
        "candidate_id": f"feature-{source['entity_id']}",
        "status": "pending",
        "reviewer_note": "",
        "approved_at": None,
        "cluster": source["cluster"],
        "entity": {"type": source["entity_type"], "id": source["entity_id"]},
        "window_start": source["window_start"],
        "window_end": source["window_end"],
        "risk_score": source["risk_score"],
        "risk_level": source["risk_level"],
        "feature_type": "log_pattern",
        "title": title,
        "summary": "摘要",
        "importance": "high",
        "template_hashes": ["hash"],
        "components": ["kernel"],
        "tags": ["oom"],
        "selection_reason": "重要异常模板",
        "occurrence_count": 2,
        "time_range": {"first_seen": source["window_start"], "last_seen": source["window_end"]},
        "affected_entities": [],
        "source_templates": [{"template_hash": "hash", "template": "OOM", "count": 2}],
        "provider": "ollama",
        "model": "qwen3:1.7b",
    }


def document():
    return {
        "summary": {
            "total_raw_logs": 10,
            "total_normalized_logs": 10,
            "total_template_events": 10,
            "total_template_windows": 6,
            "total_risk_entities": 3,
            "drain3_reduced_logs": 4,
            "drain3_compression_ratio_percent": 40.0,
        },
        "risk_entities": [
            entity("node-low", 20, log_count=2),
            entity("node-high", 90, log_count=5),
            entity("node-mid", 50, log_count=3),
        ],
    }


def test_validate_result_document_requires_risk_entities():
    with pytest.raises(FeatureJobError, match="risk_entities"):
        validate_result_document({"summary": {}})


def test_job_processes_eligible_entities_serially_by_score_and_continues_failure():
    calls = []

    def extractor(source, **kwargs):
        calls.append(source["entity_id"])
        if source["entity_id"] == "node-high":
            raise RuntimeError("model failed")
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(document(), model="qwen3:1.7b", min_score=40)

    initial = manager.get_job(job_id)
    assert initial["log_statistics"] == {
        "original_logs": 10,
        "normalized_logs": 10,
        "template_events": 10,
        "template_windows": 6,
        "drain3_reduced_logs": 4,
        "drain3_compression_ratio_percent": 40.0,
        "eligible_logs": 8,
        "analyzed_logs": 0,
        "pending_logs": 8,
        "skipped_logs": 2,
        "reused_logs": 0,
        "ollama_logs": 0,
        "cache_hit_logs": 0,
    }

    manager.run_job(job_id)
    snapshot = manager.get_job(job_id)

    assert calls == ["node-high", "node-mid"]
    assert snapshot["status"] == "completed_with_errors"
    assert snapshot["progress"] == {
        "total": 2,
        "completed": 1,
        "failed": 1,
        "percent": 100,
        "rule_matched": 0,
        "ollama_completed": 1,
    }

    assert snapshot["log_statistics"]["analyzed_logs"] == 3
    assert snapshot["log_statistics"]["pending_logs"] == 5
    states = {item["entity_id"]: item["status"] for item in snapshot["entities"]}
    assert states == {"node-high": "failed", "node-mid": "completed", "node-low": "skipped"}
    assert snapshot["features"][0]["entity"]["id"] == "node-mid"
    events, cursor = manager.wait_for_events(job_id, 0, timeout=0)
    assert cursor == len(events)
    assert [event["type"] for event in events][-1] == "job_completed"


def test_job_forwards_model_profile_id_to_extractor():
    captured = {}

    def extractor(source, **kwargs):
        captured.update(kwargs)
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(document(), model="qwen3:1.7b", model_profile_id="qwen3_1_7b_fast")

    manager.run_job(job_id)

    assert captured["model_profile_id"] == "qwen3_1_7b_fast"
    assert manager.get_job(job_id)["model_profile_id"] == "qwen3_1_7b_fast"


def test_failed_entity_can_be_retried():
    attempts = {"count": 0}

    def extractor(source, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary")
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    result = {"summary": {}, "risk_entities": [entity("node-a", 90)]}
    job_id = manager.create_job(result, model="qwen3:1.7b")
    manager.run_job(job_id)

    manager.retry_entity(job_id, "node-a", start=False)
    manager.run_job(job_id, only_entity_id="node-a")

    snapshot = manager.get_job(job_id)
    assert snapshot["entities"][0]["status"] == "completed"
    assert snapshot["features"][0]["candidate_id"] == "feature-node-a"


def test_job_retries_transient_extractor_failure_before_marking_failed():
    attempts = {"count": 0}

    def extractor(source, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("missing tags")
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
        retry_count=1,
    )
    manager.run_job(job_id)

    snapshot = manager.get_job(job_id)
    events = manager.list_events(job_id)

    assert attempts["count"] == 2
    assert snapshot["retry_count"] == 1
    assert snapshot["entities"][0]["status"] == "completed"
    assert [event["type"] for event in events if event["type"] == "entity_retrying"] == ["entity_retrying"]


def test_job_passes_selected_prompt_and_job_id_to_extractor():
    calls = []

    def extractor(source, **kwargs):
        calls.append(kwargs)
        return [candidate(source)]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
        prompt_id="feature_extract_v2_strict_en",
    )
    manager.run_job(job_id)

    assert calls[0]["prompt_id"] == "feature_extract_v2_strict_en"
    assert calls[0]["job_id"] == job_id
    assert manager.get_job(job_id)["prompt_id"] == "feature_extract_v2_strict_en"


def test_review_edit_and_export_only_approved_features():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [candidate(source)], auto_start=False)
    result = {"summary": {"total_raw_logs": 1}, "risk_entities": [entity("node-a", 90)]}
    job_id = manager.create_job(result, model="qwen3:1.7b")
    manager.run_job(job_id)

    updated = manager.update_feature(job_id, "feature-node-a", {
        "title": "人工修订标题",
        "summary": "人工确认后的摘要",
        "importance": "critical",
        "tags": ["oom", "approved"],
        "reviewer_note": "已核对模板",
        "status": "approved",
    })
    package = manager.export_approved(job_id)

    assert updated["status"] == "approved"
    assert updated["approved_at"]
    assert package["schema_version"] == "1.0"
    assert package["review_statistics"] == {"total": 1, "approved": 1, "rejected": 0, "pending": 0}
    assert package["approved_features"][0]["title"] == "人工修订标题"
    assert package["approved_features"][0]["reviewer_note"] == "已核对模板"
    assert package["approved_risk_nodes"] == [{
        "node_id": "node-a",
        "cluster": "prod-a",
        "risk_score": 90,
        "risk_level": "high",
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "log_count": 2,
        "approved_feature_ids": ["feature-node-a"],
        "affected_entities": [],
    }]
    assert "root_cause_candidate" not in package["approved_features"][0]


def test_export_requires_an_approved_feature():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [candidate(source)], auto_start=False)
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    with pytest.raises(FeatureJobError, match="至少批准"):
        manager.export_approved(job_id)


def test_export_excludes_nodes_without_approved_features_and_non_node_entities():
    manager = FeatureJobManager(extractor=lambda source, **kwargs: [candidate(source)], auto_start=False)
    job_id = manager.create_job(
        {
            "summary": {},
            "risk_entities": [
                entity("node-a", 90),
                entity("node-b", 80),
                entity("pod-a", 70, entity_type="pod"),
            ],
        },
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)
    manager.update_feature(job_id, "feature-node-a", {"status": "approved"})
    manager.update_feature(job_id, "feature-pod-a", {"status": "approved"})

    package = manager.export_approved(job_id)

    assert [node["node_id"] for node in package["approved_risk_nodes"]] == ["node-a"]


def reusable_entity(entity_id="node-a", cluster="prod-a"):
    value = entity(entity_id, 90, log_count=8)
    value["cluster"] = cluster
    value["top_templates"] = [{
        "template_hash": "hash-oom",
        "category": "memory",
        "component": "kernel",
        "template": "Out of memory: Killed process <*>",
        "count": 8,
        "first_seen": value["window_start"],
        "last_seen": value["window_end"],
    }]
    return value


def reusable_candidate(source):
    value = candidate(source, title="已批准的 OOM 特征")
    value["feature_type"] = "resource_pressure"
    value["template_hashes"] = ["hash-oom"]
    value["source_templates"] = [dict(source["top_templates"][0])]
    value["status"] = "approved"
    return value


def test_matching_rule_skips_extractor_and_uses_current_entity_facts(tmp_path):
    calls = []
    reuse_events = []

    class RecordingRuleStore(ApprovedRuleStore):
        def record_reuse(self, rule_id, **metadata):
            reuse_events.append({"rule_id": rule_id, **metadata})
            return super().record_reuse(rule_id, **metadata)

    store = RecordingRuleStore(tmp_path / "rules.json")
    store.upsert_feature(reusable_candidate(reusable_entity("seed-node")))
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: calls.append(source) or [],
        rule_store=store,
        auto_start=False,
    )
    current = reusable_entity("new-node", cluster="another-cluster")

    job_id = manager.create_job(
        {"summary": {"total_raw_logs": 8}, "risk_entities": [current]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)
    snapshot = manager.get_job(job_id)

    assert calls == []
    assert snapshot["status"] == "completed"
    assert snapshot["entities"][0]["status"] == "rule_matched"
    assert snapshot["progress"] == {
        "total": 1,
        "completed": 1,
        "failed": 0,
        "percent": 100,
        "rule_matched": 1,
        "ollama_completed": 0,
    }
    reused = snapshot["features"][0]
    assert reused["status"] == "approved"
    assert reused["origin"] == "approved_rule"
    assert reused["entity"] == {"type": "node", "id": "new-node"}
    assert reused["cluster"] == "another-cluster"
    assert reused["occurrence_count"] == 8
    assert reuse_events == [{
        "rule_id": reused["rule_id"],
        "job_id": job_id,
        "entity_id": "new-node",
        "cluster": "another-cluster",
    }]
    assert reused["source_templates"][0]["template_hash"] == "hash-oom"


def test_approving_ollama_feature_persists_reusable_rule(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    source = reusable_entity("node-a")
    manager = FeatureJobManager(
        extractor=lambda record, **kwargs: [reusable_candidate(record) | {"status": "pending"}],
        rule_store=store,
        auto_start=False,
    )
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [source]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    manager.update_feature(job_id, "feature-node-a", {"status": "approved"})

    rules = store.list_rules()
    assert len(rules) == 1
    assert rules[0]["feature_type"] == "resource_pressure"
    assert rules[0]["template_signatures"] == [
        {"template_hash": "hash-oom", "category": "memory"}
    ]


def test_approving_feature_persists_rule_lineage(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    source = reusable_entity("node-a")

    def extractor(record, **kwargs):
        return [reusable_candidate(record) | {
            "status": "pending",
            "trace_id": "trace-123",
            "prompt_id": "feature_extract_v3_compact_strict_json_en",
            "prompt_hash": "prompt-sha",
            "provider": "ollama",
            "model": "qwen3:1.7b",
            "evidence_hash": "evidence-sha",
        }]

    manager = FeatureJobManager(extractor=extractor, rule_store=store, auto_start=False)
    job_id = manager.create_job({"summary": {}, "risk_entities": [source]}, model="qwen3:1.7b")
    manager.run_job(job_id)

    manager.update_feature(job_id, "feature-node-a", {"status": "approved"})
    package = manager.export_approved(job_id)

    lineage = store.list_rules()[0]["lineage"]
    assert lineage["job_id"] == job_id
    assert lineage["candidate_id"] == "feature-node-a"
    assert lineage["trace_id"] == "trace-123"
    assert lineage["prompt_hash"] == "prompt-sha"
    assert lineage["evidence_hash"] == "evidence-sha"
    assert package["approved_features"][0]["lineage"] == lineage


def test_snapshot_reports_daily_llm_volume_speed_eta_and_reuse_savings(tmp_path):
    current = {"seconds": 100.0}
    metrics = ProcessingMetricsStore(tmp_path / "metrics.json")
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    rules.upsert_feature(reusable_candidate(reusable_entity("seed")))

    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source)],
        rule_store=rules,
        metrics_store=metrics,
        monotonic=lambda: current["seconds"],
        auto_start=False,
    )
    unmatched = entity("node-new", 80, log_count=4)
    matched = reusable_entity("node-reused")
    job_id = manager.create_job(
        {"summary": {"total_raw_logs": 12}, "risk_entities": [matched, unmatched]},
        model="qwen3:1.7b",
    )
    current["seconds"] = 104.0
    manager.run_job(job_id)
    current["seconds"] = 108.0

    snapshot = manager.get_job(job_id)
    live = snapshot["live_metrics"]

    assert live["today_llm_logs"] == 4
    assert live["saved_llm_calls"] == 1
    assert live["saved_llm_logs"] == 8
    assert live["processing_logs_per_second"] == 1.5
    assert live["rolling_60s_logs_per_second"] == 1.5
    assert live["eta_seconds"] == 0


def test_job_reports_cache_hit_event_and_savings():
    def extractor(source, **kwargs):
        return [candidate(source) | {"cache_hit": True}]

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90, log_count=7)]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    snapshot = manager.get_job(job_id)
    events = manager.list_events(job_id)

    assert [event["type"] for event in events if event["type"] == "entity_cache_hit"] == ["entity_cache_hit"]
    assert snapshot["entities"][0]["cache_hit"] is True
    assert snapshot["live_metrics"]["cache_hit_calls"] == 1
    assert snapshot["live_metrics"]["cache_hit_logs"] == 7
    assert snapshot["live_metrics"]["saved_llm_calls"] == 1
    assert snapshot["live_metrics"]["saved_llm_logs"] == 7


def test_file_store_restores_completed_job_for_review(tmp_path):
    store = FeatureJobFileStore(tmp_path / "feature_jobs")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source)],
        persistence=store,
        auto_start=False,
    )
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    manager.run_job(job_id)

    restored = FeatureJobManager(persistence=store, auto_start=False)

    assert restored.get_job(job_id)["status"] == "completed"
    assert restored.get_job(job_id)["features"][0]["status"] == "pending"
    assert (tmp_path / "feature_jobs" / job_id / "snapshot.json").exists()
    assert (tmp_path / "feature_jobs" / job_id / "events.jsonl").exists()


def test_sqlite_store_collapses_duplicate_entity_windows_before_persistence(tmp_path):
    first = entity("node-a", 90, log_count=5)
    second = entity("node-a", 70, log_count=3)
    second.update({
        "window_start": "2026-06-22T11:00:00+08:00",
        "window_end": "2026-06-22T11:05:00+08:00",
        "top_templates": [{"template_hash": "hash-node-a-later", "count": 3}],
    })
    store = SQLiteFeatureJobStore(SQLiteDatabase(tmp_path / "state.sqlite3"))
    manager = FeatureJobManager(persistence=store, auto_start=False)

    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [first, second]},
        model="qwen3.5:4b-mlx",
    )

    snapshot = manager.get_job(job_id)
    assert len(snapshot["entities"]) == 1
    assert snapshot["entities"][0]["entity_id"] == "node-a"
    with manager._lock:
        source = manager._job(job_id)["entities"][0]["source"]
    assert source["window_start"] == first["window_start"]
    assert source["window_end"] == second["window_end"]
    assert {item["template_hash"] for item in source["top_templates"]} == {
        "hash-node-a",
        "hash-node-a-later",
    }
    assert sum(item["count"] for item in source["top_templates"]) == 8


def test_group_scope_reject_marks_all_pending_semantic_candidates_without_rule(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, f"candidate-{source['entity_id']}")],
        rule_store=rules,
        auto_start=False,
    )
    first = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    second = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-b", 80)]},
        model="qwen3:1.7b",
    )
    manager.run_job(first)
    manager.run_job(second)

    rejected = manager.update_feature(
        first,
        "feature-node-a",
        {"status": "rejected", "review_scope": "approval_identity"},
    )

    assert rejected["status"] == "rejected"
    assert rejected["resolution_type"] == "group_rejected"
    assert manager.get_job(second)["features"][0]["status"] == "rejected"
    assert manager.get_job(second)["features"][0]["resolution_type"] == "group_rejected"
    assert manager.list_approval_groups()[0]["status"] == "rejected"
    assert rules.list_rules() == []


def _cni_entity(entity_id, component, template):
    value = entity(entity_id, 90)
    value["top_templates"] = [{
        "template_hash": f"hash-{entity_id}",
        "template_fingerprint": f"fingerprint-{entity_id}",
        "category": "network",
        "component": component,
        "template": template,
        "count": 3,
        "first_seen": value["window_start"],
        "last_seen": value["window_end"],
    }]
    return value


def _cni_candidate(source, candidate_id, problem_code, feature_type):
    value = candidate(source, "CNI 网络风险")
    value.update({
        "candidate_id": candidate_id,
        "feature_type": feature_type,
        "problem_code": problem_code,
        "components": [source["top_templates"][0]["component"]],
        "template_hashes": [source["top_templates"][0]["template_hash"]],
        "source_templates": [dict(source["top_templates"][0])],
    })
    return value


def test_cross_job_cni_wrappers_share_review_group_and_reuse_rule(tmp_path):
    rules = ApprovedRuleStore(tmp_path / "rules.json")
    calls = []
    specs = [
        ("node-a", "kubelet", "NetworkPlugin cni failed: no enough ips", "runtime_cni_setup_failed", "cni_network_failure"),
        ("node-b", "containerd", "CreatePodSandbox failed: cni no enough ips", "runtime_sandbox_create_failed", "pod_sandbox_network_failure"),
        ("node-c", "kubelet", "RunPodSandbox failed: cni no enough ips", "runtime_cni_setup_failed", "runtime_network_failure"),
        ("node-d", "kubelet", "CNI config syntax error", "CNI config syntax error", "cni_config_failure"),
    ]

    def extractor(source, **kwargs):
        calls.append(source["entity_id"])
        spec = next(item for item in specs if item[0] == source["entity_id"])
        return [_cni_candidate(source, f"candidate-{spec[0]}", spec[3], spec[4])]

    manager = FeatureJobManager(extractor=extractor, rule_store=rules, auto_start=False)
    job_ids = []
    for entity_id, component, template, problem_code, feature_type in specs:
        job_id = manager.create_job(
            {"summary": {}, "risk_entities": [_cni_entity(entity_id, component, template)]},
            model="qwen3.5:4b-mlx",
        )
        manager.run_job(job_id)
        job_ids.append(job_id)

    from logrisk.approval_queue import build_review_groups

    groups = build_review_groups(manager.list_persisted_candidates(status="pending"))
    ip_group = next(item for item in groups if item["problem_code"] == "kubernetes.cni.ip_exhaustion")
    config_group = next(item for item in groups if item["problem_code"] == "kubernetes.cni.config_error")
    assert len(groups) == 2
    assert ip_group["candidate_count"] == 3
    assert config_group["candidate_count"] == 1

    approved = manager.update_feature(
        job_ids[0],
        "candidate-node-a",
        {"status": "approved", "review_scope": "approval_identity"},
    )

    assert len(rules.list_rules()) == 1
    assert approved["problem_code"] == "kubernetes.cni.ip_exhaustion"
    assert all(
        manager.get_job(job_id)["features"][0]["status"] == "approved"
        for job_id in job_ids[:3]
    )
    assert manager.get_job(job_ids[3])["features"][0]["status"] == "pending"
    assert build_review_groups(manager.list_persisted_candidates(status="pending"))[0]["problem_code"] == "kubernetes.cni.config_error"

    follow_up = _cni_entity("node-e", "containerd", "CreatePodSandbox failed: cni no enough ips")
    follow_up_job = manager.create_job({"summary": {}, "risk_entities": [follow_up]}, model="qwen3.5:4b-mlx")
    manager.run_job(follow_up_job)

    assert calls == ["node-a", "node-b", "node-c", "node-d"]
    assert manager.get_job(follow_up_job)["entities"][0]["status"] == "rule_matched"


def test_feature_job_manager_lazy_loads_job_for_persisted_approval(tmp_path):
    database = SQLiteDatabase(tmp_path / "state.sqlite3")
    persistence = SQLiteFeatureJobStore(database)
    groups = SQLiteApprovalGroupStore(database)
    rules = SQLiteApprovedRuleStore(database)
    writer = FeatureJobManager(
        extractor=lambda source, **kwargs: [candidate(source, "candidate-node-a")],
        rule_store=rules,
        approval_group_store=groups,
        persistence=persistence,
        auto_start=False,
        interrupt_on_restore=False,
    )
    reader = FeatureJobManager(
        extractor=lambda source, **kwargs: [],
        rule_store=rules,
        approval_group_store=groups,
        persistence=persistence,
        auto_start=False,
        interrupt_on_restore=False,
    )
    job_id = writer.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    writer.run_job(job_id)

    updated = reader.update_feature(
        job_id,
        "feature-node-a",
        {"status": "rejected", "review_scope": "approval_identity"},
    )

    assert updated["status"] == "rejected"


def test_refresh_does_not_replace_live_job_with_older_persisted_snapshot(tmp_path):
    store = FeatureJobFileStore(tmp_path / "feature_jobs")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: [],
        persistence=store,
        auto_start=False,
        interrupt_on_restore=False,
    )
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    with manager._lock:
        manager._job(job_id)["status"] = "running"
    persisted = store.load_job(job_id)
    persisted["status"] = "queued"
    store.save(persisted)

    manager.refresh_from_persistence(job_id)

    assert manager.get_job(job_id)["status"] == "running"


def test_file_store_marks_running_job_interrupted_without_model_retry(tmp_path):
    calls = []
    store = FeatureJobFileStore(tmp_path / "feature_jobs")
    manager = FeatureJobManager(
        extractor=lambda source, **kwargs: calls.append(source),
        persistence=store,
        auto_start=False,
    )
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="qwen3:1.7b",
    )
    with manager._lock:
        job = manager._job(job_id)
        job["status"] = "running"
        job["entities"][0]["status"] = "running"
        manager._emit_locked(job, "entity_started", entity_id="node-a")

    restored = FeatureJobManager(
        extractor=lambda source, **kwargs: calls.append(source),
        persistence=store,
        auto_start=True,
    )

    snapshot = restored.get_job(job_id)
    assert snapshot["status"] == "interrupted"
    assert snapshot["entities"][0]["status"] == "interrupted"
    assert calls == []


def test_job_locks_provider_connection_and_profile_snapshots():
    captured = {}

    def extractor(source, **kwargs):
        captured.update(kwargs)
        return []

    manager = FeatureJobManager(extractor=extractor, auto_start=False)
    job_id = manager.create_job(
        {"summary": {}, "risk_entities": [entity("node-a", 90)]},
        model="remote-model",
        model_profile_id="remote-profile",
        provider="openai_compatible",
        connection_snapshot={"connection_id": "remote", "provider": "openai_compatible", "base_url": "https://example/v1"},
        profile_snapshot={"profile_id": "remote-profile", "model": "remote-model"},
    )

    manager.run_job(job_id)
    snapshot = manager.get_job(job_id)

    assert snapshot["provider"] == "openai_compatible"
    assert snapshot["connection_snapshot"]["connection_id"] == "remote"
    assert captured["connection_snapshot"]["base_url"] == "https://example/v1"
    assert captured["profile_snapshot"]["profile_id"] == "remote-profile"
