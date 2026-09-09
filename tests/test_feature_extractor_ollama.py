import json

import pytest

from logrisk.ai_harness.cache import AICache
from logrisk.ai_harness.prompt_registry import PromptRegistry, PromptTemplate
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.feature_extractor_ollama import FeatureExtractionError, extract_features_for_entity, generate_feature_candidates


def entity(score=96):
    return {
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "risk_score": score,
        "risk_level": "critical",
        "affected_entities": ["pay-api-1"],
        "top_templates": [{
            "template_hash": "oom-hash",
            "component": "kernel",
            "severity": "ERROR",
            "template": "Memory cgroup out of memory Killed process <*>",
            "category": "node_memory_pressure",
            "count": 3,
            "first_seen": "2026-06-22T10:01:02+08:00",
            "last_seen": "2026-06-22T10:02:02+08:00",
            "feature_hint": "检查内存水位",
            "samples": ["SECRET RAW LOG"],
            "raw_sample": "SECRET RAW SAMPLE",
        }],
    }


def response(features):
    payload = {
        "model": "qwen3:1.7b",
        "created_at": "2026-06-22T10:05:00Z",
        "message": {"role": "assistant", "content": json.dumps({"features": features}, ensure_ascii=False)},
        "done": True,
        "done_reason": "stop",
    }

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps(payload).encode()

    return Response()


def model_feature():
    return {
        "feature_type": "resource_pressure",
        "title": "节点内存耗尽",
        "summary": "内核 OOM 模板在窗口内重复出现",
        "importance": "critical",
        "template_hashes": ["oom-hash"],
        "components": ["kernel"],
        "tags": ["oom", "memory"],
        "selection_reason": "高风险资源压力信号",
    }


@pytest.fixture(autouse=True)
def isolated_ai_cache(monkeypatch, tmp_path):
    monkeypatch.setattr("logrisk.feature_extractor_ollama.AI_CACHE", AICache(tmp_path / "ai_cache.json"))


def test_generate_features_sanitizes_evidence_and_owns_source_facts(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        captured["timeout"] = timeout
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)

    result = generate_feature_candidates([entity()], model="qwen3:1.7b", timeout=15)

    prompt = json.dumps(captured["body"]["messages"], ensure_ascii=False)
    assert captured["body"]["stream"] is False
    assert captured["body"]["format"]["type"] == "object"
    assert captured["timeout"] == 15
    assert "oom-hash" in prompt
    assert "SECRET RAW LOG" not in prompt
    assert "SECRET RAW SAMPLE" not in prompt
    feature = result[0]
    assert feature["entity"] == {"type": "node", "id": "node-a"}
    assert feature["risk_score"] == 96
    assert feature["occurrence_count"] == 3
    assert feature["time_range"]["first_seen"] == "2026-06-22T10:01:02+08:00"
    assert feature["source_templates"][0]["template_hash"] == "oom-hash"
    assert "samples" not in feature["source_templates"][0]
    assert feature["status"] == "pending"
    assert feature["model"] == "qwen3:1.7b"
    assert feature["problem_code"] == "linux.memory.oom"
    assert feature["approval_key"].startswith("appr_")
    assert feature["problem_resolution"]["semantic_safe"] is True
    assert feature["problem_resolution"]["matched_rule"] == "linux_oom_v1"


def test_generate_features_partitions_mixed_feature_before_source_fact_attachment(monkeypatch):
    payload = entity()
    payload["top_templates"] = [
        {
            "template_hash": "hash-crash",
            "component": "kubelet",
            "severity": "ERROR",
            "template": "CrashLoopBackOff",
            "count": 2,
            "first_seen": "2026-06-22T10:01:00+08:00",
            "last_seen": "2026-06-22T10:02:00+08:00",
        },
        {
            "template_hash": "hash-stats",
            "component": "kubelet",
            "severity": "ERROR",
            "template": "Failed to get system container stats",
            "count": 3,
            "first_seen": "2026-06-22T10:03:00+08:00",
            "last_seen": "2026-06-22T10:04:00+08:00",
        },
    ]
    mixed_feature = {
        "feature_type": "mixed_kubelet_failure",
        "title": "Kubelet mixed failure",
        "summary": "Two selected kubelet evidence templates.",
        "importance": "high",
        "template_hashes": ["hash-crash", "hash-stats"],
        "components": ["kubelet"],
        "tags": ["kubelet"],
        "selection_reason": "Selected by model evidence.",
    }

    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([mixed_feature]),
    )

    candidates = generate_feature_candidates(
        [payload],
        model="qwen3:1.7b",
        cache_enabled=False,
    )

    assert len(candidates) == 2
    assert candidates[0]["candidate_id"] != candidates[1]["candidate_id"]
    assert {item["problem_code"] for item in candidates} == {
        "kubernetes.pod.crash_loop",
        "kubernetes.runtime.container_stats_failure",
    }
    assert [
        (
            item["template_hashes"],
            item["source_templates"][0]["template_hash"],
            item["source_templates"][0]["template"],
            item["occurrence_count"],
        )
        for item in candidates
    ] == [
        (["hash-crash"], "hash-crash", "CrashLoopBackOff", 2),
        (["hash-stats"], "hash-stats", "Failed to get system container stats", 3),
    ]
    assert all(item["feature_type"] != "mixed_kubelet_failure" for item in candidates)
    assert all(item["evaluator_result"]["passed"] is True for item in candidates)


def test_generate_features_isolates_unresolved_mixed_evidence(monkeypatch):
    payload = entity()
    payload["top_templates"] = [
        {
            "template_hash": "hash-known",
            "component": "kubelet",
            "severity": "ERROR",
            "template": "CrashLoopBackOff",
            "count": 2,
        },
        {
            "template_hash": "hash-opaque",
            "component": "kubelet",
            "severity": "ERROR",
            "template": "opaque vendor runtime failure",
            "count": 1,
        },
    ]
    mixed_feature = {
        "feature_type": "mixed_runtime_failure",
        "title": "Runtime mixed failure",
        "summary": "One known and one unresolved evidence template.",
        "importance": "high",
        "template_hashes": ["hash-known", "hash-opaque"],
        "components": ["kubelet"],
        "tags": ["runtime"],
        "selection_reason": "Selected by model evidence.",
    }

    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([mixed_feature]),
    )

    candidates = generate_feature_candidates(
        [payload],
        model="qwen3:1.7b",
        cache_enabled=False,
    )

    assert len(candidates) == 2
    assert [item["template_hashes"] for item in candidates] == [["hash-known"], ["hash-opaque"]]
    assert [item["occurrence_count"] for item in candidates] == [2, 1]
    assert [item["semantic_safe"] for item in candidates] == [True, False]
    assert candidates[1]["feature_type"] == "unresolved_template_evidence"
    assert [
        source["template_hash"] for candidate in candidates for source in candidate["source_templates"]
    ] == ["hash-known", "hash-opaque"]
    assert candidates[1]["problem_resolution"]["semantic_safe"] is False
    assert candidates[1]["match_mode"] == "template_set"
    assert any("待人工复核" in warning for warning in candidates[1]["evaluator_result"]["warnings"])


def test_final_hard_evaluator_failure_cannot_enter_semantic_group_or_reuse_rule(monkeypatch, tmp_path):
    from logrisk.approved_rules import ApprovedRuleStore
    from logrisk.approval_queue import build_review_groups

    payload = entity()
    payload["top_templates"] = [
        {"template_hash": "stats", "component": "kubelet", "template": "Failed to get system container stats", "count": 1},
        {"template_hash": "other", "component": "containerd", "template": "opaque unrelated evidence", "count": 1},
    ]
    invalid_final_component = {
        "feature_type": "kubelet_container_stats_failure",
        "title": "容器统计获取失败",
        "summary": "kubelet 获取容器统计失败。",
        "importance": "high",
        "template_hashes": ["stats"],
        "components": ["containerd"],
        "tags": ["统计"],
        "selection_reason": "所选模板记录统计失败。",
    }
    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([invalid_final_component]),
    )

    candidate = generate_feature_candidates([payload], model="qwen3:1.7b", cache_enabled=False)[0]
    rule_store = ApprovedRuleStore(tmp_path / "rules.json")
    rule_store.upsert_feature({
        "feature_type": "kubelet_container_stats_failure",
        "source_templates": [{
            "template_hash": "safe-rule", "category": "runtime", "component": "kubelet",
            "template": "Failed to get system container stats",
        }],
    })

    assert candidate["evaluator_result"]["passed"] is False
    assert candidate["match_mode"] == "template_set"
    assert build_review_groups([candidate])[0]["match_mode"] == "template_set"
    assert rule_store.match_feature(candidate) == []


def test_final_candidates_own_only_parent_request_evaluation_rules(monkeypatch):
    payload = entity()
    payload["top_templates"] = [
        {"template_hash": "stats", "component": "kubelet", "template": "Failed to get system container stats", "count": 1},
        {"template_hash": "crash", "component": "kubelet", "template": "CrashLoopBackOff", "count": 1},
    ]
    features = [
        {**model_feature(), "template_hashes": ["stats"], "components": ["kubelet"], "feature_type": "kubelet_container_stats_failure"},
        {**model_feature(), "template_hashes": ["crash"], "components": ["kubelet"], "feature_type": "kubelet_pod_crash_loop"},
    ]
    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", lambda *args, **kwargs: response(features))

    candidates = generate_feature_candidates([payload], model="qwen3:1.7b", cache_enabled=False)

    assert len(candidates) == 2
    for candidate in candidates:
        rule_ids = [item["rule_id"] for item in candidate["evaluator_result"]["rule_results"]]
        assert len(rule_ids) == len(set(rule_ids))
        assert len(rule_ids) == 9


def test_generate_features_uses_prompt_registry_and_writes_trace(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("custom feature prompt", encoding="utf-8")
    trace_path = tmp_path / "ai_traces.jsonl"
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "logrisk.feature_extractor_ollama.PROMPT_REGISTRY",
        PromptRegistry(prompt_dir),
        raising=False,
    )
    monkeypatch.setattr(
        "logrisk.feature_extractor_ollama.TRACE_LOGGER",
        AITraceLogger(trace_path),
        raising=False,
    )

    result = generate_feature_candidates([entity()], model="qwen3:1.7b")

    assert captured["body"]["messages"][0]["content"] == "custom feature prompt"
    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    assert trace["prompt_id"] == "feature_extract_v3_compact_strict_json_en"
    assert trace["provider"] == "ollama"
    assert trace["model"] == "qwen3:1.7b"
    assert trace["parsed_output"]["features"][0]["title"] == "节点内存耗尽"
    assert len(trace["input_evidence_hash"]) == 64
    assert len(result[0]["prompt_hash"]) == 64
    assert len(result[0]["evidence_hash"]) == 64
    assert trace["evaluator_result"]["passed"] is True
    assert trace["status"] == "success"


def test_extract_features_can_use_locked_prompt_snapshot(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)
    locked = PromptTemplate(
        prompt_id="prompt-locked",
        content="locked benchmark prompt",
        sha256="a" * 64,
        path="database:prompt/prompt-locked/v3",
        version="v3",
    )

    result = extract_features_for_entity(
        entity(),
        model="qwen3:1.7b",
        prompt_id="prompt-locked",
        prompt_template=locked,
        cache_enabled=False,
    )

    assert captured["body"]["messages"][0]["content"] == "locked benchmark prompt"
    assert result[0]["prompt_hash"] == "a" * 64


def test_omitted_prompt_id_resolves_the_selected_profile_default(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("v3 content", encoding="utf-8")
    (prompt_dir / "feature_extract_v4_atomic_evidence_en.md").write_text("v4 content", encoding="utf-8")
    profile_path = tmp_path / "profiles.yaml"
    profile_path.write_text(
        """
default_profile_id: v4_profile
profiles:
  v4_profile:
    provider: ollama
    model: qwen3:1.7b
    default_prompt_id: feature_extract_v4_atomic_evidence_en
""",
        encoding="utf-8",
    )
    captured = []

    class Client:
        def generate_json(self, messages, schema, *, model, timeout, options=None):
            captured.append(messages[0]["content"])
            return {"features": [model_feature()]}

    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))

    result = extract_features_for_entity(
        entity(), model_profile_id="v4_profile", profile_config_path=profile_path,
        model_client=Client(), cache_enabled=False,
    )

    assert captured == ["v4 content"]
    assert result[0]["prompt_id"] == "feature_extract_v4_atomic_evidence_en"


def test_explicit_and_locked_prompt_selection_record_the_content_actually_sent(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("v3 content", encoding="utf-8")
    trace_path = tmp_path / "ai_traces.jsonl"
    captured = []

    class Client:
        def generate_json(self, messages, schema, *, model, timeout, options=None):
            captured.append(messages[0]["content"])
            return {"features": [model_feature()]}

    locked = PromptTemplate("locked-v4", "locked v4 content", "b" * 64, "database:locked-v4", version="v4")
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))

    explicit = extract_features_for_entity(
        entity(), model="qwen3:1.7b", prompt_id="feature_extract_v3_compact_strict_json_en",
        model_client=Client(), cache_enabled=False,
    )
    locked_result = extract_features_for_entity(
        entity(), model="qwen3:1.7b", prompt_id="feature_extract_v3_compact_strict_json_en",
        prompt_template=locked, model_client=Client(), cache_enabled=False,
    )

    traces = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert captured == ["v3 content", "locked v4 content"]
    assert explicit[0]["prompt_id"] == "feature_extract_v3_compact_strict_json_en"
    assert locked_result[0]["prompt_id"] == "locked-v4"
    assert traces[-1]["prompt_id"] == "locked-v4"
    assert traces[-1]["prompt_hash"] == "b" * 64


def test_changed_effective_generation_options_bypass_the_ai_cache(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("v3 content", encoding="utf-8")
    profile_path = tmp_path / "profiles.yaml"
    profile_path.write_text(
        """
default_profile_id: cold
profiles:
  cold:
    provider: ollama
    model: qwen3:1.7b
    default_prompt_id: feature_extract_v3_compact_strict_json_en
    options: {temperature: 0}
  warm:
    provider: ollama
    model: qwen3:1.7b
    default_prompt_id: feature_extract_v3_compact_strict_json_en
    options: {temperature: 0.2}
""",
        encoding="utf-8",
    )
    calls = []

    class Client:
        def generate_json(self, messages, schema, *, model, timeout, options=None):
            calls.append(options)
            return {"features": [model_feature()]}

    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    client = Client()

    extract_features_for_entity(entity(), model_profile_id="cold", profile_config_path=profile_path, model_client=client)
    extract_features_for_entity(entity(), model_profile_id="cold", profile_config_path=profile_path, model_client=client)
    extract_features_for_entity(entity(), model_profile_id="warm", profile_config_path=profile_path, model_client=client)

    assert len(calls) == 2
    assert calls == [
        {"temperature": 0, "think": False, "structured_output_mode": "json_schema"},
        {"temperature": 0.2, "think": False, "structured_output_mode": "json_schema"},
    ]


def test_generation_options_exclude_secrets_from_model_call_and_trace(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("v3 content", encoding="utf-8")
    profile_path = tmp_path / "profiles.yaml"
    profile_path.write_text(
        """
default_profile_id: safe
profiles:
  safe:
    provider: ollama
    model: qwen3:1.7b
    default_prompt_id: feature_extract_v3_compact_strict_json_en
    options:
      temperature: 0
      api_key: trace-test-secret
      x-api-key: trace-hyphenated-secret
      credentials:
        bearer: trace-nested-secret
      auth:
        access-token: trace-access-token-secret
""",
        encoding="utf-8",
    )
    trace_path = tmp_path / "ai_traces.jsonl"
    calls = []

    class Client:
        def generate_json(self, messages, schema, *, model, timeout, options=None):
            calls.append(options)
            return {"features": [model_feature()]}

    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))

    extract_features_for_entity(
        entity(), model_profile_id="safe", profile_config_path=profile_path, model_client=Client(),
        cache_enabled=False,
    )

    assert calls == [{"temperature": 0, "think": False, "structured_output_mode": "json_schema"}]
    trace_text = trace_path.read_text(encoding="utf-8")
    for secret in (
        "trace-test-secret",
        "trace-hyphenated-secret",
        "trace-nested-secret",
        "trace-access-token-secret",
    ):
        assert secret not in trace_text


def test_rejects_duplicate_template_hashes_across_conflicting_features(monkeypatch):
    duplicate = {
        **model_feature(),
        "feature_type": "kernel_memory_alert",
        "title": "内核内存告警",
    }
    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([model_feature(), duplicate]),
    )

    with pytest.raises(FeatureExtractionError, match="重复分配 template_hash"):
        generate_feature_candidates([entity()], model="qwen3:1.7b", cache_enabled=False)


def test_generate_features_records_selected_remote_provider(monkeypatch, tmp_path):
    trace_path = tmp_path / "ai_traces.jsonl"

    class RemoteClient:
        def generate_json(self, messages, schema, *, model, timeout, options=None):
            return {"features": [model_feature()]}

    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))

    result = generate_feature_candidates(
        [entity()],
        model="remote-model",
        model_client=RemoteClient(),
        provider="openai_compatible",
        cache_enabled=False,
    )

    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    assert result[0]["provider"] == "openai_compatible"
    assert trace["provider"] == "openai_compatible"


def test_generate_features_uses_model_profile_budget_thinking_and_trace(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("profile prompt", encoding="utf-8")
    profile_path = tmp_path / "profiles.yaml"
    profile_path.write_text(
        """
default_profile_id: tiny
profiles:
  tiny:
    provider: ollama
    model: qwen3:1.7b
    display_name: Tiny
    parameter_size: 1.7b
    context_window_tokens: 8192
    recommended_input_tokens: 4500
    max_output_tokens: 1200
    default_prompt_id: feature_extract_v3_compact_strict_json_en
    thinking:
      enabled: false
    evidence_budget:
      max_templates: 1
      max_template_chars: 10
      max_affected_entities: 1
      max_evidence_chars: 8000
    options:
      temperature: 0
""",
        encoding="utf-8",
    )
    trace_path = tmp_path / "ai_traces.jsonl"
    captured = {}
    payload = entity()
    payload["affected_entities"] = ["pay-api-1", "pay-api-2"]
    payload["top_templates"].append({
        "template_hash": "disk-hash",
        "component": "kernel",
        "severity": "ERROR",
        "template": "Disk pressure",
        "category": "node_disk_pressure",
        "count": 1,
    })

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))

    result = generate_feature_candidates([payload], model_profile_id="tiny", profile_config_path=profile_path)

    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    sent_evidence = json.loads(captured["body"]["messages"][1]["content"])
    assert captured["body"]["model"] == "qwen3:1.7b"
    assert captured["body"]["think"] is False
    assert "think" not in captured["body"]["options"]
    assert len(sent_evidence["templates"]) == 1
    assert result[0]["model_profile_id"] == "tiny"
    assert trace["model_profile_id"] == "tiny"
    assert trace["thinking_enabled"] is False
    assert trace["context_budget"]["max_templates"] == 1
    assert trace["evidence_build_meta"]["truncated"] is True


def test_generate_features_reuses_cache_without_calling_model(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("custom feature prompt", encoding="utf-8")
    trace_path = tmp_path / "ai_traces.jsonl"
    cache = AICache(tmp_path / "ai_cache.json")
    calls = {"count": 0}

    def fake_urlopen(request, timeout):
        calls["count"] += 1
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.AI_CACHE", cache)

    first = generate_feature_candidates([entity()], model="qwen3:1.7b")
    second = generate_feature_candidates([entity()], model="qwen3:1.7b")

    traces = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert calls["count"] == 1
    assert first[0]["candidate_id"] == second[0]["candidate_id"]
    assert first[0].get("cache_hit") is False
    assert second[0].get("cache_hit") is True
    assert traces[1]["status"] == "cache_hit"


def test_cache_can_be_disabled(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("custom feature prompt", encoding="utf-8")
    calls = {"count": 0}

    def fake_urlopen(request, timeout):
        calls["count"] += 1
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.AI_CACHE", AICache(tmp_path / "ai_cache.json"))

    generate_feature_candidates([entity()], model="qwen3:1.7b", cache_enabled=False)
    generate_feature_candidates([entity()], model="qwen3:1.7b", cache_enabled=False)

    assert calls["count"] == 2


def test_generate_features_uses_selected_prompt_and_records_job_id(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v2_strict_en.md").write_text("strict prompt", encoding="utf-8")
    trace_path = tmp_path / "ai_traces.jsonl"
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        return response([model_feature()])

    monkeypatch.setattr("logrisk.ai_harness.providers.ollama.urlopen", fake_urlopen)
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))

    result = generate_feature_candidates(
        [entity()],
        model="qwen3:1.7b",
        prompt_id="feature_extract_v2_strict_en",
        job_id="job-123",
    )

    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    assert captured["body"]["messages"][0]["content"] == "strict prompt"
    assert result[0]["prompt_id"] == "feature_extract_v2_strict_en"
    assert trace["prompt_id"] == "feature_extract_v2_strict_en"
    assert trace["job_id"] == "job-123"


def test_trace_write_failure_does_not_fail_extraction(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("custom feature prompt", encoding="utf-8")

    class BrokenLogger:
        def append(self, trace):
            raise OSError("disk full")

    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([model_feature()]),
    )
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", BrokenLogger())

    assert generate_feature_candidates([entity()], model="qwen3:1.7b")[0]["title"] == "Linux 内存耗尽日志"


def test_candidate_id_is_stable(monkeypatch):
    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([model_feature()]),
    )

    first = generate_feature_candidates([entity()], model="qwen3:1.7b")[0]
    second = generate_feature_candidates([entity()], model="qwen3:1.7b")[0]

    assert first["candidate_id"] == second["candidate_id"]


def test_entities_below_threshold_do_not_call_ollama(monkeypatch):
    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not call")),
    )

    assert generate_feature_candidates([entity(score=39)], model="qwen3:1.7b") == []


def test_unknown_template_hash_is_rejected(monkeypatch):
    invalid = {**model_feature(), "template_hashes": ["invented"]}
    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([invalid]),
    )

    with pytest.raises(FeatureExtractionError, match="未知 template_hash"):
        generate_feature_candidates([entity()], model="qwen3:1.7b")


def test_evaluator_rejects_forbidden_rca_claim_and_records_trace(monkeypatch, tmp_path):
    prompt_dir = tmp_path / "prompts"
    prompt_dir.mkdir()
    (prompt_dir / "feature_extract_v3_compact_strict_json_en.md").write_text("custom feature prompt", encoding="utf-8")
    trace_path = tmp_path / "ai_traces.jsonl"
    invalid = {**model_feature(), "summary": "根因是节点内存不足，建议重启"}

    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([invalid]),
    )
    monkeypatch.setattr("logrisk.feature_extractor_ollama.PROMPT_REGISTRY", PromptRegistry(prompt_dir))
    monkeypatch.setattr("logrisk.feature_extractor_ollama.TRACE_LOGGER", AITraceLogger(trace_path))

    with pytest.raises(FeatureExtractionError, match="Evaluator"):
        generate_feature_candidates([entity()], model="qwen3:1.7b")

    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    assert trace["status"] == "evaluator_failed"
    assert trace["evaluator_result"]["passed"] is False
    assert "建议重启" in trace["evaluator_result"]["errors"][0]


@pytest.mark.parametrize(
    "changes",
    [
        {"title": ""},
        {"importance": "extreme"},
        {"tags": "oom"},
        {"template_hashes": []},
    ],
)
def test_invalid_feature_shape_is_rejected(monkeypatch, changes):
    invalid = {**model_feature(), **changes}
    monkeypatch.setattr(
        "logrisk.ai_harness.providers.ollama.urlopen",
        lambda *args, **kwargs: response([invalid]),
    )

    with pytest.raises(FeatureExtractionError):
        generate_feature_candidates([entity()], model="qwen3:1.7b")
