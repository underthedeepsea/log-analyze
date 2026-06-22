import json

import pytest

from logrisk.feature_extractor_ollama import FeatureExtractionError, generate_feature_candidates


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


def test_generate_features_sanitizes_evidence_and_owns_source_facts(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["body"] = json.loads(request.data)
        captured["timeout"] = timeout
        return response([model_feature()])

    monkeypatch.setattr("logrisk.feature_extractor_ollama.urlopen", fake_urlopen)

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


def test_candidate_id_is_stable(monkeypatch):
    monkeypatch.setattr(
        "logrisk.feature_extractor_ollama.urlopen",
        lambda *args, **kwargs: response([model_feature()]),
    )

    first = generate_feature_candidates([entity()], model="qwen3:1.7b")[0]
    second = generate_feature_candidates([entity()], model="qwen3:1.7b")[0]

    assert first["candidate_id"] == second["candidate_id"]


def test_entities_below_threshold_do_not_call_ollama(monkeypatch):
    monkeypatch.setattr(
        "logrisk.feature_extractor_ollama.urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not call")),
    )

    assert generate_feature_candidates([entity(score=39)], model="qwen3:1.7b") == []


def test_unknown_template_hash_is_rejected(monkeypatch):
    invalid = {**model_feature(), "template_hashes": ["invented"]}
    monkeypatch.setattr(
        "logrisk.feature_extractor_ollama.urlopen",
        lambda *args, **kwargs: response([invalid]),
    )

    with pytest.raises(FeatureExtractionError, match="未知 template_hash"):
        generate_feature_candidates([entity()], model="qwen3:1.7b")


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
        "logrisk.feature_extractor_ollama.urlopen",
        lambda *args, **kwargs: response([invalid]),
    )

    with pytest.raises(FeatureExtractionError):
        generate_feature_candidates([entity()], model="qwen3:1.7b")
