from __future__ import annotations

import json
from pathlib import Path

from logrisk.ai_eval.runner import load_cases, run_eval


def entity():
    return {
        "window_start": "2026-07-07T10:00:00+08:00",
        "window_end": "2026-07-07T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "risk_score": 92,
        "risk_level": "critical",
        "affected_entities": [],
        "top_templates": [
            {
                "template_hash": "oom-hash",
                "component": "kernel",
                "severity": "ERROR",
                "template": "Memory cgroup out of memory, pod eviction triggered",
                "category": "node_memory_pressure",
                "count": 3,
            }
        ],
    }


def feature(source):
    return {
        "feature_type": "node_memory_pressure",
        "title": "节点内存压力日志",
        "summary": "检测到 kernel 组件 out of memory 与 eviction 相关日志。",
        "importance": "critical",
        "template_hashes": ["oom-hash"],
        "components": ["kernel"],
        "tags": ["内核", "OOM", "驱逐"],
        "selection_reason": "该模板来自 kernel 组件，日志模式包含 out of memory 和 eviction。",
        "entity": {"type": source["entity_type"], "id": source["entity_id"]},
        "source_templates": source["top_templates"],
    }


def test_eval_runner_writes_metrics_and_case_results(tmp_path):
    case_dir = tmp_path / "eval_cases"
    case_dir.mkdir()
    (case_dir / "oom_eviction_node_pressure.json").write_text(
        json.dumps(
            {
                "name": "oom_eviction_node_pressure",
                "input_entity": entity(),
                "expected": {
                    "must_include_feature_type": ["node_memory_pressure"],
                    "must_include_template_keywords": ["out of memory", "eviction"],
                    "must_not_claim": ["etcd", "network root cause"],
                    "expected_entity_id": "node-a",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "eval_results.json"

    result = run_eval(
        case_dir=case_dir,
        output_path=output_path,
        extractor=lambda source, **kwargs: [feature(source)],
        model="qwen3:1.7b",
        prompt_id="feature_extract_v3_compact_strict_json_en",
    )

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert result == saved
    assert saved["prompt_id"] == "feature_extract_v3_compact_strict_json_en"
    assert saved["model"] == "qwen3:1.7b"
    assert saved["cases_total"] == 1
    assert saved["cases_passed"] == 1
    assert saved["pass_rate"] == 1.0
    assert saved["json_valid_rate"] == 1.0
    assert saved["schema_valid_rate"] == 1.0
    assert saved["template_reference_accuracy"] == 1.0
    assert saved["forbidden_claim_count"] == 0
    assert saved["case_results"][0]["passed"] is True


def test_eval_runner_reports_forbidden_claim_and_bad_template_reference(tmp_path):
    case_dir = tmp_path / "eval_cases"
    case_dir.mkdir()
    (case_dir / "bad.json").write_text(
        json.dumps(
            {
                "name": "bad",
                "input_entity": entity(),
                "expected": {
                    "must_include_feature_type": ["node_memory_pressure"],
                    "must_include_template_keywords": ["out of memory"],
                    "must_not_claim": ["建议重启"],
                    "expected_entity_id": "node-a",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    bad_feature = feature(entity()) | {
        "summary": "建议重启节点。",
        "template_hashes": ["missing-hash"],
        "source_templates": [{"template_hash": "missing-hash", "template": "unrelated"}],
    }

    result = run_eval(case_dir=case_dir, output_path=tmp_path / "out.json", extractor=lambda source, **kwargs: [bad_feature])

    assert result["cases_passed"] == 0
    assert result["pass_rate"] == 0.0
    assert result["template_reference_accuracy"] == 0.0
    assert result["forbidden_claim_count"] == 1
    assert result["case_results"][0]["passed"] is False


def test_default_eval_cases_include_milestone_seed_cases():
    cases = load_cases(Path("eval_cases"))

    assert len(cases) >= 5
    assert {
        "oom_eviction_node_pressure",
        "containerd_runtime_failed",
        "disk_pressure",
        "pod_only_business_error",
        "normal_warning_false_positive",
    } <= {case["name"] for case in cases}
