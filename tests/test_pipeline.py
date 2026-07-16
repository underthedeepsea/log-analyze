import json
from pathlib import Path

from logrisk.semantic.store import SemanticDictionaryStore
from pipeline.manual_import_pipeline import analyze_records, parse_args, run_pipeline


def semantic_snapshot(tmp_path):
    return SemanticDictionaryStore(
        tmp_path / "semantic",
        Path("configs/semantic_dictionary").resolve(),
    ).active_snapshot()


def test_run_pipeline_stops_after_risk_scoring(tmp_path):
    output_dir = tmp_path / "output"

    result = run_pipeline(
        input_path="examples/sample_k8s_logs.jsonl",
        output_dir=str(output_dir),
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=str(tmp_path / "state"),
        window_seconds=300,
    )

    assert result["summary"]["total_raw_logs"] == 10
    assert result["summary"]["total_template_windows"] == 6
    assert result["summary"]["drain3_reduced_logs"] == 4
    assert result["summary"]["drain3_compression_ratio_percent"] == 40.0
    assert result["risk_entities"]
    assert "rca_results" not in result
    assert not (output_dir / "rca_results.json").exists()
    assert json.loads((output_dir / "result.json").read_text())["risk_entities"]
    for filename in (
        "normalized_logs.json",
        "template_events.json",
        "template_windows.json",
        "risk_entities.json",
        "result.json",
    ):
        assert (output_dir / filename).exists()


def test_pipeline_cli_has_no_rca_or_ollama_flags():
    args = parse_args(["--input", "logs.json"])

    assert not hasattr(args, "rca_provider")
    assert not hasattr(args, "ollama_model")


def test_run_pipeline_accepts_plain_text(tmp_path):
    source = tmp_path / "events.log"
    source.write_text(
        "Jun 23 10:00:00 node-a kernel: out of memory\n"
        "Jun 23 10:00:01 node-a kernel: killed process 123\n",
        encoding="utf-8",
    )

    result = run_pipeline(
        input_path=str(source),
        output_dir=str(tmp_path / "output"),
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=str(tmp_path / "state"),
        window_seconds=300,
    )

    assert result["summary"]["total_raw_logs"] == 2
    assert result["summary"]["total_normalized_logs"] == 2


def test_analyze_records_returns_result_without_debug_file_contract(tmp_path):
    result = analyze_records(
        records=[{"message": "Jun 23 10:00:00 node-a kernel: out of memory"}],
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=str(tmp_path / "state"),
    )

    assert result["summary"]["total_raw_logs"] == 1
    assert "risk_entities" in result
    assert "debug_files" not in result


def test_pipeline_keeps_structural_template_and_distinct_semantic_values(tmp_path):
    result = analyze_records(
        records=[
            {"message": "HTTP request failed with status 500", "component": "nginx", "node": "node-a"},
            {"message": "HTTP request failed with status 503", "component": "nginx", "node": "node-a"},
        ],
        config_path="configs/drain3_recommended.ini",
        rules_path="configs/risk_rules.yaml",
        state_dir=str(tmp_path / "state"),
        semantic_snapshot=semantic_snapshot(tmp_path),
    )

    assert result["summary"]["semantic_enrichment"] is True
    assert len(result["top_templates"]) == 1
    assert result["top_templates"][0]["semantic_fields"]["http_status"] == [
        {"value": 500, "count": 1},
        {"value": 503, "count": 1},
    ]
    assert "状态码" in result["top_templates"][0]["semantic_tags"]
