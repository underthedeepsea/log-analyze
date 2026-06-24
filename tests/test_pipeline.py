import json

from pipeline.manual_import_pipeline import analyze_records, parse_args, run_pipeline


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
