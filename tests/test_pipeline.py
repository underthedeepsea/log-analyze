import json

from pipeline.manual_import_pipeline import parse_args, run_pipeline


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
