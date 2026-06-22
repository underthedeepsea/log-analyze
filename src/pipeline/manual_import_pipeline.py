from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from logrisk.aggregator import aggregate_template_events
from logrisk.drain_miner import mine_template_events
from logrisk.io_utils import read_json_or_jsonl, write_json
from logrisk.normalizer import normalize_records
from logrisk.rca_mock import generate_mock_rca
from logrisk.risk_engine import load_rules, score_risk_entities


def run_pipeline(
    input_path: str,
    output_dir: str,
    config_path: str,
    rules_path: str,
    state_dir: str,
    window_seconds: int = 300,
    mock_llm: bool = True,
) -> Dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    raw_records = read_json_or_jsonl(input_path)

    normalized = normalize_records(raw_records)
    write_json(output / "normalized_logs.json", normalized)

    template_events = mine_template_events(
        normalized,
        config_path=config_path,
        state_dir=state_dir,
    )
    write_json(output / "template_events.json", template_events)

    template_windows = aggregate_template_events(
        template_events,
        window_seconds=window_seconds,
    )
    write_json(output / "template_windows.json", template_windows)

    rules = load_rules(rules_path)
    risk_entities = score_risk_entities(template_windows, rules)
    write_json(output / "risk_entities.json", risk_entities)

    if mock_llm:
        rca_results = generate_mock_rca(risk_entities)
    else:
        # Placeholder for future real LLM gateway.
        rca_results = []
    write_json(output / "rca_results.json", rca_results)

    result = {
        "summary": {
            "total_raw_logs": len(raw_records),
            "total_normalized_logs": len(normalized),
            "total_template_events": len(template_events),
            "total_template_windows": len(template_windows),
            "total_risk_entities": len(risk_entities),
            "critical_entities": sum(1 for x in risk_entities if x.get("risk_level") == "critical"),
            "high_entities": sum(1 for x in risk_entities if x.get("risk_level") == "high"),
        },
        "risk_entities": risk_entities,
        "rca_results": rca_results,
        "top_templates": sorted(template_windows, key=lambda x: x.get("count", 0), reverse=True)[:20],
        "debug_files": {
            "normalized_logs": str(output / "normalized_logs.json"),
            "template_events": str(output / "template_events.json"),
            "template_windows": str(output / "template_windows.json"),
            "risk_entities": str(output / "risk_entities.json"),
            "rca_results": str(output / "rca_results.json"),
        },
    }
    write_json(output / "result.json", result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="人工导入日志文件，验证 Drain3 + 风险评分 + Mock RCA 后端全流程")
    parser.add_argument("--input", required=True, help="输入日志文件，支持 .json 或 .jsonl")
    parser.add_argument("--output-dir", default="output", help="输出目录")
    parser.add_argument("--config", default="configs/drain3_recommended.ini", help="Drain3 配置文件")
    parser.add_argument("--rules", default="configs/risk_rules.yaml", help="风险评分规则文件")
    parser.add_argument("--state-dir", default="output/drain3_state", help="Drain3 状态目录")
    parser.add_argument("--window-seconds", type=int, default=300, help="聚合窗口秒数")
    parser.add_argument("--mock-llm", action="store_true", help="使用 Mock RCA，不调用真实 LLM")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_pipeline(
        input_path=args.input,
        output_dir=args.output_dir,
        config_path=args.config,
        rules_path=args.rules,
        state_dir=args.state_dir,
        window_seconds=args.window_seconds,
        mock_llm=args.mock_llm,
    )
    print("Pipeline finished.")
    print(result["summary"])


if __name__ == "__main__":
    main()
