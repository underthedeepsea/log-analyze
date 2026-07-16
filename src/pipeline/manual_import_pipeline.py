from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Dict

from logrisk.aggregator import aggregate_template_events
from logrisk.drain_miner import mine_template_events
from logrisk.io_utils import read_json_or_jsonl, write_json
from logrisk.normalizer import normalize_records
from logrisk.risk_engine import load_rules, score_risk_entities
from logrisk.semantic.extractor import SemanticExtractor


def _process_records(
    records: list[Dict[str, Any]],
    config_path: str,
    rules_path: str,
    state_dir: str,
    window_seconds: int = 300,
    drain_worker_count: int = 1,
    drain_partition_by_node: bool = False,
    drain_progress_callback: Callable[[Dict[str, int]], None] | None = None,
    semantic_snapshot: Dict[str, Any] | None = None,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    normalized = normalize_records(records)
    if semantic_snapshot:
        extractor = SemanticExtractor.from_snapshot(semantic_snapshot)
        normalized = [extractor.enrich(record) for record in normalized]
    template_events, mining_metadata = mine_template_events(
        normalized,
        config_path=config_path,
        state_dir=state_dir,
        worker_count=drain_worker_count,
        partition_by_node=drain_partition_by_node,
        progress_callback=drain_progress_callback,
        return_metadata=True,
    )
    template_windows = aggregate_template_events(
        template_events,
        window_seconds=window_seconds,
    )
    rules = load_rules(rules_path)
    risk_entities = score_risk_entities(template_windows, rules)
    reduced_logs = max(0, len(records) - len(template_windows))
    compression_ratio = round(reduced_logs / len(records) * 100, 2) if records else 0.0

    result = {
        "summary": {
            "total_raw_logs": len(records),
            "total_normalized_logs": len(normalized),
            "total_template_events": len(template_events),
            "total_template_windows": len(template_windows),
            "drain3_reduced_logs": reduced_logs,
            "drain3_compression_ratio_percent": compression_ratio,
            "drain3_parallel": mining_metadata["parallel"],
            "drain3_worker_count": mining_metadata["worker_count"],
            "drain3_partition_count": mining_metadata["partition_count"],
            "total_risk_entities": len(risk_entities),
            "critical_entities": sum(1 for x in risk_entities if x.get("risk_level") == "critical"),
            "high_entities": sum(1 for x in risk_entities if x.get("risk_level") == "high"),
            "semantic_enrichment": semantic_snapshot is not None,
            "semantic_dictionary_versions": (semantic_snapshot or {}).get("versions", {}),
        },
        "risk_entities": risk_entities,
        "top_templates": sorted(template_windows, key=lambda x: x.get("count", 0), reverse=True)[:20],
    }
    return result, {
        "normalized_logs": normalized,
        "template_events": template_events,
        "template_windows": template_windows,
        "risk_entities": risk_entities,
    }


def analyze_records(
    records: list[Dict[str, Any]],
    config_path: str,
    rules_path: str,
    state_dir: str,
    window_seconds: int = 300,
    drain_worker_count: int = 1,
    drain_partition_by_node: bool = False,
    drain_progress_callback: Callable[[Dict[str, int]], None] | None = None,
    semantic_snapshot: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    result, _ = _process_records(
        records,
        config_path,
        rules_path,
        state_dir,
        window_seconds,
        drain_worker_count,
        drain_partition_by_node,
        drain_progress_callback,
        semantic_snapshot,
    )
    return result


def run_pipeline(
    input_path: str,
    output_dir: str,
    config_path: str,
    rules_path: str,
    state_dir: str,
    window_seconds: int = 300,
) -> Dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    raw_records = read_json_or_jsonl(input_path)
    result, artifacts = _process_records(
        raw_records,
        config_path,
        rules_path,
        state_dir,
        window_seconds,
    )
    debug_files = {}
    for name, value in artifacts.items():
        path = output / f"{name}.json"
        write_json(path, value)
        debug_files[name] = str(path)
    result["debug_files"] = debug_files
    write_json(output / "result.json", result)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="人工导入日志文件，验证 Drain3 + 风险评分后端流程")
    parser.add_argument("--input", required=True, help="输入日志文件，支持 .json、.jsonl、.txt 或 .log")
    parser.add_argument("--output-dir", default="output", help="输出目录")
    parser.add_argument("--config", default="configs/drain3_recommended.ini", help="Drain3 配置文件")
    parser.add_argument("--rules", default="configs/risk_rules.yaml", help="风险评分规则文件")
    parser.add_argument("--state-dir", default="output/drain3_state", help="Drain3 状态目录")
    parser.add_argument("--window-seconds", type=int, default=300, help="聚合窗口秒数")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    result = run_pipeline(
        input_path=args.input,
        output_dir=args.output_dir,
        config_path=args.config,
        rules_path=args.rules,
        state_dir=args.state_dir,
        window_seconds=args.window_seconds,
    )
    print("Pipeline finished.")
    print(result["summary"])


if __name__ == "__main__":
    main()
