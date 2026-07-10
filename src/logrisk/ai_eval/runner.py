from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path
from typing import Any, Callable

from logrisk.ai_eval.evaluators import evaluate_case, summarize_results
from logrisk.feature_extractor_ollama import FEATURE_PROMPT_ID, extract_features_for_entity


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CASE_DIR = ROOT / "eval_cases"
DEFAULT_OUTPUT = ROOT / "output" / "eval_results.json"
Extractor = Callable[..., list[dict[str, Any]]]


def load_cases(case_dir: str | Path = DEFAULT_CASE_DIR) -> list[dict[str, Any]]:
    path = Path(case_dir)
    return [json.loads(item.read_text(encoding="utf-8")) for item in sorted(path.glob("*.json"))]


def run_eval(
    *,
    case_dir: str | Path = DEFAULT_CASE_DIR,
    output_path: str | Path = DEFAULT_OUTPUT,
    extractor: Extractor = extract_features_for_entity,
    model: str | None = None,
    prompt_id: str = FEATURE_PROMPT_ID,
    base_url: str = "http://127.0.0.1:11434",
    timeout: float = 120,
    model_profile_id: str | None = None,
) -> dict[str, Any]:
    resolved_model = model or os.getenv("OLLAMA_MODEL") or "qwen3:1.7b"
    case_results = []
    for case in load_cases(case_dir):
        try:
            features = extractor(
                case["input_entity"],
                model=resolved_model,
                base_url=base_url,
                timeout=timeout,
                prompt_id=prompt_id,
                model_profile_id=model_profile_id,
                cache_enabled=False,
                job_id="eval-" + str(case.get("name") or "case"),
            )
            case_results.append(evaluate_case(case, features))
        except Exception as exc:
            case_results.append(evaluate_case(case, [], str(exc)))
    result = summarize_results(
        run_id=str(uuid.uuid4()),
        prompt_id=prompt_id,
        model=resolved_model,
        case_results=case_results,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local LOGRISK AI eval cases.")
    parser.add_argument("--cases", default=str(DEFAULT_CASE_DIR))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--model", default=os.getenv("OLLAMA_MODEL") or "qwen3:1.7b")
    parser.add_argument("--prompt-id", default=FEATURE_PROMPT_ID)
    parser.add_argument("--ollama-url", default=os.getenv("OLLAMA_HOST") or "http://127.0.0.1:11434")
    parser.add_argument("--timeout", type=float, default=float(os.getenv("OLLAMA_TIMEOUT") or 120))
    parser.add_argument("--model-profile-id", default=None)
    args = parser.parse_args()
    result = run_eval(
        case_dir=args.cases,
        output_path=args.output,
        model=args.model,
        prompt_id=args.prompt_id,
        base_url=args.ollama_url,
        timeout=args.timeout,
        model_profile_id=args.model_profile_id,
    )
    print(json.dumps({key: result[key] for key in ("cases_total", "cases_passed", "pass_rate")}, ensure_ascii=False))
    return 0 if result["cases_passed"] == result["cases_total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
