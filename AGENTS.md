# Repository Guidelines

## Project Structure & Module Organization

This repository is a file-driven log-risk analysis prototype. Code lives in `src/logrisk/`: `io_utils.py` handles JSON/JSONL, `normalizer.py` cleans records, `drain_miner.py` extracts templates, `aggregator.py` builds windows, `risk_engine.py` scores entities, and `rca_mock.py` produces mock RCA. `src/pipeline/manual_import_pipeline.py` orchestrates the stages.

Keep configuration in `configs/`, sample inputs in `examples/`, shell entry points in `scripts/`, and pytest modules in `tests/`. The HTML prototype is a reference asset. Pipeline artifacts and Drain3 state belong in generated `output/`.

## Build, Test, and Development Commands

Set up the project with Python 3 and an isolated environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- `pytest` — run the suite; `pyproject.toml` adds `src/` and discovers `tests/`.
- `pytest tests/test_normalizer.py -v` — run one focused module.
- `bash scripts/run_manual_pipeline.sh` — process `examples/sample_k8s_logs.jsonl` and print `output/result.json` summary; this script also requires `jq`.
- `PYTHONPATH=src python3 -m pipeline.manual_import_pipeline --help` — inspect CLI options.

There is no packaging or compilation step.

## Coding Style & Naming Conventions

Use four-space indentation, PEP 8 spacing, type hints, and `from __future__ import annotations`. Name modules, functions, and variables with `snake_case`, classes with `PascalCase`, and constants with `UPPER_SNAKE_CASE`. Preserve stage boundaries: reusable transformations belong in `logrisk`, while CLI and file-flow orchestration belong in `pipeline`. No formatter or linter is configured, so match adjacent code and avoid unrelated reformatting.

## Testing Guidelines

Tests use pytest and `test_<behavior>` function names. Mirror source responsibilities, such as `tests/test_risk_engine.py`. Add regression cases for malformed or incomplete records, timestamp windows, stable template hashes, rule matching, and generated pipeline summaries. Tests must be deterministic and must write temporary state outside tracked fixtures. No coverage threshold is currently configured.

## Architecture and Configuration Constraints

This phase intentionally excludes Kafka, Elasticsearch, databases, and real LLM calls. RCA must consume aggregated, scored evidence—not raw log streams. Do not use Drain3's incremental cluster ID as a persistent identifier; retain stable template hashes. Keep intermediate JSON artifacts because they support debugging. Treat `configs/risk_rules.yaml` as reviewed application logic: test changes to weights, regexes, or risk categories.

## Commit & Pull Request Guidelines

There is no commit history yet. Use focused Conventional Commit subjects, for example `fix: parse nested klog prefix`. Pull requests should describe the affected stage, link issues, list verification commands, and include representative input/output when behavior changes. Never commit production logs, secrets, `.venv/`, or generated `output/` state.
