# Repository Guidelines

## Project Structure & Module Organization

Core Python logic lives in `src/logrisk/`: normalization, Drain3 mining, aggregation, risk scoring, Ollama feature extraction, and in-memory review jobs are separate modules. `src/pipeline/manual_import_pipeline.py` creates `result.json`; `src/pipeline/dashboard_server.py` hosts the local review application. Keep the dependency-free frontend in `frontend/index.html`, runtime configuration in `configs/`, sample inputs in `examples/`, launchers in `scripts/`, and pytest modules in `tests/`. Generated artifacts belong in `output/`.

## Build, Test, and Development Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

- `bash scripts/run_manual_pipeline.sh` — generate risk-analysis artifacts from sample logs.
- `bash scripts/run_dashboard.sh` — serve the feature review UI at `http://127.0.0.1:8080`.
- `pytest tests/test_feature_jobs.py -v` — run a focused module.
- `bash -n scripts/*.sh` — validate launcher syntax.

There is no packaging, npm, or compilation step.

## Coding Style & Naming Conventions

Use four-space indentation, PEP 8 spacing, type hints, and `from __future__ import annotations`. Use `snake_case` for Python names, `PascalCase` for classes, and `UPPER_SNAKE_CASE` for constants. Keep reusable transformations in `logrisk` and orchestration in `pipeline`. The frontend must use escaped uploaded/model text when rendering HTML. No formatter or linter is configured; match adjacent code.

## Testing Guidelines

Tests use pytest and `test_<behavior>` names. Add regression coverage for malformed records, schema failures, sanitization, job transitions, HTTP routes, and export filtering. HTTP tests may bind a random loopback port and must not require a live Ollama instance. Preserve deterministic tests by injecting the extractor and using temporary output/state directories. No coverage threshold is configured.

## Architecture and Security Constraints

This repository identifies and reviews log features; it does not implement RCA. Ollama must receive only aggregated, sanitized evidence and must never receive `samples`, `raw_sample`, or raw log streams. Only approved features may be exported for manual import into the external RCA system. Do not add Kafka, Elasticsearch, a database, external LLM services, or frontend CDNs. Bind the dashboard to `127.0.0.1` by default.

## Commit & Pull Request Guidelines

Use focused Conventional Commit subjects such as `feat: add feature approval export`. Pull requests should identify affected pipeline stages, list verification commands, link issues, and include screenshots for UI changes. Never commit production logs, secrets, `.venv/`, `.superpowers/`, caches, or generated `output/` state.

Every code update must also update `releas.md`. Versions use `1.<feature>.<bug>`: feature releases increment the middle number and reset the final number; bug-only releases increment the final number.
