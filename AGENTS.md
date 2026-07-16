# Repository Guidelines
DO NOT send optional commentary
## Project Structure & Module Organization

Core Python logic lives in `src/logrisk/`: normalization, Drain3 mining, aggregation, risk scoring, provider clients, SQLite stores, metrics, and review jobs are separate modules. `src/pipeline/manual_import_pipeline.py` creates `result.json`; `src/pipeline/dashboard_server.py` hosts the application. Keep migrations in `database/migrations/`, the PostgreSQL-ready data dictionary in `database/schema.yaml`, React source in `frontend/src/`, committed runtime assets in `frontend/dist/`, configuration in `configs/`, samples in `examples/`, launchers in `scripts/`, and pytest modules in `tests/`. Generated artifacts belong in `output/`; runtime state belongs in ignored `state/`.

## Build, Test, and Development Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

- `bash scripts/run_manual_pipeline.sh` — generate risk-analysis artifacts from sample logs.
- `bash scripts/dashboard.sh start|stop|restart|status` — manage the local Dashboard process.
- `bash scripts/run_dashboard.sh` — run the Dashboard in the foreground.
- `pytest tests/test_feature_jobs.py -v` — run a focused module.
- `bash -n scripts/*.sh` — validate launcher syntax.

The committed pure React bundle runs without Node.js, npm, Vite, a CDN, or a compilation step.

## Coding Style & Naming Conventions

Use four-space indentation, PEP 8 spacing, type hints, and `from __future__ import annotations`. Use `snake_case` for Python names, `PascalCase` for classes, and `UPPER_SNAKE_CASE` for constants. Keep reusable transformations in `logrisk` and orchestration in `pipeline`. React must render uploaded/model text as text nodes and never use raw HTML injection. No formatter or linter is configured; match adjacent code.

## Testing Guidelines

Tests use pytest and `test_<behavior>` names. Add regression coverage for malformed records, schema failures, sanitization, job transitions, HTTP routes, export filtering, migrations, and transaction rollback. HTTP tests may bind a random loopback port and must not require a live model service. Preserve deterministic tests with injected extractors, fake Provider HTTP servers, temporary SQLite databases, and temporary output/state directories. No coverage threshold is configured.

## Architecture and Security Constraints

This repository identifies and reviews log features; it does not implement RCA. Every model Provider must receive only aggregated, sanitized evidence and must never receive `samples`, `raw_sample`, or raw log streams. Only approved features may be exported for manual import into the external RCA system. SQLite via Python's standard library and explicitly configured OpenAI-compatible APIs are supported; do not add Kafka, Elasticsearch, an ORM, another database, automatic Provider fallback, or frontend CDNs without an explicit requirement. Bind the dashboard to `127.0.0.1` by default.

Keep tracked seed data in `prompts/`, `configs/ai_harness.yaml`, `configs/model_profiles.yaml`, `configs/risk_rules.yaml`, `configs/drain3_recommended.ini`, `configs/drain3_profiles/`, and `configs/semantic_dictionary/`. Runtime business state belongs in ignored `state/logrisk.sqlite3`; local database files, WAL/SHM files, logs, uploads, exports, and legacy state backups must never be committed. API keys must be read from environment variables and must not enter SQLite, Trace, logs, errors, or frontend responses. Do not add Phoenix, MLflow, or LangSmith until cross-team sharing, long-term metric queries, or distributed tracing becomes explicit.

## Commit & Pull Request Guidelines

Use focused Conventional Commit subjects such as `feat: add feature approval export`. Pull requests should identify affected pipeline stages, list verification commands, link issues, and include screenshots for UI changes. Never commit production logs, secrets, `.venv/`, `.superpowers/`, caches, or generated `output/` state.

Keep `docs/` and internal design, specification, milestone, and implementation-plan documents local only. Before every push, inspect `git diff --cached --name-only` and remove any such files from the commit.

Every code update must also update `releas.md`. Versions use `1.<feature>.<bug>`: feature releases increment the middle number and reset the final number; bug-only releases increment the final number.
