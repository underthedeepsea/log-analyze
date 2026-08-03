# Repository Guidelines
DO NOT send optional commentary
## Project Structure & Module Organization

Core Python logic lives in `src/logrisk/`: normalization, Drain3 mining, aggregation, risk scoring, provider clients, database providers, metrics, review jobs, release readiness, and the production runtime domain are separate modules. `src/pipeline/manual_import_pipeline.py` creates `result.json`; `src/pipeline/dashboard_server.py` hosts the application; `src/pipeline/database_migrate.py` performs offline SQLite-to-PostgreSQL metadata migration. Keep SQLite migrations in `database/migrations/`, PostgreSQL migrations in `database/postgres/migrations/`, the data dictionary in `database/schema.yaml`, React source in `frontend/src/`, committed runtime assets in `frontend/dist/`, configuration in `configs/`, samples in `examples/`, launchers in `scripts/`, and pytest modules in `tests/`. Generated artifacts belong in `output/`; runtime state belongs in ignored `state/`. The committed extension-provider contract and Token template live in `src/logrisk/ai_harness/providers/extensions/`; use `LOCAL_PROVIDER_DEVELOPMENT_GUIDE.md` for internal adaptation.

## Build, Test, and Development Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

For an external PostgreSQL deployment, install the optional driver with `pip install -r requirements-postgres.txt`.

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

This repository identifies and reviews log features; it does not implement RCA. Every model Provider must receive only aggregated, sanitized evidence and must never receive `samples`, `raw_sample`, or raw log streams. Only approved features may be exported for manual import into the external RCA system. Multi-source correlation must remain deterministic: use explicit entity identifiers, configured aliases, hierarchy relations, source pairs and time/risk thresholds; never add LLM correlation, fuzzy entity matching or cross-cluster linking. SQLite via Python's standard library is the default store; PostgreSQL is an explicit external production Provider through `psycopg`, `LOGRISK_DATABASE_PROVIDER=postgres` and `LOGRISK_DATABASE_URL`. File Checkpoints may persist only file identity, committed offsets, Drain3 configuration hashes and sanitized template aggregates. Kafka support is limited to the registered internal adapter contract in `incremental_sources.py`: do not add Kafka clients, Broker connections, credential reads, consumer auto-start, or raw-record persistence without a new explicit requirement. Do not add Elasticsearch, an ORM, another database, automatic Provider fallback, dual writes, or frontend CDNs without an explicit requirement. Bind the dashboard to `127.0.0.1` by default.

PACAS/RBAC is the production identity authority. The runtime module may trust only configured identity headers from configured trusted-proxy CIDRs; it must not implement local users, Bearer Tokens, passwords, sessions, duplicate role systems, credential persistence, or automatic authorization fallbacks. When external identity enforcement is enabled, writes fail closed without a trusted actor and configured role. Runtime audits may contain only sanitized operation metadata, actor/role names and request IDs—never authentication headers, Token values, cookies, raw logs, model content, API keys or DSNs. Retention must be dry-run-first, skip active tasks and protected source/export artifacts, and delete only resolved file paths under approved `state/` or `output/` roots; never delete SQLite/WAL files, configuration or arbitrary paths.

Keep tracked seed data in `prompts/`, `configs/ai_harness.yaml`, `configs/model_profiles.yaml`, `configs/risk_rules.yaml`, `configs/drain3_recommended.ini`, `configs/drain3_profiles/`, and `configs/semantic_dictionary/`. Runtime business state belongs in ignored `state/logrisk.sqlite3` by default; local database files, WAL/SHM files, PostgreSQL candidate connection files, logs, uploads, exports, and legacy state backups must never be committed. API keys, extension Tokens, signing material and PostgreSQL passwords must be read from environment variables and must not enter SQLite, PostgreSQL business tables, Trace, logs, errors, or frontend responses. Private protocol changes must stay inside an explicitly registered extension adapter; do not alter feature jobs, approval, export, Trace or front-end task flows to support one private Provider. PostgreSQL migration is an offline one-time metadata import: do not copy raw logs, chunks, Drain3 `.bin`, or exports; verify row counts, primary keys, canonical digests, foreign keys, and Artifact paths before changing the runtime Provider. Do not add Phoenix, MLflow, or LangSmith until cross-team sharing, long-term metric queries, or distributed tracing becomes explicit.

## Commit & Pull Request Guidelines

Use focused Conventional Commit subjects such as `feat: add feature approval export`. Pull requests should identify affected pipeline stages, list verification commands, link issues, and include screenshots for UI changes. Never commit production logs, secrets, `.venv/`, `.superpowers/`, caches, or generated `output/` state.

Keep `docs/` and internal design, specification, milestone, and implementation-plan documents local only. Before every push, inspect `git diff --cached --name-only` and remove any such files from the commit.

Every code update must also update `releas.md`. Versions use `1.<feature>.<bug>`: feature releases increment the middle number and reset the final number; bug-only releases increment the final number.

## Required GitHub Release Workflow

Use this workflow whenever the user asks to update GitHub. Create the version branch *before* development; name it exactly after the release, such as `1.24.2`. Keep it both locally and on GitHub after merge: it is the permanent version reference.

1. Update `releas.md` (and `README.md` when its version or user-facing behavior changes). Stage only explicit source, test, config, migration, bundle, and release files.
2. Before committing, run `git diff --cached --name-only` and `git diff --cached --check`. Remove `docs/`, internal plans/specifications, `state/`, database/WAL files, logs, uploads, exports, secrets, and generated artifacts. Never use broad `git add .`.
3. Create one Conventional Commit, then publish the branch with `git push -u origin <version>`. Confirm the branch exists remotely before opening the PR.
4. Create a Chinese GitHub PR with `gh pr create --base main --head <version>`. Its body must state new capabilities, fixes, and only verification that was actually run.
5. Merge with `gh pr merge <number> --merge`. **Do not pass `--delete-branch`**: deleting the remote version branch is prohibited.
6. Confirm merge state and commit with `gh pr view <number> --json state,mergeCommit,url`. If ordinary Git network commands are unreliable, use `gh api` to check refs; do not blindly repeat pushes. If a version branch was accidentally removed, recreate `refs/heads/<version>` at the release commit through `gh api`.
7. Create both the immutable tag and Release on the merged `main` commit. Keep the tag as `<version>` and set the Release title to `v<version> · <简短中文主题>`, matching existing Releases. Release notes must be standard Markdown with actual line breaks: a `##` summary heading, blank lines, `### Added`/`### Fixed` sections, and bullet lists. Write notes to a temporary UTF-8 `.md` file and use `gh release create <version> --target <merge-sha> --title "v<version> · <主题>" --notes-file <notes.md>`; never pass an escaped `\\n` string to `--notes`, because GitHub renders it as literal text.
8. Confirm both the tag and rendered Release name/body with `gh release view <version> --json name,body,url`. If the title is generic or `body` contains literal `\\n`, immediately repair it with `gh release edit <version> --title "v<version> · <主题>" --notes-file <notes.md>` before handoff.

Release notes are cumulative from the latest existing GitHub Release through the current version. If an intermediate version has no Release, include every `releas.md` entry in that gap. For example, the `1.24.2` Release must include the complete changes from `1.23.1`, `1.24.0`, `1.24.1`, and `1.24.2`, because `v1.23.0` was the preceding published Release. Start the body with a concise range note, such as `> 包含 1.23.1 至 1.24.2 的全部更新。`.

Do not rerun already-passed tests solely for a GitHub update. Run verification again only when code changed after the last acceptance, a prior verification found a problem, or the user explicitly requests it. In the final handoff, provide the branch, PR, and Release links.
