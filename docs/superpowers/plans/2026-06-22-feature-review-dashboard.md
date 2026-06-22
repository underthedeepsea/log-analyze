# Feature Review Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local dashboard that extracts candidate log features with Ollama, streams progress, supports per-feature review, and exports approved evidence without implementing RCA.

**Architecture:** Keep the existing batch pipeline through risk scoring, replace RCA modules with a structured Ollama feature extractor, and add an in-memory serial job manager. A standard-library HTTP server hosts a single-page dashboard and exposes JSON/SSE endpoints on loopback only.

**Tech Stack:** Python 3.10 standard library, pytest, vanilla HTML/CSS/JavaScript, Ollama `/api/chat`.

---

### Task 1: Replace RCA generation with feature extraction

**Files:**
- Create: `src/logrisk/feature_extractor_ollama.py`
- Create: `tests/test_feature_extractor_ollama.py`
- Delete: `src/logrisk/rca_mock.py`
- Delete: `src/logrisk/rca_ollama.py`

- [ ] Write tests defining sanitized evidence, structured feature output, stable IDs, threshold filtering, and Ollama failures.
- [ ] Run `python -m pytest tests/test_feature_extractor_ollama.py -q` and confirm import/behavior failures.
- [ ] Implement `extract_features_for_entity()` and `generate_feature_candidates()` using `/api/chat`, strict JSON validation, and server-owned facts.
- [ ] Run the focused tests and confirm all pass.

### Task 2: Implement in-memory serial extraction jobs

**Files:**
- Create: `src/logrisk/feature_jobs.py`
- Create: `tests/test_feature_jobs.py`

- [ ] Write tests for upload validation, descending-risk serial processing, progress events, failure continuation, retry, edit/approve/reject transitions, and approved-only export.
- [ ] Run focused tests and confirm failures because the job layer does not exist.
- [ ] Implement thread-safe in-memory jobs with one worker per active job, per-entity state, subscriber queues, stable snapshots, and JSON export package generation.
- [ ] Run focused tests and confirm all pass.

### Task 3: Add the local HTTP/SSE service

**Files:**
- Create: `src/pipeline/dashboard_server.py`
- Create: `tests/test_dashboard_server.py`
- Create: `scripts/run_dashboard.sh`

- [ ] Write HTTP tests for static UI, Ollama status, job creation, snapshot, SSE framing, retry, feature patch, export, malformed JSON, and unknown resources.
- [ ] Run focused tests and confirm route failures.
- [ ] Implement a `ThreadingHTTPServer` bound to `127.0.0.1`, JSON helpers, SSE heartbeat/event delivery, and CLI/environment configuration.
- [ ] Add an executable launcher that sets `PYTHONPATH` and starts the dashboard.
- [ ] Run focused tests and shell syntax validation.

### Task 4: Build the operations dashboard

**Files:**
- Create: `frontend/index.html`

- [ ] Implement the approved dark operations layout with file picker/drop zone, model and threshold controls, summary metrics, serial queue, progress bar, feature review editor, approval/rejection actions, retry, and JSON export.
- [ ] Parse and validate files client-side, create jobs without blocking, consume SSE with snapshot fallback, render candidates as they arrive, and cleanly display errors/empty states.
- [ ] Add accessible labels, keyboard-focus states, responsive layout, and no external assets or CDNs.

### Task 5: Remove RCA pipeline semantics and update project guidance

**Files:**
- Modify: `src/pipeline/manual_import_pipeline.py`
- Modify: `scripts/run_manual_pipeline.sh`
- Modify: `README.md`
- Modify: `CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md`
- Modify: `AGENTS.md`
- Modify: `.gitignore`
- Modify: `tests/test_pipeline.py`
- Delete: `tests/test_rca_ollama.py`

- [ ] Write/adjust pipeline tests to require risk/debug artifacts and forbid RCA output/API flags.
- [ ] Remove provider/model RCA dispatch and `rca_results.json` from the batch pipeline while retaining risk scoring output.
- [ ] Update commands, architecture constraints, dashboard instructions, feature export schema, and `.superpowers/` ignore rules.
- [ ] Run pipeline and documentation consistency checks.

### Task 6: Verify automated and live behavior

**Files:**
- Verify all changed files.

- [ ] Run `python -m pytest -q` and require zero failures.
- [ ] Run `bash -n scripts/run_manual_pipeline.sh scripts/run_dashboard.sh` and `git diff --check`.
- [ ] Run the sample batch pipeline and confirm no RCA artifacts are generated.
- [ ] Start the dashboard, upload sample `result.json`, and verify progress, feature review, approval, and approved-only export with `qwen3:1.7b`.
- [ ] Inspect the UI in the browser at desktop and narrow viewport widths and resolve visible defects.
