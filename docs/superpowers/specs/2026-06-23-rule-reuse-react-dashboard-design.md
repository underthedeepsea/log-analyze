# Rule Reuse and React Dashboard Design

## Goal

Add a persistent, globally reusable approved-rule library; rebuild the Dashboard with a prebuilt React frontend using a white-and-orange visual system; support raw text log uploads; and provide reliable start, stop, restart, and status commands.

The system remains a log feature identification and human-review tool. It does not perform RCA, and Ollama receives only aggregated, sanitized template evidence.

## Input and Analysis Flow

The Dashboard and CLI accept four input forms:

- Existing `result.json` documents containing `risk_entities`.
- JSON arrays or objects containing logs.
- JSONL files containing one record per line.
- UTF-8 `.txt` or `.log` files, where every non-empty line becomes one log record.

Raw inputs run through the existing normalization, Drain3 mining, aggregation, and risk-scoring pipeline before a feature job is created. Input detection uses content first and the filename suffix second. Invalid UTF-8, empty files, malformed structured data, and uploads over 10 MB return explicit errors.

The local HTTP API may receive raw file content for preprocessing, but raw lines never enter an Ollama request. Ollama continues to receive only entity facts and sanitized template evidence.

## Approved Rule Library

Approved rules are stored in `state/approved_rules.json`. The store uses a process lock and atomic replacement (`temporary file + os.replace`) so interrupted writes cannot corrupt the active file. A missing file means an empty rule library; malformed existing state is a startup or job-creation error rather than being silently ignored.

Rules are global across clusters and nodes. A reusable rule is identified by its normalized feature type plus every referenced `(template_hash, category)` pair. An entity matches when it contains all pairs required by the approved rule. Duplicate approvals update the existing rule instead of adding another copy.

When a new job is created, rule matching runs before any Ollama call:

1. Matching entities receive an entity status of `rule_matched`.
2. The service creates an attached feature with `status: approved`, `origin: approved_rule`, and the persistent `rule_id`.
3. Current entity, time window, risk score, occurrence count, and sanitized source templates are reconstructed from the current input; historical entity facts are never copied.
4. The entity skips Ollama completely.
5. Unmatched eligible entities remain in the Ollama queue.

Approving an Ollama-generated feature writes or updates the reusable rule. Rejecting or editing a feature does not delete historical rules. Rule deletion is outside this release.

## Job State and Metrics

Job snapshots add explicit rule-reuse counters and preserve existing queue semantics. `rule_matched` entities count as processed and analyzed, but are reported separately from `completed` Ollama entities.

The React Dashboard shows:

- Raw log count, Drain3 template windows, reduced log count, and compression percentage.
- A live `raw logs → template windows` Drain3 compression visualization.
- Reused rules, skipped Ollama calls, Ollama-completed entities, and pending approvals.
- Today's LLM-associated log volume: the sum of `top_templates[].count` for entities actually submitted to Ollama. This is an associated aggregate count, not raw logs transmitted to the model.
- Current-job processing speed in associated log lines per second, a rolling 60-second trend, percentage complete, and estimated remaining time.

Daily LLM volume is persisted atomically in `state/processing_metrics.json`, keyed by the local calendar date, so Dashboard restarts do not reset the daily value. Live speed is calculated from timestamped job events and resets with each job.

## React Frontend

Use pure React static files without Vite. Source lives under `frontend/src/`; the self-contained runtime is committed under `frontend/dist/`. React and ReactDOM are vendored locally—there are no frontend CDNs or runtime build steps. Normal Dashboard startup serves `frontend/dist/index.html` and does not require Node.js or npm.

The approved V3 design uses:

- White surfaces, a light-gray workspace, and orange as the primary action/status color.
- Navigation for overview, recognition queue, human review, approved-rule library, and exports.
- A circular rule-reuse benefit component with a subtle pulse animation.
- An animated analysis progress bar, Drain3 compression flow, analysis-speed trend, and ETA.
- `规则复用 / 跳过 LLM` badges for reused features and entities.
- Responsive layouts and escaped rendering for all uploaded or model-generated text.

The approved-rule library view is read-only in this release and displays rule ID, feature type, template/category signature, approval time, reuse count, and most recent reuse time.

## HTTP API Changes

- `POST /api/inputs/analyze` accepts a filename and UTF-8 content, detects the format, and returns a normal `result` document.
- `GET /api/rules` returns approved-rule summaries.
- Existing job snapshots include rule-reuse and live-processing metrics.
- Existing feature approval routes persist newly approved rules.
- Static requests serve the committed React bundle and assets with safe path handling.

Existing `POST /api/jobs`, SSE progress, review, retry, and export behavior remains compatible.

## Process Control

Create `scripts/dashboard.sh` with:

```bash
bash scripts/dashboard.sh start
bash scripts/dashboard.sh stop
bash scripts/dashboard.sh restart
bash scripts/dashboard.sh status
```

The script resolves the repository root independently of the caller's current directory, uses `.venv/bin/python` when available, stores the PID in `state/dashboard.pid`, and writes output to `state/dashboard.log`. `start` refuses duplicate live processes and removes stale PID files. `stop` sends `TERM` and waits for shutdown. `restart` performs a complete stop followed by start. The existing `run_dashboard.sh` remains as a foreground-compatible wrapper.

## Error Handling and Security

- State write failures fail the approval operation and leave the previous file intact.
- Rule matching never falls back to Ollama after a successful match.
- Ollama failure affects only its current entity and remains retryable.
- Raw uploads are size-limited, decoded strictly as UTF-8, and processed locally.
- Uploaded/model strings are rendered as React text, never injected with raw HTML.
- The server binds to `127.0.0.1` by default.
- No Kafka, Elasticsearch, database, external LLM, or frontend CDN is introduced.

## Testing and Release

Tests cover text/JSON/JSONL detection, plain-text pipeline output, atomic rule persistence, global matching, skipped extractor calls, rule-derived feature facts, duplicate approvals, malformed state, persisted daily metrics, HTTP routes, React bundle contracts, static assets, and process-control script behavior. Browser verification covers desktop/mobile layout, animation presence, navigation, upload, rule badges, progress updates, review, and export.

This feature release increments the project to `1.2.0` and updates `releas.md`, `README.md`, `AGENTS.md`, and the development guide.
