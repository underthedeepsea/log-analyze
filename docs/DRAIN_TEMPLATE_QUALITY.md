# Drain Template Quality Center

M11 adds a repeatable quality baseline for Drain3. The center evaluates grouping accuracy, template text, semantic preservation, stability, downstream consistency, and throughput. It does not use an LLM to create Gold labels and does not modify production Drain3 configuration automatically.

## Local state

All runtime state remains file based under `state/drain_quality/`:

- `datasets.json` stores versioned Gold datasets;
- `annotations.jsonl` and `reviews.jsonl` are append-only audit streams;
- `eval_runs/<run_id>/summary.json` stores evaluation reports;
- `tune_runs/<run_id>/summary.json` stores grid-search ranking;
- `template_overrides.json` stores the current governed template catalog;
- `template_events.jsonl` and `profile_events.jsonl` preserve governance history.

JSON state is written through a temporary sibling and `os.replace()`. Legacy state without an index wrapper is read lazily where supported.

## Template governance

The original `template_hash` and template are immutable. Editing, ignoring, merging, soft deletion, restoration, and rollback create a new override version. Every operation requires confirmation and the current expected version. A stale version is rejected instead of overwriting another operator’s change.

Overrides affect templates returned by subsequent Dashboard analysis. They do not rewrite the Drain3 cluster tree. Profile promotion and rollback only record an audited human decision; operators apply production INI changes separately after Shadow validation.

## Separate frontend deployment

The frontend resolves its backend in this order:

1. browser setting `localStorage["logrisk.apiBase"]`;
2. deployment default in `frontend/dist/config.js`;
3. the current page origin.

Set a deployment default before serving the static bundle:

```javascript
window.LOGRISK_CONFIG = { apiBase: "https://logrisk-api.example.internal" };
```

The backend allows cross-origin requests only from explicitly configured origins:

```bash
DASHBOARD_CORS_ORIGINS=https://logrisk.example.internal bash scripts/dashboard.sh restart
```

Multiple origins are comma separated. The settings page tests `/api/health` before saving a browser override.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_drain_eval_metrics.py tests/test_drain_eval_store.py
.venv/bin/python -m pytest -q tests/test_drain_template_store.py tests/test_drain_quality_api.py
```
