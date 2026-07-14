# M11 Drain Template Quality Center Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a file-backed Drain3 quality center with repeatable metrics, annotation and profile governance, versioned template overrides, and a frontend that can connect to a separately deployed backend.

**Architecture:** Focused `logrisk.drain_eval` modules own schemas, metrics, append-only audit events and atomic state. `dashboard_server.py` exposes those services through local HTTP APIs with configurable CORS. The committed React source and bundle resolve one runtime API base and provide quality, template governance, and backend settings views.

**Tech Stack:** Python 3.10 standard library, Drain3, pytest, React 18 committed browser bundle, CSS, JSON/JSONL and INI files.

## Global Constraints

- Version is `1.17.0`; every code update also updates `releas.md`.
- Do not send raw logs, `samples`, or `raw_sample` to Ollama.
- Do not add a database, Redis, Celery, Kafka, frontend CDN, Vite, or a new dependency.
- Every persisted structure contains `schema_version`; mutable JSON writes use a temporary file plus `os.replace()`.
- Annotation, review and template-governance history is append-only.
- Preserve original `template_hash`, Trace, Approved Rule and Lineage fields.
- Template deletion is soft deletion; production-effective changes require confirmation and optimistic version checks.
- Profile promotion requires human confirmation and never rewrites production Drain3 configuration automatically.

---

### Task 1: Quality schemas and deterministic metrics

**Files:**
- Create: `src/logrisk/drain_eval/__init__.py`
- Create: `src/logrisk/drain_eval/schema.py`
- Create: `src/logrisk/drain_eval/labeled_metrics.py`
- Create: `src/logrisk/drain_eval/unlabeled_metrics.py`
- Create: `src/logrisk/drain_eval/stability.py`
- Create: `src/logrisk/drain_eval/downstream_metrics.py`
- Test: `tests/test_drain_eval_metrics.py`
- Test: `tests/test_drain_eval_store.py`

**Interfaces:**
- Produces: `validate_gold_record(record)`, `evaluate_labeled(rows)`, `evaluate_unlabeled(clusters)`, `evaluate_stability(runs)`, and `evaluate_downstream(expected, actual)`.

- [ ] **Step 1: Add failing tests for perfect, over-merged and over-split grouping**

```python
perfect = evaluate_labeled(rows(["a", "a", "b", "b"]))
assert perfect["pairwise_grouping_f1"] == 1.0
assert evaluate_labeled(rows(["a", "a", "a", "a"]))["over_merge_rate"] > 0
assert evaluate_labeled(rows(["a", "b", "c", "d"]))["over_split_rate"] == 1.0
```

- [ ] **Step 2: Verify RED**

Run: `.venv/bin/python -m pytest -q tests/test_drain_eval_metrics.py tests/test_drain_eval_store.py`
Expected: collection fails because `logrisk.drain_eval` does not exist.

- [ ] **Step 3: Implement validation and metric functions**

Use pair combinations for grouping precision/recall, whitespace/template-mask tokenization for template F1, exact key/value matching for semantic recall, and literal containment for protected-token preservation. Return rounded numeric fields with stable empty-input defaults.

- [ ] **Step 4: Verify GREEN**

Run: `.venv/bin/python -m pytest -q tests/test_drain_eval_metrics.py`
Expected: all metric tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/logrisk/drain_eval tests/test_drain_eval_metrics.py
git commit -m "feat: add Drain quality metrics"
```

### Task 2: Dataset, annotation, evaluation and profile stores

**Files:**
- Create: `src/logrisk/drain_eval/dataset.py`
- Create: `src/logrisk/drain_eval/annotation_store.py`
- Create: `src/logrisk/drain_eval/service.py`
- Create: `src/logrisk/drain_eval/report.py`
- Create: `configs/drain3_profiles/kernel_v1.ini`
- Create: `configs/drain3_profiles/kubelet_v1.ini`
- Create: `configs/drain3_profiles/containerd_v1.ini`
- Create: `configs/drain3_profiles/audit_v1.ini`
- Create: `configs/drain3_profiles/podlog_v1.ini`
- Test: `tests/test_drain_eval_store.py`

**Interfaces:**
- Consumes: metric functions from Task 1.
- Produces: `DatasetStore`, `AnnotationStore`, and `DrainQualityService` methods `create_eval_run`, `get_eval_run`, `list_profiles`, `promote_profile`, and `rollback_profile`.

- [ ] **Step 1: Add failing store and event-replay tests**

```python
dataset = DatasetStore(tmp_path).create({"name": "gold", "records": [gold_record]})
assert dataset["schema_version"] == "drain_dataset_v1"
events = AnnotationStore(tmp_path)
events.append({"cluster_id": "c1", "action": "split", "target_cluster_ids": ["a", "b"]})
assert events.replay()["c1"]["status"] == "split"
```

- [ ] **Step 2: Verify RED**

Run: `.venv/bin/python -m pytest -q tests/test_drain_eval_store.py`
Expected: missing stores or methods fail.

- [ ] **Step 3: Implement atomic state and append-only events**

Store the versioned dataset index in `datasets.json`; rewrite JSONL through a temporary sibling for atomic event append. Join predictions to Gold records by `record_id`, calculate all metric sections, and write `eval_runs/<run_id>/summary.json` atomically.

- [ ] **Step 4: Add profile discovery and confirmed promotion**

Read profiles from `configs/drain3_profiles/*.ini`. Persist promotion events separately without changing any INI. Reject promotion unless `confirmed is True`; rollback appends a compensating event.

- [ ] **Step 5: Verify GREEN and commit**

Run: `.venv/bin/python -m pytest -q tests/test_drain_eval_store.py`
Expected: all tests pass.

```bash
git add src/logrisk/drain_eval configs/drain3_profiles tests/test_drain_eval_store.py
git commit -m "feat: add Drain quality state stores"
```

### Task 3: Versioned template catalog and override layer

**Files:**
- Create: `src/logrisk/drain_eval/template_store.py`
- Test: `tests/test_drain_template_store.py`

**Interfaces:**
- Produces: `TemplateStore.import_templates`, `list_templates`, `get_template`, `change_template`, `history`, `rollback`, and `apply_override`.

- [ ] **Step 1: Add failing governance tests**

```python
item = store.import_templates([{"template_hash": "h1", "template": "error <NUM>", "component": "kernel", "count": 2}])[0]
edited = store.change_template("h1", {"action": "edit", "template": "error <CODE>", "expected_version": 1, "confirmed": True})
assert edited["original_template"] == "error <NUM>"
assert edited["effective_template"] == "error <CODE>"
assert store.rollback("h1", 1, confirmed=True)["effective_template"] == "error <NUM>"
```

- [ ] **Step 2: Verify RED**

Run: `.venv/bin/python -m pytest -q tests/test_drain_template_store.py`
Expected: module import fails.

- [ ] **Step 3: Implement minimal override and audit model**

Persist the current catalog in `template_overrides.json`; append before/after events to `template_events.jsonl`. Support `edit`, `ignore`, `restore`, `merge`, and `delete`. Keep original fields immutable, reject absent confirmation or stale `expected_version`, and implement rollback as a new event.

- [ ] **Step 4: Verify GREEN and commit**

Run: `.venv/bin/python -m pytest -q tests/test_drain_template_store.py`
Expected: all tests pass.

```bash
git add src/logrisk/drain_eval/template_store.py tests/test_drain_template_store.py
git commit -m "feat: add versioned Drain template governance"
```

### Task 4: HTTP APIs, health check and configurable CORS

**Files:**
- Modify: `src/pipeline/dashboard_server.py`
- Test: `tests/test_drain_quality_api.py`
- Test: `tests/test_dashboard_server.py`

**Interfaces:**
- Consumes: `DrainQualityService` and `TemplateStore`.
- Produces: `/api/health`, dataset, annotation, eval-run, profile and template-governance routes.

- [ ] **Step 1: Add failing API and CORS tests**

```python
status, payload = request_json(base + "/api/health")
assert payload["service"] == "logrisk-dashboard"
status, dataset = request_json(base + "/api/drain-quality/datasets", "POST", body)
assert status == 201
```

Send `OPTIONS` with `Origin: http://127.0.0.1:3000` and assert configured CORS headers; assert an unconfigured origin receives no allow-origin header.

- [ ] **Step 2: Verify RED**

Run: `.venv/bin/python -m pytest -q tests/test_drain_quality_api.py`
Expected: `build_server` rejects `drain_quality_root` or routes return 404.

- [ ] **Step 3: Wire services and routes**

Add optional `drain_quality_root` and `cors_origins` parameters to `build_server`. Add GET/POST route matching, `OPTIONS`, CORS headers in `_json` and static responses, and include `DrainQualityError` in 400 handling. Keep existing call sites compatible through defaults.

- [ ] **Step 4: Verify GREEN and commit**

Run: `.venv/bin/python -m pytest -q tests/test_drain_quality_api.py tests/test_dashboard_server.py`
Expected: all API tests pass.

```bash
git add src/pipeline/dashboard_server.py tests/test_drain_quality_api.py tests/test_dashboard_server.py
git commit -m "feat: expose Drain quality APIs"
```

### Task 5: Runtime backend selection and quality-center UI

**Files:**
- Create: `frontend/config.js`
- Modify: `frontend/dist/index.html`
- Modify: `frontend/src/app.js`
- Modify: `frontend/src/styles.css`
- Modify: `frontend/dist/assets/app.js`
- Modify: `frontend/dist/assets/app.css`
- Test: `tests/test_frontend_contract.py`

**Interfaces:**
- Consumes: Task 4 HTTP APIs.
- Produces: one `apiUrl(path)` resolver and UI routes `/drain-quality` and `/settings`.

- [ ] **Step 1: Add failing frontend contracts**

Assert source contains `LOGRISK_CONFIG`, `logrisk.apiBase`, backend connection testing, all Drain quality endpoints, metric labels, annotation actions, template edit/delete/rollback controls, and confirmation UI.

- [ ] **Step 2: Verify RED**

Run: `.venv/bin/python -m pytest -q tests/test_frontend_contract.py`
Expected: new backend and quality-center strings are absent.

- [ ] **Step 3: Implement the API base resolver and settings page**

Resolve `localStorage.getItem("logrisk.apiBase") || window.LOGRISK_CONFIG.apiBase || window.location.origin`, validate `http:`/`https:`, strip trailing slash, and route every fetch/EventSource URL through the resolver. Settings tests `/api/health`, saves only after a successful response, and can restore the deployment default.

- [ ] **Step 4: Implement quality and template management pages**

Add overview metrics, annotation workbench, suspicious templates, profile comparison, template management and publication panels. Use text nodes only. Require explicit confirmation before edit, merge, ignore, delete, restore, rollback or profile promotion.

- [ ] **Step 5: Synchronize the committed bundle**

Copy the reviewed source JS/CSS to the committed bundle because this repository intentionally has no build step; add `/config.js` before application scripts in `frontend/dist/index.html`.

- [ ] **Step 6: Verify GREEN and commit**

Run: `.venv/bin/python -m pytest -q tests/test_frontend_contract.py`
Expected: all frontend contracts pass.

```bash
git add frontend tests/test_frontend_contract.py
git commit -m "feat: add Drain quality center UI"
```

### Task 6: Configuration, documentation and release verification

**Files:**
- Modify: `configs/runtime.yaml`
- Create: `docs/DRAIN_TEMPLATE_QUALITY.md`
- Modify: `README.md`
- Modify: `releas.md`

**Interfaces:**
- Documents all public APIs, state paths, backend deployment settings, CORS configuration, template override semantics, migration and rollback.

- [ ] **Step 1: Add runtime defaults and operator documentation**

Document `DASHBOARD_CORS_ORIGINS`, `frontend/config.js`, browser override priority, `state/drain_quality/`, confirmation behavior and the rule that original hashes are immutable.

- [ ] **Step 2: Update version records**

Set README current version to `1.17.0`. Add a `1.17.0` section to `releas.md` covering M11 metrics, Gold datasets, annotations, Profile governance, template overrides, configurable backend and CORS.

- [ ] **Step 3: Run focused and full verification**

```bash
.venv/bin/python -m pytest -q tests/test_drain_eval_metrics.py tests/test_drain_eval_store.py tests/test_drain_template_store.py tests/test_drain_quality_api.py tests/test_frontend_contract.py
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src
bash -n scripts/*.sh
git diff --check
```

Expected: all commands exit 0; HTTP and multiprocessing tests may require running outside the filesystem/network sandbox.

- [ ] **Step 4: Commit**

```bash
git add configs/runtime.yaml docs/DRAIN_TEMPLATE_QUALITY.md README.md releas.md
git commit -m "docs: document M11 Drain quality center"
```
