# Drain3 Config Governance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a file-backed, versioned governance workflow for `drain3_recommended.ini` algorithm parameters and masking rules, with validation, evaluation gates, human publication, runtime activation, and the approved React UI.

**Architecture:** Add one focused `DrainConfigStore` beside the existing Drain Quality stores. It owns full INI snapshots, validation, catalog metadata, the atomic active pointer, and append-only publication events. `DrainQualityService` connects config versions to existing eval runs, while the Dashboard resolves one immutable config snapshot at task creation and passes its path through both inline and large-file pipelines.

**Tech Stack:** Python 3 standard library, Drain3, pytest, pure React via `React.createElement`, committed CSS/JS runtime bundle, JSON/INI file persistence.

## Global Constraints

- Version is `1.18.0`; create and work on branch `1.18.0`.
- `configs/drain3_recommended.ini` is a read-only baseline and must never be rewritten by the UI.
- Runtime state remains under ignored `state/`; do not add a database or dependency.
- Published configurations affect only newly created tasks; in-flight tasks retain their captured configuration snapshot.
- Publishing requires a completed evaluation that passes all hard gates and explicit human confirmation.
- Never persist full production raw logs or send them to Ollama.
- Every code update must update `releas.md`; source and committed frontend bundle must remain identical.

---

### Task 1: Versioned INI store and validation

**Files:**
- Create: `src/logrisk/drain_eval/config_store.py`
- Create: `tests/test_drain_config_store.py`

**Interfaces:**
- Produces: `DrainConfigStore(root: str | Path, baseline_path: str | Path)`.
- Produces: `list_configs()`, `get_version(config_id, version)`, `create_candidate(payload)`, `save_version(config_id, payload)`, `validate_version(config_id, version)`, `publish(config_id, version, payload)`, `rollback(config_id, version, payload)`, and `active_snapshot()`.
- Produces snapshots containing `config_id`, `version`, `status`, `content_hash`, `path`, `parameters`, `masking_rules`, and `ini_content`.

- [ ] **Step 1: Write failing store tests**

```python
def test_baseline_is_read_only_and_candidate_versions_are_append_only(tmp_path):
    store = DrainConfigStore(tmp_path / "quality", BASELINE)
    baseline = store.active_snapshot()
    candidate = store.create_candidate({"source_config_id": "baseline", "name": "test", "operator": "qa"})
    changed = store.save_version(candidate["config_id"], {
        "expected_version": 1,
        "ini_content": candidate["ini_content"].replace("sim_th = 0.40", "sim_th = 0.45"),
        "operator": "qa",
    })
    assert baseline["path"] == str(BASELINE)
    assert candidate["version"] == 1
    assert changed["version"] == 2
    assert store.get_version(candidate["config_id"], 1)["parameters"]["sim_th"] == 0.40
```

Add cases for invalid INI, out-of-range `sim_th`, invalid masking JSON, invalid regex, stale `expected_version`, generated safe IDs, atomic active pointer, and rollback events.

- [ ] **Step 2: Run the tests and confirm RED**

Run: `.venv/bin/python -m pytest tests/test_drain_config_store.py -q`

Expected: collection fails because `logrisk.drain_eval.config_store` does not exist.

- [ ] **Step 3: Implement the minimal file store**

Use `configparser.ConfigParser(interpolation=None)`, `json.loads()` for `[MASKING].masking`, `re.compile()` for each rule, `hashlib.sha256()` for content identity, and the existing `atomic_json()` helper. Validate these exact bounds:

```python
PARAMETER_RANGES = {
    "sim_th": (0.0, 1.0),
    "depth": (3, 15),
    "max_children": (1, 10000),
    "max_clusters": (1, 1000000),
}
```

Store full immutable INI files at `configs/<config_id>/<version>.ini`, metadata in `config_catalog.json`, the active reference in `active_config.json`, and publication/rollback records in `config_events.jsonl`. The baseline appears as virtual `config_id="baseline"`, `version=1`, `status="baseline"` and cannot be edited or published.

- [ ] **Step 4: Run store tests and confirm GREEN**

Run: `.venv/bin/python -m pytest tests/test_drain_config_store.py -q`

Expected: all store tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/logrisk/drain_eval/config_store.py tests/test_drain_config_store.py
git commit -m "feat: add versioned Drain3 config store"
```

### Task 2: Evaluation gate and immutable runtime snapshots

**Files:**
- Modify: `src/logrisk/drain_eval/service.py`
- Modify: `src/pipeline/dashboard_server.py`
- Modify: `src/logrisk/input_jobs.py`
- Modify: `tests/test_drain_quality_api.py`
- Modify: `tests/test_dashboard_server.py`

**Interfaces:**
- Consumes: `DrainConfigStore` from Task 1.
- Produces: `DrainQualityService.configs` and `publish_config(config_id, version, payload)`.
- Produces: input-job metadata keys `drain_config_id`, `drain_config_version`, `drain_config_hash`, and `drain_config_path`.

- [ ] **Step 1: Write failing service and task-snapshot tests**

```python
def test_publish_requires_passing_eval_and_confirmation(service):
    candidate = service.configs.create_candidate({"source_config_id": "baseline", "name": "candidate"})
    with pytest.raises(DrainQualityError, match="评测"):
        service.publish_config(candidate["config_id"], 1, {"confirmed": True, "eval_run_id": "missing"})

def test_new_input_job_captures_active_config_snapshot(dashboard):
    _, job = request_json(dashboard + "/api/inputs/analyze", "POST", {"upload_id": "upload_test"})
    assert job["drain_config_id"]
    assert job["drain_config_hash"]
```

Use an eval summary whose metrics contain `downstream.critical_risk_recall=1.0`, `labeled.over_merge_rate=0.02`, and `downstream.normal_log_false_positive_rate=0.02`; verify each regression blocks publication.

- [ ] **Step 2: Run focused tests and confirm RED**

Run: `.venv/bin/python -m pytest tests/test_drain_quality_api.py tests/test_dashboard_server.py -q`

Expected: failures for missing config service/API metadata.

- [ ] **Step 3: Connect config versions to eval runs**

Extend eval summaries with optional `config_id`, `config_version`, and `config_hash`. Implement publication gate lookup by `eval_run_id` and require the run to reference the same candidate version. Check:

```python
gate_passed = (
    downstream["critical_risk_recall"] >= 1.0
    and labeled["over_merge_rate"] <= 0.02
    and downstream["normal_log_false_positive_rate"] <= 0.02
)
```

Only after the checks succeed should `DrainConfigStore.publish()` atomically update `active_config.json`.

- [ ] **Step 4: Capture runtime configuration once per task**

Resolve `server.drain_quality.configs.active_snapshot()` before starting inline analysis or a large-file input job. Persist the four metadata fields on the job and pass the captured `path` to `analyze_records()` / `run_large_file_pipeline()` instead of the hard-coded baseline. Do not re-resolve the active pointer inside a running worker.

- [ ] **Step 5: Run focused tests and confirm GREEN**

Run: `.venv/bin/python -m pytest tests/test_drain_quality_api.py tests/test_dashboard_server.py -q`

Expected: all focused API and snapshot tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/logrisk/drain_eval/service.py src/pipeline/dashboard_server.py src/logrisk/input_jobs.py tests/test_drain_quality_api.py tests/test_dashboard_server.py
git commit -m "feat: gate and activate Drain3 configurations"
```

### Task 3: Configuration governance HTTP API

**Files:**
- Modify: `src/pipeline/dashboard_server.py`
- Modify: `tests/test_drain_quality_api.py`

**Interfaces:**
- Consumes: store and service methods from Tasks 1–2.
- Produces: the seven `/api/drain-quality/configs` routes defined in the design.

- [ ] **Step 1: Write failing API contract tests**

Cover list/detail, copying baseline, saving a second version, validation, failed publication, successful publication, rollback, invalid IDs, stale versions, and malformed INI. Assert errors are JSON with non-2xx status and do not change the active hash.

- [ ] **Step 2: Run the API test and confirm RED**

Run: `.venv/bin/python -m pytest tests/test_drain_quality_api.py -q`

Expected: new endpoints return 404.

- [ ] **Step 3: Add explicit route handlers**

Add fixed-path checks before regex routes and constrain identifiers with `[A-Za-z0-9_-]+`. Map `DrainQualityError` to HTTP 400 and optimistic version conflicts to HTTP 409. Return:

```json
{
  "items": [],
  "active": {"config_id": "baseline", "version": 1, "content_hash": "..."}
}
```

Do not accept a filesystem path from any request payload.

- [ ] **Step 4: Run API tests and confirm GREEN**

Run: `.venv/bin/python -m pytest tests/test_drain_quality_api.py -q`

Expected: all API tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/dashboard_server.py tests/test_drain_quality_api.py
git commit -m "feat: expose Drain3 config governance API"
```

### Task 4: Approved React configuration governance UI

**Files:**
- Modify: `frontend/src/app.js`
- Modify: `frontend/src/styles.css`
- Modify: `tests/test_frontend_contract.py`
- Modify: `frontend/dist/assets/app.js`
- Modify: `frontend/dist/assets/app.css`

**Interfaces:**
- Consumes: Task 3 API responses.
- Produces: a “Drain3 配置” quality-center tab with library, structured editor, masking rules, INI source, diff, validation, evaluation association, publishing, and rollback.

- [ ] **Step 1: Write failing frontend contract tests**

Assert source contains API paths and stable component classes:

```python
for text in (
    "/api/drain-quality/configs",
    "drain-config-governance",
    "config-version-list",
    "config-structured-editor",
    "masking-rule-table",
    "config-ini-editor",
    "config-version-diff",
    "配置校验",
    "人工发布",
):
    assert text in source
```

- [ ] **Step 2: Run frontend contract test and confirm RED**

Run: `.venv/bin/python -m pytest tests/test_frontend_contract.py -q`

Expected: missing API and component strings.

- [ ] **Step 3: Add API methods and state**

Add list/detail/create/save/validate/publish/rollback methods to the existing `api` object. Load configs together with other Drain Quality data and preserve errors in the existing global error banner.

- [ ] **Step 4: Implement the approved layout**

Render summary metrics, version sidebar, and detail subtabs. Baseline is read-only and exposes “复制为候选”. Candidate views use native inputs/selects/textareas; masking rules are editable rows with add, disable, reorder, and a bounded test-input preview. Publication requires selecting a matching completed eval run and `window.confirm()`.

- [ ] **Step 5: Add scoped responsive CSS and sync the runtime bundle**

Scope styles under `.drain-config-governance`, retain the existing orange/white tokens, and collapse the two-column layout below 900px. Then run:

```bash
cp frontend/src/app.js frontend/dist/assets/app.js
cp frontend/src/styles.css frontend/dist/assets/app.css
```

- [ ] **Step 6: Run frontend checks and confirm GREEN**

Run:

```bash
.venv/bin/python -m pytest tests/test_frontend_contract.py -q
node --check frontend/src/app.js
node --check frontend/dist/assets/app.js
cmp frontend/src/app.js frontend/dist/assets/app.js
cmp frontend/src/styles.css frontend/dist/assets/app.css
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add frontend/src frontend/dist/assets tests/test_frontend_contract.py
git commit -m "feat: add Drain3 configuration governance UI"
```

### Task 5: Documentation, release, and full verification

**Files:**
- Modify: `README.md`
- Modify: `releas.md`
- Modify: `tests/test_frontend_contract.py`

**Interfaces:**
- Produces: user-facing version and operating instructions for `1.18.0`.

- [ ] **Step 1: Update release contract test**

Require `## 1.18.0 - 2026-07-14` in `releas.md` and `当前版本：\`1.18.0\`` in `README.md`.

- [ ] **Step 2: Document operation and safety**

Document the baseline path, state paths, candidate/eval/publish flow, hard gates, new-task-only activation, rollback semantics, and the Dashboard route. Add an `1.18.0` release entry without rewriting earlier versions.

- [ ] **Step 3: Run complete verification**

Run:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src
bash -n scripts/*.sh
node --check frontend/src/app.js
node --check frontend/dist/assets/app.js
git diff --check
```

Expected: all tests and static checks pass.

- [ ] **Step 4: Perform browser validation**

Start/restart with `bash scripts/dashboard.sh restart`; open `http://127.0.0.1:8080/drain-quality`; verify version selection, structured fields, masking-rule editing, INI source, validation errors, evaluation selection, publish state, rollback, and mobile wrapping. Confirm no duplicate page header or native unstyled button remains.

- [ ] **Step 5: Commit**

```bash
git add README.md releas.md tests/test_frontend_contract.py
git commit -m "docs: release Drain3 config governance"
```
