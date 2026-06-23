# Rule Reuse and React Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist globally approved log-feature rules, skip Ollama for future matches, accept raw text logs, replace the Dashboard with the approved React V3 design, and add reliable process controls.

**Architecture:** Add small file-backed stores for approved rules and daily metrics, then inject them into `FeatureJobManager`. Refactor the existing pipeline so HTTP uploads and CLI files share the same parser and analysis path. Serve a committed Vite/React production bundle from the dependency-free Python HTTP server; Node.js is required only when rebuilding frontend assets.

**Tech Stack:** Python 3 standard library, Drain3, pytest, React 18, Vite, CSS, Bash, SSE.

---

## File Map

- Create `src/logrisk/input_parser.py`: content-based JSON, JSONL, and plain-text parsing.
- Create `src/logrisk/approved_rules.py`: atomic approved-rule persistence and matching.
- Create `src/logrisk/processing_metrics.py`: atomic daily LLM-volume persistence.
- Modify `src/pipeline/manual_import_pipeline.py`: reusable in-memory analysis entrypoint.
- Modify `src/logrisk/feature_jobs.py`: rule reuse, persisted approvals, and live metrics.
- Modify `src/pipeline/dashboard_server.py`: upload analysis, rule listing, and static bundle serving.
- Replace `frontend/index.html` with `frontend/index.html` as Vite's source entry; create `frontend/src/` modules and commit `frontend/dist/`.
- Create `frontend/package.json`, `frontend/package-lock.json`, and `frontend/vite.config.js`.
- Create `scripts/dashboard.sh`; retain `scripts/run_dashboard.sh` as a foreground wrapper.
- Add focused pytest modules and update release documentation to `1.2.0`.

### Task 1: Parse Plain Text and Share the Pipeline

**Files:**
- Create: `src/logrisk/input_parser.py`
- Modify: `src/logrisk/io_utils.py`
- Modify: `src/pipeline/manual_import_pipeline.py`
- Create: `tests/test_input_parser.py`
- Modify: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing parser and pipeline tests**

```python
def test_plain_text_uses_each_non_empty_line_as_a_record():
    rows = parse_log_content("events.log", "first error\n\nsecond error\n")
    assert rows == [{"message": "first error"}, {"message": "second error"}]

def test_jsonl_content_is_detected_without_relying_on_suffix():
    rows = parse_log_content("upload.txt", '{"message":"one"}\n{"message":"two"}')
    assert [row["message"] for row in rows] == ["one", "two"]

def test_run_pipeline_accepts_plain_text(tmp_path):
    source = tmp_path / "events.log"
    source.write_text("kernel: out of memory\nkernel: killed process", encoding="utf-8")
    result = run_pipeline(str(source), str(tmp_path / "out"), CONFIG, RULES, str(tmp_path / "state"))
    assert result["summary"]["total_raw_logs"] == 2
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `pytest tests/test_input_parser.py tests/test_pipeline.py -q`

Expected: collection fails because `logrisk.input_parser` and `parse_log_content` do not exist.

- [ ] **Step 3: Implement strict content parsing**

```python
def parse_log_content(filename: str, content: str) -> list[dict[str, Any]]:
    if not isinstance(content, str) or not content.strip():
        raise ValueError("输入日志不能为空")
    stripped = content.strip()
    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError:
        decoded = None
    if decoded is not None:
        return normalize_json_container(decoded)
    lines = [line.strip() for line in stripped.splitlines() if line.strip()]
    if lines and all(line.startswith(("{", '"')) for line in lines):
        try:
            return normalize_json_container([json.loads(line) for line in lines])
        except json.JSONDecodeError:
            pass
    return [{"message": line} for line in lines]
```

Make `read_json_or_jsonl()` decode UTF-8 and delegate to this parser. Extract `analyze_records(records, config_path, rules_path, state_dir)` from `run_pipeline()`; `run_pipeline()` remains responsible for writing debug artifacts and `result.json`.

- [ ] **Step 4: Run parser and pipeline tests and verify GREEN**

Run: `pytest tests/test_input_parser.py tests/test_pipeline.py -q`

Expected: all tests pass, including existing sample counts `10 raw / 6 windows / 40%`.

- [ ] **Step 5: Commit the input work**

```bash
git add src/logrisk/input_parser.py src/logrisk/io_utils.py src/pipeline/manual_import_pipeline.py tests/test_input_parser.py tests/test_pipeline.py
git commit -m "feat: support plain text log inputs"
```

### Task 2: Add the Atomic Approved-Rule Store

**Files:**
- Create: `src/logrisk/approved_rules.py`
- Create: `tests/test_approved_rules.py`

- [ ] **Step 1: Write failing persistence and matching tests**

```python
def test_approved_rule_persists_and_matches_globally(tmp_path):
    store = ApprovedRuleStore(tmp_path / "approved_rules.json")
    rule = store.upsert_feature(approved_feature())
    reloaded = ApprovedRuleStore(tmp_path / "approved_rules.json")
    assert reloaded.match_entity(entity(cluster="another", entity_id="other"))[0]["rule_id"] == rule["rule_id"]

def test_duplicate_signature_updates_instead_of_appending(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    store.upsert_feature(approved_feature(title="old"))
    store.upsert_feature(approved_feature(title="new"))
    assert [rule["title"] for rule in store.list_rules()] == ["new"]

def test_malformed_existing_rule_file_is_rejected(tmp_path):
    path = tmp_path / "rules.json"
    path.write_text("not json", encoding="utf-8")
    with pytest.raises(ApprovedRuleError, match="规则库"):
        ApprovedRuleStore(path).list_rules()
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_approved_rules.py -q`

Expected: import failure for `ApprovedRuleStore`.

- [ ] **Step 3: Implement canonical signatures and atomic writes**

```python
def rule_signature(feature_type: str, sources: list[dict[str, Any]]) -> str:
    pairs = sorted({(str(item["template_hash"]), str(item.get("category") or "")) for item in sources})
    raw = json.dumps([feature_type.strip().lower(), pairs], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _write_locked(self, rules: list[dict[str, Any]]) -> None:
    self.path.parent.mkdir(parents=True, exist_ok=True)
    temporary = self.path.with_suffix(self.path.suffix + ".tmp")
    temporary.write_text(json.dumps({"schema_version": "1.0", "rules": rules}, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, self.path)
```

Store only reusable editorial fields and template/category signatures—never historical entity IDs, raw samples, or model prompt content. Track `approved_at`, `updated_at`, `reuse_count`, and `last_reused_at`. Match globally by requiring all rule signature pairs in an entity's `top_templates`.

- [ ] **Step 4: Run and verify GREEN**

Run: `pytest tests/test_approved_rules.py -q`

Expected: persistence, duplicate update, global matching, and malformed-state tests pass.

- [ ] **Step 5: Commit the rule store**

```bash
git add src/logrisk/approved_rules.py tests/test_approved_rules.py
git commit -m "feat: persist approved feature rules"
```

### Task 3: Reuse Approved Rules Before Ollama

**Files:**
- Modify: `src/logrisk/feature_jobs.py`
- Modify: `tests/test_feature_jobs.py`

- [ ] **Step 1: Write failing job-level reuse tests**

```python
def test_matching_rule_skips_extractor_and_creates_approved_feature(tmp_path):
    calls = []
    store = ApprovedRuleStore(tmp_path / "rules.json")
    store.upsert_feature(candidate(entity("seed", 90)) | {"status": "approved"})
    manager = FeatureJobManager(extractor=lambda source, **kw: calls.append(source), rule_store=store, auto_start=False)
    job_id = manager.create_job({"summary": {}, "risk_entities": [matching_entity("new-node")]}, model="qwen3:1.7b")
    manager.run_job(job_id)
    snapshot = manager.get_job(job_id)
    assert calls == []
    assert snapshot["entities"][0]["status"] == "rule_matched"
    assert snapshot["features"][0]["origin"] == "approved_rule"
    assert snapshot["features"][0]["entity"]["id"] == "new-node"

def test_approving_llm_feature_persists_rule(tmp_path):
    store = ApprovedRuleStore(tmp_path / "rules.json")
    manager = FeatureJobManager(extractor=extractor, rule_store=store, auto_start=False)
    # create, run, approve
    assert len(store.list_rules()) == 1
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_feature_jobs.py -q`

Expected: `FeatureJobManager` rejects `rule_store` and does not expose `rule_matched`.

- [ ] **Step 3: Integrate matching and current-fact reconstruction**

```python
matches = self.rule_store.match_entity(source) if score >= min_score else []
status = "rule_matched" if matches else ("queued" if score >= min_score else "skipped")
record = {**base_record, "status": status, "matched_rule_ids": [item["rule_id"] for item in matches]}
for rule in matches:
    feature = feature_from_rule(rule, source)
    job["features"][feature["candidate_id"]] = feature
    record["feature_ids"].append(feature["candidate_id"])
    self.rule_store.record_reuse(rule["rule_id"])
```

`feature_from_rule()` copies title, summary, importance, tags, and feature type from the rule, but rebuilds entity, cluster, window, risk, occurrence count, affected entities, and source templates from the current entity. Updating a feature to `approved` calls `rule_store.upsert_feature()` before emitting `feature_updated`; a failed write returns an error and does not claim approval.

Update progress/log statistics so `rule_matched` counts as finished and analyzed, with new fields `rule_matched`, `ollama_completed`, `reused_logs`, and `ollama_logs`.

- [ ] **Step 4: Run and verify GREEN**

Run: `pytest tests/test_feature_jobs.py tests/test_approved_rules.py -q`

Expected: existing retry/export tests and new skip/reuse tests pass.

- [ ] **Step 5: Commit job integration**

```bash
git add src/logrisk/feature_jobs.py tests/test_feature_jobs.py
git commit -m "feat: reuse approved rules before ollama"
```

### Task 4: Persist Daily LLM Volume and Compute Live Speed

**Files:**
- Create: `src/logrisk/processing_metrics.py`
- Modify: `src/logrisk/feature_jobs.py`
- Create: `tests/test_processing_metrics.py`
- Modify: `tests/test_feature_jobs.py`

- [ ] **Step 1: Write failing daily-volume and speed tests**

```python
def test_daily_llm_volume_survives_reload(tmp_path):
    path = tmp_path / "metrics.json"
    ProcessingMetricsStore(path, today=lambda: date(2026, 6, 23)).add_llm_logs(120)
    assert ProcessingMetricsStore(path, today=lambda: date(2026, 6, 23)).today_llm_logs() == 120

def test_snapshot_reports_speed_eta_and_reuse_savings(fake_clock):
    snapshot = completed_manager(fake_clock).get_job("job")
    assert snapshot["live_metrics"]["today_llm_logs"] == 5
    assert snapshot["live_metrics"]["processing_logs_per_second"] >= 0
    assert snapshot["live_metrics"]["saved_llm_logs"] == 3
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_processing_metrics.py tests/test_feature_jobs.py -q`

Expected: metrics store and `live_metrics` are missing.

- [ ] **Step 3: Implement atomic daily counters and event samples**

```python
def add_llm_logs(self, count: int) -> int:
    with self._lock:
        payload = self._read_locked()
        key = self.today().isoformat()
        payload["days"][key] = int(payload["days"].get(key, 0)) + max(0, int(count))
        self._write_locked(payload)
        return payload["days"][key]
```

When an entity is about to call Ollama, persist its aggregate `log_count` once and append a timestamped job sample. `live_metrics` returns daily LLM volume, saved LLM logs/calls, current and rolling-60-second logs-per-second, and ETA seconds. Inject clock/today callables so tests remain deterministic.

- [ ] **Step 4: Run and verify GREEN**

Run: `pytest tests/test_processing_metrics.py tests/test_feature_jobs.py -q`

Expected: persisted daily totals and deterministic speed/ETA tests pass.

- [ ] **Step 5: Commit metrics work**

```bash
git add src/logrisk/processing_metrics.py src/logrisk/feature_jobs.py tests/test_processing_metrics.py tests/test_feature_jobs.py
git commit -m "feat: track daily ollama volume and live speed"
```

### Task 5: Extend the Dashboard HTTP API

**Files:**
- Modify: `src/pipeline/dashboard_server.py`
- Modify: `tests/test_dashboard_server.py`

- [ ] **Step 1: Write failing API and static-asset tests**

```python
def test_plain_text_analysis_endpoint(dashboard):
    status, payload, _ = request_json(base + "/api/inputs/analyze", "POST", {"filename": "events.log", "content": "error one\nerror two"})
    assert status == 200
    assert payload["result"]["summary"]["total_raw_logs"] == 2

def test_rule_list_route(dashboard):
    status, payload, _ = request_json(base + "/api/rules")
    assert status == 200
    assert payload == {"rules": []}

def test_serves_bundled_asset_with_correct_content_type(dashboard):
    with urlopen(base + "/assets/app.js") as response:
        assert "javascript" in response.headers["Content-Type"]
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_dashboard_server.py -q`

Expected: `/api/inputs/analyze`, `/api/rules`, and asset requests return 404.

- [ ] **Step 3: Implement injected input analysis and safe static serving**

```python
if path == "/api/inputs/analyze":
    filename, content = require_upload_payload(payload)
    records = parse_log_content(filename, content)
    result = self.server.input_analyzer(records)
    self._json(HTTPStatus.OK, {"result": result})
    return

if path == "/api/rules":
    self._json(HTTPStatus.OK, {"rules": self.server.manager.list_rules()})
    return
```

Build the default manager with `ApprovedRuleStore(state/approved_rules.json)` and `ProcessingMetricsStore(state/processing_metrics.json)`. Serve `/` from `frontend/dist/index.html`; serve `/assets/<name>` only when the resolved path remains below `frontend/dist/assets`. Preserve injectable paths and analyzers for tests.

- [ ] **Step 4: Run and verify GREEN**

Run: `pytest tests/test_dashboard_server.py -q`

Expected: existing jobs/SSE/review/export routes plus new upload/rules/assets tests pass.

- [ ] **Step 5: Commit API work**

```bash
git add src/pipeline/dashboard_server.py tests/test_dashboard_server.py
git commit -m "feat: expose text analysis and rule APIs"
```

### Task 6: Build the Approved React V3 Dashboard

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/package-lock.json`
- Create: `frontend/vite.config.js`
- Replace: `frontend/index.html`
- Create: `frontend/src/main.jsx`
- Create: `frontend/src/App.jsx`
- Create: `frontend/src/api.js`
- Create: `frontend/src/styles.css`
- Create: `frontend/src/components/Sidebar.jsx`
- Create: `frontend/src/components/MetricsGrid.jsx`
- Create: `frontend/src/components/LiveProcessing.jsx`
- Create: `frontend/src/components/Workspace.jsx`
- Create: `frontend/src/components/ReviewEditor.jsx`
- Create: `frontend/src/components/RuleLibrary.jsx`
- Create: `frontend/dist/index.html`
- Create: `frontend/dist/assets/*`
- Replace: `tests/test_frontend_contract.py`

- [ ] **Step 1: Write failing source and bundle contract tests**

```python
def test_react_source_has_all_workspaces():
    source = Path("frontend/src/App.jsx").read_text()
    for label in ("特征总览", "识别队列", "人工审批", "批准规则库", "导出记录"):
        assert label in source

def test_v3_metrics_and_animations_are_present():
    sources = "".join(path.read_text() for path in Path("frontend/src").rglob("*.*"))
    for text in ("Drain3 实时压缩", "今日 LLM 分析日志", "分析速度", "规则复用收益"):
        assert text in sources
    assert "@keyframes" in sources

def test_committed_bundle_is_self_contained():
    html = Path("frontend/dist/index.html").read_text()
    assert "/assets/" in html
    assert "cdn" not in html.lower()
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_frontend_contract.py -q`

Expected: React source and dist files do not exist.

- [ ] **Step 3: Scaffold the local React build**

```json
{
  "private": true,
  "scripts": {"build": "vite build", "dev": "vite --host 127.0.0.1"},
  "dependencies": {"react": "18.3.1", "react-dom": "18.3.1"},
  "devDependencies": {"@vitejs/plugin-react": "4.3.4", "vite": "6.0.7"}
}
```

Use functional components and hooks. `api.js` owns JSON requests, SSE subscription, export downloads, and explicit error parsing. `App.jsx` owns current workspace, uploaded result, active job snapshot, selected feature, rule list, and Ollama status.

- [ ] **Step 4: Implement the V3 design and behavior**

```jsx
<MetricCard label="Drain3 压缩率" value={`${stats.drain3_compression_ratio_percent ?? 0}%`} tone="orange" />
<MetricCard label="今日 LLM 分析日志" value={live.today_llm_logs ?? 0} />
<MetricCard label="分析速度" value={`${live.processing_logs_per_second ?? 0} 条/秒`} />
<LiveProcessing progress={snapshot.progress} statistics={stats} live={live} />
```

Implement the approved white/light-gray/orange layout, circular pulsing reuse benefit, animated progress scan, Drain3 flow, 60-second speed SVG, ETA, rule badges, file picker accepting `.json,.jsonl,.txt,.log`, review editing, export, retry, responsive navigation, loading/empty/error states, and text-only React rendering. Show rules read-only with signature, approval/reuse timestamps, and reuse count.

- [ ] **Step 5: Install, build, and verify the committed production bundle**

Run:

```bash
cd frontend
npm install
npm run build
cd ..
pytest tests/test_frontend_contract.py -q
```

Expected: Vite build succeeds, `frontend/dist/assets/` contains local hashed JS/CSS, and contract tests pass.

- [ ] **Step 6: Commit React source and bundle**

```bash
git add frontend tests/test_frontend_contract.py
git commit -m "feat: rebuild dashboard with react"
```

### Task 7: Add Start, Stop, Restart, and Status Controls

**Files:**
- Create: `scripts/dashboard.sh`
- Modify: `scripts/run_dashboard.sh`
- Create: `tests/test_dashboard_script.py`
- Modify: `.gitignore`

- [ ] **Step 1: Write failing script contract tests**

```python
def test_dashboard_script_supports_process_commands():
    script = Path("scripts/dashboard.sh").read_text()
    for command in ("start", "stop", "restart", "status", "foreground"):
        assert command in script
    assert "dashboard.pid" in script
    assert "dashboard.log" in script

def test_scripts_have_valid_shell_syntax():
    subprocess.run(["bash", "-n", "scripts/dashboard.sh"], check=True)
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_dashboard_script.py -q`

Expected: `scripts/dashboard.sh` is missing.

- [ ] **Step 3: Implement macOS-compatible process control**

```bash
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${DASHBOARD_STATE_DIR:-$ROOT_DIR/state}"
PID_FILE="$STATE_DIR/dashboard.pid"
LOG_FILE="$STATE_DIR/dashboard.log"
PYTHON="$ROOT_DIR/.venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="${PYTHON_BIN:-python3}"
```

Implement `is_running` with `kill -0`, stale PID cleanup, `nohup` start, TERM-and-wait stop, `restart` as stop then start, `status` with nonzero exit when stopped, and `foreground` using `exec`. Change `run_dashboard.sh` to resolve its repository root and call `dashboard.sh foreground`. Add `state/` to `.gitignore`.

- [ ] **Step 4: Run and verify GREEN**

Run: `pytest tests/test_dashboard_script.py -q && bash -n scripts/*.sh`

Expected: tests and shell syntax pass.

- [ ] **Step 5: Commit process controls**

```bash
git add scripts/dashboard.sh scripts/run_dashboard.sh tests/test_dashboard_script.py .gitignore
git commit -m "feat: add dashboard process controls"
```

### Task 8: Documentation, Release 1.2.0, and Full Verification

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md`
- Modify: `releas.md`
- Add: `examples/sample_plain_logs.log`

- [ ] **Step 1: Add a failing release/documentation contract**

```python
def test_release_docs_describe_v120_and_runtime_without_node():
    release = Path("releas.md").read_text()
    readme = Path("README.md").read_text()
    assert "## 1.2.0 - 2026-06-23" in release
    assert "dashboard.sh restart" in readme
    assert ".txt" in readme and "普通启动不需要 Node.js" in readme
```

- [ ] **Step 2: Run and verify RED**

Run: `pytest tests/test_frontend_contract.py::test_release_docs_describe_v120_and_runtime_without_node -q`

Expected: release/docs assertions fail.

- [ ] **Step 3: Update documentation and release notes**

Document plain-text input, global approved-rule matching, rule-library state files, metric definitions, React rebuild commands, committed bundle policy, process commands, Ollama setup, security boundary, and backup advice for `state/approved_rules.json`. Add `1.2.0` release notes dated `2026-06-23` and keep the mandatory `releas.md` update rule in contributor/development guides.

- [ ] **Step 4: Run the full automated verification**

Run:

```bash
pytest -q
bash -n scripts/*.sh
git diff --check
```

Expected: all tests pass with no shell or whitespace errors.

- [ ] **Step 5: Run process and browser acceptance**

Run:

```bash
bash scripts/dashboard.sh restart
bash scripts/dashboard.sh status
```

In the in-app browser, verify desktop and narrow layouts; upload `examples/sample_plain_logs.log`; observe Drain3 compression, progress animation, today's LLM volume, speed, ETA, rule-reuse circle, queue updates, approval persistence, rule library, restart persistence, and approved-feature export. Confirm no raw logs appear in Ollama request fixtures or exported packages.

- [ ] **Step 6: Commit the release**

```bash
git add README.md AGENTS.md CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md releas.md examples/sample_plain_logs.log tests/test_frontend_contract.py
git commit -m "docs: release version 1.2.0"
```

