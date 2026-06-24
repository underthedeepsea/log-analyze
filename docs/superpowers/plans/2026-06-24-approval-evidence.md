# Approval Evidence Workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore feature selection in the approval workspace and show sanitized Drain3 template evidence beside the selected feature before approval.

**Architecture:** Keep the queue-oriented `Workspace` unchanged and create approval-specific `FeatureList` and `FeatureEvidence` React components in the existing pure React static application. `App` owns the selected candidate ID and normalizes it whenever snapshot features change. The committed `frontend/dist/` files mirror source because runtime startup performs no build.

**Tech Stack:** React 18 static UMD runtime, JavaScript `createElement`, CSS, pytest contract tests, in-app browser acceptance.

---

## File Map

- Modify `frontend/src/app.js`: approval feature list, evidence rendering, and selection synchronization.
- Modify `frontend/src/styles.css`: three-column desktop layout and stacked responsive layout.
- Modify `frontend/dist/assets/app.js`: committed runtime copy of source JS.
- Modify `frontend/dist/assets/app.css`: committed runtime copy of source CSS.
- Modify `tests/test_frontend_contract.py`: regression contracts for selection, evidence, safety, and release metadata.
- Modify `README.md`, `CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md`, and `releas.md`: publish bug fix `1.2.1` dated `2026-06-24`.

### Task 1: Restore Feature Selection and Add Evidence Panel

**Files:**
- Modify: `tests/test_frontend_contract.py`
- Modify: `frontend/src/app.js`
- Modify: `frontend/src/styles.css`

- [ ] **Step 1: Write failing approval-workspace contract tests**

Add these tests:

```python
def test_approval_workspace_has_selectable_features_evidence_and_editor():
    source = source_text()
    assert 'function FeatureList(props)' in source
    assert 'function FeatureEvidence(props)' in source
    assert 'className: "approval-workspace"' in source
    assert 'className: "approval-feature-list"' in source
    assert 'className: "evidence-panel"' in source
    assert 'onClick: function () { props.onSelect(feature.candidate_id); }' in source


def test_feature_evidence_contains_sanitized_template_fields_and_notice():
    source = source_text()
    for field in ("template_hash", "component", "category", "severity", "count", "first_seen", "last_seen"):
        assert field in source
    assert "当前展示 Drain3 脱敏特征模板，系统未保存原始日志" in source
    assert "暂无脱敏模板证据" in source
    assert "dangerouslySetInnerHTML" not in source


def test_feature_selection_defaults_and_recovers_from_stale_id():
    source = source_text()
    assert "setSelectedId(function (current)" in source
    assert "features.some(function (feature) { return feature.candidate_id === current; })" in source
    assert "features.length ? features[0].candidate_id : null" in source
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
pytest tests/test_frontend_contract.py::test_approval_workspace_has_selectable_features_evidence_and_editor \
       tests/test_frontend_contract.py::test_feature_evidence_contains_sanitized_template_fields_and_notice \
       tests/test_frontend_contract.py::test_feature_selection_defaults_and_recovers_from_stale_id -q
```

Expected: FAIL because approval-specific components, evidence fields, and selection synchronization are absent.

- [ ] **Step 3: Implement focused feature list and evidence components**

Add the following component boundaries to `frontend/src/app.js`:

```javascript
function FeatureList(props) {
  const features = props.features || [];
  return h("section", { className: "surface approval-feature-list" },
    h("div", { className: "surface-head" }, h("b", null, "候选特征"), h("span", null, features.length + " 条")),
    h("div", { className: "feature-list" },
      features.length === 0 && h("div", { className: "empty-state" }, "暂无可审批特征"),
      features.map(function (feature) {
        return h("button", {
          className: "feature-row " + (props.selectedId === feature.candidate_id ? "active" : ""),
          key: feature.candidate_id,
          onClick: function () { props.onSelect(feature.candidate_id); },
        }, h("div", null, h("b", null, feature.title), h("span", null,
          (feature.entity && feature.entity.id || "unknown") + " · " + feature.summary)),
        h("span", { className: "status-chip " + feature.status },
          feature.origin === "approved_rule" ? "规则复用" : feature.status));
      })
    )
  );
}

function FeatureEvidence(props) {
  const feature = props.feature;
  const templates = feature && feature.source_templates || [];
  return h("section", { className: "surface evidence-panel" },
    h("div", { className: "surface-head" }, h("b", null, "特征日志证据"),
      h("span", null, templates.length + " 个模板")),
    h("div", { className: "evidence-body" },
      h("div", { className: "evidence-notice" }, "当前展示 Drain3 脱敏特征模板，系统未保存原始日志"),
      !feature && h("div", { className: "empty-state" }, "选择特征后查看日志证据"),
      feature && templates.length === 0 && h("div", { className: "empty-state" }, "暂无脱敏模板证据"),
      templates.map(function (template, index) {
        const firstSeen = template.first_seen || feature.window_start || "—";
        const lastSeen = template.last_seen || feature.window_end || "—";
        return h("article", { className: "evidence-template", key: (template.template_hash || "template") + "-" + index },
          h("div", { className: "evidence-meta" },
            h("span", null, template.component || "unknown"),
            h("span", null, template.category || "unknown"),
            h("span", null, template.severity || "unknown"),
            h("b", null, String(template.count || 0) + " 次")),
          h("code", null, template.template || "无模板文本"),
          h("small", null, "Hash " + (template.template_hash || "—")),
          h("small", null, firstSeen + " — " + lastSeen));
      })
    )
  );
}
```

Replace the review route with:

```javascript
view === "review" && h("section", { className: "approval-workspace" },
  h(FeatureList, { features: snapshot && snapshot.features || [], selectedId: selectedId, onSelect: setSelectedId }),
  h(FeatureEvidence, { feature: selected }),
  h(ReviewEditor, { feature: selected, onSave: save }))
```

Add selection synchronization inside `App`:

```javascript
useEffect(function () {
  const features = snapshot && snapshot.features || [];
  setSelectedId(function (current) {
    if (current && features.some(function (feature) { return feature.candidate_id === current; })) return current;
    return features.length ? features[0].candidate_id : null;
  });
}, [snapshot]);
```

- [ ] **Step 4: Implement three-column and responsive styling**

Replace the obsolete `.review-layout` hiding rules with:

```css
.approval-workspace{display:grid;grid-template-columns:minmax(220px,.72fr) minmax(320px,1.15fr) minmax(320px,1fr);gap:12px;align-items:start}
.approval-feature-list,.evidence-panel,.review-editor{min-height:600px}
.approval-feature-list .feature-list,.evidence-body{max-height:650px;overflow:auto}
.evidence-body{padding:12px}
.evidence-notice{padding:9px 10px;border:1px solid #ffd7c1;background:#fff8f4;color:#955331;border-radius:8px;font-size:9px;line-height:1.5;margin-bottom:10px}
.evidence-template{border:1px solid #e7e9ed;background:#fafbfc;border-radius:9px;padding:10px;margin-bottom:9px}
.evidence-template:first-of-type{border-color:#ffd7c1;background:#fff8f4}
.evidence-meta{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.evidence-meta span{font-size:8px;background:#eef1f4;color:#66717d;border-radius:999px;padding:4px 6px}
.evidence-meta b{font-size:9px;color:#df5712;margin-left:auto}
.evidence-template code{display:block;white-space:pre-wrap;overflow-wrap:anywhere;font-size:10px;line-height:1.6;color:#33404d;margin-bottom:8px}
.evidence-template small{display:block;color:#8b949f;font-size:8px;line-height:1.55}
@media(max-width:1180px){.approval-workspace{grid-template-columns:1fr}.approval-feature-list,.evidence-panel,.review-editor{min-height:auto}.approval-feature-list .feature-list,.evidence-body{max-height:420px}}
```

Ensure the existing mobile media query does not hide `.approval-workspace` or `.approval-feature-list`.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run: `pytest tests/test_frontend_contract.py -q && node --check frontend/src/app.js`

Expected: all frontend contract tests pass and JavaScript syntax is valid.

### Task 2: Refresh Runtime Assets and Publish 1.2.1

**Files:**
- Modify: `frontend/dist/assets/app.js`
- Modify: `frontend/dist/assets/app.css`
- Modify: `tests/test_frontend_contract.py`
- Modify: `README.md`
- Modify: `CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md`
- Modify: `releas.md`

- [ ] **Step 1: Write the failing release metadata test**

Update the existing release test to require:

```python
def test_release_docs_describe_current_bugfix_version():
    release = Path("releas.md").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")
    guide = Path("CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md").read_text(encoding="utf-8")
    assert "## 1.2.1 - 2026-06-24" in release
    assert "当前版本：`1.2.1`" in readme
    assert "审批页" in guide and "脱敏模板证据" in guide
```

- [ ] **Step 2: Run the release test and verify RED**

Run: `pytest tests/test_frontend_contract.py::test_release_docs_describe_current_bugfix_version -q`

Expected: FAIL because documentation still reports `1.2.0`.

- [ ] **Step 3: Copy source assets into the committed runtime**

Run:

```bash
cp frontend/src/app.js frontend/dist/assets/app.js
cp frontend/src/styles.css frontend/dist/assets/app.css
```

Then run:

```bash
cmp frontend/src/app.js frontend/dist/assets/app.js
cmp frontend/src/styles.css frontend/dist/assets/app.css
node --check frontend/dist/assets/app.js
```

Expected: both `cmp` commands and JavaScript syntax check exit successfully.

- [ ] **Step 4: Update release documentation**

Set README current version to `1.2.1`. Add this release entry:

```markdown
## 1.2.1 - 2026-06-24

### Fixed

- 修复人工审批页隐藏候选特征列表、无法选择审批对象的问题。
- 审批页新增 Drain3 脱敏模板证据，展示模板、Hash、组件、类别、严重度、计数和时间范围。
- 进入审批页时自动选中首条特征，快照更新后自动修复失效选择。
```

In the development guide, state that the approval page displays sanitized template evidence and never raw logs.

- [ ] **Step 5: Run documentation and frontend tests**

Run: `pytest tests/test_frontend_contract.py -q`

Expected: all tests pass.

### Task 3: Full Verification and Browser Acceptance

**Files:**
- Verify all modified files; no additional production file is expected.

- [ ] **Step 1: Run complete automated verification**

Run:

```bash
pytest -q
bash -n scripts/*.sh
node --check frontend/src/app.js
node --check frontend/dist/assets/app.js
git diff --check
```

Expected: all pytest tests pass; shell, JavaScript, and whitespace checks exit zero.

- [ ] **Step 2: Start the local Dashboard with deterministic test data**

Use the existing injectable `FeatureJobManager` test pattern to create a job with two features whose `source_templates` have different titles and template strings. Serve the final `frontend/dist/index.html` on a loopback test port without calling a live Ollama model.

- [ ] **Step 3: Verify desktop approval behavior in the in-app browser**

At a 1280×800 viewport:

1. Navigate to “人工审批”.
2. Confirm the first feature is active without an extra click.
3. Confirm the three panels are visible.
4. Click the second feature and verify its title appears in the editor and its template appears in the evidence panel.
5. Confirm evidence metadata includes Hash, component/category/severity, count, and time range.
6. Confirm approve and reject buttons remain enabled.

- [ ] **Step 4: Verify mobile behavior and safety**

At a 390×844 viewport, confirm `scrollWidth === clientWidth`, the three panels stack in the specified order, and no uploaded/template string is inserted through `dangerouslySetInnerHTML`. Reset the browser viewport afterward.

- [ ] **Step 5: Commit the completed bug fix**

```bash
git add frontend/src/app.js frontend/src/styles.css frontend/dist/assets/app.js frontend/dist/assets/app.css \
  tests/test_frontend_contract.py README.md CODEX_WORK_GUIDE_LOG_RISK_ANALYSIS.md releas.md
git commit -m "fix: restore approval selection and evidence"
```

