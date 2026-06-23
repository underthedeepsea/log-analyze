# Approval Evidence Workspace Design

## Goal

Fix the React approval workspace so reviewers can select any feature and inspect its log evidence before approving or rejecting it. Release this focused bug fix as `1.2.1`.

## Root Cause

The current approval route reuses `Workspace`, but CSS hides `.feature-surface` inside `.review-layout`. Only the entity queue remains visible, so there is no feature control to select. `ReviewEditor` also renders only aggregate facts and ignores the existing sanitized `source_templates` attached to every feature.

## Approved Layout

Use the selected three-column desktop layout:

1. **Feature list** — all candidate and rule-reused features; the selected row has an orange active state.
2. **Feature log evidence** — sanitized templates associated with the selected feature.
3. **Review editor** — editable title, summary, tags, note, and approve/reject actions.

On narrow screens, stack the same sections vertically in feature-list, evidence, editor order. The queue page remains unchanged.

When the reviewer enters the approval workspace with features available and no current selection, automatically select the first feature. Manual selection always overrides the default. If the selected feature disappears after a snapshot refresh, select the first remaining feature or return to the empty state.

## Evidence Display

The task does not retain raw logs. The evidence panel therefore renders `feature.source_templates` only and displays a clear notice that these are Drain3-sanitized feature templates rather than raw log lines.

For each source template show:

- Template text and template Hash.
- Component, category, and severity.
- Occurrence count.
- `first_seen` and `last_seen`, falling back to the feature window when absent.

Missing optional fields display `unknown` or `—`. Empty evidence displays an explicit “no sanitized template evidence” state. Uploaded/model strings remain React text nodes; raw HTML injection is forbidden.

The change must not expose `samples`, `raw_sample`, or the original raw-log stream, and it does not change the Ollama request or export schema.

## Component Changes

- Split the approval route from the queue-oriented `Workspace` composition.
- Add a focused feature-list component and feature-evidence component in `frontend/src/app.js`.
- Keep selection state in `App`; synchronize its default and stale-selection behavior with snapshot feature updates.
- Add responsive CSS classes for `.approval-workspace`, `.approval-feature-list`, and `.evidence-panel`.
- Copy updated application JS/CSS into committed `frontend/dist/assets/` because runtime startup performs no build.

## Testing and Acceptance

Contract tests verify that the approval route contains all three panels, renders every evidence field, includes the no-raw-log notice, auto-selects a feature, and never uses `dangerouslySetInnerHTML`.

Browser acceptance uses a deterministic local job snapshot with at least two features. Verify that:

1. Entering “人工审批” selects and displays the first feature.
2. Clicking the second feature updates both evidence and editor content.
3. Template text, metadata, count, time range, and Hash are visible.
4. Desktop shows three columns and mobile has no horizontal overflow.
5. Approve/reject actions remain usable.

Update `releas.md`, README version, and the development guide to `1.2.1` before completion.
