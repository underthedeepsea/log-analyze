# Development workflows

Read the section relevant to the current change. Repository-wide constraints are in [AGENTS.md](AGENTS.md).

## Production runtime

For Django 4.2 + Airflow 2.3.2 `CeleryExecutor` production deployments, Django must access LOGRISK only through the Application Container and API Facade; it must not define Django Models or directly query LOGRISK tables. Django, Airflow metadata, and LOGRISK business state use separate databases. Airflow DAG conf/XCom may contain only stable IDs, statuses and counts—never raw logs, Prompt/model payloads, secrets or DSNs. Store artifacts under the configured `LOGRISK_SHARED_ROOT`; retain only controlled relative paths and metadata in the database. The DAG must stay parse-safe and defer all database, model, and shared-storage access to task execution.

PACAS/RBAC is the production identity authority. The runtime module may trust only configured identity headers from configured trusted-proxy CIDRs; it must not implement local users, Bearer Tokens, passwords, sessions, duplicate role systems, credential persistence, or automatic authorization fallbacks. When external identity enforcement is enabled, writes fail closed without a trusted actor and configured role. Runtime audits may contain only sanitized operation metadata, actor/role names and request IDs—never authentication headers, Token values, cookies, raw logs, model content, API keys or DSNs. Retention must be dry-run-first, skip active tasks and protected source/export artifacts, and delete only resolved file paths under approved `state/` or `output/` roots; never delete SQLite/WAL files, configuration or arbitrary paths.

Django and Airflow Workers must never auto-migrate LOGRISK databases. Production setup uses explicit `python manage.py logrisk_migrate --check --json`, `python manage.py logrisk_migrate --json` and `python manage.py logrisk_check --json`; see [DJANGO_AIRFLOW_DEPLOYMENT_GUIDE.md](DJANGO_AIRFLOW_DEPLOYMENT_GUIDE.md) for deployment details.

## Bounded Agent execution

These constraints apply to LOGRISK's application Agents, not coding subagents used to develop the repository.

Agentic execution is optional and disabled by default. Planner input may contain only sanitized entity/risk/template summaries, and plans must pass strict Schema, step-budget and registered-tool allow-list validation. Agent tools may read sanitized Evidence, approved rules and installed knowledge assets, evaluate candidates, and register pending candidates only. They must never approve, export, publish rules, perform RCA or remediation, or bypass the deterministic Evaluator and existing human approval flow. Django persists the Run before dispatching only `agent_run_id` and `request_id` to the `logrisk_agent_run` DAG; Airflow Worker execution must recover the locked snapshot from the LOGRISK database.
Agent execution must remain bounded and default-disabled. M21 may schedule only the committed fixed-role DAG (`evidence_specialist`, `rule_specialist`, `feature_specialist`) with explicit acyclic dependencies, budgets, checkpoints and registered tool allow-lists. Do not add dynamic Agent delegation, recursive workflow creation, runtime role/tool injection, unrestricted shell/network/database tools, autonomous production action, or automatic Provider fallback. Parallel nodes must be dependency-independent and must still pass the deterministic Evaluator and human approval Gate.

## Knowledge packages

Knowledge packages are data-only `.logrisk-package.zip` archives. Validate the strict `manifest.json`, declared asset SHA256 values, platform range, exact dependencies, Zip Slip/symlink/path safety, archive limits, and forbidden raw-log/credential keys before installation. Supported asset types are `drain3_profile`, `semantic_dictionary`, `feature_prompt`, `risk_semantics`, `approved_rule_candidates`, and `gold_dataset`; do not add executable scripts, plugins, remote URLs, or arbitrary extensions. Store package bytes only under the controlled Artifact root and package metadata in the active database Provider. New versions start with assets `disabled`; materialization may only create a candidate reference through a registered domain adapter and must never publish production configuration, alter active rules, or bypass human approval. Keep adapter code in `src/logrisk/knowledge_packages/asset_adapters.py` or an explicitly registered domain hook, and keep the archive contract independent from feature-job, approval, export, and Trace flows.

## GitHub release

Use this workflow whenever the user asks to update GitHub. Create the version branch *before* development; name it exactly after the release, such as `1.24.2`. Keep it both locally and on GitHub after merge: it is the permanent version reference.

1. Update `releas.md` (and `README.md` when its version or user-facing behavior changes). Stage only explicit source, test, config, migration, bundle, and release files.
2. Before committing, run `git diff --cached --name-only` and `git diff --cached --check`. Remove `docs/`, internal plans/specifications, `state/`, database/WAL files, logs, uploads, exports, secrets, and generated artifacts. Never use broad `git add .`.
3. Create one Conventional Commit, then publish the branch with `git push -u origin <version>`. Confirm the branch exists remotely before opening the PR.
4. Create a Chinese GitHub PR with `gh pr create --base main --head <version>`. Its body must state new capabilities, fixes, affected pipeline stages, verification commands actually run, relevant issue links, and screenshots for UI changes.
5. Merge with `gh pr merge <number> --merge`. **Do not pass `--delete-branch`**: deleting the remote version branch is prohibited.
6. Confirm merge state and commit with `gh pr view <number> --json state,mergeCommit,url`. If ordinary Git network commands are unreliable, use `gh api` to check refs; do not blindly repeat pushes. If a version branch was accidentally removed, recreate `refs/heads/<version>` at the release commit through `gh api`.
7. Create both the immutable tag and Release on the merged `main` commit. Keep the tag as `<version>` and set the Release title to `v<version> · <简短中文主题>`, matching existing Releases. Release notes must be standard Markdown with actual line breaks: a `##` summary heading, blank lines, `### Added`/`### Fixed` sections, and bullet lists. Write notes to a temporary UTF-8 `.md` file and use `gh release create <version> --target <merge-sha> --title "v<version> · <主题>" --notes-file <notes.md>`; never pass an escaped `\\n` string to `--notes`, because GitHub renders it as literal text.
8. Confirm both the tag and rendered Release name/body with `gh release view <version> --json name,body,url`. If the title is generic or `body` contains literal `\\n`, immediately repair it with `gh release edit <version> --title "v<version> · <主题>" --notes-file <notes.md>` before handoff.

Release notes are cumulative from the latest existing GitHub Release through the current version. If an intermediate version has no Release, include every `releas.md` entry in that gap. For example, the `1.24.2` Release must include the complete changes from `1.23.1`, `1.24.0`, `1.24.1`, and `1.24.2`, because `v1.23.0` was the preceding published Release. Start the body with a concise range note, such as `> 包含 1.23.1 至 1.24.2 的全部更新。`.

Do not rerun already-passed tests solely for a GitHub update. Run verification again only when code changed after the last acceptance, a prior verification found a problem, or the user explicitly requests it. In the final handoff, provide the branch, PR, and Release links.
