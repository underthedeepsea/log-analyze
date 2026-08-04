from __future__ import annotations

import json
from typing import Any, Callable

from django.core.management.base import BaseCommand

from logrisk.orchestration import AirflowOrchestratorError, InputOrchestrationConflict, OrchestrationConflict
from logrisk_django.service_factory import (
    get_airflow_orchestrator,
    get_container,
    get_input_airflow_orchestrator,
)


class Command(BaseCommand):
    help = "从 Airflow 查询活动 DAG Run，并以稳定标识同步 LOGRISK 编排状态。"

    def add_arguments(self, parser: object) -> None:
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--dry-run", action="store_true", dest="dry_run")
        parser.add_argument("--json", action="store_true", dest="json_output")

    def handle(self, *args: object, **options: object) -> str:
        limit = max(1, min(int(options.get("limit") or 100), 500))
        dry_run = bool(options.get("dry_run"))
        container = get_container()
        report = {
            "dry_run": dry_run,
            "limit": limit,
            "synced": [],
            "unchanged": [],
            "errors": [],
        }
        self._reconcile_feature_runs(container, report, limit=limit, dry_run=dry_run)
        self._reconcile_input_runs(container, report, limit=limit, dry_run=dry_run)
        if options.get("json_output"):
            return json.dumps(report, ensure_ascii=False, sort_keys=True)
        return (
            f"Airflow 编排同步：更新 {len(report['synced'])} 个，"
            f"未变化 {len(report['unchanged'])} 个，错误 {len(report['errors'])} 个"
        )

    def _reconcile_feature_runs(self, container: Any, report: dict[str, Any], *, limit: int, dry_run: bool) -> None:
        airflow = get_airflow_orchestrator()
        for run in container.orchestration.list_active(limit=limit):
            self._reconcile(
                run,
                report,
                dry_run=dry_run,
                get_external=airflow.get_run,
                apply=lambda value, current=run: container.orchestration.reconcile_external(
                    current["orchestration_run_id"], value.state, expected_version=current["state_version"],
                ),
                kind="orchestration",
                id_key="orchestration_run_id",
                job_match=lambda value, current=run: (
                    value.job_id == current["job_id"]
                    and value.orchestration_run_id == current["orchestration_run_id"]
                ),
            )

    def _reconcile_input_runs(self, container: Any, report: dict[str, Any], *, limit: int, dry_run: bool) -> None:
        airflow = get_input_airflow_orchestrator()
        for run in container.input_orchestration.list_active(limit=limit):
            self._reconcile(
                run,
                report,
                dry_run=dry_run,
                get_external=airflow.get_run,
                apply=lambda value, current=run: container.input_orchestration.reconcile_external(
                    current["input_orchestration_run_id"], value.state, expected_version=current["state_version"],
                ),
                kind="input_orchestration",
                id_key="input_orchestration_run_id",
                job_match=lambda value, current=run: (
                    value.input_job_id == current["input_job_id"]
                    and value.input_orchestration_run_id == current["input_orchestration_run_id"]
                ),
            )

    def _reconcile(
        self,
        run: dict[str, Any],
        report: dict[str, Any],
        *,
        dry_run: bool,
        get_external: Callable[[str], Any],
        apply: Callable[[Any], dict[str, Any]],
        kind: str,
        id_key: str,
        job_match: Callable[[Any], bool],
    ) -> None:
        resource_id = str(run[id_key])
        try:
            external = get_external(str(run["external_run_id"]))
            if external.external_run_id != str(run["external_run_id"]) or not job_match(external):
                raise ValueError("Airflow DAG Run 与 LOGRISK 编排标识不匹配")
            item = {
                "kind": kind,
                "resource_id": resource_id,
                "airflow_state": str(external.state),
                "status": str(run["status"]),
            }
            if dry_run:
                report["unchanged"].append(item)
                return
            updated = apply(external)
            item["status"] = str(updated["status"])
            report["synced"].append(item)
        except (AirflowOrchestratorError, InputOrchestrationConflict, OrchestrationConflict, KeyError, TypeError, ValueError) as exc:
            report["errors"].append({
                "kind": kind,
                "resource_id": resource_id,
                "code": str(getattr(exc, "code", "reconcile_failed")),
            })
