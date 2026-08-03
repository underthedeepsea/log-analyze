from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from logrisk.orchestration import AirflowOrchestratorError
from logrisk_django.service_factory import (
    get_airflow_orchestrator,
    get_container,
    get_input_airflow_orchestrator,
)


class Command(BaseCommand):
    help = "重试仅处于待分派或分派失败状态的 LOGRISK Airflow 运行。"

    def add_arguments(self, parser: object) -> None:
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--json", action="store_true", dest="json_output")

    def handle(self, *args: object, **options: object) -> str:
        container = get_container()
        airflow = get_airflow_orchestrator()
        input_airflow = get_input_airflow_orchestrator()
        retried: list[str] = []
        failed: list[str] = []
        input_retried: list[str] = []
        input_failed: list[str] = []
        for run in container.orchestration.list_reconcilable(limit=int(options["limit"])):
            current = run
            if current["status"] == "dispatch_failed":
                current = container.orchestration.retry_dispatch(
                    current["orchestration_run_id"], expected_version=current["state_version"]
                )
            try:
                external = airflow.trigger(current["job_id"], current["orchestration_run_id"], current["request_id"])
                container.orchestration.mark_dispatched(
                    current["orchestration_run_id"], airflow.dag_id, external.external_run_id,
                    expected_version=current["state_version"],
                )
                retried.append(current["orchestration_run_id"])
            except AirflowOrchestratorError as exc:
                container.orchestration.mark_dispatch_failed(
                    current["orchestration_run_id"], expected_version=current["state_version"],
                    error_code=exc.code, error_summary=str(exc),
                )
                failed.append(current["orchestration_run_id"])
        for run in container.input_orchestration.list_reconcilable(limit=int(options["limit"])):
            current = run
            if current["status"] == "dispatch_failed":
                current = container.input_orchestration.retry_dispatch(
                    current["input_orchestration_run_id"], expected_version=current["state_version"]
                )
            try:
                external = input_airflow.trigger_input(
                    current["input_job_id"], current["input_orchestration_run_id"], current["request_id"]
                )
                container.input_orchestration.mark_dispatched(
                    current["input_orchestration_run_id"], input_airflow.dag_id, external.external_run_id,
                    expected_version=current["state_version"],
                )
                input_retried.append(current["input_orchestration_run_id"])
            except AirflowOrchestratorError as exc:
                container.input_orchestration.mark_dispatch_failed(
                    current["input_orchestration_run_id"], expected_version=current["state_version"],
                    error_code=exc.code, error_summary=str(exc),
                )
                input_failed.append(current["input_orchestration_run_id"])
        payload = {
            "retried": retried,
            "failed": failed,
            "input_retried": input_retried,
            "input_failed": input_failed,
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True) if options.get("json_output") else (
            f"已重试 {len(retried)} 个特征编排和 {len(input_retried)} 个输入编排任务；"
            f"失败 {len(failed) + len(input_failed)} 个"
        )
